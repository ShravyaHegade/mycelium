"""Tests for OutcomeEmitter telemetry and the DTTR metric.

Covers the flat NDJSON row model, fault-tolerant emission, ``compute_dttr``
definitions (silent duplicates vs authorized re-executions, redispatched /
long-running denominator), the ``@ledger``/``@ledger_sync`` hook points,
operator release + reconciler ``NOT_EXECUTED`` authorization, config wiring,
and the ``mycelium outcomes dttr`` CLI.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from mycelium import (
    ConfigError,
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    OutcomeEmitError,
    ReconcileResult,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    compute_dttr,
    execution_scope,
    ledger,
    ledger_sync,
    load_config,
    load_config_from_string,
)
from mycelium.__main__ import main
from mycelium.outcome_emit import (
    EVENT_BODY_COMPLETE,
    EVENT_BODY_FAIL,
    EVENT_BODY_START,
    EVENT_RELEASE,
    EVENT_RESOLUTION,
    GATE_ALLOW,
    GATE_HARD_BLOCK,
    GATE_RETURN,
    FileOutcomeStorage,
    InMemoryOutcomeStorage,
    OutcomeEmitter,
    OutcomeRow,
)
from mycelium.outcome_export import FanoutOutcomeStorage, WebhookOutcomeStorage

_BINDING = ToolTransitionBinding.for_tool(
    agent_id="test",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)

_SCOPE = TransitionScope(thread_id="t", run_id="r")


# ---------------------------------------------------------------------------
# Row model + storage
# ---------------------------------------------------------------------------


def test_outcome_row_round_trips_through_dict() -> None:
    row = OutcomeRow(
        ts=1.5,
        agent_id="a",
        tool="charge",
        request_id="req-1",
        event=EVENT_BODY_START,
        gate=None,
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE.value,
        tool_body_executed=True,
        authorized_reexec=True,
        owner="w1",
    )
    restored = OutcomeRow.from_dict(row.to_dict())
    assert restored == row
    assert restored.tool_body_executed is True
    assert restored.authorized_reexec is True


def test_file_outcome_storage_is_append_only_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    storage = FileOutcomeStorage(path)
    emitter = OutcomeEmitter(agent_id="a", storage=storage)
    emitter.emit_event(tool="t", request_id="r1", event=EVENT_RESOLUTION, gate=GATE_ALLOW)
    emitter.emit_event(
        tool="t",
        request_id="r1",
        event=EVENT_BODY_START,
        tool_body_executed=True,
    )
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["event"] == EVENT_RESOLUTION
    assert len(storage.list_all()) == 2

    reopened = FileOutcomeStorage(path)
    assert len(reopened.list_all()) == 2


def test_file_outcome_storage_skips_malformed_lines(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    path.write_text("{broken}\n", encoding="utf-8")
    storage = FileOutcomeStorage(path)
    assert storage.list_all() == []


def test_emitter_never_raises_on_storage_failure() -> None:
    class BrokenStorage(InMemoryOutcomeStorage):
        def append(self, row: OutcomeRow) -> None:
            raise OSError("disk full")

    emitter = OutcomeEmitter(agent_id="a", storage=BrokenStorage())
    emitter.emit_event(tool="t", request_id="r1", event=EVENT_RESOLUTION)
    assert emitter.storage.list_all() == []


# ---------------------------------------------------------------------------
# compute_dttr
# ---------------------------------------------------------------------------


def _resolution(request_id: str, tool: str = "t", ts: float = 0.0) -> OutcomeRow:
    return OutcomeRow(
        ts=ts,
        agent_id="a",
        tool=tool,
        request_id=request_id,
        event=EVENT_RESOLUTION,
        gate=GATE_ALLOW,
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
    )


def _body_start(
    request_id: str, tool: str = "t", ts: float = 1.0, authorized: bool = False
) -> OutcomeRow:
    return OutcomeRow(
        ts=ts,
        agent_id="a",
        tool=tool,
        request_id=request_id,
        event=EVENT_BODY_START,
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        tool_body_executed=True,
        authorized_reexec=authorized,
    )


def test_dttr_clean_transition_is_zero() -> None:
    rows = [
        _resolution("r1"),
        _body_start("r1"),
    ]
    report = compute_dttr(rows)
    assert report.dttr == 0.0
    assert report.silent_duplicates == 0
    assert report.long_running_or_redispatched == 0


def test_dttr_counts_silent_duplicate_executions() -> None:
    # Two body executions, no NOT_EXECUTED authorization anywhere → 1 silent.
    rows = [
        _resolution("r1", ts=0.0),
        _body_start("r1", ts=1.0, authorized=False),
        _resolution("r1", ts=2.0),
        _body_start("r1", ts=3.0, authorized=False),
    ]
    report = compute_dttr(rows)
    assert report.silent_duplicates == 1
    assert report.long_running_or_redispatched == 1
    assert report.dttr == 1.0


def test_dttr_authorized_reexec_is_not_silent() -> None:
    # Second run followed a consumed NOT_EXECUTED → authorized, not silent.
    rows = [
        _resolution("r1", ts=0.0),
        _body_start("r1", ts=1.0, authorized=False),
        _resolution("r1", ts=2.0),
        _body_start("r1", ts=3.0, authorized=True),
    ]
    report = compute_dttr(rows)
    assert report.silent_duplicates == 0
    # Still redispatched (2 resolution events) so it enters the denominator.
    assert report.long_running_or_redispatched == 1
    assert report.dttr == 0.0


def test_dttr_redispatched_denominator() -> None:
    # Two dispatches resolving to RETURN (second redispatch got the cached
    # result) with a single body execution → 0 silent, 1 long/redispatched.
    rows = [
        _resolution("r1", ts=0.0),
        _body_start("r1", ts=1.0),
        OutcomeRow(
            ts=2.0,
            agent_id="a",
            tool="t",
            request_id="r1",
            event=EVENT_BODY_COMPLETE,
            terminal_outcome=TerminalOutcome.COMPLETED.value,
        ),
        OutcomeRow(
            ts=3.0,
            agent_id="a",
            tool="t",
            request_id="r1",
            event=EVENT_RESOLUTION,
            gate=GATE_RETURN,
            terminal_outcome=TerminalOutcome.COMPLETED.value,
        ),
    ]
    report = compute_dttr(rows)
    assert report.silent_duplicates == 0
    assert report.long_running_or_redispatched == 1
    assert report.dttr == 0.0
    assert report.transitions == 1


def test_dttr_long_running_duration_rule() -> None:
    rows = [
        _resolution("r1", ts=0.0),
        _body_start("r1", ts=5.0),
        _body_start("r2", ts=5.0),
    ]
    assert compute_dttr(rows, long_running_after=None).long_running_or_redispatched == 0
    assert compute_dttr(rows, long_running_after=2.0).long_running_or_redispatched == 1


def test_dttr_empty_rows() -> None:
    report = compute_dttr([])
    assert report.dttr == 0.0
    assert report.transitions == 0
    assert report.silent_duplicates == 0


def test_dttr_accepts_dicts() -> None:
    rows = [_resolution("r1").to_dict(), _body_start("r1").to_dict()]
    assert compute_dttr(rows).dttr == 0.0


# ---------------------------------------------------------------------------
# @ledger_sync / @ledger integration
# ---------------------------------------------------------------------------


def _sync_ledgered() -> tuple[Any, OutcomeEmitter]:
    emitter = OutcomeEmitter(agent_id="test", storage=InMemoryOutcomeStorage())

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=emitter,
    )
    def charge(amount: int) -> str:
        return f"charged-{amount}"

    return charge, emitter


def test_ledger_sync_emits_resolution_and_body_rows() -> None:
    charge, emitter = _sync_ledgered()
    with execution_scope(_SCOPE):
        assert charge(5) == "charged-5"
    events = [row.event for row in emitter.storage.list_all()]
    assert events == [EVENT_RESOLUTION, EVENT_BODY_START, EVENT_BODY_COMPLETE]
    report = compute_dttr(emitter.storage.list_all())
    assert report.silent_duplicates == 0
    assert report.dttr == 0.0


def test_ledger_sync_redispatch_returns_cached_without_body() -> None:
    charge, emitter = _sync_ledgered()
    with execution_scope(_SCOPE):
        assert charge(5) == "charged-5"
        assert charge(5) == "charged-5"
    events = [row.event for row in emitter.storage.list_all()]
    body_rows = [row for row in emitter.storage.list_all() if row.tool_body_executed]
    assert len(body_rows) == 1  # body ran exactly once across both dispatches
    assert events.count(EVENT_RESOLUTION) == 2  # two dispatches, both resolved
    assert events[3] == EVENT_RESOLUTION
    report = compute_dttr(emitter.storage.list_all())
    assert report.silent_duplicates == 0
    assert report.long_running_or_redispatched == 1
    assert report.dttr == 0.0


def test_ledger_sync_body_failure_emits_fail_row() -> None:
    emitter = OutcomeEmitter(agent_id="test", storage=InMemoryOutcomeStorage())

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=emitter,
    )
    def explode() -> str:
        raise ValueError("boom")

    with execution_scope(_SCOPE):
        with pytest.raises(ValueError):
            explode()
    events = [row.event for row in emitter.storage.list_all()]
    assert events == [EVENT_RESOLUTION, EVENT_BODY_START, EVENT_BODY_FAIL]
    fail_row = emitter.storage.list_all()[-1]
    assert fail_row.error_class == "ValueError"
    assert fail_row.terminal_outcome == TerminalOutcome.FAILED_BEFORE_EFFECT.value


def test_ledger_async_emits_rows() -> None:
    emitter = OutcomeEmitter(agent_id="test", storage=InMemoryOutcomeStorage())

    @ledger(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=emitter,
    )
    async def send(tag: str) -> str:
        return f"sent-{tag}"

    async def run() -> None:
        with execution_scope(_SCOPE):
            assert await send("x") == "sent-x"

    asyncio_run(run())
    events = [row.event for row in emitter.storage.list_all()]
    assert events == [EVENT_RESOLUTION, EVENT_BODY_START, EVENT_BODY_COMPLETE]


def test_ledger_sync_hard_block_emits_resolution_row() -> None:
    from mycelium import side_effect

    emitter = OutcomeEmitter(agent_id="test", storage=InMemoryOutcomeStorage())
    fail_first = {"v": True}

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=emitter,
    )
    def dangerous(key: int) -> str:
        with side_effect():
            if fail_first["v"]:
                fail_first["v"] = False
                raise ValueError("boom")
        return f"done-{key}"

    with execution_scope(_SCOPE):
        with pytest.raises(ValueError):
            dangerous(key=1)
        # FAILED_AFTER_EFFECT + no reconciler/ref → HARD_BLOCK.
        with pytest.raises(LedgerHardBlockError):
            dangerous(key=1)

    gates = [
        row.gate for row in emitter.storage.list_all() if row.event == EVENT_RESOLUTION
    ]
    assert GATE_HARD_BLOCK in gates
    assert gates.count(GATE_HARD_BLOCK) == 1


# ---------------------------------------------------------------------------
# Operator release + reconciler NOT_EXECUTED authorization
# ---------------------------------------------------------------------------


def test_operator_release_not_executed_authorizes_reexec() -> None:
    from mycelium import record_external_operation, side_effect

    emitter = OutcomeEmitter(agent_id="test", storage=InMemoryOutcomeStorage())
    fail_first = {"v": True}

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=emitter,
    )
    def charge(amount: int) -> str:
        with side_effect():
            record_external_operation("pi_7")
            if fail_first["v"]:
                fail_first["v"] = False
                raise ValueError("provider timeout")
        return f"charged-{amount}"

    with execution_scope(_SCOPE):
        with pytest.raises(ValueError):
            charge(amount=5)
        # Operator verifies the effect never happened → one authorized re-run.
        ledger_instance = get_ledger_for(charge)
        request_id = next(
            row.request_id
            for row in emitter.storage.list_all()
            if row.event == EVENT_BODY_FAIL
        )
        ledger_instance.release(
            request_id,
            verified="not_executed",
            by="ops",
            reason="provider showed no charge",
        )
        assert charge(amount=5) == "charged-5"

    rows = emitter.storage.list_all()
    body_rows = [row for row in rows if row.tool_body_executed]
    assert len(body_rows) == 2
    assert body_rows[0].authorized_reexec is False
    assert body_rows[1].authorized_reexec is True
    release_rows = [row for row in rows if row.event == EVENT_RELEASE]
    assert len(release_rows) == 1
    assert release_rows[0].authorized_reexec is True
    report = compute_dttr(rows)
    assert report.silent_duplicates == 0
    assert report.dttr == 0.0


class _Reconciler:
    def __init__(self) -> None:
        self.calls = 0

    def reconcile(self, entry: Any) -> ReconcileResult:
        self.calls += 1
        return ReconcileResult.not_executed()


def test_reconciler_not_executed_authorizes_reexec() -> None:
    from mycelium import record_external_operation, side_effect

    emitter = OutcomeEmitter(agent_id="test", storage=InMemoryOutcomeStorage())
    reconciler = _Reconciler()
    fail_first = {"v": True}

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=emitter,
        reconciler=reconciler,
    )
    def charge(amount: int) -> str:
        with side_effect():
            record_external_operation("pi_9")
            if fail_first["v"]:
                fail_first["v"] = False
                raise ValueError("provider timeout")
        return f"charged-{amount}"

    with execution_scope(_SCOPE):
        with pytest.raises(ValueError):
            charge(amount=5)
        # Redispatch: FAILED_AFTER_EFFECT + external_operation_ref → HARD_BLOCK
        # → reconciler proves NOT_EXECUTED → exactly one authorized re-run.
        assert charge(amount=5) == "charged-5"

    rows = emitter.storage.list_all()
    body_rows = [row for row in rows if row.tool_body_executed]
    assert len(body_rows) == 2
    assert body_rows[0].authorized_reexec is False
    assert body_rows[1].authorized_reexec is True
    assert compute_dttr(rows).silent_duplicates == 0


# ---------------------------------------------------------------------------
# Config wiring
# ---------------------------------------------------------------------------


def test_config_builds_outcome_emitter_from_yaml(tmp_path: Path) -> None:
    path = tmp_path / "mycelium.yaml"
    path.write_text(
        """
transition:
  agent_id: "acme"
  policy_version: "1"
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
""",
        encoding="utf-8",
    )
    config = load_config(path)
    emitter = config.build_outcome_emitter()
    assert emitter is not None
    assert emitter.agent_id == "acme"
    assert isinstance(emitter.storage, FileOutcomeStorage)


def test_config_fans_out_to_generic_webhook(tmp_path: Path) -> None:
    config = load_config_from_string(
        f"""
transition:
  agent_id: acme
  policy_version: "1"
outcome_emit:
  storage: file
  path: {tmp_path / "outcomes.jsonl"}
  exporters:
    - type: webhook
      url: https://events.example.test/mycelium
      secret: test-secret
"""
    )
    emitter = config.build_outcome_emitter()
    assert emitter is not None
    assert isinstance(emitter.storage, FanoutOutcomeStorage)
    assert any(
        isinstance(sink, WebhookOutcomeStorage)
        for sink in emitter.storage._sinks
    )


def test_config_apply_tool_wires_outcome_emitter(tmp_path: Path) -> None:
    path = tmp_path / "mycelium.yaml"
    path.write_text(
        """
transition:
  agent_id: "acme"
  policy_version: "1"
  scope_from:
    run_id: run_id
action_ledger:
  storage: memory
  tools: [charge]
outcome_emit:
  storage: memory
tools:
  charge:
    callable: tests.test_outcome_emit:charge
    side_effect_class: non_idempotent_mutate
""",
        encoding="utf-8",
    )
    config = load_config(path)

    def charge(amount: int) -> str:
        return f"charged-{amount}"

    wrapped = config.apply_tool("charge", charge)
    emitter = config.build_outcome_emitter()
    assert emitter is not None
    with execution_scope(_SCOPE):
        assert wrapped(5) == "charged-5"
    rows = emitter.storage.list_all()
    assert len(rows) >= 3
    assert any(row.event == EVENT_BODY_COMPLETE for row in rows)


def test_config_outcome_emit_must_be_mapping() -> None:
    with pytest.raises(Exception):
        load_config_from_string("outcome_emit: 42\n")


def test_config_outcome_emit_agent_id_not_supported() -> None:
    with pytest.raises(Exception):
        load_config_from_string(
            "transition:\n  agent_id: a\n  policy_version: '1'\noutcome_emit:\n"
            "  agent_id: x\n  storage: memory\n"
        )


def test_production_without_outcome_emit_raises() -> None:
    with pytest.raises(ConfigError, match="outcome_emit"):
        load_config_from_string(
            """
profile: production
tools:
  ping: {}
"""
        )


def test_production_memory_outcome_storage_raises() -> None:
    with pytest.raises(ConfigError, match="memory storage"):
        load_config_from_string(
            """
profile: production
outcome_emit:
  storage: memory
tools:
  ping: {}
"""
        )


def test_production_durable_outcome_storage_loads(tmp_path: Path) -> None:
    cfg = load_config_from_string(
        f"""
profile: production
outcome_emit:
  storage: file
  path: {tmp_path / "outcomes.jsonl"}
tools:
  ping: {{}}
"""
    )
    emitter = cfg.build_outcome_emitter()
    assert emitter is not None
    assert emitter.fail_closed
    assert isinstance(emitter.storage, FileOutcomeStorage)


def test_development_may_omit_outcome_emit() -> None:
    cfg = load_config_from_string("tools:\n  ping: {}\n")
    assert cfg.outcome_emit is None
    assert cfg.build_outcome_emitter() is None


def test_production_emit_failure_blocks_before_tool(tmp_path: Path) -> None:
    class _BoomStorage:
        def append(self, row: Any) -> None:
            raise OSError("disk full")

        def list_all(self) -> list[Any]:
            return []

    calls = {"n": 0}

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=OutcomeEmitter(
            "prod", storage=_BoomStorage(), on_failure="error"
        ),
    )
    def charge(amount: int) -> str:
        calls["n"] += 1
        return "paid"

    with pytest.raises(OutcomeEmitError, match="disk full|failed to record"):
        charge(1, request_id="charge:ORD-boom")
    assert calls["n"] == 0


def test_emit_failure_does_not_replace_tool_exception() -> None:
    class _FailAfterStart:
        def __init__(self) -> None:
            self.n = 0
            self._rows: list[Any] = []

        def append(self, row: Any) -> None:
            self.n += 1
            if row.event == EVENT_BODY_FAIL:
                raise OSError("disk full")
            self._rows.append(row)

        def list_all(self) -> list[Any]:
            return list(self._rows)

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=OutcomeEmitter(
            "prod", storage=_FailAfterStart(), on_failure="error"
        ),
    )
    def explode(amount: int) -> str:
        raise RuntimeError("provider down")

    with pytest.raises(RuntimeError, match="provider down"):
        explode(1, request_id="charge:ORD-keep")


def test_restart_preserves_emitted_evidence(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=_BINDING,
        outcome_emitter=OutcomeEmitter(
            "a", storage=FileOutcomeStorage(path), on_failure="error"
        ),
    )
    def charge(amount: int) -> str:
        return "paid"

    charge(1, request_id="charge:ORD-persist")
    restored = FileOutcomeStorage(path).list_all()
    assert restored
    assert any(row.request_id == "charge:ORD-persist" for row in restored)
    assert any(row.event == EVENT_BODY_COMPLETE for row in restored)
    assert any(row.gate == GATE_ALLOW for row in restored)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_outcomes_dttr_on_file(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    emitter = OutcomeEmitter(agent_id="a", storage=FileOutcomeStorage(path))
    emitter.emit_event(tool="t", request_id="r1", event=EVENT_RESOLUTION, gate=GATE_ALLOW)
    emitter.emit_event(
        tool="t", request_id="r1", event=EVENT_BODY_START, tool_body_executed=True
    )
    emitter.emit_event(
        tool="t",
        request_id="r1",
        event=EVENT_RESOLUTION,
        gate=GATE_RETURN,
        terminal_outcome=TerminalOutcome.COMPLETED.value,
    )
    assert main(["outcomes", "dttr", "--file", str(path)]) == 0


def test_cli_outcomes_dttr_json(tmp_path: Path) -> None:
    path = tmp_path / "outcomes.jsonl"
    storage = FileOutcomeStorage(path)
    storage.append(_body_start("r1"))
    assert main(["outcomes", "dttr", "--file", str(path), "--json"]) == 0


def test_cli_outcomes_dttr_missing_config(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["outcomes", "dttr", "-c", str(tmp_path / "nope.yaml")]) == 2
    assert "no outcome log specified" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_ledger_for(func: Any) -> Any:
    from mycelium import get_ledger

    ledger_instance = get_ledger(func)
    assert ledger_instance is not None
    return ledger_instance


def asyncio_run(coro: Any) -> None:
    import asyncio

    asyncio.run(coro)
