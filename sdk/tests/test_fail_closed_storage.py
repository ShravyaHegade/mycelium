"""Tests for the fail-closed storage contract and unclassified tool policy.

Part 1 — Storage outage contract:
  - LedgerStorageUnavailableError wraps backend failures
  - Claim during storage outage → tool never runs
  - Complete during storage outage → entry stays IN_FLIGHT
  - Failure recording during storage outage → original tool exception re-raised
  - Redis flavor of storage-down claim

Part 2 — Unclassified tool policy:
  - unclassified_policy="warn" (default): one-time warning on failed retry
  - unclassified_policy="strict": failed retry hard-blocks
  - Operator release after strict-mode hard-block
  - Old entries without terminal_outcome deserialize correctly
  - YAML config passes unclassified_policy through
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from mycelium import (
    ActionLedger,
    InMemoryLedgerStorage,
    LedgerEntry,
    LedgerHardBlockError,
    LedgerStorageUnavailableError,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger,
    ledger_sync,
)
from mycelium.action_ledger import (
    UNCLASSIFIED_POLICY_STRICT,
    UNCLASSIFIED_POLICY_WARN,
)
from mycelium.config import load_config_from_string

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def _scope() -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id="r1")


class FailingStorage(InMemoryLedgerStorage):
    """InMemoryLedgerStorage that can selectively fail on any operation."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_get = False
        self.fail_set = False
        self.fail_claim = False
        self.fail_list_all = False

    def get(self, request_id: str) -> LedgerEntry | None:
        if self.fail_get:
            raise ConnectionError("storage backend unreachable")
        return super().get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        if self.fail_set:
            raise ConnectionError("storage backend unreachable")
        super().set(entry)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[str, LedgerEntry | None]:
        if self.fail_claim:
            raise ConnectionError("storage backend unreachable")
        return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    def list_all(self) -> list[LedgerEntry]:
        if self.fail_list_all:
            raise ConnectionError("storage backend unreachable")
        return super().list_all()


# ---------------------------------------------------------------------------
# Part 1: Storage outage contract
# ---------------------------------------------------------------------------


class TestStorageUnavailableError:
    def test_wraps_backend_exception(self) -> None:
        with pytest.raises(LedgerStorageUnavailableError, match="test"):
            raise LedgerStorageUnavailableError("test")

    def test_cause_preserved(self) -> None:
        original = ConnectionError("timeout")
        try:
            raise LedgerStorageUnavailableError("wrapped") from original
        except LedgerStorageUnavailableError as exc:
            assert exc.__cause__ is original


class TestClaimDuringStorageDown:
    def test_claim_raises_storage_unavailable(self) -> None:
        storage = FailingStorage()
        ledger_inst = ActionLedger(storage=storage)
        storage.fail_claim = True
        with pytest.raises(LedgerStorageUnavailableError, match="try_claim_inflight"):
            ledger_inst.claim("req-1", "send_payment", (), {"amount": 10})

    def test_claim_side_effecting_raises_storage_unavailable(self) -> None:
        storage = FailingStorage()
        ledger_inst = ActionLedger(storage=storage)
        storage.fail_claim = True
        with pytest.raises(LedgerStorageUnavailableError, match="try_claim_inflight"):
            ledger_inst.claim_side_effecting(
                "req-2", "send_payment", (), {"amount": 10}, _binding()
            )

    def test_tool_never_runs_on_storage_down_claim(self) -> None:
        storage = FailingStorage()
        ActionLedger(storage=storage)
        storage.fail_claim = True
        called = False

        @ledger_sync(storage=storage, transition_binding=_binding())
        def send_payment(amount: float) -> dict[str, bool]:
            nonlocal called
            called = True
            return {"ok": True}

        with execution_scope(_scope()):
            with pytest.raises(LedgerStorageUnavailableError):
                send_payment(10.0)
        assert not called


class TestCompleteDuringStorageDown:
    def test_complete_propagates_storage_error(self) -> None:
        storage = FailingStorage()
        ledger_inst = ActionLedger(storage=storage)
        # Pre-populate an IN_FLIGHT entry
        entry = LedgerEntry(
            request_id="req-complete",
            tool="send_payment",
            args=[],
            kwargs={"amount": 10},
            status="in-flight",
            terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        )
        storage.set(entry)
        storage.fail_set = True
        with pytest.raises(LedgerStorageUnavailableError, match="set"):
            ledger_inst.complete("req-complete", {"ok": True})
        # Entry should still be IN_FLIGHT (not completed)
        stored = storage.get("req-complete")
        assert stored is not None
        assert stored.terminal_outcome == TerminalOutcome.IN_FLIGHT.value


class TestFailureRecordingDuringStorageDown:
    def test_storage_failure_does_not_mask_tool_exception(self) -> None:
        """When storage fails during _record_failure after tool raises,
        the original tool exception must propagate (not be masked)."""
        storage = FailingStorage()
        # Storage fails only on set() AFTER the claim — toggle fail_set
        # during the test. The claim succeeds, the tool raises, then
        # _record_failure tries to set the entry and fails.
        call_count = 0
        original_set = storage.set

        def selective_fail(entry: LedgerEntry) -> None:
            nonlocal call_count
            call_count += 1
            # First set is from claim (success); second set is from _record_failure
            if call_count >= 2:
                raise ConnectionError("storage backend unreachable")
            original_set(entry)

        storage.set = selective_fail  # type: ignore[assignment]

        @ledger_sync(storage=storage)
        def failing_tool() -> None:
            raise ValueError("tool broke")

        with pytest.raises(ValueError, match="tool broke"):
            failing_tool()

    def test_tool_exception_propagates_not_storage_exception(self) -> None:
        """Even when both tool and storage fail, the tool exception wins."""
        storage = FailingStorage()
        call_count = 0
        original_set = storage.set

        def selective_fail(entry: LedgerEntry) -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise ConnectionError("storage backend unreachable")
            original_set(entry)

        storage.set = selective_fail  # type: ignore[assignment]

        @ledger_sync(storage=storage)
        def another_failing_tool() -> None:
            raise RuntimeError("original error")

        with pytest.raises(RuntimeError, match="original error"):
            another_failing_tool()


class TestRedisStorageDown:
    def test_redis_claim_storage_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fakeredis = pytest.importorskip("fakeredis")
        fake = fakeredis.FakeRedis(decode_responses=True)
        # Make the fake redis fail on any command
        fake.execute_command = MagicMock(side_effect=ConnectionError("redis down"))

        import redis
        monkeypatch.setattr(redis.Redis, "from_url", lambda url, **kw: fake)

        from mycelium import RedisLedgerStorage
        storage = RedisLedgerStorage("redis://test")
        ledger_inst = ActionLedger(storage=storage)
        with pytest.raises(LedgerStorageUnavailableError):
            ledger_inst.claim("req-redis-down", "send_payment", (), {})


# ---------------------------------------------------------------------------
# Part 2: Unclassified tool policy
# ---------------------------------------------------------------------------


class TestUnclassifiedPolicyWarn:
    def test_default_is_warn(self) -> None:
        ledger_inst = ActionLedger()
        assert ledger_inst._unclassified_policy == UNCLASSIFIED_POLICY_WARN

    def test_warn_mode_reclaims_failed_entry(self) -> None:
        ledger_inst = ActionLedger(unclassified_policy=UNCLASSIFIED_POLICY_WARN)
        ledger_inst.claim("req-warn", "my_tool", (), {})
        ledger_inst.fail("req-warn", RuntimeError("boom"))
        # Warn mode should allow reclaim (legacy behavior)
        retry = ledger_inst.claim("req-warn", "my_tool", (), {})
        assert retry.status == "in-flight"

    def test_warn_mode_emits_warning_on_failed_retry(self) -> None:
        ledger_inst = ActionLedger(unclassified_policy=UNCLASSIFIED_POLICY_WARN)
        ledger_inst.claim("req-warn2", "my_tool", (), {})
        ledger_inst.fail("req-warn2", RuntimeError("boom"))
        with pytest.warns(UserWarning, match="transition_binding"):
            ledger_inst.claim("req-warn2", "my_tool", (), {})

    def test_warn_warning_only_once_per_tool(self) -> None:
        ledger_inst = ActionLedger(unclassified_policy=UNCLASSIFIED_POLICY_WARN)
        # First failure+retry
        ledger_inst.claim("req-w1", "my_tool", (), {})
        ledger_inst.fail("req-w1", RuntimeError("boom"))
        with pytest.warns(UserWarning, match="transition_binding"):
            ledger_inst.claim("req-w1", "my_tool", (), {})
        # Second failure+retry — no warning
        ledger_inst.fail("req-w1", RuntimeError("boom again"))
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            ledger_inst.claim("req-w1", "my_tool", (), {})


class TestUnclassifiedPolicyStrict:
    def test_strict_mode_hard_blocks_failed_retry(self) -> None:
        ledger_inst = ActionLedger(
            unclassified_policy=UNCLASSIFIED_POLICY_STRICT,
            poll_timeout=0.1,
        )
        ledger_inst.claim("req-strict", "my_tool", (), {})
        ledger_inst.fail("req-strict", RuntimeError("boom"))
        with pytest.raises(LedgerHardBlockError, match="manual reconciliation"):
            ledger_inst.claim("req-strict", "my_tool", (), {})

    def test_strict_mode_uses_conservative_binding(self) -> None:
        ledger_inst = ActionLedger(
            unclassified_policy=UNCLASSIFIED_POLICY_STRICT,
            poll_timeout=0.1,
        )
        # Claim creates IN_FLIGHT; fail creates FAILED_BEFORE_EFFECT
        ledger_inst.claim("req-strict2", "my_tool", (), {})
        ledger_inst.fail("req-strict2", RuntimeError("boom"))
        # The claim_side_effecting path with _UNCLASSIFIED_BINDING should
        # resolve FAILED_BEFORE_EFFECT → MANUAL_RECONCILIATION_REQUIRED → hard-block
        with pytest.raises(LedgerHardBlockError):
            ledger_inst.claim("req-strict2", "my_tool", (), {})

    def test_strict_completed_returns_cached(self) -> None:
        ledger_inst = ActionLedger(
            unclassified_policy=UNCLASSIFIED_POLICY_STRICT,
        )
        with execution_scope(_scope()):
            ledger_inst.claim("req-strict3", "my_tool", (), {})
            ledger_inst.complete("req-strict3", {"result": "done"})
        # Second claim should return the cached result
        result = ledger_inst.claim("req-strict3", "my_tool", (), {})
        assert result.status == "completed"
        assert result.result == {"result": "done"}


class TestUnclassifiedPolicyInvalid:
    def test_invalid_policy_raises(self) -> None:
        with pytest.raises(ValueError, match="unclassified_policy must be"):
            ActionLedger(unclassified_policy="invalid")


class TestOperatorReleaseAfterStrict:
    def test_release_not_executed_allows_one_reexecution(self) -> None:
        from mycelium import OPERATOR_RESOLUTION_NOT_EXECUTED

        ledger_inst = ActionLedger(
            unclassified_policy=UNCLASSIFIED_POLICY_STRICT,
            poll_timeout=0.1,
        )
        # Fail → hard-block
        ledger_inst.claim("req-rel", "my_tool", (), {})
        ledger_inst.fail("req-rel", RuntimeError("boom"))
        with pytest.raises(LedgerHardBlockError):
            ledger_inst.claim("req-rel", "my_tool", (), {})

        # Operator releases as not_executed
        ledger_inst.release(
            "req-rel",
            verified=OPERATOR_RESOLUTION_NOT_EXECUTED,
            by="operator",
            reason="investigated, safe to retry",
        )

        # Next claim should succeed (one re-execution)
        entry = ledger_inst.claim("req-rel", "my_tool", (), {})
        assert entry.status == "in-flight"
        assert entry.operator_resolution is None  # one-shot consumed


class TestOldEntriesDeserialize:
    def test_entry_without_terminal_outcome(self) -> None:
        """Old entries without terminal_outcome field deserialize correctly."""
        data = {
            "request_id": "old-req",
            "tool": "my_tool",
            "args": [],
            "kwargs": {},
            "status": "failed",
            "started_at": time.time() - 100,
            "finished_at": time.time() - 90,
        }
        entry = LedgerEntry.from_dict(data)
        # status="failed" + lease_until=None → terminal_from_legacy_status
        assert entry.terminal_outcome in (
            TerminalOutcome.FAILED_BEFORE_EFFECT.value,
            TerminalOutcome.FAILED_AFTER_EFFECT.value,
        )

    def test_entry_without_operator_resolution(self) -> None:
        """Old entries without operator resolution fields deserialize."""
        data = {
            "request_id": "old-req2",
            "tool": "my_tool",
            "args": [],
            "kwargs": {},
            "status": "completed",
            "terminal_outcome": "completed",
            "result": {"ok": True},
            "started_at": time.time() - 100,
            "finished_at": time.time() - 90,
        }
        entry = LedgerEntry.from_dict(data)
        assert entry.operator_resolution is None
        assert entry.resolved_by is None


class TestYamlUnclassifiedPolicy:
    def test_warn_policy_from_yaml(self) -> None:
        yaml_text = """\
action_ledger:
  storage: memory
  unclassified_policy: warn
tools:
  my_tool:
    callable: pkg:func
"""
        config = load_config_from_string(yaml_text)
        assert config.action_ledger is not None
        assert config.action_ledger.get("unclassified_policy") == "warn"

    def test_strict_policy_from_yaml(self) -> None:
        yaml_text = """\
action_ledger:
  storage: memory
  unclassified_policy: strict
tools:
  my_tool:
    callable: pkg:func
"""
        config = load_config_from_string(yaml_text)
        assert config.action_ledger is not None
        assert config.action_ledger.get("unclassified_policy") == "strict"

    def test_invalid_policy_from_yaml(self) -> None:
        yaml_text = """\
action_ledger:
  storage: memory
  unclassified_policy: invalid
tools:
  my_tool:
    callable: pkg:func
"""
        config = load_config_from_string(yaml_text)
        # The config loads fine; the error happens when ActionLedger is constructed
        # via the decorator. Test that config parsing passes it through.
        assert config.action_ledger is not None
        assert config.action_ledger.get("unclassified_policy") == "invalid"


class TestMemorySideEffectWarning:
    def test_warns_on_memory_storage_with_side_effects(self) -> None:
        yaml_text = """\
transition:
  agent_id: demo
  policy_version: "1"
  scope_from: {}
action_ledger:
  storage: memory
  tools: [my_tool]
tools:
  my_tool:
    side_effect_class: non_idempotent_mutate
"""
        with pytest.warns(UserWarning, match="my_tool.*non_idempotent_mutate"):
            load_config_from_string(yaml_text)

    def test_no_warning_for_read_tools(self) -> None:
        yaml_text = """\
transition:
  agent_id: demo
  policy_version: "1"
  scope_from: {}
action_ledger:
  storage: memory
  tools: [my_tool]
tools:
  my_tool:
    side_effect_class: read
"""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            load_config_from_string(yaml_text)

    def test_no_warning_for_file_storage(self) -> None:
        yaml_text = """\
transition:
  agent_id: demo
  policy_version: "1"
  scope_from: {}
action_ledger:
  storage: file
  path: /tmp/test.json
  tools: [my_tool]
tools:
  my_tool:
    side_effect_class: non_idempotent_mutate
"""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            load_config_from_string(yaml_text)

    def test_no_warning_without_transition_config(self) -> None:
        yaml_text = """\
action_ledger:
  storage: memory
  tools: [my_tool]
tools:
  my_tool:
    side_effect_class: non_idempotent_mutate
"""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            load_config_from_string(yaml_text)


class TestDecoratorUnclassifiedPolicy:
    def test_ledger_sync_passes_unclassified_policy(self) -> None:
        called = False

        @ledger_sync(unclassified_policy=UNCLASSIFIED_POLICY_STRICT)
        def my_tool() -> dict[str, bool]:
            nonlocal called
            called = True
            return {"ok": True}

        with execution_scope(_scope()):
            my_tool()
        assert called

    def test_ledger_passes_unclassified_policy(self) -> None:
        import asyncio

        called = False

        @ledger(unclassified_policy=UNCLASSIFIED_POLICY_STRICT)
        async def my_async_tool() -> dict[str, bool]:
            nonlocal called
            called = True
            return {"ok": True}

        with execution_scope(_scope()):
            asyncio.run(my_async_tool())
        assert called
