"""In-process two-worker transition-envelope proof (no Redis required).

The ``test_proof_two_worker_redis.py`` and ``test_multiprocess_concurrency.py``
proofs spawn OS processes and (optionally) Redis. These tests run the same
envelope deterministically inside one process with two threads sharing a
durable backend, so the claim → POLL → stored-result-return contract is
always exercised in CI, even with no Redis reachable.

Scenarios covered:

- Worker A claims and runs a slow side effect; Worker B redispatches the same
  transition key while A is ``IN_FLIGHT``. B must POLL, not re-execute, and
  return A's stored result — the side effect runs exactly once.
- Worker A crashes (lease lapses to ``EXPIRED``) mid-flight; Worker B's
  redispatch resolves the envelope fail-closed (reclaim / hard-block by class)
  without a second side effect.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    LedgerHardBlockError,
    SideEffectBoundary,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
)


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="proof",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def test_two_workers_thread_redispatch_single_execution() -> None:
    """B redispatches while A is in-flight → POLL → both return A's result."""
    storage = InMemoryLedgerStorage()
    started = threading.Event()
    release = threading.Event()
    exec_count = 0
    results: list[tuple[str, dict]] = []
    errors: list[BaseException] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_binding(),
        lease_ttl=30.0,
        poll_interval=0.01,
        poll_timeout=5.0,
    )
    def analyze_market(task: str) -> dict:
        nonlocal exec_count
        exec_count += 1
        started.set()
        assert release.wait(timeout=5.0), "owner never released"
        return {"task": task, "result": "done"}

    def worker_a() -> None:
        try:
            with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
                results.append(("A", analyze_market(task="analyze_market", tool_call_id="call_1")))
        except BaseException as exc:  # noqa: BLE001 — surface in parent thread
            errors.append(exc)
        finally:
            release.set()

    def worker_b() -> None:
        try:
            assert started.wait(timeout=5.0), "A never claimed"
            with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
                results.append(("B", analyze_market(task="analyze_market", tool_call_id="call_1")))
        except BaseException as exc:  # noqa: BLE001 — surface in parent thread
            errors.append(exc)

    thread_a = threading.Thread(target=worker_a, name="worker-a")
    thread_b = threading.Thread(target=worker_b, name="worker-b")
    thread_a.start()
    assert started.wait(timeout=5.0), "A never started"
    thread_b.start()
    # Let B enter its POLL loop before A completes; otherwise B just sees a
    # COMPLETED entry and this becomes the trivial RETURN path.
    time.sleep(0.1)
    release.set()
    thread_b.join(timeout=10.0)
    thread_a.join(timeout=10.0)

    assert errors == [], errors
    assert exec_count == 1, "side effect must run exactly once"
    by_worker = dict(results)
    assert set(by_worker) == {"A", "B"}
    assert by_worker["A"] == by_worker["B"] == {"task": "analyze_market", "result": "done"}

    entries = storage.list_all()
    assert len(entries) == 1, "one envelope must survive"
    assert entries[0].resolved_terminal_outcome() == TerminalOutcome.COMPLETED


def test_two_workers_file_ledger_redispatch_single_execution(tmp_path: Path) -> None:
    """Same envelope on the durable file backend: one execution, two results."""
    storage = FileLedgerStorage(tmp_path / "ledger.json")
    started = threading.Event()
    release = threading.Event()
    exec_count = 0
    results: list[tuple[str, dict]] = []
    errors: list[BaseException] = []

    @ledger_sync(
        storage=storage,
        transition_binding=_binding(),
        lease_ttl=30.0,
        poll_interval=0.01,
        poll_timeout=5.0,
    )
    def analyze_market(task: str) -> dict:
        nonlocal exec_count
        exec_count += 1
        started.set()
        assert release.wait(timeout=5.0), "owner never released"
        return {"task": task, "result": "done"}

    def worker_a() -> None:
        try:
            with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
                results.append(("A", analyze_market(task="analyze_market", tool_call_id="call_2")))
        except BaseException as exc:  # noqa: BLE001 — surface in parent thread
            errors.append(exc)
        finally:
            release.set()

    def worker_b() -> None:
        try:
            assert started.wait(timeout=5.0), "A never claimed"
            with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
                results.append(("B", analyze_market(task="analyze_market", tool_call_id="call_2")))
        except BaseException as exc:  # noqa: BLE001 — surface in parent thread
            errors.append(exc)

    thread_a = threading.Thread(target=worker_a, name="worker-a-file")
    thread_b = threading.Thread(target=worker_b, name="worker-b-file")
    thread_a.start()
    assert started.wait(timeout=5.0), "A never started"
    thread_b.start()
    time.sleep(0.1)
    release.set()
    thread_b.join(timeout=10.0)
    thread_a.join(timeout=10.0)

    assert errors == [], errors
    assert exec_count == 1, "side effect must run exactly once"
    by_worker = dict(results)
    assert set(by_worker) == {"A", "B"}
    assert by_worker["A"] == by_worker["B"] == {"task": "analyze_market", "result": "done"}


def test_owner_crash_expired_envelope_does_not_reexecute_crossed() -> None:
    """A died mid-effect (CROSSED + expired lease) → B hard-blocks, no re-run."""
    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(
        storage=storage,
        lease_ttl=0.05,
        poll_interval=0.01,
        poll_timeout=0.5,
    )
    request_id = "call_crash"
    binding = _binding()

    claimed = ledger.claim_side_effecting(
        request_id,
        "analyze_market",
        (),
        {"task": "analyze_market"},
        binding,
        lease_ttl=0.05,
    )
    ledger.advance_boundary(request_id, SideEffectBoundary.CROSSED)
    time.sleep(0.1)  # lease lapses → EXPIRED

    assert claimed.resolved_terminal_outcome() in (
        TerminalOutcome.IN_FLIGHT,
        TerminalOutcome.EXPIRED,
    )
    stored = storage.get(request_id)
    assert stored is not None
    assert stored.resolved_terminal_outcome() == TerminalOutcome.EXPIRED

    try:
        ledger.claim_side_effecting(
            request_id,
            "analyze_market",
            (),
            {"task": "analyze_market"},
            binding,
            poll_timeout=0.2,
        )
    except LedgerHardBlockError:
        pass  # expected: CROSSED + EXPIRED → manual reconciliation
    else:
        raise AssertionError("crash-window redispatch must hard-block, not re-execute")
