"""Real OS-process concurrency tests (Phase 4 / multi-process concurrency).

Threaded races already live in ``test_atomicity_contract.py`` and
``test_proof_two_worker_redis.py``. These tests go one level further: they
spawn real OS processes (``multiprocessing`` spawn context) that share a
durable backend, so the cross-process file lock and Redis atomic claim paths
are exercised, not just in-process locks.

Scenarios covered:

- A. Two processes, shared ``FileLedgerStorage``, same transition key on a
  payment tool: exactly one tool-body execution; the loser polls or returns
  the stored completed result.
- B. Two processes racing to reclaim an expired ``IN_FLIGHT`` transition
  (strict payment class) while a Reconciler proves ``NOT_EXECUTED``: at most
  one re-execution, never two completed side effects for one logical payment.
- C. Real Redis (skipped when unreachable): two processes start *nearly
  together* (synchronized start) and contend for the same claim.

All tests clean up child processes in ``finally``.
"""

from __future__ import annotations

import json
import multiprocessing as mp
import os
import time
from pathlib import Path
from typing import Any

import pytest

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    InMemoryLedgerStorage,
    ReconcileResult,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    ledger_sync,
)
from mycelium.proofs.langgraph_7417_redis import (
    ENV_REDIS_URL,
    redis_reachable,
    resolve_redis_url,
)

_MP_CTX = mp.get_context("spawn")


# ---------------------------------------------------------------------------
# Cross-process helpers
# ---------------------------------------------------------------------------


def _append_count(path: str) -> None:
    """Atomically record one tool-body execution (O_APPEND, line each)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, b"x\n")
    finally:
        os.close(fd)


def _append_line(path: str, line: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, line.encode("utf-8") + b"\n")
    finally:
        os.close(fd)


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line]


def _count_executions(path: Path) -> int:
    return len(_read_lines(path))


def _payment_binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="mp",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


# ---------------------------------------------------------------------------
# Scenario A: contested claim on a shared file ledger (decorator path)
# ---------------------------------------------------------------------------


def _contended_file_worker(payload: dict[str, Any]) -> None:
    """Claim/execute/complete a payment tool via @ledger_sync; record outcome.

    Both workers use the same ``tool_call_id`` so they derive the same
    transition key. Exactly one may run the tool body; the other polls and
    returns the stored result.
    """
    import json

    from mycelium import FileLedgerStorage

    storage = FileLedgerStorage(payload["ledger_path"])

    @ledger_sync(
        storage=storage,
        transition_binding=_payment_binding(),
        lease_ttl=float(payload["lease_ttl"]),
        poll_interval=0.02,
        poll_timeout=float(payload["poll_timeout"]),
    )
    def charge(amount: float) -> dict[str, Any]:
        _append_count(payload["exec_file"])
        return {"charged": True, "amount": str(amount)}

    try:
        with execution_scope(
            TransitionScope(thread_id="t1", run_id="r1")
        ):
            result = charge(amount=10.0, tool_call_id=payload["tool_call_id"])
        _append_line(payload["out_file"], json.dumps(result, default=str))
    except Exception as exc:  # noqa: BLE001 — surface to parent
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")


def test_two_processes_file_ledger_single_execution(tmp_path: Path) -> None:
    ledger_path = tmp_path / "ledger.json"
    exec_file = tmp_path / "executions.txt"
    out_file = tmp_path / "out.txt"
    err_file = tmp_path / "err.txt"
    request_id = "mp_charge_contended"

    payload = {
        "ledger_path": str(ledger_path),
        "exec_file": str(exec_file),
        "out_file": str(out_file),
        "err_file": str(err_file),
        "tool_call_id": request_id,
        "lease_ttl": 30.0,
        "poll_timeout": 10.0,
    }

    procs = [
        _MP_CTX.Process(target=_contended_file_worker, args=(payload,), name="mp-a"),
        _MP_CTX.Process(target=_contended_file_worker, args=(payload,), name="mp-b"),
    ]
    try:
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=20.0)
        for proc in procs:
            assert not proc.is_alive(), "worker process timed out"
            assert proc.exitcode == 0, f"worker exit {proc.exitcode}"
    finally:
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2.0)

    assert _read_lines(err_file) == [], _read_lines(err_file)
    assert _count_executions(exec_file) == 1, "side effect must run exactly once"
    results = _read_lines(out_file)
    assert len(results) == 2, f"both workers should return: {results}"
    assert results[0] == results[1], f"identical results expected: {results}"
    assert json.loads(results[0]) == {"charged": True, "amount": "10.0"}


# ---------------------------------------------------------------------------
# Scenario B: racing reclaim on an expired IN_FLIGHT with a NOT_EXECUTED
# reconciler (strict payment class)
# ---------------------------------------------------------------------------


class _NotExecutedReconciler:
    """Reconciler that proves the effect never happened (read-only)."""

    def reconcile(self, entry: Any) -> ReconcileResult:
        return ReconcileResult.not_executed()


def _reclaim_file_worker(payload: dict[str, Any]) -> None:
    """Race to reclaim an expired payment transition via the claim path.

    Both processes hit the HARD_BLOCK gate on the expired entry, reconcile
    ``NOT_EXECUTED``, and race the CAS reset. Exactly one may run the tool.
    """
    import json

    from mycelium import FileLedgerStorage

    storage = FileLedgerStorage(payload["ledger_path"])
    ledger = ActionLedger(
        storage=storage,
        reconciler=_NotExecutedReconciler(),
        lease_ttl=float(payload["lease_ttl"]),
        poll_interval=0.02,
        poll_timeout=float(payload["poll_timeout"]),
    )
    request_id = payload["request_id"]
    try:
        entry = ledger.claim_side_effecting(
            request_id,
            "charge",
            (),
            {"amount": 10.0},
            _payment_binding(),
        )
        outcome = entry.resolved_terminal_outcome()
        if outcome == TerminalOutcome.COMPLETED:
            _append_line(payload["out_file"], json.dumps({"cached": entry.result}))
            return
        if outcome == TerminalOutcome.IN_FLIGHT:
            _append_count(payload["exec_file"])
            ledger.complete(request_id, {"charged": True})
            _append_line(payload["out_file"], json.dumps({"ran": True}))
            return
        _append_line(payload["err_file"], f"unexpected outcome {outcome.value}")
    except Exception as exc:  # noqa: BLE001 — surface to parent
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")


def _seed_expired_payment(storage: InMemoryLedgerStorage, request_id: str) -> None:

    from mycelium import LedgerEntry, SideEffectBoundary

    entry = LedgerEntry(
        request_id=request_id,
        tool="charge",
        args=[],
        kwargs={"amount": 10.0},
        status="in-flight",
        terminal_outcome=TerminalOutcome.IN_FLIGHT.value,
        lease_until=time.time() - 1,
        owner="dead-worker:1",
        side_effect_boundary=SideEffectBoundary.NOT_CROSSED.value,
        external_operation_ref="pi_expired_mp",
        idempotency_key=request_id,
    )
    storage.set(entry)


def test_two_processes_reclaim_expired_payment_single_reexec(tmp_path: Path) -> None:
    """Two processes race reclaim of an expired IN_FLIGHT payment; at most one
    re-executes when reconcile proves NOT_EXECUTED."""
    ledger_path = tmp_path / "ledger.json"
    exec_file = tmp_path / "executions.txt"
    out_file = tmp_path / "out.txt"
    err_file = tmp_path / "err.txt"
    request_id = "mp_reclaim_expired"

    # Seed the expired ambiguous transition before any worker starts.
    seed_storage = FileLedgerStorage(ledger_path)
    seed = InMemoryLedgerStorage()
    _seed_expired_payment(seed, request_id)
    for entry in seed.list_all():
        seed_storage.set(entry)

    payload = {
        "ledger_path": str(ledger_path),
        "exec_file": str(exec_file),
        "out_file": str(out_file),
        "err_file": str(err_file),
        "request_id": request_id,
        "lease_ttl": 30.0,
        "poll_timeout": 10.0,
    }

    procs = [
        _MP_CTX.Process(target=_reclaim_file_worker, args=(payload,), name="mp-reclaim-a"),
        _MP_CTX.Process(target=_reclaim_file_worker, args=(payload,), name="mp-reclaim-b"),
    ]
    try:
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=20.0)
        for proc in procs:
            assert not proc.is_alive(), "worker process timed out"
            assert proc.exitcode == 0, f"worker exit {proc.exitcode}"
    finally:
        for proc in procs:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2.0)

    assert _read_lines(err_file) == [], _read_lines(err_file)
    assert _count_executions(exec_file) == 1, (
        "at most one re-execution when reconcile proves NOT_EXECUTED"
    )
    results = _read_lines(out_file)
    assert len(results) == 2, f"both workers should resolve: {results}"

    # The logical payment never gets two completed side effects: one worker
    # ran and completed, the other saw the completed result.
    ran = [json.loads(line) for line in results]
    completed_values = [r for r in ran if "ran" in r]
    cached_values = [r for r in ran if "cached" in r]
    assert len(completed_values) == 1, f"exactly one worker ran: {ran}"
    assert len(cached_values) == 1, f"exactly one worker saw cached: {ran}"
    assert cached_values[0]["cached"] == {"charged": True}

    stored = FileLedgerStorage(ledger_path).get(request_id)
    assert stored is not None
    assert stored.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


# ---------------------------------------------------------------------------
# Scenario C: real Redis, two processes started nearly together
# ---------------------------------------------------------------------------


def _redis_contested_worker(payload: dict[str, Any]) -> None:
    """Both workers synchronize on a ready counter, then race the claim."""
    import json

    import redis

    from mycelium import (
        RedisLedgerStorage,
    )

    url = payload["url"]
    keys = payload["keys"]
    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        client.incr(keys["ready"])
        deadline = time.time() + float(payload["ready_timeout"])
        while int(client.get(keys["ready"]) or 0) < 2:
            if time.time() >= deadline:
                raise TimeoutError("peer worker never signaled ready")
            time.sleep(0.02)

        storage = RedisLedgerStorage(url, prefix=payload["prefix"], in_flight_ttl=3600.0)

        @ledger_sync(
            storage=storage,
            transition_binding=_payment_binding(),
            lease_ttl=float(payload["lease_ttl"]),
            poll_interval=0.02,
            poll_timeout=float(payload["poll_timeout"]),
        )
        def charge(amount: float) -> dict[str, Any]:
            client.incr(keys["exec"])
            return {"charged": True, "amount": str(amount)}

        with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
            result = charge(amount=10.0, tool_call_id=payload["request_id"])
        client.rpush(keys["results"], json.dumps(result, default=str))
    except Exception as exc:  # noqa: BLE001 — surface to parent
        client.set(keys["error"], f"{type(exc).__name__}: {exc}")
    finally:
        client.close()


_REDIS_URL = resolve_redis_url()

pytest.importorskip("redis")
pytestmark = pytest.mark.skipif(
    not redis_reachable(_REDIS_URL),
    reason=(
        f"real Redis required at {_REDIS_URL!r} "
        f"(set {ENV_REDIS_URL} or start redis-server)"
    ),
)


def test_two_processes_redis_contested_claim(tmp_path: Path) -> None:
    """Both processes start together and contend for the same payment claim on
    real Redis; exactly one tool-body execution, identical returned results."""
    import redis

    url = _REDIS_URL
    run_id = f"mp_contested_{os.getpid()}_{int(time.time())}"
    prefix = f"mycelium:proof:mp:{run_id}:ledger:"
    keys = {
        "ready": f"mycelium:proof:mp:{run_id}:ready",
        "exec": f"mycelium:proof:mp:{run_id}:executions",
        "results": f"mycelium:proof:mp:{run_id}:results",
        "error": f"mycelium:proof:mp:{run_id}:error",
    }
    request_id = f"call_charge_mp_{run_id}"

    client = redis.Redis.from_url(url, decode_responses=True)
    try:
        to_delete = [keys["ready"], keys["exec"], keys["results"], keys["error"]]
        for key in client.scan_iter(match=f"{prefix}*"):
            to_delete.append(key)
        if to_delete:
            client.delete(*to_delete)

        payload = {
            "url": url,
            "keys": keys,
            "prefix": prefix,
            "request_id": request_id,
            "lease_ttl": 30.0,
            "poll_timeout": 10.0,
            "ready_timeout": 5.0,
        }
        procs = [
            _MP_CTX.Process(target=_redis_contested_worker, args=(payload,), name="mp-redis-a"),
            _MP_CTX.Process(target=_redis_contested_worker, args=(payload,), name="mp-redis-b"),
        ]
        try:
            for proc in procs:
                proc.start()
            for proc in procs:
                proc.join(timeout=25.0)
            for proc in procs:
                assert not proc.is_alive(), "worker process timed out"
                assert proc.exitcode == 0, f"worker exit {proc.exitcode}"
        finally:
            for proc in procs:
                if proc.is_alive():
                    proc.terminate()
                    proc.join(timeout=2.0)

        error = client.get(keys["error"])
        assert not error, error
        executions = int(client.get(keys["exec"]) or 0)
        assert executions == 1, f"side effect must run exactly once, got {executions}"
        results = client.lrange(keys["results"], 0, -1)
        assert len(results) == 2, f"both workers should return: {results}"
        assert results[0] == results[1], f"identical results expected: {results}"
        assert json.loads(results[0]) == {"charged": True, "amount": "10.0"}
    finally:
        to_delete = [keys["ready"], keys["exec"], keys["results"], keys["error"]]
        for key in client.scan_iter(match=f"{prefix}*"):
            to_delete.append(key)
        if to_delete:
            client.delete(*to_delete)
        client.close()
