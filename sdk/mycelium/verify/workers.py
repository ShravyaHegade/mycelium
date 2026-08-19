"""Synthetic tools, providers, and subprocess workers for verify."""

from __future__ import annotations

import multiprocessing as mp
import os
import time
from dataclasses import dataclass, field
from typing import Any

from mycelium.action_ledger import (
    ARGS_DRIFT_HARD,
    REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    ActionLedger,
    _active_transition_var,
    _ActiveTransition,
    ledger_sync,
    mark_maybe_crossed,
    record_external_operation,
    side_effect,
)
from mycelium.reconcile import ReconcileResult, ReconcileStatus
from mycelium.transition import (
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
)
from mycelium.verify.isolation import IsolationGateStorage, VerificationNamespace

_MP_CTX = mp.get_context("spawn")

SYNTHETIC_TOOL = "verify_charge"


def synthetic_binding(
    *,
    keyed: bool = False,
    key_ttl: float | None = None,
) -> ToolTransitionBinding:
    cls = SideEffectClass.KEYED_MUTATE if keyed else SideEffectClass.NON_IDEMPOTENT_MUTATE
    return ToolTransitionBinding.for_tool(
        agent_id="mycelium-verify",
        policy_version="verify",
        side_effect_class=cls,
        provider_idempotency_key_param="idempotency_key" if keyed else None,
        provider_idempotency_key_ttl=key_ttl,
    )


@dataclass
class SyntheticProvider:
    """Isolated fake provider — never a real business system."""

    effects: list[str] = field(default_factory=list)
    keys_seen: list[str] = field(default_factory=list)

    def charge(
        self,
        amount: int,
        *,
        op_id: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        if idempotency_key is not None:
            self.keys_seen.append(idempotency_key)
        self.effects.append(op_id)
        return {"charged": True, "amount": amount, "op_id": op_id}


@dataclass
class SyntheticReconciler:
    status: ReconcileStatus = ReconcileStatus.UNKNOWN
    result: Any = None
    calls: int = 0
    raise_error: bool = False
    delay_matches: bool = False
    conflicting: bool = False
    mutating: bool = False
    zero_matches_in_window: bool = False

    def reconcile(self, entry: Any) -> ReconcileResult:
        self.calls += 1
        if self.mutating:
            raise RuntimeError("reconciler must be read-only")
        if self.raise_error:
            raise TimeoutError("synthetic provider timeout")
        if self.conflicting or self.delay_matches or self.zero_matches_in_window:
            return ReconcileResult.unknown()
        if self.status == ReconcileStatus.COMPLETED:
            return ReconcileResult.completed(self.result or {"reconciled": True})
        if self.status == ReconcileStatus.NOT_EXECUTED:
            return ReconcileResult.not_executed()
        return ReconcileResult.unknown()


def make_ledger(
    storage: IsolationGateStorage,
    *,
    binding: ToolTransitionBinding | None = None,
    reconciler: SyntheticReconciler | None = None,
    lease_ttl: float = 30.0,
    poll_timeout: float = 5.0,
    reclaim_requires_death_signal: bool = False,
    presumed_dead_after: float | None = None,
    outcome_emitter: Any = None,
) -> ActionLedger:
    return ActionLedger(
        storage=storage,
        reconciler=reconciler,
        lease_ttl=lease_ttl,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=poll_timeout,
        on_args_drift=ARGS_DRIFT_HARD,
        request_identity_policy=REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
        reclaim_requires_death_signal=reclaim_requires_death_signal,
        presumed_dead_after=presumed_dead_after,
        outcome_emitter=outcome_emitter,
    )


def make_tool(
    storage: IsolationGateStorage,
    counter_path: str,
    *,
    provider: SyntheticProvider | None = None,
    fail_after_effect: bool = False,
    fail_before_effect: bool = False,
    keyed: bool = False,
    key_ttl: float | None = None,
    record_ref: bool = True,
    reconciler: SyntheticReconciler | None = None,
    lease_ttl: float = 30.0,
    poll_timeout: float = 5.0,
    reclaim_requires_death_signal: bool = False,
    presumed_dead_after: float | None = None,
    outcome_emitter: Any = None,
):
    binding = synthetic_binding(keyed=keyed, key_ttl=key_ttl)

    @ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=lease_ttl,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=poll_timeout,
        on_args_drift=ARGS_DRIFT_HARD,
        request_identity_policy=REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
        reconciler=reconciler,
        reclaim_requires_death_signal=reclaim_requires_death_signal,
        presumed_dead_after=presumed_dead_after,
        outcome_emitter=outcome_emitter,
    )
    def verify_charge(
        amount: int,
        op_id: str = "op",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        _append_count(counter_path)
        if fail_before_effect:
            raise RuntimeError("synthetic failure before effect boundary")
        if provider is None:
            return {"charged": True, "amount": amount}
        with side_effect():
            if record_ref:
                record_external_operation(op_id)
            result = provider.charge(amount, op_id=op_id, idempotency_key=idempotency_key)
            if fail_after_effect:
                raise RuntimeError("synthetic provider returned; ledger incomplete")
            return result

    return verify_charge


def _append_count(path: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, b"x\n")
    finally:
        os.close(fd)


def _append_line(path: str, line: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    try:
        os.write(fd, (line + "\n").encode("utf-8"))
    finally:
        os.close(fd)


def count_executions(path: str) -> int:
    return len(read_lines(path))


def read_lines(path: str) -> list[str]:
    if not path or not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def unclean_worker_exits(procs: list[mp.Process]) -> list[str]:
    return [f"pid={proc.pid} exit={proc.exitcode}" for proc in procs if proc.exitcode != 0]


def contention_round_failure(
    procs: list[mp.Process],
    *,
    executions: int,
    out_file: str,
    err_file: str,
    workers: int,
    ready_file: str | None = None,
) -> str | None:
    """Return a failure reason, or None when the round is empirically valid."""
    if ready_file is not None:
        ready = read_lines(ready_file)
        if len(ready) < workers:
            return f"barrier saw {len(ready)}/{workers} workers"
    if executions != 1:
        return f"body_executions={executions}, expected 1"
    bad = unclean_worker_exits(procs)
    if bad:
        return "workers did not exit cleanly: " + ", ".join(bad)
    outputs = read_lines(out_file)
    errors = read_lines(err_file)
    if errors:
        return f"workers reported errors: {errors[:3]}"
    if len(outputs) != workers:
        return f"resolved workers={len(outputs)}/{workers}"
    if len(set(outputs)) != 1:
        return f"inconsistent terminal results: {outputs!r}"
    return None


def concurrent_reconcile_failure(
    procs: list[mp.Process],
    *,
    executions: int,
    out_file: str,
    err_file: str,
    workers: int,
) -> str | None:
    """NOT_EXECUTED concurrent redispatches must authorize exactly one body."""
    if executions != 1:
        errors = read_lines(err_file)
        extra = f" errors={errors[:3]}" if errors else ""
        return f"concurrent reconcilers executed {executions} times (expected 1){extra}"
    bad = unclean_worker_exits(procs)
    if bad:
        return "workers did not exit cleanly: " + ", ".join(bad)
    outputs = read_lines(out_file)
    errors = read_lines(err_file)
    if not outputs:
        return "no worker recorded a successful resolution"
    if len(set(outputs)) != 1:
        return f"inconsistent terminal results: {outputs!r}"
    if len(outputs) + len(errors) < workers:
        return f"silent workers: resolved={len(outputs)} errors={len(errors)} expected {workers}"
    unexpected = [
        line
        for line in errors
        if line.split(":", 1)[0]
        not in {
            "LedgerHardBlockError",
            "LedgerPollTimeoutError",
            "LedgerPendingError",
        }
    ]
    if unexpected:
        return f"unexpected worker errors: {unexpected[:3]}"
    return None


def fence_rejection_failure(storage: Any, request_id: str) -> str | None:
    """Prove a stale-fence write is CAS-rejected on the winning entry.

    A superseded worker carries a fence below the stored one. Even reusing the
    winner's own outcome/owner, a write stamped with ``stored_fence - 1`` must
    be refused by the storage CAS. Returns a failure reason, or ``None`` when
    the fence gate empirically rejects the stale write.
    """
    from dataclasses import replace

    from mycelium.transition import TerminalOutcome

    entry = storage.get(request_id)
    if entry is None:
        return f"winning entry {request_id!r} missing; cannot prove fence"
    stored_fence = getattr(entry, "fence", None)
    if stored_fence is None:
        return "winning entry carries no fence token"
    if stored_fence < 1:
        return f"winning entry fence={stored_fence}, expected >= 1 after a claim"

    stale = replace(entry, fence=stored_fence - 1, result={"stale": True})
    accepted = storage.try_transition(
        stale,
        expected_terminal_outcomes=frozenset(
            {
                TerminalOutcome.IN_FLIGHT.value,
                TerminalOutcome.COMPLETED.value,
            }
        ),
        expected_fence=stored_fence - 1,
    )
    if accepted:
        return (
            f"stale-fence write accepted (stored fence={stored_fence}); "
            "fencing gate did not reject the superseded worker"
        )

    after = storage.get(request_id)
    if after is None or getattr(after, "fence", None) != stored_fence:
        return "stored fence mutated after a stale-fence write was refused"
    return None


def storage_from_payload(payload: dict[str, Any]) -> IsolationGateStorage:
    backend = payload["backend"]
    if backend == "file":
        from mycelium.action_ledger import FileLedgerStorage

        inner: Any = FileLedgerStorage(payload["path"])
    elif backend == "sqlite":
        from mycelium.storage.sqlite_ledger import SqliteLedgerStorage

        inner = SqliteLedgerStorage(
            payload["path"], table=payload.get("table", "mycelium_verify_ledger")
        )
    elif backend == "redis":
        from mycelium.storage.redis_ledger import RedisLedgerStorage

        inner = RedisLedgerStorage(payload["url"], prefix=payload["prefix"], in_flight_ttl=None)
    elif backend == "postgres":
        from mycelium.storage.postgres_ledger import PostgresLedgerStorage
        from mycelium.verify.isolation import _PostgresPrefixedStorage

        inner = _PostgresPrefixedStorage(
            PostgresLedgerStorage(payload["dsn"], table=payload["table"]),
            prefix=payload["prefix_ns"],
            dsn=payload["dsn"],
            table=payload["table"],
        )
    else:
        raise RuntimeError(f"unsupported worker backend {backend}")
    ns = VerificationNamespace(
        run_id=payload["run_id"],
        prefix=payload["prefix_ns"],
        started_at=0.0,
        backend=backend,
    )
    return IsolationGateStorage(inner, ns)


def contention_worker(payload: dict[str, Any]) -> None:
    """Spawn-safe worker: isolated storage factory via file/sqlite path."""
    try:
        storage = storage_from_payload(payload)
    except Exception as exc:  # noqa: BLE001
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")
        return

    ready = payload.get("ready_file")
    if ready:
        _append_line(ready, "ready")
        barrier = payload.get("barrier_file")
        deadline = time.time() + 5.0
        while barrier and not os.path.exists(barrier) and time.time() < deadline:
            time.sleep(0.01)

    @ledger_sync(
        storage=storage,
        transition_binding=synthetic_binding(),
        lease_ttl=30.0,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=float(payload.get("poll_timeout", 8.0)),
        on_args_drift=ARGS_DRIFT_HARD,
        request_identity_policy=REQUEST_IDENTITY_POLICY_REQUIRE_EXPLICIT,
    )
    def verify_charge(amount: int) -> dict[str, Any]:
        _append_count(payload["exec_file"])
        return {"charged": True, "amount": amount}

    try:
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            result = verify_charge(1, request_id=payload["request_id"])
        _append_line(payload["out_file"], str(result))
    except Exception as exc:  # noqa: BLE001 — record for parent
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")


def crash_worker(payload: dict[str, Any]) -> None:
    """Hard-exit at a named crash phase (not an ordinary exception)."""
    try:
        storage = storage_from_payload(payload)
    except Exception as exc:  # noqa: BLE001
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")
        os._exit(2)

    binding = synthetic_binding()
    ledger = make_ledger(
        storage,
        binding=binding,
        lease_ttl=float(payload.get("lease_ttl", 1.0)),
        reclaim_requires_death_signal=bool(payload.get("reclaim_requires_death_signal", False)),
    )
    phase = payload["phase"]
    request_id = payload["request_id"]
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        claimed = ledger.claim_side_effecting(
            request_id,
            SYNTHETIC_TOOL,
            (1,),
            {
                "request_id": request_id,
                "thread_id": "verify",
                "run_id": "verify",
            },
            binding,
        )
        token = _active_transition_var.set(
            _ActiveTransition(
                ledger,
                request_id,
                binding,
                {},
                claimed.owner,
                claimed.fence,
            )
        )
        try:
            if phase == "after_claim":
                _append_line(payload["ready_file"], "after_claim")
                os._exit(1)
            ledger.record_decision(
                request_id,
                {"allowed": True, "verdicts": [], "denied_reasons": []},
                expected_owner=claimed.owner,
                expected_fence=claimed.fence,
            )
            _append_count(payload["exec_file"])
            if phase == "after_body_start":
                _append_line(payload["ready_file"], "after_body_start")
                os._exit(1)
            mark_maybe_crossed()
            if phase == "after_boundary":
                _append_line(payload["ready_file"], "after_boundary")
                os._exit(1)
            record_external_operation(payload.get("op_id", "verify-op"))
            _append_line(payload.get("effect_file", payload["exec_file"] + ".fx"), "effect")
            if phase == "after_effect":
                _append_line(payload["ready_file"], "after_effect")
                time.sleep(60)
                os._exit(1)
            ledger.complete(
                request_id,
                {"charged": True},
                expected_fence=claimed.fence,
                _expected_owner=claimed.owner,
            )
        finally:
            _active_transition_var.reset(token)


def reconcile_worker(payload: dict[str, Any]) -> None:
    """Redispatch an ambiguous ticket with a scripted reconciler."""
    try:
        storage = storage_from_payload(payload)
    except Exception as exc:  # noqa: BLE001
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")
        return
    status = ReconcileStatus(payload.get("reconcile_status", "NOT_EXECUTED"))
    reconciler = SyntheticReconciler(status=status)
    ready = payload.get("ready_file")
    if ready:
        _append_line(ready, "ready")
        barrier = payload.get("barrier_file")
        deadline = time.time() + 5.0
        while barrier and not os.path.exists(barrier) and time.time() < deadline:
            time.sleep(0.01)
    tool = make_tool(
        storage,
        payload["exec_file"],
        reconciler=reconciler,
        poll_timeout=float(payload.get("poll_timeout", 8.0)),
    )
    try:
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            result = tool(1, request_id=payload["request_id"], op_id=payload.get("op_id", "op"))
        _append_line(payload["out_file"], str(result))
    except Exception as exc:  # noqa: BLE001
        _append_line(payload["err_file"], f"{type(exc).__name__}: {exc}")


def spawn_workers(target, payloads: list[dict[str, Any]]) -> list[mp.Process]:
    procs = [_MP_CTX.Process(target=target, args=(payload,), daemon=True) for payload in payloads]
    for proc in procs:
        proc.start()
    return procs


def join_workers(procs: list[mp.Process], timeout: float) -> None:
    for proc in procs:
        proc.join(timeout=timeout)
    for proc in procs:
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=2)


def terminate_owned(procs: list[mp.Process]) -> None:
    for proc in procs:
        try:
            if proc.is_alive():
                proc.kill()
        except ValueError:
            continue
    for proc in procs:
        try:
            proc.join(timeout=2)
            proc.close()
        except ValueError:
            continue


__all__ = [
    "SYNTHETIC_TOOL",
    "SyntheticProvider",
    "SyntheticReconciler",
    "contention_worker",
    "contention_round_failure",
    "concurrent_reconcile_failure",
    "count_executions",
    "crash_worker",
    "fence_rejection_failure",
    "join_workers",
    "make_ledger",
    "make_tool",
    "read_lines",
    "reconcile_worker",
    "spawn_workers",
    "storage_from_payload",
    "unclean_worker_exits",
    "synthetic_binding",
    "terminate_owned",
]
