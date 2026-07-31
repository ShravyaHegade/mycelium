"""Hypothesis property tests for the single-key side-effect state machine.

A *transition key* is one durable envelope for a side-effecting tool call.
These tests generate arbitrary interleavings of every operation the runtime
can apply to a single key -- claim, complete, fail before/after effect, crash
(lease lapse), operator release, provider reconcile, stuck-marking, heartbeat
renewal and worker-death signals -- and assert the durable record upholds the
same guarantees the multiprocess / crash-window tests check imperatively:

- ``executions <= 1 + not_executed_verdicts``: the tool body may run at most
  once, plus exactly one extra run per *provably not executed* verdict
  (operator release or provider reconcile).  No interleaving can double-charge.
- Once COMPLETED, a key stays COMPLETED with a stable result and grants no
  further executions; redispatches return the cached result.
- Mutators (complete/fail/mark_*) CAS strictly out of ``IN_FLIGHT``; a
  terminal-over-terminal write is refused.
- The durable record always round-trips ``to_dict()``/``from_dict()`` with an
  unchanged ``request_id``.
- Key derivation is sound: identical dispatch kwargs produce the same
  ``request_id`` (framework redispatches dedupe), changing a real argument
  produces a different key, and ``tool_call_id``/other bookkeeping kwargs are
  excluded from the *args fingerprint* while still binding the key to the
  dispatch identity.

Both the file backend and Redis (via fakeredis) must uphold the properties.
"""

import tempfile
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from mycelium import (
    ActionLedger,
    FileLedgerStorage,
    LedgerEntry,
    LedgerError,
    ReconcileResult,
    ReconcileStatus,
    RedisLedgerStorage,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
)

fakeredis = pytest.importorskip("fakeredis")
FakeRedis = fakeredis.FakeRedis

# A NON_IDEMPOTENT_MUTATE payment charge: SINGLE_USE, no auto-reclaim, no
# provider idempotency key.  Every ambiguity must be resolved by reconcile or
# an operator release -- the strictest possible retry policy.
_BINDING = ToolTransitionBinding.for_tool(
    agent_id="prop",
    policy_version="1",
    side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
)
_TOOL = "charge"
_KWARGS = {"amount": 10.0, "tool_call_id": "c_prop"}
_RESULT = {"charged": True}

# Weighted op pool; duplicates make the more interesting ops common.
_OPS = [
    "claim_ok",
    "claim_ok",
    "claim_fail_before",
    "claim_fail_after",
    "claim_crash",
    "expire",
    "expire",
    "release_not_executed",
    "release_completed",
    "reconcile_not_executed",
    "reconcile_completed",
    "reconcile_unknown",
    "mark_unknown",
    "mark_blocked",
    "renew",
    "mark_worker_dead",
    "read",
]


class _ScriptedReconciler:
    """Provider reconciler whose verdict the model can flip between ops."""

    def __init__(self, model: "_TransitionModel") -> None:
        self._model = model
        self.verdict = ReconcileResult.unknown()

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        if self.verdict.status == ReconcileStatus.NOT_EXECUTED:
            self._model.not_executed_verdicts += 1
        return self.verdict


class _TransitionModel:
    """Drives one transition key through an op sequence and checks invariants."""

    def __init__(self, storage) -> None:
        self.reconciler = _ScriptedReconciler(self)
        self.ledger = ActionLedger(
            storage,
            lease_ttl=0.2,
            poll_interval=0.0005,
            poll_timeout=0.005,
            reconciler=self.reconciler,
        )
        self.storage = storage
        self.rid = self.ledger.derive_request_id(
            _TOOL, (), dict(_KWARGS), transition_binding=_BINDING
        )
        self.executions = 0
        self.not_executed_verdicts = 0
        self.completed_once = False
        self.completed_result = None
        self.completed_executions = -1

    # --- operations ---

    def _claim(self) -> LedgerEntry | None:
        try:
            entry = self.ledger.claim_side_effecting(
                self.rid,
                _TOOL,
                (),
                dict(_KWARGS),
                _BINDING,
                lease_ttl=0.2,
                poll_interval=0.0005,
                poll_timeout=0.005,
            )
        except LedgerError:
            return None
        if entry is None:
            return None
        if entry.is_terminal_completed():
            self._note_completed(entry)
            return None
        if entry.resolved_terminal_outcome() == TerminalOutcome.IN_FLIGHT:
            # A returned IN_FLIGHT entry is a fresh, owned claim: the tool
            # body runs exactly once for it.
            self.executions += 1
            return entry
        return None

    def _note_completed(self, entry: LedgerEntry) -> None:
        if self.completed_once:
            assert entry.result == self.completed_result, (
                f"result changed after completion: {self.completed_result!r} -> {entry.result!r}"
            )
        else:
            self.completed_once = True
            self.completed_result = entry.result
            self.completed_executions = self.executions

    def _op_claim_ok(self) -> None:
        claimed = self._claim()
        if claimed is not None:
            try:
                self._note_completed(self.ledger.complete(self.rid, _RESULT))
            except LedgerError:
                pass

    def _op_claim_fail_before(self) -> None:
        claimed = self._claim()
        if claimed is not None:
            try:
                self.ledger.fail(self.rid, RuntimeError("before effect"))
            except LedgerError:
                pass

    def _op_claim_fail_after(self) -> None:
        claimed = self._claim()
        if claimed is not None:
            try:
                self.ledger.attach_external_operation_ref(self.rid, "pi_prop")
                self.ledger.fail(self.rid, RuntimeError("after effect"), failed_after_effect=True)
            except LedgerError:
                pass

    def _op_claim_crash(self) -> None:
        claimed = self._claim()
        if claimed is not None:
            try:
                self.ledger.attach_external_operation_ref(self.rid, "pi_prop")
            except LedgerError:
                pass

    def _op_expire(self) -> None:
        entry = self.storage.get(self.rid)
        if entry is None or entry.terminal_outcome != TerminalOutcome.IN_FLIGHT.value:
            return
        crashed = replace(entry, lease_until=time.time() - 0.05, last_heartbeat_at=None)
        self.storage.set(crashed)

    def _op_release_not_executed(self) -> None:
        try:
            self.ledger.release(
                self.rid,
                verified="not_executed",
                by="prop@ops",
                reason="provider shows no effect",
            )
        except LedgerError:
            return
        self.not_executed_verdicts += 1

    def _op_release_completed(self) -> None:
        try:
            entry = self.ledger.release(
                self.rid,
                verified="completed",
                result=_RESULT,
                by="prop@ops",
                reason="provider confirms effect",
            )
        except LedgerError:
            return
        self._note_completed(entry)

    def _op_reconcile_not_executed(self) -> None:
        self.reconciler.verdict = ReconcileResult.not_executed()

    def _op_reconcile_completed(self) -> None:
        self.reconciler.verdict = ReconcileResult.completed(_RESULT)

    def _op_reconcile_unknown(self) -> None:
        self.reconciler.verdict = ReconcileResult.unknown()

    def _op_mark_unknown(self) -> None:
        try:
            self.ledger.mark_unknown(self.rid, error="prop")
        except LedgerError:
            pass

    def _op_mark_blocked(self) -> None:
        try:
            self.ledger.mark_blocked(self.rid, error="prop")
        except LedgerError:
            pass

    def _op_renew(self) -> None:
        try:
            self.ledger.renew_lease(self.rid)
        except LedgerError:
            pass

    def _op_mark_worker_dead(self) -> None:
        from mycelium.action_ledger import _ledger_owner

        try:
            self.ledger.mark_worker_dead(
                _ledger_owner(),
                by="prop@ops",
                reason="worker killed by orchestrator",
                override_heartbeat=True,
            )
        except LedgerError:
            pass

    def _op_read(self) -> None:
        self.ledger.get(self.rid)

    # --- invariants ---

    def check(self) -> None:
        # Key derivation: identical dispatch kwargs dedupe onto one key.
        same_rid = self.ledger.derive_request_id(
            _TOOL, (), dict(_KWARGS), transition_binding=_BINDING
        )
        assert same_rid == self.rid, "identical redispatches must map to one key"
        # Real arguments are part of the key.
        other_rid = self.ledger.derive_request_id(
            _TOOL, (), {**_KWARGS, "amount": 99.0}, transition_binding=_BINDING
        )
        assert other_rid != self.rid, "real arguments must be part of the transition key"
        # tool_call_id is the dispatch identity: it binds the key (a redispatch
        # must reuse it) but is excluded from the args fingerprint.
        rekeyed_rid = self.ledger.derive_request_id(
            _TOOL, (), {**_KWARGS, "tool_call_id": "c_other"}, transition_binding=_BINDING
        )
        assert rekeyed_rid != self.rid, "dispatch identity must bind the transition key"
        from mycelium.transition import args_fingerprint

        assert args_fingerprint((), dict(_KWARGS)) == args_fingerprint(
            (), {**_KWARGS, "tool_call_id": "c_other"}
        ), "bookkeeping kwargs must be excluded from the args fingerprint"

        # The single-key budget: at most one execution, plus exactly one extra
        # per provably-not-executed verdict.
        assert self.executions <= 1 + self.not_executed_verdicts, (
            f"double-execution: {self.executions} runs with "
            f"{self.not_executed_verdicts} not-executed verdicts"
        )

        entry = self.storage.get(self.rid)
        if entry is None:
            return
        # Durable record round-trips with an unchanged request_id.
        assert LedgerEntry.from_dict(entry.to_dict()).request_id == self.rid

        if self.completed_once:
            stored = entry.resolved_terminal_outcome()
            assert stored == TerminalOutcome.COMPLETED, (
                f"completed key regressed to {stored!r} ({entry.terminal_outcome!r})"
            )
            assert entry.result == self.completed_result
            assert self.executions == self.completed_executions, (
                f"execution after completion: {self.completed_executions} -> {self.executions}"
            )
            # A COMPLETED key cannot be re-marked.
            if entry.terminal_outcome == TerminalOutcome.COMPLETED.value:
                with pytest.raises(LedgerError):
                    self.ledger.complete(self.rid, _RESULT)


def _run_sequence(ops: list[str], make_storage) -> None:
    # _reconcile_cas_lost is module-global CAS-race signalling; reset it so one
    # model run can never contaminate the next (file -> redis within an
    # example, or across examples).
    from mycelium import action_ledger

    action_ledger._reconcile_cas_lost.val = False
    with tempfile.TemporaryDirectory() as tmp:
        model = _TransitionModel(make_storage(tmp))
        for op in ops:
            getattr(model, f"_op_{op}")()
            model.check()


def _make_file_storage(tmp: str):
    return FileLedgerStorage(Path(tmp) / "ledger.json")


def _make_redis_storage(tmp: str):
    return RedisLedgerStorage("redis://test")


def _fake_redis_from_url(url: str, **kwargs: object):
    return FakeRedis(decode_responses=True)


@settings(max_examples=80, deadline=None)
@given(ops=st.lists(st.sampled_from(_OPS), min_size=0, max_size=40))
def test_transition_key_invariants(ops: list[str]) -> None:
    _run_sequence(ops, _make_file_storage)
    with patch("redis.Redis.from_url", side_effect=_fake_redis_from_url):
        _run_sequence(ops, _make_redis_storage)
