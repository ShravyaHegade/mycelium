"""Storage outage: fail closed; never fall back to another backend."""

from __future__ import annotations

import tempfile
import time

from mycelium.action_ledger import LedgerStorageUnavailableError
from mycelium.outcome_emit import OutcomeEmitError
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.isolation import FaultInjectingStorage, IsolationGateStorage
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import SyntheticProvider, count_executions, make_tool


class _SelectiveBoomEmitter:
    """Fail closed on body events only, so claim telemetry cannot hide a tool error."""

    fail_closed = True

    def emit_event(self, **kwargs: object) -> None:
        if kwargs.get("event") in {"body_fail", "body_complete"}:
            raise ConnectionError("outcome storage down")


@verify_scenario("storage-outage")
def run_storage_outage(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    tmp = tempfile.NamedTemporaryFile(prefix="mycelium-verify-outage-", delete=False)
    tmp.close()
    inner = iso.open_raw_inner()
    expected_type = type(inner).__name__
    fault = FaultInjectingStorage(inner)
    storage = IsolationGateStorage(fault, iso.namespace)
    decisions: list[str] = []
    failures: list[str] = []

    def _run(request_suffix: str, **tool_kwargs):
        rid = iso.track(iso.namespace.request_id("outage", request_suffix))
        with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
            tool = make_tool(storage, tmp.name, **tool_kwargs)
            return rid, tool(1, request_id=rid)

    # 1. Backend unavailable before claim.
    before = count_executions(tmp.name)
    fault.fail_get = True
    fault.fail_claim = True
    try:
        _run("pre-claim")
        failures.append("pre-claim outage did not surface")
    except LedgerStorageUnavailableError:
        decisions.append("pre-claim:LedgerStorageUnavailableError")
    except Exception as exc:  # noqa: BLE001
        if "outage" in str(exc).lower() or "unavailable" in str(exc).lower():
            decisions.append(f"pre-claim:{type(exc).__name__}")
        else:
            failures.append(f"pre-claim hid outage as {type(exc).__name__}: {exc}")
    if count_executions(tmp.name) != before:
        failures.append("synthetic tool ran although the claim could not be recorded")
    if fault.inner_type != expected_type:
        failures.append(f"backend fallback detected ({expected_type} -> {fault.inner_type})")
    fault.fail_get = False
    fault.fail_claim = False

    # 2. Failure while claiming.
    before = count_executions(tmp.name)
    fault.fail_claim = True
    try:
        _run("during-claim")
        failures.append("claim outage did not surface")
    except (LedgerStorageUnavailableError, Exception) as exc:
        if isinstance(exc, LedgerStorageUnavailableError) or "outage" in str(exc).lower():
            decisions.append("during-claim:unavailable")
        else:
            failures.append(f"during-claim: {type(exc).__name__}: {exc}")
    if count_executions(tmp.name) != before:
        failures.append("body ran during claim outage")
    fault.fail_claim = False

    def _expect_outage(phase: str, invoke) -> None:
        try:
            invoke()
            failures.append(f"{phase} outage did not surface")
        except LedgerStorageUnavailableError:
            decisions.append(f"{phase}:unavailable")
        except Exception as exc:  # noqa: BLE001
            text = str(exc).lower()
            if "outage" in text or "unavailable" in text:
                decisions.append(f"{phase}:{type(exc).__name__}")
            else:
                failures.append(f"{phase} hid outage as {type(exc).__name__}: {exc}")

    # 3. Failure while recording body start (side_effect → maybe_crossed set).
    rid = iso.track(iso.namespace.request_id("outage", "body-start"))
    fault._set_count = 0
    fault.fail_set = True
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(storage, tmp.name, provider=SyntheticProvider())
        _expect_outage("body-start", lambda: tool(1, request_id=rid, op_id="body-start"))
    fault.fail_set = False

    # 4. Failure while writing the external-operation boundary (2nd set).
    rid = iso.track(iso.namespace.request_id("outage", "boundary"))
    fault._set_count = 0
    fault.fail_nth_set = 2
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(storage, tmp.name, provider=SyntheticProvider())
        _expect_outage("boundary-write", lambda: tool(1, request_id=rid, op_id="boundary"))
    fault.fail_nth_set = None
    fault._set_count = 0

    # 5. Failure while completing. Silent success is a false PASS.
    rid = iso.track(iso.namespace.request_id("outage", "complete"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(storage, tmp.name)
        fault.fail_transition = True
        try:
            _expect_outage("complete", lambda: tool(1, request_id=rid))
        finally:
            fault.fail_transition = False

    # 6. Recovery + redispatch must not duplicate if a successful claim exists.
    recover_id = iso.track(iso.namespace.request_id("outage", "recover"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(storage, tmp.name)
        tool(1, request_id=recover_id)
        before = count_executions(tmp.name)
        tool(1, request_id=recover_id)
        after = count_executions(tmp.name)
    if after != before:
        failures.append("recovery redispatch executed the body again")
    else:
        decisions.append("recovery:no duplicate")

    # 7. Outcome-emission failure must not hide a tool exception (fail-closed).
    emitter = _SelectiveBoomEmitter()
    boom_id = iso.track(iso.namespace.request_id("outage", "emit"))
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = make_tool(
            storage,
            tmp.name,
            provider=SyntheticProvider(),
            fail_after_effect=True,
            outcome_emitter=emitter,
        )
        try:
            tool(1, request_id=boom_id, op_id="emit-op")
            failures.append("fail-after-effect did not raise")
        except RuntimeError as exc:
            if "ledger incomplete" in str(exc):
                decisions.append("emit:original exception preserved")
            else:
                failures.append(f"unexpected RuntimeError: {exc}")
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, OutcomeEmitError) or "outcome" in str(exc).lower():
                failures.append("telemetry failure replaced the tool exception")
            else:
                decisions.append(f"emit:{type(exc).__name__}")

    executions = count_executions(tmp.name)
    ok = not failures
    limitations = ["fault-injecting wrapper; not a live network partition"]
    if iso.backend in {"file", "sqlite", "memory"}:
        limitations.append("single-node / process-local verification only")
    if iso.backend == "redis":
        limitations.append("Redis persistence remains operator-asserted")
    return VerificationEvidence(
        scenario="storage-outage",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=7,
        body_executions=executions,
        ledger_decisions=decisions,
        terminal_outcome="FAIL_CLOSED",
        duration=time.time() - started,
        expected_behavior=(
            "no body if claim cannot be recorded; body-start and "
            "boundary-write outages surface; completion outage surfaces; "
            "no backend fallback; recovery does not duplicate; telemetry "
            "cannot hide a tool exception"
        ),
        observed_behavior="; ".join(failures or decisions),
        artifacts=[tmp.name],
        limitations=limitations,
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary="failed closed; no fallback" if ok else "; ".join(failures)[:200],
        remediation="" if ok else "Keep fail-closed storage errors; do not swap backends.",
    )
