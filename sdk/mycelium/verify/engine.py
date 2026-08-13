"""Empirical verification engine — separate from CLI rendering."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time
from pathlib import Path
from typing import Any

from mycelium.config import PROFILE_DEVELOPMENT, ConfigError, load_config
from mycelium.doctor.engine import run_doctor
from mycelium.doctor.types import DoctorStatus
from mycelium.storage._helpers import redact_secrets
from mycelium.verify.isolation import IsolationRefused, IsolationSession, establish_isolation
from mycelium.verify.registry import (
    ScenarioContext,
    ensure_builtin_scenarios_registered,
    get_scenario,
    resolve_scenario_names,
)
from mycelium.verify.types import VerificationEvidence, VerificationReport, VerificationStatus
from mycelium.verify.workers import terminate_owned

ensure_builtin_scenarios_registered()


def _scenario_child(fn: Any, ctx: ScenarioContext, conn: Any) -> None:
    os.setsid()
    ctx.isolation._track_callback = lambda request_id: conn.send(("track", request_id))
    try:
        try:
            result = ("ok", fn(ctx), list(ctx.isolation.tracked_ids))
        except IsolationRefused as exc:
            result = ("refused", redact_secrets(str(exc)), list(ctx.isolation.tracked_ids))
        except Exception as exc:  # noqa: BLE001
            result = (
                "error",
                (type(exc).__name__, redact_secrets(str(exc))),
                list(ctx.isolation.tracked_ids),
            )
        conn.send(("result", result))
    finally:
        terminate_owned(ctx.owned_procs)
        conn.close()


def _stop_scenario(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        waited, _ = os.waitpid(pid, os.WNOHANG)
        if waited:
            return
        time.sleep(0.01)
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    os.waitpid(pid, 0)


def _run_scenario(fn: Any, ctx: ScenarioContext) -> tuple[str, Any, list[str]]:
    parent_conn, child_conn = mp.Pipe(duplex=False)
    pid = os.fork()
    if pid == 0:
        parent_conn.close()
        try:
            _scenario_child(fn, ctx, child_conn)
        finally:
            os._exit(0)
    child_conn.close()
    tracked: list[str] = []
    payload = None
    deadline = time.monotonic() + max(0.0, ctx.timeout_seconds)
    try:
        while time.monotonic() < deadline:
            if parent_conn.poll(min(0.05, max(0.0, deadline - time.monotonic()))):
                kind, value = parent_conn.recv()
                if kind == "track":
                    tracked.append(value)
                else:
                    payload = value
                    break
        if payload is None:
            _stop_scenario(pid)
            while parent_conn.poll():
                try:
                    kind, value = parent_conn.recv()
                except EOFError:
                    break
                if kind == "track":
                    tracked.append(value)
            return "timeout", None, tracked
        os.waitpid(pid, 0)
        result, value, child_tracked = payload
        return result, value, list(dict.fromkeys([*tracked, *child_tracked]))
    finally:
        parent_conn.close()


def _tally(
    scenarios: list[VerificationEvidence],
) -> tuple[int, int, int, int, int]:
    passed = sum(1 for item in scenarios if item.status == VerificationStatus.PASS)
    warned = sum(1 for item in scenarios if item.status == VerificationStatus.WARN)
    failed = sum(1 for item in scenarios if item.status == VerificationStatus.FAIL)
    skipped = sum(1 for item in scenarios if item.status == VerificationStatus.SKIP)
    errors = sum(1 for item in scenarios if item.status == VerificationStatus.ERROR)
    return passed, warned, failed, skipped, errors


def _overall(report: VerificationReport) -> VerificationStatus:
    if report.refused:
        return VerificationStatus.ERROR
    if report.framework_error:
        return VerificationStatus.ERROR
    if _doctor_blocking(report.doctor):
        return VerificationStatus.FAIL
    if report.error_count > 0 or report.failure_count > 0:
        return VerificationStatus.FAIL
    if report.warning_count > 0 or report.isolation_status == VerificationStatus.WARN:
        return VerificationStatus.WARN
    if report.scenarios and all(s.status == VerificationStatus.SKIP for s in report.scenarios):
        return VerificationStatus.SKIP
    if not report.scenarios:
        return VerificationStatus.FAIL
    return VerificationStatus.PASS


def _backend_label(session: IsolationSession | None, fallback: str) -> str:
    if session is None:
        return fallback
    names = {
        "postgres": "PostgreSQL",
        "redis": "Redis",
        "sqlite": "SQLite",
        "file": "file",
        "memory": "memory",
    }
    return names.get(session.backend, session.backend)


def _doctor_blocking(doctor: dict[str, Any] | None) -> bool:
    if doctor is None:
        return True
    if doctor.get("load_error"):
        return True
    if doctor.get("overall_status") == DoctorStatus.FAIL.value:
        return True
    if int(doctor.get("failure_count") or 0) > 0:
        return True
    return False


def exit_code_for_verify(
    report: VerificationReport,
    *,
    strict: bool = False,
) -> int:
    if report.refused:
        return 3
    if report.framework_error:
        return 2
    doctor = report.doctor or {}
    if doctor.get("load_error"):
        return 2
    if report.failure_count > 0 or report.error_count > 0:
        return 1
    if _doctor_blocking(report.doctor):
        return 1
    if strict and (report.warning_count > 0 or int(doctor.get("warning_count") or 0) > 0):
        return 1
    if report.scenarios and not all(
        item.status == VerificationStatus.PASS for item in report.scenarios
    ):
        # SKIP/WARN without --strict still means not all required passed.
        if any(item.status == VerificationStatus.FAIL for item in report.scenarios):
            return 1
        if any(item.status == VerificationStatus.ERROR for item in report.scenarios):
            return 1
        if strict:
            return 1
        if all(
            item.status in {VerificationStatus.PASS, VerificationStatus.SKIP}
            for item in report.scenarios
        ):
            # Selected scenarios that SKIP are not a hard failure unless strict.
            if any(item.status == VerificationStatus.SKIP for item in report.scenarios):
                return 1
        return 1
    return 0


def run_verify(
    config_path: str | Path,
    *,
    scenarios: list[str],
    timeout_seconds: float = 30.0,
    rounds: int = 5,
    workers: int = 2,
    keep_artifacts: bool = False,
    connectivity: bool = True,
    doctor_timeout: float = 2.0,
    verbose: bool = False,
) -> VerificationReport:
    """Run Doctor, isolate a namespace, then execute selected scenarios."""
    started = time.time()
    path = Path(config_path)
    try:
        names = resolve_scenario_names(list(scenarios))
    except ValueError as exc:
        return VerificationReport(
            overall_status=VerificationStatus.ERROR,
            config_path=str(path),
            profile=PROFILE_DEVELOPMENT,
            topology=None,
            backend="unknown",
            started_at=started,
            completed_at=time.time(),
            framework_error=str(exc),
        )

    doctor = run_doctor(
        path,
        connectivity=connectivity,
        timeout_seconds=doctor_timeout,
        verbose=verbose,
    )
    doctor_payload = doctor.to_dict()
    topology = None
    backend = "unknown"
    profile = doctor.profile

    if doctor.load_error is not None:
        report = VerificationReport(
            overall_status=VerificationStatus.ERROR,
            config_path=str(path),
            profile=profile,
            topology=topology,
            backend=backend,
            started_at=started,
            completed_at=time.time(),
            doctor=doctor_payload,
            framework_error=doctor.load_error,
            production_ready=False,
            empirically_verified=False,
        )
        report.overall_status = _overall(report)
        return report

    try:
        config = load_config(path)
    except ConfigError as exc:
        message = redact_secrets(str(exc))
        report = VerificationReport(
            overall_status=VerificationStatus.ERROR,
            config_path=str(path),
            profile=profile,
            topology=topology,
            backend=backend,
            started_at=started,
            completed_at=time.time(),
            doctor=doctor_payload,
            framework_error=message,
        )
        report.overall_status = _overall(report)
        return report

    profile = config.profile
    topology = (config.deployment or {}).get("topology")
    backend = str((config.action_ledger or {}).get("storage", "memory"))

    if _doctor_blocking(doctor_payload):
        report = VerificationReport(
            overall_status=VerificationStatus.FAIL,
            config_path=str(path),
            profile=profile,
            topology=topology,
            backend=backend,
            started_at=started,
            completed_at=time.time(),
            doctor=doctor_payload,
            isolation_status=VerificationStatus.SKIP,
            isolation_detail="Doctor reported a blocking failure; scenarios were not run",
            production_ready=False,
            empirically_verified=False,
        )
        report.overall_status = _overall(report)
        return report

    session: IsolationSession | None = None
    evidence: list[VerificationEvidence] = []
    refused = False
    isolation_status = VerificationStatus.ERROR
    isolation_detail = ""
    framework_error = None
    cleanup_error: str | None = None
    try:
        session = establish_isolation(config)
        isolation_status = VerificationStatus.PASS
        isolation_detail = f"namespace={session.namespace.prefix} topology={session.topology_label}"
        if session.backend in {"file", "sqlite"}:
            isolation_detail += " (single-node verification only)"
        if session.backend == "redis":
            isolation_detail += " (Redis persistence remains operator-asserted)"
        backend = session.backend
        ctx = ScenarioContext(
            isolation=session,
            timeout_seconds=timeout_seconds,
            rounds=rounds,
            workers=workers,
            keep_artifacts=keep_artifacts,
        )
        stop = False
        for name in names:
            if stop:
                evidence.append(
                    VerificationEvidence(
                        scenario=name,
                        backend=backend,
                        namespace=session.namespace.prefix,
                        status=VerificationStatus.SKIP,
                        summary="skipped because namespace integrity is uncertain",
                        expected_behavior="independent scenario",
                        observed_behavior="prior isolation failure",
                        remediation="Re-run after isolation is restored.",
                    )
                )
                continue
            fn = get_scenario(name)
            if fn is None:
                evidence.append(
                    VerificationEvidence(
                        scenario=name,
                        backend=backend,
                        namespace=session.namespace.prefix,
                        status=VerificationStatus.ERROR,
                        summary=f"scenario {name!r} is not registered",
                        remediation="Use a built-in scenario name.",
                    )
                )
                continue
            result, value, tracked_ids = _run_scenario(fn, ctx)
            for request_id in tracked_ids:
                session.track(request_id)
            if result == "refused":
                refused = True
                stop = True
                isolation_status = VerificationStatus.FAIL
                isolation_detail = value
                evidence.append(
                    VerificationEvidence(
                        scenario=name,
                        backend=backend,
                        namespace=session.namespace.prefix,
                        status=VerificationStatus.ERROR,
                        summary="isolation refused during scenario",
                        observed_behavior=value,
                        remediation="Fix namespace isolation before re-running.",
                    )
                )
                continue
            if result == "timeout":
                evidence.append(
                    VerificationEvidence(
                        scenario=name,
                        backend=backend,
                        namespace=session.namespace.prefix,
                        status=VerificationStatus.ERROR,
                        summary=f"scenario exceeded {timeout_seconds:g}s deadline",
                        observed_behavior=(
                            "scenario interrupted and verifier-owned subprocesses terminated "
                            "at wall-clock deadline"
                        ),
                        remediation="Increase --timeout or investigate the blocking operation.",
                    )
                )
                continue
            if result == "error":
                error_type, message = value
                evidence.append(
                    VerificationEvidence(
                        scenario=name,
                        backend=backend,
                        namespace=session.namespace.prefix,
                        status=VerificationStatus.ERROR,
                        summary=f"scenario raised {error_type}",
                        observed_behavior=message,
                        remediation="See scenario evidence; this is a verifier error.",
                    )
                )
                continue
            evidence.append(value)
    except IsolationRefused as exc:
        refused = True
        isolation_status = VerificationStatus.FAIL
        isolation_detail = redact_secrets(str(exc))
    except Exception as exc:  # pragma: no cover - unexpected framework error
        framework_error = redact_secrets(str(exc))
    finally:
        if session is not None:
            try:
                session.cleanup(keep_artifacts=keep_artifacts)
            except Exception as exc:
                cleanup_error = redact_secrets(str(exc))

    passed, warned, failed, skipped, errors = _tally(evidence)
    if cleanup_error:
        warned += 1
        if isolation_status == VerificationStatus.PASS:
            isolation_status = VerificationStatus.WARN
        isolation_detail = (
            f"{isolation_detail}; cleanup failed: {cleanup_error}"
            if isolation_detail
            else f"cleanup failed: {cleanup_error}"
        )
    empirically = bool(evidence) and all(
        item.status == VerificationStatus.PASS for item in evidence
    )
    production_ready = (
        bool(doctor.production_ready)
        and empirically
        and not refused
        and isolation_status == VerificationStatus.PASS
        and cleanup_error is None
    )
    report = VerificationReport(
        overall_status=VerificationStatus.PASS,
        config_path=str(path),
        profile=profile,
        topology=topology or (session.topology_label if session is not None else None),
        backend=_backend_label(session, backend),
        scenarios=evidence,
        pass_count=passed,
        warning_count=warned,
        failure_count=failed,
        skipped_count=skipped,
        error_count=errors,
        production_ready=production_ready,
        empirically_verified=empirically,
        started_at=started,
        completed_at=time.time(),
        isolation_status=isolation_status,
        isolation_detail=isolation_detail,
        doctor=doctor_payload,
        refused=refused,
        framework_error=framework_error,
    )
    report.overall_status = _overall(report)
    return report


__all__ = [
    "exit_code_for_verify",
    "run_verify",
]
