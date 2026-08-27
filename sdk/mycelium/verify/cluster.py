"""Optional multi-worker deployment verification and signed attestation."""

from __future__ import annotations

import hashlib
import hmac
import json
import multiprocessing as mp
import os
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from queue import Empty
from typing import Any

from mycelium.action_ledger import (
    _active_transition_var,
    _ActiveTransition,
    mark_maybe_crossed,
    record_external_operation,
)
from mycelium.audit_receipt import sign_payload
from mycelium.config import ConfigError, load_config
from mycelium.doctor.engine import run_doctor
from mycelium.storage._helpers import redact_secrets
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.cluster_provider import (
    HttpSandboxProvider,
    SandboxReconciler,
    load_sandbox_provider_config,
)
from mycelium.verify.cluster_proxy import BackendFaultProxy, proxy_worker_payload
from mycelium.verify.isolation import IsolationRefused, IsolationSession, establish_isolation
from mycelium.verify.types import VerificationStatus
from mycelium.verify.workers import (
    SYNTHETIC_TOOL,
    make_ledger,
    make_tool,
    storage_from_payload,
    synthetic_binding,
)

CLUSTER_ATTESTATION_SCHEMA_VERSION = 1
CLUSTER_ATTESTATION_SIGNATURE_ALGORITHM = "HMAC-SHA256"
REQUIRED_CLUSTER_CHECKS = frozenset(
    {
        "two_workers_started",
        "backend_interruption_fails_closed",
        "worker_hard_kill_after_provider_effect",
        "surviving_worker_reconciles_without_duplicate",
        "isolated_backend_cleanup",
    }
)
_MP_CTX = mp.get_context("spawn")


@dataclass(frozen=True)
class ClusterCheck:
    name: str
    status: str
    detail: str


@dataclass
class DeploymentAttestation:
    attestation_id: str
    schema_version: int
    generated_at: float
    status: str
    config_sha256: str
    backend: str
    topology: str
    namespace: str
    provider_adapter: str
    worker_count: int
    checks: list[ClusterCheck]
    signer_key_id: str
    signature_algorithm: str = CLUSTER_ATTESTATION_SIGNATURE_ALGORITHM
    signature: str = ""

    @property
    def verified(self) -> bool:
        names = [check.name for check in self.checks]
        return (
            self.status == "VERIFIED"
            and self.backend in {"redis", "postgres"}
            and self.topology == "multi_node"
            and self.worker_count == 2
            and len(names) == len(set(names))
            and set(names) == REQUIRED_CLUSTER_CHECKS
            and all(check.status == "PASS" for check in self.checks)
        )

    def payload(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("signature", None)
        return data

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "signature": self.signature}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeploymentAttestation:
        return cls(
            attestation_id=str(data["attestation_id"]),
            schema_version=int(data["schema_version"]),
            generated_at=float(data["generated_at"]),
            status=str(data["status"]),
            config_sha256=str(data["config_sha256"]),
            backend=str(data["backend"]),
            topology=str(data["topology"]),
            namespace=str(data["namespace"]),
            provider_adapter=str(data["provider_adapter"]),
            worker_count=int(data["worker_count"]),
            checks=[ClusterCheck(**item) for item in data.get("checks", [])],
            signer_key_id=str(data["signer_key_id"]),
            signature_algorithm=str(data["signature_algorithm"]),
            signature=str(data["signature"]),
        )


@dataclass
class ClusterVerificationResult:
    status: str
    config_path: str
    started_at: float
    completed_at: float
    attestation: DeploymentAttestation | None = None
    error: str | None = None
    refused: bool = False
    cleanup_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": "cluster",
            "status": self.status,
            "config_path": self.config_path,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "attestation": self.attestation.to_dict() if self.attestation else None,
            "error": self.error,
            "refused": self.refused,
            "cleanup_error": self.cleanup_error,
        }


def _emit(queue: Any, kind: str, detail: str) -> None:
    queue.put((kind, detail))


def _cluster_crash_worker(payload: dict[str, Any], start: Any, queue: Any) -> None:
    try:
        storage = storage_from_payload(payload)
        provider = HttpSandboxProvider(payload["provider"])
        binding = synthetic_binding()
        ledger = make_ledger(storage, binding=binding, lease_ttl=float(payload["lease_ttl"]))
        _emit(queue, "worker_a_ready", "ready")
        if not start.wait(float(payload["timeout"])):
            raise TimeoutError("worker A start barrier timed out")
        request_id = str(payload["request_id"])
        with execution_scope(TransitionScope(thread_id="verify-cluster", run_id="verify-cluster")):
            claimed = ledger.claim_side_effecting(
                request_id,
                SYNTHETIC_TOOL,
                (1,),
                {
                    "request_id": request_id,
                    "thread_id": "verify-cluster",
                    "run_id": "verify-cluster",
                },
                binding,
            )
            token = _active_transition_var.set(
                _ActiveTransition(ledger, request_id, binding, {}, claimed.owner, claimed.fence)
            )
            try:
                ledger.record_decision(
                    request_id,
                    {"allowed": True, "verdicts": [], "denied_reasons": []},
                    expected_owner=claimed.owner,
                    expected_fence=claimed.fence,
                )
                mark_maybe_crossed()
                record_external_operation(str(payload["operation_id"]))
                provider.execute(str(payload["operation_id"]))
                _emit(
                    queue, "provider_effect", "sandbox operation completed; worker A awaiting kill"
                )
                time.sleep(float(payload["timeout"]) * 4)
            finally:
                _active_transition_var.reset(token)
    except Exception as exc:  # noqa: BLE001
        _emit(queue, "worker_a_error", f"{type(exc).__name__}: {redact_secrets(str(exc))}")


def _cluster_recovery_worker(
    payload: dict[str, Any], outage_start: Any, recovery_start: Any, queue: Any
) -> None:
    try:
        _emit(queue, "worker_b_ready", "ready")
        if not outage_start.wait(float(payload["timeout"])):
            raise TimeoutError("worker B outage barrier timed out")
        outage_storage = storage_from_payload(payload)
        outage_provider = HttpSandboxProvider(payload["provider"])
        outage_tool = make_tool(
            outage_storage,
            str(payload["counter_file"]),
            provider=outage_provider,
            lease_ttl=float(payload["lease_ttl"]),
            poll_timeout=1.0,
        )
        try:
            with execution_scope(
                TransitionScope(thread_id="verify-cluster", run_id="verify-cluster")
            ):
                outage_tool(
                    1,
                    op_id=str(payload["outage_operation_id"]),
                    request_id=str(payload["outage_request_id"]),
                )
        except Exception as exc:  # expected: unavailable storage must fail before provider call
            _emit(queue, "outage_refused", type(exc).__name__)
        else:
            _emit(queue, "outage_unsafe", "operation unexpectedly succeeded during interruption")

        if not recovery_start.wait(float(payload["timeout"])):
            raise TimeoutError("worker B recovery barrier timed out")
        recovery_storage = storage_from_payload(payload)
        recovery_tool = make_tool(
            recovery_storage,
            str(payload["counter_file"]),
            provider=HttpSandboxProvider(payload["provider"]),
            reconciler=SandboxReconciler(payload["provider"]),
            lease_ttl=float(payload["lease_ttl"]),
            poll_timeout=3.0,
        )
        with execution_scope(TransitionScope(thread_id="verify-cluster", run_id="verify-cluster")):
            result = recovery_tool(
                1,
                request_id=str(payload["request_id"]),
            )
        _emit(queue, "reconciled", json.dumps(result, sort_keys=True, default=str))
    except Exception as exc:  # noqa: BLE001
        _emit(queue, "worker_b_error", f"{type(exc).__name__}: {redact_secrets(str(exc))}")


def _wait_for(queue: Any, expected: set[str], timeout: float) -> tuple[str, str]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            kind, detail = queue.get(timeout=min(0.1, max(0.01, deadline - time.monotonic())))
        except Empty:
            continue
        if kind in expected or kind.endswith("_error") or kind == "outage_unsafe":
            return str(kind), str(detail)
    return "timeout", f"timed out waiting for {sorted(expected)}"


def _worker_payload(session: IsolationSession, proxied: dict[str, object]) -> dict[str, Any]:
    payload = dict(proxied)
    payload.update(
        {
            "run_id": session.namespace.run_id,
            "prefix_ns": session.namespace.prefix,
        }
    )
    return payload


def _run_cluster_checks(
    session: IsolationSession,
    proxy: BackendFaultProxy,
    payload: dict[str, Any],
    *,
    timeout: float,
) -> list[ClusterCheck]:
    queue = _MP_CTX.Queue()
    a_start = _MP_CTX.Event()
    outage_start = _MP_CTX.Event()
    recovery_start = _MP_CTX.Event()
    worker_a = _MP_CTX.Process(target=_cluster_crash_worker, args=(payload, a_start, queue))
    worker_b = _MP_CTX.Process(
        target=_cluster_recovery_worker,
        args=(payload, outage_start, recovery_start, queue),
    )
    checks: list[ClusterCheck] = []
    worker_a.start()
    worker_b.start()
    try:
        ready = {
            _wait_for(queue, {"worker_a_ready", "worker_b_ready"}, timeout)[0] for _ in range(2)
        }
        checks.append(
            ClusterCheck(
                "two_workers_started",
                "PASS" if ready == {"worker_a_ready", "worker_b_ready"} else "FAIL",
                f"ready={sorted(ready)}",
            )
        )

        proxy.interrupt()
        outage_start.set()
        kind, detail = _wait_for(queue, {"outage_refused"}, timeout)
        counter = Path(payload["counter_file"])
        body_count = (
            len(counter.read_text(encoding="utf-8").splitlines()) if counter.exists() else 0
        )
        outage_ok = kind == "outage_refused" and body_count == 0
        checks.append(
            ClusterCheck(
                "backend_interruption_fails_closed",
                "PASS" if outage_ok else "FAIL",
                f"worker={kind}:{detail}; provider_body_executions={body_count}",
            )
        )
        proxy.restore()

        a_start.set()
        kind, detail = _wait_for(queue, {"provider_effect"}, timeout)
        if kind == "provider_effect" and worker_a.is_alive():
            worker_a.kill()
            worker_a.join(timeout=2)
            killed = worker_a.exitcode is not None and worker_a.exitcode != 0
        else:
            killed = False
        checks.append(
            ClusterCheck(
                "worker_hard_kill_after_provider_effect",
                "PASS" if killed else "FAIL",
                f"event={kind}:{detail}; exit={worker_a.exitcode}",
            )
        )

        time.sleep(float(payload["lease_ttl"]) + 0.15)
        recovery_start.set()
        kind, detail = _wait_for(queue, {"reconciled"}, timeout)
        worker_b.join(timeout=2)
        counter_count = (
            len(Path(payload["counter_file"]).read_text(encoding="utf-8").splitlines())
            if Path(payload["counter_file"]).exists()
            else 0
        )
        recovered = kind == "reconciled" and counter_count == 0 and worker_b.exitcode == 0
        checks.append(
            ClusterCheck(
                "surviving_worker_reconciles_without_duplicate",
                "PASS" if recovered else "FAIL",
                (
                    f"event={kind}:{detail}; retry_body_executions={counter_count}; "
                    f"exit={worker_b.exitcode}"
                ),
            )
        )
    finally:
        for proc in (worker_a, worker_b):
            if proc.is_alive():
                proc.kill()
            proc.join(timeout=2)
            proc.close()
        queue.close()
    return checks


def verify_deployment_attestation_signature(
    attestation: DeploymentAttestation, signing_key: str
) -> bool:
    if attestation.schema_version != CLUSTER_ATTESTATION_SCHEMA_VERSION:
        return False
    if attestation.signature_algorithm != CLUSTER_ATTESTATION_SIGNATURE_ALGORITHM:
        return False
    return hmac.compare_digest(
        sign_payload(attestation.payload(), signing_key), attestation.signature
    )


def deployment_attestation_is_verified(
    attestation: DeploymentAttestation, signing_key: str
) -> bool:
    """Require both an authentic signature and the complete passing check set."""
    return attestation.verified and verify_deployment_attestation_signature(
        attestation, signing_key
    )


def run_cluster_verify(
    config_path: str | Path,
    *,
    timeout_seconds: float = 20.0,
    connectivity: bool = True,
    keep_artifacts: bool = False,
) -> ClusterVerificationResult:
    """Run the explicit cluster test. No cluster work occurs without config opt-in."""
    started = time.time()
    path = Path(config_path)
    session: IsolationSession | None = None
    proxy: BackendFaultProxy | None = None
    result: ClusterVerificationResult | None = None
    try:
        config = load_config(path)
        cluster_raw = dict((config.verify or {}).get("cluster") or {})
        if not bool(cluster_raw.get("enabled")):
            raise IsolationRefused(
                "cluster verification is disabled; set verify.cluster.enabled: true"
            )
        topology = str((config.deployment or {}).get("topology") or "")
        if topology != "multi_node":
            raise IsolationRefused("cluster verification requires deployment.topology: multi_node")
        backend = str((config.action_ledger or {}).get("storage", "memory"))
        if backend not in {"redis", "postgres"}:
            raise IsolationRefused(
                "cluster verification requires a shared Redis or PostgreSQL action ledger"
            )
        signing_raw = dict(cluster_raw.get("attestation") or {})
        key_env = str(signing_raw.get("signing_key_env") or "")
        signing_key = os.environ.get(key_env, "") if key_env else ""
        key_id = str(signing_raw.get("key_id") or "")
        if not signing_key or not key_id:
            raise IsolationRefused(
                "cluster attestation requires verify.cluster.attestation.signing_key_env "
                "and key_id, with a non-empty environment variable"
            )
        provider = load_sandbox_provider_config(dict(cluster_raw.get("provider") or {}))
        doctor = run_doctor(
            path, connectivity=connectivity, timeout_seconds=min(3.0, timeout_seconds)
        )
        if doctor.load_error or doctor.failure_count:
            raise IsolationRefused(
                "Doctor has blocking failures; cluster verification was not started"
            )

        session = establish_isolation(config, keep_artifacts=keep_artifacts)
        proxy, proxied = proxy_worker_payload(session.worker_payload)
        payload = _worker_payload(session, proxied)
        lease_ttl = max(0.25, min(1.0, timeout_seconds / 10))
        operation_id = f"mycelium-verify-{session.namespace.run_id}"
        request_id = session.track(session.namespace.request_id("cluster", "crash-recovery"))
        outage_request = session.track(session.namespace.request_id("cluster", "backend-outage"))
        payload.update(
            {
                "provider": provider.worker_payload(),
                "lease_ttl": lease_ttl,
                "timeout": timeout_seconds,
                "operation_id": operation_id,
                "request_id": request_id,
                "outage_operation_id": f"{operation_id}-outage",
                "outage_request_id": outage_request,
                "counter_file": session.artifact_file("cluster-provider-body-"),
            }
        )
        namespace = session.namespace.prefix
        checks = _run_cluster_checks(session, proxy, payload, timeout=timeout_seconds)
        cleanup_detail: str | None = (
            "namespaced backend records retained by explicit --keep-artifacts"
            if keep_artifacts
            else None
        )
        try:
            session.cleanup(keep_artifacts=keep_artifacts)
        except Exception as exc:  # cleanup is part of the signed verdict
            cleanup_detail = redact_secrets(str(exc))
        checks.append(
            ClusterCheck(
                "isolated_backend_cleanup",
                "PASS" if cleanup_detail is None else "FAIL",
                "namespaced verification records cleaned"
                if cleanup_detail is None
                else cleanup_detail,
            )
        )
        session = None
        status = (
            "VERIFIED" if checks and all(check.status == "PASS" for check in checks) else "FAILED"
        )
        attestation = DeploymentAttestation(
            attestation_id=f"deployment-attestation-{uuid.uuid4().hex}",
            schema_version=CLUSTER_ATTESTATION_SCHEMA_VERSION,
            generated_at=time.time(),
            status=status,
            config_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            backend=backend,
            topology=topology,
            namespace=namespace,
            provider_adapter=provider.adapter_name,
            worker_count=2,
            checks=checks,
            signer_key_id=key_id,
        )
        attestation.signature = sign_payload(attestation.payload(), signing_key)
        result = ClusterVerificationResult(
            status=VerificationStatus.PASS.value
            if status == "VERIFIED"
            else VerificationStatus.FAIL.value,
            config_path=str(path),
            started_at=started,
            completed_at=time.time(),
            attestation=attestation,
            cleanup_error=cleanup_detail,
        )
        return result
    except (ConfigError, IsolationRefused) as exc:
        result = ClusterVerificationResult(
            status=VerificationStatus.ERROR.value,
            config_path=str(path),
            started_at=started,
            completed_at=time.time(),
            error=redact_secrets(str(exc)),
            refused=isinstance(exc, IsolationRefused),
        )
        return result
    except Exception as exc:  # noqa: BLE001
        result = ClusterVerificationResult(
            status=VerificationStatus.ERROR.value,
            config_path=str(path),
            started_at=started,
            completed_at=time.time(),
            error=f"{type(exc).__name__}: {redact_secrets(str(exc))}",
        )
        return result
    finally:
        if proxy is not None:
            proxy.close()
        if session is not None:
            try:
                session.cleanup(keep_artifacts=keep_artifacts)
            except Exception as exc:  # cleanup is surfaced by CLI stderr on failure
                if result is not None:
                    result.cleanup_error = redact_secrets(str(exc))


def cluster_exit_code(result: ClusterVerificationResult) -> int:
    if result.refused:
        return 3
    if result.status == VerificationStatus.PASS.value and result.cleanup_error is None:
        return 0
    if result.status == VerificationStatus.FAIL.value:
        return 1
    return 2


__all__ = [
    "CLUSTER_ATTESTATION_SCHEMA_VERSION",
    "ClusterCheck",
    "ClusterVerificationResult",
    "DeploymentAttestation",
    "REQUIRED_CLUSTER_CHECKS",
    "cluster_exit_code",
    "deployment_attestation_is_verified",
    "run_cluster_verify",
    "verify_deployment_attestation_signature",
]
