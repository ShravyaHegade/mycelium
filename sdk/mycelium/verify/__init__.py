"""``mycelium verify`` — empirical production-guarantee scenarios."""

from mycelium.verify.cluster import (
    ClusterVerificationResult,
    DeploymentAttestation,
    cluster_exit_code,
    deployment_attestation_is_verified,
    run_cluster_verify,
    verify_deployment_attestation_signature,
)
from mycelium.verify.engine import exit_code_for_verify, run_verify
from mycelium.verify.isolation import (
    IsolationRefused,
    IsolationSession,
    VerificationNamespace,
    establish_isolation,
    register_isolation_adapter,
)
from mycelium.verify.registry import known_scenarios, register_scenario, resolve_scenario_names
from mycelium.verify.render import render_human, render_json
from mycelium.verify.types import VerificationEvidence, VerificationReport, VerificationStatus

__all__ = [
    "ClusterVerificationResult",
    "DeploymentAttestation",
    "IsolationRefused",
    "IsolationSession",
    "VerificationEvidence",
    "VerificationNamespace",
    "VerificationReport",
    "VerificationStatus",
    "cluster_exit_code",
    "deployment_attestation_is_verified",
    "establish_isolation",
    "exit_code_for_verify",
    "known_scenarios",
    "register_isolation_adapter",
    "register_scenario",
    "render_human",
    "render_json",
    "resolve_scenario_names",
    "run_cluster_verify",
    "run_verify",
    "verify_deployment_attestation_signature",
]
