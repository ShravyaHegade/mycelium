"""``mycelium verify`` — empirical production-guarantee scenarios."""

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
    "IsolationRefused",
    "IsolationSession",
    "VerificationEvidence",
    "VerificationNamespace",
    "VerificationReport",
    "VerificationStatus",
    "establish_isolation",
    "exit_code_for_verify",
    "known_scenarios",
    "register_isolation_adapter",
    "register_scenario",
    "render_human",
    "render_json",
    "resolve_scenario_names",
    "run_verify",
]
