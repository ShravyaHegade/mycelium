"""``mycelium doctor`` — read-only production-safety verification."""

from mycelium.doctor.engine import exit_code_for_report, run_doctor, run_doctor_on_config
from mycelium.doctor.registry import DoctorContext, doctor_check, register_check
from mycelium.doctor.render import render_human, render_json
from mycelium.doctor.types import (
    EVIDENCE_CONNECTIVITY,
    EVIDENCE_NOT_VERIFIABLE,
    EVIDENCE_OPERATOR,
    EVIDENCE_RUNTIME,
    EVIDENCE_STATIC,
    DoctorCheck,
    DoctorReport,
    DoctorStatus,
)

__all__ = [
    "EVIDENCE_CONNECTIVITY",
    "EVIDENCE_NOT_VERIFIABLE",
    "EVIDENCE_OPERATOR",
    "EVIDENCE_RUNTIME",
    "EVIDENCE_STATIC",
    "DoctorCheck",
    "DoctorContext",
    "DoctorReport",
    "DoctorStatus",
    "doctor_check",
    "exit_code_for_report",
    "register_check",
    "render_human",
    "render_json",
    "run_doctor",
    "run_doctor_on_config",
]
