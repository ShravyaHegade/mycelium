"""Structured types for ``mycelium verify`` empirical scenarios."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from mycelium.action_ledger import LedgerError


class VerificationStatus(str, Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    SKIP = "SKIP"
    ERROR = "ERROR"


class IsolationRefused(LedgerError):
    """Safe isolation could not be proven; verification must not proceed."""

    def __init__(self, message: str, *, artifacts: list[str] | None = None) -> None:
        super().__init__(message)
        self.artifacts = list(artifacts or [])


@dataclass
class VerificationEvidence:
    scenario: str
    backend: str
    namespace: str
    attempts: int = 0
    body_executions: int = 0
    ledger_decisions: list[str] = field(default_factory=list)
    terminal_outcome: str | None = None
    duration: float = 0.0
    expected_behavior: str = ""
    observed_behavior: str = ""
    artifacts: list[str] = field(default_factory=list)
    limitations: list[str] = field(default_factory=list)
    status: VerificationStatus = VerificationStatus.ERROR
    summary: str = ""
    remediation: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload


@dataclass
class VerificationReport:
    overall_status: VerificationStatus
    config_path: str
    profile: str
    topology: str | None
    backend: str
    scenarios: list[VerificationEvidence] = field(default_factory=list)
    pass_count: int = 0
    warning_count: int = 0
    failure_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    production_ready: bool = False
    empirically_verified: bool = False
    started_at: float = 0.0
    completed_at: float = 0.0
    isolation_status: VerificationStatus = VerificationStatus.ERROR
    isolation_detail: str = ""
    doctor: dict[str, Any] | None = None
    refused: bool = False
    framework_error: str | None = None
    artifacts: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_status": self.overall_status.value,
            "config_path": self.config_path,
            "profile": self.profile,
            "topology": self.topology,
            "backend": self.backend,
            "scenarios": [item.to_dict() for item in self.scenarios],
            "pass_count": self.pass_count,
            "warning_count": self.warning_count,
            "failure_count": self.failure_count,
            "skipped_count": self.skipped_count,
            "error_count": self.error_count,
            "production_ready": self.production_ready,
            "empirically_verified": self.empirically_verified,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "isolation_status": self.isolation_status.value,
            "isolation_detail": self.isolation_detail,
            "doctor": self.doctor,
            "refused": self.refused,
            "framework_error": self.framework_error,
            "artifacts": self.artifacts,
        }


__all__ = [
    "IsolationRefused",
    "VerificationEvidence",
    "VerificationReport",
    "VerificationStatus",
]
