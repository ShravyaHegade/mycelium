"""Adversarial conformance kit for read-only provider reconcilers."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

from mycelium.audit_receipt import sign_payload
from mycelium.reconcile import Reconciler, ReconcileResult, ReconcileStatus

ADAPTER_REPORT_SCHEMA_VERSION = 1
PROVIDER_CONFORMANCE_SUITE_VERSION = "1"
ADAPTER_REPORT_SIGNATURE_ALGORITHM = "hmac-sha256"
REQUIRED_PROVIDER_CONFORMANCE_CASES = frozenset(
    {
        "exactly_one_match",
        "zero_matches_fail_closed",
        "provider_indexing_lag",
        "duplicate_matches_fail_closed",
        "ambiguous_provider_responses",
        "malformed_handles_rejected_locally",
        "no_false_not_executed",
        "read_only_no_forbidden_writes",
    }
)


@dataclass(frozen=True)
class ProviderObservation:
    """One scripted provider lookup response used by a conformance fixture."""

    matches: tuple[Any, ...] = ()
    error: Exception | None = None
    malformed_response: bool = False


class ProviderCallAudit:
    """Records synthetic provider reads and forbidden mutation attempts."""

    def __init__(self) -> None:
        self.reads: list[str] = []
        self.writes: list[str] = []

    def record_read(self, operation: str) -> None:
        self.reads.append(str(operation))

    def record_write(self, operation: str) -> None:
        self.writes.append(str(operation))


class ProviderConformanceFixture(Protocol):
    """Provider-specific bridge consumed by the generic conformance runner."""

    adapter_name: str
    adapter_version: str
    valid_handle: Any
    malformed_handles: tuple[Any, ...]

    def build_reconciler(
        self,
        observations: tuple[ProviderObservation, ...],
        audit: ProviderCallAudit,
    ) -> Reconciler: ...

    def make_entry(self, handle: Any) -> Any: ...

    def source_bytes(self) -> bytes: ...


@dataclass(frozen=True)
class AdapterConformanceCase:
    name: str
    passed: bool
    expected: str
    actual: str
    details: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "expected": self.expected,
            "actual": self.actual,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AdapterConformanceCase:
        return cls(
            name=str(data["name"]),
            passed=bool(data["passed"]),
            expected=str(data["expected"]),
            actual=str(data["actual"]),
            details=dict(data.get("details") or {}),
        )


@dataclass(frozen=True)
class AdapterVerificationReport:
    """Tamper-evident report for one adapter source and suite revision."""

    report_id: str
    schema_version: int
    suite_version: str
    adapter_name: str
    adapter_version: str
    adapter_source_sha256: str
    generated_at: float
    status: str
    cases: tuple[AdapterConformanceCase, ...]
    signature_algorithm: str
    signer_key_id: str
    signature: str
    limitations: tuple[str, ...]

    @property
    def verified(self) -> bool:
        names = [case.name for case in self.cases]
        return (
            self.status == "VERIFIED"
            and len(names) == len(set(names))
            and set(names) == REQUIRED_PROVIDER_CONFORMANCE_CASES
            and all(case.passed for case in self.cases)
        )

    def payload(self) -> dict[str, Any]:
        return {
            "report_id": self.report_id,
            "schema_version": self.schema_version,
            "suite_version": self.suite_version,
            "adapter_name": self.adapter_name,
            "adapter_version": self.adapter_version,
            "adapter_source_sha256": self.adapter_source_sha256,
            "generated_at": self.generated_at,
            "status": self.status,
            "cases": [case.to_dict() for case in self.cases],
            "signature_algorithm": self.signature_algorithm,
            "signer_key_id": self.signer_key_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "signature": self.signature}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AdapterVerificationReport:
        return cls(
            report_id=str(data["report_id"]),
            schema_version=int(data["schema_version"]),
            suite_version=str(data["suite_version"]),
            adapter_name=str(data["adapter_name"]),
            adapter_version=str(data["adapter_version"]),
            adapter_source_sha256=str(data["adapter_source_sha256"]),
            generated_at=float(data["generated_at"]),
            status=str(data["status"]),
            cases=tuple(
                AdapterConformanceCase.from_dict(case)
                for case in data.get("cases", [])
            ),
            signature_algorithm=str(data["signature_algorithm"]),
            signer_key_id=str(data["signer_key_id"]),
            signature=str(data["signature"]),
            limitations=tuple(str(item) for item in data.get("limitations", [])),
        )


@dataclass
class _Invocation:
    actual: str
    status: ReconcileStatus | None
    audit: ProviderCallAudit
    error_type: str | None = None


def _invoke(
    fixture: ProviderConformanceFixture,
    observations: tuple[ProviderObservation, ...],
    handle: Any,
    *,
    reconciler: Reconciler | None = None,
    audit: ProviderCallAudit | None = None,
) -> _Invocation:
    call_audit = audit or ProviderCallAudit()
    adapter = reconciler or fixture.build_reconciler(observations, call_audit)
    try:
        result = adapter.reconcile(fixture.make_entry(handle))
    except Exception as exc:
        return _Invocation(
            actual=f"ERROR:{type(exc).__name__}",
            status=None,
            audit=call_audit,
            error_type=type(exc).__name__,
        )
    if not isinstance(result, ReconcileResult):
        return _Invocation(
            actual=f"MALFORMED_RESULT:{type(result).__name__}",
            status=None,
            audit=call_audit,
        )
    return _Invocation(actual=result.status.value, status=result.status, audit=call_audit)


def _case(
    name: str,
    passed: bool,
    expected: str,
    actual: str,
    **details: Any,
) -> AdapterConformanceCase:
    return AdapterConformanceCase(
        name=name,
        passed=passed,
        expected=expected,
        actual=actual,
        details=details,
    )


def run_provider_conformance_cases(
    fixture: ProviderConformanceFixture,
) -> tuple[AdapterConformanceCase, ...]:
    """Run provider-independent fail-closed and read-only scenarios."""

    cases: list[AdapterConformanceCase] = []
    invocations: list[_Invocation] = []

    exact = _invoke(
        fixture,
        (ProviderObservation(matches=({"id": "provider-object-1"},)),),
        fixture.valid_handle,
    )
    invocations.append(exact)
    cases.append(
        _case(
            "exactly_one_match",
            exact.status == ReconcileStatus.COMPLETED,
            "COMPLETED",
            exact.actual,
            reads=len(exact.audit.reads),
        )
    )

    zero = _invoke(fixture, (ProviderObservation(),), fixture.valid_handle)
    invocations.append(zero)
    cases.append(
        _case(
            "zero_matches_fail_closed",
            zero.status == ReconcileStatus.UNKNOWN,
            "UNKNOWN",
            zero.actual,
            reason="a zero result may be provider indexing lag",
        )
    )

    lag_audit = ProviderCallAudit()
    lag_adapter = fixture.build_reconciler(
        (
            ProviderObservation(),
            ProviderObservation(matches=({"id": "provider-object-late"},)),
        ),
        lag_audit,
    )
    lag_first = _invoke(
        fixture,
        (),
        fixture.valid_handle,
        reconciler=lag_adapter,
        audit=lag_audit,
    )
    lag_second = _invoke(
        fixture,
        (),
        fixture.valid_handle,
        reconciler=lag_adapter,
        audit=lag_audit,
    )
    invocations.extend((lag_first, lag_second))
    lag_actual = f"{lag_first.actual}->{lag_second.actual}"
    cases.append(
        _case(
            "provider_indexing_lag",
            lag_first.status == ReconcileStatus.UNKNOWN
            and lag_second.status == ReconcileStatus.COMPLETED,
            "UNKNOWN->COMPLETED",
            lag_actual,
            reads=len(lag_audit.reads),
        )
    )

    duplicate = _invoke(
        fixture,
        (
            ProviderObservation(
                matches=(
                    {"id": "provider-object-a"},
                    {"id": "provider-object-b"},
                )
            ),
        ),
        fixture.valid_handle,
    )
    invocations.append(duplicate)
    cases.append(
        _case(
            "duplicate_matches_fail_closed",
            duplicate.status == ReconcileStatus.UNKNOWN,
            "UNKNOWN",
            duplicate.actual,
        )
    )

    ambiguity_results = [
        _invoke(
            fixture,
            (ProviderObservation(error=TimeoutError("synthetic provider timeout")),),
            fixture.valid_handle,
        ),
        _invoke(
            fixture,
            (ProviderObservation(malformed_response=True),),
            fixture.valid_handle,
        ),
    ]
    invocations.extend(ambiguity_results)
    ambiguity_safe = all(
        invocation.status in (None, ReconcileStatus.UNKNOWN)
        for invocation in ambiguity_results
    ) and all(
        invocation.status != ReconcileStatus.NOT_EXECUTED
        for invocation in ambiguity_results
    )
    cases.append(
        _case(
            "ambiguous_provider_responses",
            ambiguity_safe,
            "UNKNOWN_OR_ERROR",
            ",".join(item.actual for item in ambiguity_results),
        )
    )

    malformed_results = [
        _invoke(fixture, (), handle) for handle in fixture.malformed_handles
    ]
    invocations.extend(malformed_results)
    malformed_safe = bool(malformed_results) and all(
        item.status == ReconcileStatus.UNKNOWN
        and not item.audit.reads
        and not item.audit.writes
        for item in malformed_results
    )
    cases.append(
        _case(
            "malformed_handles_rejected_locally",
            malformed_safe,
            "UNKNOWN_WITHOUT_PROVIDER_CALL",
            ",".join(item.actual for item in malformed_results) or "NO_CASES",
            handles_tested=len(malformed_results),
        )
    )

    uncertain = [zero, lag_first, duplicate, *ambiguity_results, *malformed_results]
    false_not_executed = [
        item.actual
        for item in uncertain
        if item.status == ReconcileStatus.NOT_EXECUTED
    ]
    cases.append(
        _case(
            "no_false_not_executed",
            not false_not_executed,
            "NO_NOT_EXECUTED_FOR_UNCERTAIN_EVIDENCE",
            "SAFE" if not false_not_executed else ",".join(false_not_executed),
        )
    )

    writes = [write for invocation in invocations for write in invocation.audit.writes]
    cases.append(
        _case(
            "read_only_no_forbidden_writes",
            not writes,
            "NO_PROVIDER_WRITES",
            "NO_PROVIDER_WRITES" if not writes else ",".join(writes),
            reads=sum(len(item.audit.reads) for item in invocations),
            writes=writes,
        )
    )

    return tuple(cases)


def create_adapter_verification_report(
    fixture: ProviderConformanceFixture,
    *,
    signing_key: str,
    signer_key_id: str,
    generated_at: float | None = None,
) -> AdapterVerificationReport:
    """Run the suite and sign a report that binds results to adapter source."""

    if not signing_key:
        raise ValueError("adapter verification report requires a signing key")
    if not signer_key_id:
        raise ValueError("adapter verification report requires signer_key_id")
    cases = run_provider_conformance_cases(fixture)
    status = "VERIFIED" if all(case.passed for case in cases) else "FAILED"
    unsigned = AdapterVerificationReport(
        report_id=f"adapter-report-{uuid.uuid4().hex}",
        schema_version=ADAPTER_REPORT_SCHEMA_VERSION,
        suite_version=PROVIDER_CONFORMANCE_SUITE_VERSION,
        adapter_name=fixture.adapter_name,
        adapter_version=fixture.adapter_version,
        adapter_source_sha256=hashlib.sha256(fixture.source_bytes()).hexdigest(),
        generated_at=time.time() if generated_at is None else float(generated_at),
        status=status,
        cases=cases,
        signature_algorithm=ADAPTER_REPORT_SIGNATURE_ALGORITHM,
        signer_key_id=signer_key_id,
        signature="",
        limitations=(
            "Synthetic conformance does not verify live provider permissions or consistency.",
            "Use read-only provider credentials/scopes in production.",
        ),
    )
    return AdapterVerificationReport(
        **{**unsigned.__dict__, "signature": sign_payload(unsigned.payload(), signing_key)}
    )


def verify_adapter_report_signature(
    report: AdapterVerificationReport,
    signing_key: str,
) -> bool:
    """Verify report schema, algorithm, source/result integrity, and HMAC."""

    if report.schema_version != ADAPTER_REPORT_SCHEMA_VERSION:
        return False
    if report.signature_algorithm != ADAPTER_REPORT_SIGNATURE_ALGORITHM:
        return False
    expected = sign_payload(report.payload(), signing_key)
    return hmac.compare_digest(expected, report.signature)


def adapter_report_is_verified(
    report: AdapterVerificationReport,
    signing_key: str,
    *,
    fixture: ProviderConformanceFixture | None = None,
) -> bool:
    """Require an authentic passing report and, when supplied, matching source."""

    if not verify_adapter_report_signature(report, signing_key) or not report.verified:
        return False
    return fixture is None or adapter_report_matches_fixture(report, fixture)


def adapter_report_matches_fixture(
    report: AdapterVerificationReport,
    fixture: ProviderConformanceFixture,
) -> bool:
    """Check that a report describes the exact installed adapter source."""

    source_digest = hashlib.sha256(fixture.source_bytes()).hexdigest()
    return (
        report.adapter_name == fixture.adapter_name
        and report.adapter_version == fixture.adapter_version
        and report.adapter_source_sha256 == source_digest
        and report.suite_version == PROVIDER_CONFORMANCE_SUITE_VERSION
    )


def adapter_report_json(report: AdapterVerificationReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


__all__ = [
    "ADAPTER_REPORT_SCHEMA_VERSION",
    "ADAPTER_REPORT_SIGNATURE_ALGORITHM",
    "PROVIDER_CONFORMANCE_SUITE_VERSION",
    "REQUIRED_PROVIDER_CONFORMANCE_CASES",
    "AdapterConformanceCase",
    "AdapterVerificationReport",
    "ProviderCallAudit",
    "ProviderConformanceFixture",
    "ProviderObservation",
    "adapter_report_matches_fixture",
    "adapter_report_is_verified",
    "adapter_report_json",
    "create_adapter_verification_report",
    "run_provider_conformance_cases",
    "verify_adapter_report_signature",
]
