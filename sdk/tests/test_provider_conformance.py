"""Provider-adapter conformance kit, signed reports, and CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from mycelium import (
    AdapterVerificationReport,
    GmailConformanceFixture,
    ProviderCallAudit,
    ProviderObservation,
    ReconcileResult,
    adapter_report_is_verified,
    adapter_report_matches_fixture,
    create_adapter_verification_report,
    run_provider_conformance_cases,
    verify_adapter_report_signature,
)
from mycelium.__main__ import main


def test_gmail_passes_complete_conformance_suite() -> None:
    cases = run_provider_conformance_cases(GmailConformanceFixture())
    assert {case.name for case in cases} == {
        "exactly_one_match",
        "zero_matches_fail_closed",
        "provider_indexing_lag",
        "duplicate_matches_fail_closed",
        "ambiguous_provider_responses",
        "malformed_handles_rejected_locally",
        "no_false_not_executed",
        "read_only_no_forbidden_writes",
    }
    assert all(case.passed for case in cases)


def test_signed_report_binds_source_and_case_results() -> None:
    report = create_adapter_verification_report(
        GmailConformanceFixture(),
        signing_key="verification-secret",
        signer_key_id="ci-provider-key-1",
        generated_at=123.0,
    )
    assert report.verified
    assert report.status == "VERIFIED"
    assert len(report.adapter_source_sha256) == 64
    assert report.signer_key_id == "ci-provider-key-1"
    assert verify_adapter_report_signature(report, "verification-secret")
    assert adapter_report_matches_fixture(report, GmailConformanceFixture())
    assert adapter_report_is_verified(
        report,
        "verification-secret",
        fixture=GmailConformanceFixture(),
    )
    assert not verify_adapter_report_signature(report, "wrong-secret")

    class _ChangedSourceFixture(GmailConformanceFixture):
        def source_bytes(self) -> bytes:
            return b"changed adapter source"

    assert not adapter_report_is_verified(
        report,
        "verification-secret",
        fixture=_ChangedSourceFixture(),
    )

    tampered = report.to_dict()
    tampered["cases"][0]["actual"] = "NOT_EXECUTED"
    restored = AdapterVerificationReport.from_dict(tampered)
    assert not verify_adapter_report_signature(restored, "verification-secret")
    assert not adapter_report_is_verified(restored, "verification-secret")


class _AlwaysNotExecuted:
    def reconcile(self, entry: Any) -> ReconcileResult:
        return ReconcileResult.not_executed()


class _UnsafeNotExecutedFixture(GmailConformanceFixture):
    adapter_name = "unsafe-not-executed"

    def build_reconciler(
        self,
        observations: tuple[ProviderObservation, ...],
        audit: ProviderCallAudit,
    ) -> _AlwaysNotExecuted:
        return _AlwaysNotExecuted()


def test_false_not_executed_prevents_verified_status() -> None:
    report = create_adapter_verification_report(
        _UnsafeNotExecutedFixture(),
        signing_key="secret",
        signer_key_id="test",
    )
    assert report.status == "FAILED"
    assert not report.verified
    assert verify_adapter_report_signature(report, "secret")
    assert not adapter_report_is_verified(report, "secret")
    by_name = {case.name: case for case in report.cases}
    assert not by_name["no_false_not_executed"].passed


class _WriteAttemptingReconciler:
    def __init__(self, audit: ProviderCallAudit) -> None:
        self.audit = audit

    def reconcile(self, entry: Any) -> ReconcileResult:
        self.audit.record_write("provider.objects.create")
        return ReconcileResult.unknown()


class _WriteAttemptFixture(GmailConformanceFixture):
    adapter_name = "write-attempt"

    def build_reconciler(
        self,
        observations: tuple[ProviderObservation, ...],
        audit: ProviderCallAudit,
    ) -> _WriteAttemptingReconciler:
        return _WriteAttemptingReconciler(audit)


def test_forbidden_provider_write_prevents_verified_status() -> None:
    cases = run_provider_conformance_cases(_WriteAttemptFixture())
    read_only = next(
        case for case in cases if case.name == "read_only_no_forbidden_writes"
    )
    assert not read_only.passed
    assert "provider.objects.create" in read_only.details["writes"]


def test_provider_verify_cli_writes_and_verifies_report(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("TEST_ADAPTER_SIGNING_KEY", "cli-secret")
    output = tmp_path / "gmail-adapter-report.json"
    assert (
        main(
            [
                "providers",
                "verify",
                "gmail",
                "--signing-key-env",
                "TEST_ADAPTER_SIGNING_KEY",
                "--key-id",
                "test-key",
                "--output",
                str(output),
            ]
        )
        == 0
    )
    assert "VERIFIED" in capsys.readouterr().out
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "VERIFIED"
    assert payload["signature"]

    assert (
        main(
            [
                "providers",
                "verify-report",
                str(output),
                "--signing-key-env",
                "TEST_ADAPTER_SIGNING_KEY",
                "--json",
            ]
        )
        == 0
    )
    verified = json.loads(capsys.readouterr().out)
    assert verified == {
        "adapter": "gmail",
        "authentic": True,
        "report_status": "VERIFIED",
        "source_matches": True,
        "verified": True,
    }


def test_provider_verify_cli_requires_signing_key(capsys) -> None:
    assert (
        main(
            [
                "providers",
                "verify",
                "gmail",
                "--signing-key-env",
                "MISSING_ADAPTER_SIGNING_KEY",
            ]
        )
        == 2
    )
    assert "is not set" in capsys.readouterr().err
