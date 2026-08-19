"""Tests for the deterministic simulation invariant checks and scenario."""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from mycelium import VerificationStatus, run_verify
from mycelium.action_ledger import LedgerEntry
from mycelium.verify.invariants import (
    check_at_most_one_committed,
    check_provider_mapping,
    committed_effect_ids,
)


def _entry(
    request_id: str,
    *,
    terminal: str,
    ref: str | None = None,
    effect_id: str | None = None,
) -> LedgerEntry:
    return LedgerEntry(
        request_id=request_id,
        tool="charge",
        args=[],
        kwargs={},
        status="completed" if terminal == "COMPLETED" else "failed",
        terminal_outcome=terminal,
        external_operation_ref=ref,
        idempotency_key=effect_id or request_id,
    )


def test_clean_single_commit_no_violations() -> None:
    entries = [_entry("r1", terminal="COMPLETED", ref="op-1")]
    assert check_at_most_one_committed(entries) == []
    violations, warnings = check_provider_mapping(entries, ["op-1"])
    assert violations == []
    assert warnings == []


def test_duplicate_commit_is_violation() -> None:
    entries = [
        _entry("r1", terminal="COMPLETED", ref="provider-1", effect_id="op-1"),
        _entry("r2", terminal="COMPLETED", ref="provider-2", effect_id="op-1"),
    ]
    violations = check_at_most_one_committed(entries)
    assert len(violations) == 1
    assert violations[0].effect_id == "op-1"
    assert "committed 2 times" in violations[0].message
    mapped, _warnings = check_provider_mapping(
        entries, [("op-1", "provider-1"), ("op-1", "provider-2")]
    )
    assert len(mapped) == 1


def test_duplicate_provider_executions_are_violation() -> None:
    entries = [_entry("r1", terminal="COMPLETED", ref="provider-1")]
    violations, _warnings = check_provider_mapping(
        entries, [("r1", "provider-1"), ("r1", "provider-2")]
    )
    assert len(violations) == 1
    assert "executed by provider 2 times" in violations[0].message


def test_unknown_parked_is_warning_not_violation() -> None:
    entries = [_entry("r1", terminal="UNKNOWN", ref="op-1")]
    assert check_at_most_one_committed(entries) == []
    violations, warnings = check_provider_mapping(entries, ["op-1"])
    assert violations == []
    assert any("no COMPLETED" in warning for warning in warnings)


def test_distinct_effects_are_independent() -> None:
    entries = [
        _entry("r1", terminal="COMPLETED", ref="op-1"),
        _entry("r2", terminal="COMPLETED", ref="op-2"),
    ]
    assert check_at_most_one_committed(entries) == []
    violations, warnings = check_provider_mapping(entries, ["op-1", "op-2"])
    assert violations == []
    assert warnings == []


def test_unattributed_entries_are_ignored() -> None:
    entries = [
        _entry("r1", terminal="COMPLETED", ref=None),
        _entry("r2", terminal="COMPLETED", ref=None),
    ]
    assert committed_effect_ids(entries) == {"r1": ["r1"], "r2": ["r2"]}


def test_failed_after_effect_not_committed() -> None:
    entries = [_entry("r1", terminal="FAILED_AFTER_EFFECT", ref="op-1")]
    assert check_at_most_one_committed(entries) == []
    violations, warnings = check_provider_mapping(entries, ["op-1"])
    assert violations == []
    assert warnings


def _sqlite_dev(tmp_path: Path) -> Path:
    path = tmp_path / "mycelium.yaml"
    path.write_text(
        f"""
transition:
  agent_id: verify-agent
  policy_version: "1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "app-ledger.db"}
  tools: [charge]
tools:
  charge:
    callable: verify_probe_tools:charge
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
""",
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize("scenario", ["simulation"])
def test_simulation_scenario_passes_on_shared_backend(tmp_path: Path, scenario: str) -> None:
    mod = types.ModuleType("verify_probe_tools")
    mod.charge = lambda order_id: {"charged": True}  # type: ignore[attr-defined]
    sys.modules["verify_probe_tools"] = mod
    try:
        report = run_verify(
            _sqlite_dev(tmp_path),
            scenarios=[scenario],
            connectivity=False,
            timeout_seconds=25,
        )
        evidence = report.scenarios[0]
        assert evidence.status == VerificationStatus.PASS
        assert evidence.terminal_outcome == "COMMITTED"
        decisions = " ".join(evidence.ledger_decisions)
        assert "invariant held" in decisions
        assert "fence takeover: B sole COMMITTED" in decisions
    finally:
        del sys.modules["verify_probe_tools"]


def test_simulation_scenario_known() -> None:
    from mycelium.verify.registry import known_scenarios

    assert "simulation" in known_scenarios()
