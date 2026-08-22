"""Tests for the deep effect-protocol proof harness."""

from __future__ import annotations

import types
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from mycelium.verify.engine import run_verify
from mycelium.verify.proof.crash_sweep import (
    CRASH_SWEEP_SCRIPTS,
    run_crash_point_sweeps,
    run_effect_id_alias_crash_sweeps,
    run_expired_unknown_hard_block_sweeps,
    run_fence_takeover_crash_sweeps,
)
from mycelium.verify.proof.harness import standard_proof_binding
from mycelium.verify.proof.interleavings import (
    LEGAL_PREFIXES,
    enumerate_property_cases,
    run_enumerated_properties,
    run_property_case,
)
from mycelium.verify.types import VerificationStatus


def test_crash_sweep_scripts_non_empty() -> None:
    assert len(CRASH_SWEEP_SCRIPTS) >= 5


def test_crash_point_sweeps_pass() -> None:
    failures, decisions = run_crash_point_sweeps()
    assert not failures, failures
    assert decisions


def test_effect_id_alias_crash_sweeps_pass() -> None:
    failures, _ = run_effect_id_alias_crash_sweeps()
    assert not failures, failures


def test_fence_takeover_crash_sweeps_pass() -> None:
    failures, _ = run_fence_takeover_crash_sweeps()
    assert not failures, failures


def test_unknown_hard_block_crash_sweeps_pass() -> None:
    failures, _ = run_expired_unknown_hard_block_sweeps()
    assert not failures, failures


def test_enumerated_properties_pass() -> None:
    failures, _ = run_enumerated_properties()
    assert not failures, failures


@settings(max_examples=40, deadline=None)
@given(st.sampled_from(LEGAL_PREFIXES))
def test_hypothesis_legal_prefixes_hold_invariants(steps: tuple[str, ...]) -> None:
    index = LEGAL_PREFIXES.index(steps)
    case = enumerate_property_cases()[index * 2]
    failures = run_property_case(case, binding=standard_proof_binding())
    assert not failures, failures


def _sqlite_dev(tmp_path: Path) -> Path:
    path = tmp_path / "mycelium.yaml"
    path.write_text(
        f"""
transition:
  agent_id: verify-agent
  policy_version: "1"
action_ledger:
  storage: sqlite
  path: {tmp_path / "proof-ledger.db"}
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


def test_effect_protocol_proof_scenario_passes(tmp_path: Path) -> None:
    mod = types.ModuleType("verify_probe_tools")
    mod.charge = lambda order_id: {"charged": True}  # type: ignore[attr-defined]
    import sys

    sys.modules["verify_probe_tools"] = mod
    try:
        report = run_verify(
            _sqlite_dev(tmp_path),
            scenarios=["effect-protocol-proof"],
            connectivity=False,
            timeout_seconds=60,
        )
        evidence = report.scenarios[0]
        assert evidence.status == VerificationStatus.PASS, evidence.observed_behavior
    finally:
        sys.modules.pop("verify_probe_tools", None)
