"""Tests for expanded mycelium demo feature proofs."""

from __future__ import annotations

from mycelium.proofs.feature_demo import (
    prove_lease_auto_renew,
    prove_operator_release,
    prove_read_unknown_safe_retry,
    prove_reconcile_completed,
    prove_repair_gate,
)
from mycelium.quickstart import run_demo


def test_prove_lease_auto_renew() -> None:
    result = prove_lease_auto_renew()
    assert result["lease_validity"] == "HELD"
    assert result["peer_gate"] == "POLL"


def test_prove_repair_gate() -> None:
    result = prove_repair_gate()
    assert result["executions"] == 1
    assert result["repaired_idempotency_key"]


def test_prove_reconcile_completed() -> None:
    result = prove_reconcile_completed()
    assert result["executions"] == 1
    assert result["reconcile_calls"] == 1
    assert result["result"] == {"charged": True}


def test_prove_read_unknown_safe_retry() -> None:
    result = prove_read_unknown_safe_retry()
    assert result["executions"] == 2
    assert result["class"] == "read"


def test_prove_operator_release() -> None:
    result = prove_operator_release()
    assert result["executions"] == 2
    assert result["operator_resolution_applied"] == "not_executed"
    assert result["final_outcome"] == "COMPLETED"


def test_run_demo_exits_zero() -> None:
    assert run_demo(redis=False) == 0
