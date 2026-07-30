from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mycelium.providers.gmail import GmailReconciler
from mycelium.reconcile import ReconcileResult, ReconcileStatus


@dataclass
class FakeEntry:
    external_operation_ref: str | None


class FakeGmailService:
    def __init__(self) -> None:
        self._store: dict[str, list[dict[str, Any]]] = {}
        self.list_called: list[str] = []

    def _list(self, q: str) -> dict[str, Any]:
        self.list_called.append(q)
        msgs = self._store.get(q, [])
        if not msgs:
            return {"messages": [], "resultSizeEstimate": 0}
        return {"messages": msgs, "resultSizeEstimate": len(msgs)}

    def users(self) -> FakeGmailService:
        return self

    def messages(self) -> FakeGmailService:
        return self

    def list(self, userId: str = "me", q: str = "") -> FakeGmailService:
        self._pending_q = q
        return self

    def execute(self) -> dict[str, Any]:
        return self._list(self._pending_q)


def test_missing_external_operation_ref_returns_unknown() -> None:
    service = FakeGmailService()
    reconciler = GmailReconciler(service)
    entry = FakeEntry(external_operation_ref=None)
    result = reconciler.reconcile(entry)
    assert result == ReconcileResult.unknown()
    assert service.list_called == []


def test_empty_external_operation_ref_returns_unknown() -> None:
    service = FakeGmailService()
    reconciler = GmailReconciler(service)
    entry = FakeEntry(external_operation_ref="")
    result = reconciler.reconcile(entry)
    assert result == ReconcileResult.unknown()
    assert service.list_called == []


def test_zero_matches_returns_unknown() -> None:
    service = FakeGmailService()
    service._store = {}
    reconciler = GmailReconciler(service)
    entry = FakeEntry(external_operation_ref="msg-123")
    result = reconciler.reconcile(entry)
    assert result == ReconcileResult.unknown()
    assert service.list_called == ["in:sent rfc822msgid:msg-123"]


def test_one_match_returns_completed() -> None:
    service = FakeGmailService()
    service._store = {"in:sent rfc822msgid:msg-1": [{"id": "18472", "threadId": "t-1"}]}
    reconciler = GmailReconciler(service)
    entry = FakeEntry(external_operation_ref="msg-1")
    result = reconciler.reconcile(entry)
    assert result.status == ReconcileStatus.COMPLETED
    assert result.result == {"id": "18472", "threadId": "t-1"}
    assert service.list_called == ["in:sent rfc822msgid:msg-1"]


def test_two_matches_returns_unknown() -> None:
    service = FakeGmailService()
    service._store = {
        "in:sent rfc822msgid:msg-2": [
            {"id": "a", "threadId": "t-1"},
            {"id": "b", "threadId": "t-2"},
        ]
    }
    reconciler = GmailReconciler(service)
    entry = FakeEntry(external_operation_ref="msg-2")
    result = reconciler.reconcile(entry)
    assert result == ReconcileResult.unknown()
    assert service.list_called == ["in:sent rfc822msgid:msg-2"]


def test_api_error_propagates() -> None:
    class _BrokenService:
        def users(self) -> _BrokenService:
            return self

        def messages(self) -> _BrokenService:
            return self

        def list(self, userId: str = "me", q: str = "") -> _BrokenService:
            return self

        def execute(self) -> dict[str, Any]:
            raise RuntimeError("Gmail API unavailable")

    service = _BrokenService()
    reconciler = GmailReconciler(service)
    entry = FakeEntry(external_operation_ref="msg-3")
    import pytest

    with pytest.raises(RuntimeError, match="Gmail API unavailable"):
        reconciler.reconcile(entry)
