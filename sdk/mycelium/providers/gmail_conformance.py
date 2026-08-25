"""Synthetic conformance fixture for the shipped Gmail reconciler."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

import mycelium.providers.gmail as gmail_module
from mycelium.provider_conformance import ProviderCallAudit, ProviderObservation
from mycelium.providers.gmail import GmailReconciler


@dataclass(frozen=True)
class _Entry:
    external_operation_ref: str | None


class _ScriptedGmailService:
    """Gmail-shaped test double that records reads and traps common writes."""

    def __init__(
        self,
        observations: tuple[ProviderObservation, ...],
        audit: ProviderCallAudit,
    ) -> None:
        self._observations = list(observations)
        self._audit = audit
        self._pending_query = ""

    def users(self) -> _ScriptedGmailService:
        return self

    def messages(self) -> _ScriptedGmailService:
        return self

    def list(self, userId: str = "me", q: str = "") -> _ScriptedGmailService:
        self._audit.record_read("gmail.users.messages.list")
        self._pending_query = q
        return self

    def execute(self) -> Any:
        if not self._observations:
            raise RuntimeError("conformance fixture exhausted scripted observations")
        observation = self._observations.pop(0)
        if observation.error is not None:
            raise observation.error
        if observation.malformed_response:
            return ["malformed", "provider", "response"]
        messages = list(observation.matches)
        return {"messages": messages, "resultSizeEstimate": len(messages)}

    def _forbidden(self, operation: str) -> _ScriptedGmailService:
        self._audit.record_write(operation)
        raise AssertionError(f"provider conformance forbids {operation}")

    def send(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.send")

    def insert(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.insert")

    def import_(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.import")

    def modify(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.modify")

    def delete(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.delete")

    def trash(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.trash")

    def untrash(self, **kwargs: Any) -> _ScriptedGmailService:
        return self._forbidden("gmail.users.messages.untrash")


class GmailConformanceFixture:
    adapter_name = "gmail"
    adapter_version = "1"
    valid_handle = "conformance-message@example.com"
    malformed_handles = (
        None,
        "",
        "   ",
        "message id@example.com",
        "message\t@example.com",
        "message\n@example.com",
        "message\x00@example.com",
    )

    def build_reconciler(
        self,
        observations: tuple[ProviderObservation, ...],
        audit: ProviderCallAudit,
    ) -> GmailReconciler:
        return GmailReconciler(_ScriptedGmailService(observations, audit))

    def make_entry(self, handle: Any) -> _Entry:
        return _Entry(external_operation_ref=handle)

    def source_bytes(self) -> bytes:
        return inspect.getsource(gmail_module).encode("utf-8")


__all__ = ["GmailConformanceFixture"]
