from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mycelium.reconcile import ReconcileResult

if TYPE_CHECKING:
    from mycelium.action_ledger import LedgerEntry


class GmailReconciler:
    """Fail-closed reconciler for Gmail sent-email operations.

    Injected ``service`` must expose a ``users().messages().list(userId='me',
    q=...).execute()`` interface compatible with the Google Gmail API client
    (duck-typed; no hard google dep).

    Reconciliation is read-only — never sends, never retries.
    """

    def __init__(self, service: Any) -> None:
        self._service = service

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        ref = entry.external_operation_ref
        if not ref:
            return ReconcileResult.unknown()

        try:
            result = (
                self._service.users()
                .messages()
                .list(userId="me", q=f"in:sent rfc822msgid:{ref}")
                .execute()
            )
        except Exception:
            raise

        messages = result.get("messages", [])
        if len(messages) == 1:
            return ReconcileResult.completed(messages[0])
        return ReconcileResult.unknown()
