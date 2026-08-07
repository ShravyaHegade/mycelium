from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mycelium.reconcile import ReconcileResult

if TYPE_CHECKING:
    from mycelium.action_ledger import LedgerEntry


def canonicalize_message_id(ref: str) -> str | None:
    """Strip outer whitespace and wrap bare RFC 5322 Message-IDs in ``<>``.

    Returns ``None`` when the ref is missing, empty after strip, or contains
    interior whitespace / control characters — callers treat that as
    ``UNKNOWN`` without querying Gmail (avoids ``q=`` query-split).
    """
    stripped = ref.strip()
    if not stripped:
        return None
    if any(ch.isspace() or ord(ch) < 32 or ord(ch) == 127 for ch in stripped):
        return None
    if stripped.startswith("<") and stripped.endswith(">"):
        return stripped
    return f"<{stripped}>"


class GmailReconciler:
    """Fail-closed reconciler for Gmail sent-email operations.

    Injected ``service`` must expose a ``users().messages().list(userId='me',
    q=...).execute()`` interface compatible with the Google Gmail API client
    (duck-typed; no hard google dep).

    Reconciliation is read-only — never sends, never retries.

    Message-IDs are canonicalized (strip + wrap bare ids in ``<>``) for the
    sent-log query and the completed receipt so bracketed, bare, and
    whitespace-padded forms resolve identically. Interior whitespace and
    control characters are rejected (``UNKNOWN``, no Gmail call).

    **Consumer Gmail constraint:** the Gmail API may rewrite the MIME
    Message-ID at send (e.g. to ``…@mail.gmail.com``). A pre-transport
    ``rfc822msgid:`` lookup can then always miss → reconciler stays
    ``UNKNOWN`` (fail-closed; not a defect). Operator release is still
    required on that account class.
    """

    def __init__(self, service: Any) -> None:
        self._service = service

    def reconcile(self, entry: LedgerEntry) -> ReconcileResult:
        ref = entry.external_operation_ref
        if not ref:
            return ReconcileResult.unknown()

        canonical = canonicalize_message_id(ref)
        if canonical is None:
            return ReconcileResult.unknown()

        try:
            result = (
                self._service.users()
                .messages()
                .list(userId="me", q=f"in:sent rfc822msgid:{canonical}")
                .execute()
            )
        except Exception:
            raise

        messages = result.get("messages", [])
        if len(messages) == 1:
            msg = messages[0]
            if isinstance(msg, dict):
                receipt: dict[str, Any] = {**msg, "message_id": canonical}
            else:
                receipt = {"provider_message": msg, "message_id": canonical}
            return ReconcileResult.completed(receipt)
        return ReconcileResult.unknown()
