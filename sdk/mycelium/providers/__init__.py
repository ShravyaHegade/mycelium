from mycelium.providers.gmail import GmailReconciler, canonicalize_message_id
from mycelium.providers.gmail_conformance import GmailConformanceFixture


def get_provider_conformance_fixture(name: str) -> GmailConformanceFixture:
    normalized = str(name).strip().lower()
    if normalized == "gmail":
        return GmailConformanceFixture()
    raise ValueError(f"unknown shipped provider adapter {name!r}")


__all__ = [
    "GmailConformanceFixture",
    "GmailReconciler",
    "canonicalize_message_id",
    "get_provider_conformance_fixture",
]
