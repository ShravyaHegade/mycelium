"""Lightweight, pluggable authorization for operator releases."""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class OperatorReleaseRequest:
    """The security-relevant fields presented to an operator authorizer."""

    operator_id: str
    request_id: str
    tool: str
    verified: str


@runtime_checkable
class OperatorAuthorizer(Protocol):
    """Host-supplied policy hook for an operator release.

    Implementations return ``True`` only when ``credential`` authenticates the
    claimed operator and that operator may perform this exact release.
    """

    def authorize_release(
        self,
        request: OperatorReleaseRequest,
        *,
        credential: str | None,
    ) -> bool: ...


class StaticTokenOperatorAuthorizer:
    """Small-deployment authorizer backed by one secret token per operator.

    Keep tokens outside source control (for example in environment variables or
    a secret manager). This is intentionally a simple bridge, not an SSO or IAM
    system. Tokens identify their owner but are not independently scoped or
    short-lived.
    """

    def __init__(self, tokens: Mapping[str, str]) -> None:
        if not tokens:
            raise ValueError("at least one operator token is required")
        normalized: dict[str, str] = {}
        for operator_id, token in tokens.items():
            if not operator_id or not token:
                raise ValueError("operator ids and tokens must be non-empty")
            normalized[str(operator_id)] = str(token)
        self._tokens = normalized

    def authorize_release(
        self,
        request: OperatorReleaseRequest,
        *,
        credential: str | None,
    ) -> bool:
        expected = self._tokens.get(request.operator_id)
        if expected is None or credential is None:
            return False
        return hmac.compare_digest(expected, credential)


__all__ = [
    "OperatorAuthorizer",
    "OperatorReleaseRequest",
    "StaticTokenOperatorAuthorizer",
]
