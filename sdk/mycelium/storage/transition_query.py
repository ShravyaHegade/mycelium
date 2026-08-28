"""Shared transition query and retention primitives for ledger backends."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

E = TypeVar("E")


@dataclass(frozen=True)
class TransitionPage(Generic[E]):
    entries: list[E]
    next_cursor: str | None = None


def encode_cursor(started_at: float, request_id: str) -> str:
    raw = json.dumps([float(started_at), request_id], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str | None) -> tuple[float, str] | None:
    if cursor is None:
        return None
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        value: Any = json.loads(base64.urlsafe_b64decode(padded).decode())
        if not isinstance(value, list) or len(value) != 2:
            raise ValueError
        return float(value[0]), str(value[1])
    except (ValueError, TypeError, json.JSONDecodeError, binascii.Error) as exc:
        raise ValueError("invalid transition pagination cursor") from exc


def entry_sort_key(entry: Any) -> tuple[float, str]:
    return float(getattr(entry, "started_at", 0.0) or 0.0), str(entry.request_id)
