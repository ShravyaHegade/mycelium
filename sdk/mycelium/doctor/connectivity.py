"""Safe, read-only connectivity probes for doctor (no persistent records)."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from mycelium.storage._helpers import redact_secrets


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    kind: str  # ok | unreachable | unauthorized | unsupported | timeout | error
    message: str


def _classify_error(exc: BaseException) -> str:
    text = str(exc).lower()
    name = type(exc).__name__.lower()
    if "timeout" in text or "timed out" in text or "timeout" in name:
        return "timeout"
    if "auth" in text or "password" in text or "permission" in text or "denied" in text:
        return "unauthorized"
    if "unsupported" in text or "not supported" in text:
        return "unsupported"
    if isinstance(exc, (ConnectionError, OSError, socket.timeout)):
        return "unreachable"
    return "error"


def probe_postgres(dsn: str, *, timeout_seconds: float = 2.0) -> ProbeResult:
    """Connect and ``SELECT 1`` only — never creates tables or inserts rows."""
    try:
        import psycopg
    except ImportError as exc:
        return ProbeResult(
            ok=False,
            kind="unsupported",
            message=f"psycopg not installed ({redact_secrets(str(exc))})",
        )
    try:
        with psycopg.connect(dsn, connect_timeout=max(1, int(timeout_seconds))) as conn:
            conn.execute("SELECT 1")
        return ProbeResult(ok=True, kind="ok", message="PostgreSQL accepted SELECT 1")
    except Exception as exc:
        return ProbeResult(
            ok=False,
            kind=_classify_error(exc),
            message=redact_secrets(str(exc)),
        )


def probe_redis(url: str, *, timeout_seconds: float = 2.0) -> ProbeResult:
    """``PING`` only — never writes keys or stream entries."""
    try:
        import redis
    except ImportError as exc:
        return ProbeResult(
            ok=False,
            kind="unsupported",
            message=f"redis package not installed ({redact_secrets(str(exc))})",
        )
    try:
        client = redis.Redis.from_url(
            url,
            socket_connect_timeout=timeout_seconds,
            socket_timeout=timeout_seconds,
            decode_responses=True,
        )
        client.ping()
        return ProbeResult(ok=True, kind="ok", message="Redis PING succeeded")
    except Exception as exc:
        return ProbeResult(
            ok=False,
            kind=_classify_error(exc),
            message=redact_secrets(str(exc)),
        )


def probe_sqlite_path(path: str) -> ProbeResult:
    """Confirm the SQLite path's parent directory exists (no DB writes)."""
    from pathlib import Path

    target = Path(path)
    parent = target.parent if target.suffix else target
    if not parent.exists():
        return ProbeResult(
            ok=False,
            kind="unreachable",
            message=f"SQLite parent directory does not exist: {parent}",
        )
    if not parent.is_dir():
        return ProbeResult(
            ok=False,
            kind="error",
            message=f"SQLite parent path is not a directory: {parent}",
        )
    return ProbeResult(
        ok=True,
        kind="ok",
        message=f"SQLite parent directory is present: {parent}",
    )


def probe_file_path(path: str) -> ProbeResult:
    """Confirm a file backend's parent directory exists (no writes)."""
    from pathlib import Path

    target = Path(path)
    parent = target.parent
    if not parent.exists():
        return ProbeResult(
            ok=False,
            kind="unreachable",
            message=f"file parent directory does not exist: {parent}",
        )
    return ProbeResult(
        ok=True,
        kind="ok",
        message=f"file parent directory is present: {parent}",
    )


def safe_backend_label(raw: dict[str, Any] | None, *, storage_key: str = "storage") -> str:
    """Human label for a storage mapping without secrets."""
    if not raw:
        return "unset"
    storage = str(raw.get(storage_key, "memory"))
    if storage in ("file", "sqlite"):
        path = raw.get("path")
        return f"{storage} path={path!r}" if path else storage
    if storage == "postgres":
        if raw.get("table"):
            return f"postgres table={raw.get('table')!r}"
        return "postgres"
    if storage == "redis":
        prefix = raw.get("key_prefix") or raw.get("prefix")
        if prefix:
            return f"redis key_prefix={prefix!r}"
        return "redis"
    return storage


def host_hint_from_url(url: str) -> str:
    """Return a redacted host:port hint for evidence (never credentials)."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname or "unknown-host"
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme or 'url'}://{host}{port}"
    except Exception:
        return "url://redacted"


__all__ = [
    "ProbeResult",
    "host_hint_from_url",
    "probe_file_path",
    "probe_postgres",
    "probe_redis",
    "probe_sqlite_path",
    "safe_backend_label",
]
