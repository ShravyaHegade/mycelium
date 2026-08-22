"""Isolated, namespaced storage for empirical verification.

Never writes synthetic records into the application's real file/SQLite
ledger. PostgreSQL/Redis use a unique verification prefix and delete only
those exact keys/rows on cleanup.
"""

from __future__ import annotations

import shutil
import tempfile
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from mycelium.action_ledger import InMemoryLedgerStorage, LedgerEntry, LedgerStorage
from mycelium.config import MyceliumConfig
from mycelium.storage._helpers import redact_secrets, resolve_storage_url
from mycelium.verify.types import IsolationRefused

VERIFY_PREFIX = "mycelium:verify:"
SINGLE_NODE_BACKENDS = frozenset({"memory", "file", "sqlite"})
DISTRIBUTED_BACKENDS = frozenset({"postgres", "redis"})
_ADAPTERS: dict[
    str,
    Callable[[VerificationNamespace, dict[str, Any], Path], IsolationSession],
] = {}


@dataclass(frozen=True)
class VerificationNamespace:
    run_id: str
    prefix: str
    started_at: float
    backend: str

    def request_id(self, scenario: str, suffix: str | None = None) -> str:
        token = suffix if suffix is not None else uuid.uuid4().hex
        return f"{self.prefix}{scenario}:{token}"

    def owns(self, request_id: str) -> bool:
        return request_id.startswith(self.prefix)


class IsolationGateStorage:
    """Refuse any ledger access whose request_id is outside the namespace."""

    def __init__(self, inner: LedgerStorage, namespace: VerificationNamespace) -> None:
        self._inner = inner
        self._namespace = namespace
        self.backend_type = type(inner).__name__

    def _guard(self, request_id: str) -> str:
        if not self._namespace.owns(request_id):
            raise IsolationRefused(
                f"refusing ledger access outside verification namespace "
                f"({self._namespace.prefix!r})"
            )
        return request_id

    def get(self, request_id: str) -> LedgerEntry | None:
        return self._inner.get(self._guard(request_id))

    def set(self, entry: LedgerEntry) -> None:
        self._guard(entry.request_id)
        self._inner.set(entry)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[str, LedgerEntry | None]:
        self._guard(entry.request_id)
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        self._guard(entry.request_id)
        return self._inner.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
            expected_fence=expected_fence,
            expected_effect_state=expected_effect_state,
        )

    def list_all(self) -> list[LedgerEntry]:
        rows = self._inner.list_all()
        owned: list[LedgerEntry] = []
        for row in rows:
            if not self._namespace.owns(row.request_id):
                raise IsolationRefused("list_all returned a row outside the verification namespace")
            owned.append(row)
        return owned

    def resolve_request_id(self, effect_id: str) -> str | None:
        canonical = self._inner.resolve_request_id(effect_id)
        if canonical is None:
            return None
        return self._guard(canonical)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        entry = self._inner.get_by_effect_id(effect_id)
        if entry is None:
            return None
        self._guard(entry.request_id)
        return entry


class FaultInjectingStorage:
    """Deterministic outage wrapper. Does not swap the inner backend type."""

    def __init__(self, inner: LedgerStorage) -> None:
        self._inner = inner
        self.fail_get = False
        self.fail_set = False
        self.fail_claim = False
        self.fail_transition = False
        self.fail_list_all = False
        self.fail_nth_set: int | None = None
        self._set_count = 0
        self.fail_nth_transition: int | None = None
        self._transition_count = 0

    @property
    def inner_type(self) -> str:
        return type(self._inner).__name__

    def _maybe_fail(self, flag: bool, op: str) -> None:
        if flag:
            raise ConnectionError(f"injected {op} outage")

    def get(self, request_id: str) -> LedgerEntry | None:
        self._maybe_fail(self.fail_get, "get")
        return self._inner.get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        self._set_count += 1
        nth = self.fail_nth_set
        self._maybe_fail(
            self.fail_set or (nth is not None and self._set_count >= nth),
            "set",
        )
        self._inner.set(entry)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[str, LedgerEntry | None]:
        self._maybe_fail(self.fail_claim, "try_claim_inflight")
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        self._transition_count += 1
        nth = self.fail_nth_transition
        self._maybe_fail(
            self.fail_transition
            or (nth is not None and self._transition_count >= nth),
            "try_transition",
        )
        return self._inner.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
            expected_fence=expected_fence,
            expected_effect_state=expected_effect_state,
        )

    def list_all(self) -> list[LedgerEntry]:
        self._maybe_fail(self.fail_list_all, "list_all")
        return self._inner.list_all()

    def resolve_request_id(self, effect_id: str) -> str | None:
        return self._inner.resolve_request_id(effect_id)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        return self._inner.get_by_effect_id(effect_id)


class VerificationAdapter(Protocol):
    """Extension point for custom storage backends."""

    def open(self) -> LedgerStorage: ...

    def open_fresh(self) -> LedgerStorage: ...

    def cleanup(self, request_ids: list[str]) -> None: ...


@dataclass
class IsolationSession:
    namespace: VerificationNamespace
    backend: str
    topology_label: str
    restart_capable: bool
    multiprocess_capable: bool
    persistence_asserted: bool
    artifacts: list[str] = field(default_factory=list)
    tracked_ids: list[str] = field(default_factory=list)
    worker_payload: dict[str, Any] = field(default_factory=dict)
    _factory: Callable[[], LedgerStorage] = field(
        repr=False, default=lambda: InMemoryLedgerStorage()
    )
    _cleanup: Callable[[list[str]], None] = field(repr=False, default=lambda ids: None)
    _tmp: Path | None = None
    _artifact_tmp: Path | None = None
    _track_callback: Callable[[str], None] | None = field(repr=False, default=None)
    _closed: bool = False

    def open_raw_inner(self) -> LedgerStorage:
        return self._factory()

    def open_storage(self) -> IsolationGateStorage:
        return IsolationGateStorage(self._factory(), self.namespace)

    def open_fresh_client(self) -> IsolationGateStorage:
        return IsolationGateStorage(self._factory(), self.namespace)

    def probe(self) -> None:
        """Prove the backend accepts namespaced access without listing production."""
        marker = self.namespace.request_id("probe", "isolation")
        self.open_storage().get(marker)

    def track(self, request_id: str) -> str:
        if not self.namespace.owns(request_id):
            raise IsolationRefused("synthetic request_id escaped verification namespace")
        if request_id not in self.tracked_ids:
            self.tracked_ids.append(request_id)
            if self._track_callback is not None:
                self._track_callback(request_id)
        return request_id

    def prepare_artifacts(self) -> None:
        if self._artifact_tmp is None:
            self._artifact_tmp = Path(tempfile.mkdtemp(prefix="mycelium-verify-artifacts-"))

    def artifact_file(self, prefix: str) -> str:
        import os

        self.prepare_artifacts()
        assert self._artifact_tmp is not None
        fd, path = tempfile.mkstemp(prefix=prefix, dir=self._artifact_tmp)
        os.close(fd)
        return path

    def artifact_dir(self, prefix: str) -> Path:
        self.prepare_artifacts()
        assert self._artifact_tmp is not None
        return Path(tempfile.mkdtemp(prefix=prefix, dir=self._artifact_tmp))

    def artifact_paths(self) -> list[str]:
        if self._artifact_tmp is None or not self._artifact_tmp.exists():
            return []
        return [
            str(self._artifact_tmp),
            *(str(path) for path in sorted(self._artifact_tmp.rglob("*"))),
        ]

    def cleanup(self, *, keep_artifacts: bool = False) -> None:
        if self._closed:
            return
        errors: list[BaseException] = []
        if not keep_artifacts:
            try:
                self._cleanup(list(self.tracked_ids))
            except Exception as exc:
                errors.append(exc)
            if self._tmp is not None:
                try:
                    shutil.rmtree(self._tmp)
                except Exception as exc:
                    errors.append(exc)
            if self._artifact_tmp is not None:
                try:
                    shutil.rmtree(self._artifact_tmp)
                except Exception as exc:
                    errors.append(exc)
        self._closed = True
        if errors:
            raise RuntimeError(
                "verification cleanup failed; namespaced records may remain: "
                + redact_secrets(str(errors[0]))
            ) from errors[0]


def register_isolation_adapter(
    storage_type: str,
    opener: Callable[[VerificationNamespace, dict[str, Any], Path], IsolationSession],
) -> None:
    _ADAPTERS[storage_type] = opener


def _ledger_raw(config: MyceliumConfig) -> dict[str, Any]:
    return dict(config.action_ledger or {"storage": "memory"})


def _memory_session(
    ns: VerificationNamespace, raw: dict[str, Any], workdir: Path
) -> IsolationSession:
    shared = InMemoryLedgerStorage()
    return IsolationSession(
        namespace=ns,
        backend="memory",
        topology_label="process_local",
        restart_capable=False,
        multiprocess_capable=False,
        persistence_asserted=False,
        worker_payload={"backend": "memory"},
        _factory=lambda: shared,
        _cleanup=lambda ids: None,
    )


def _file_session(
    ns: VerificationNamespace, raw: dict[str, Any], workdir: Path
) -> IsolationSession:
    from mycelium.action_ledger import FileLedgerStorage

    tmp = Path(tempfile.mkdtemp(prefix="mycelium-verify-"))
    path = tmp / "ledger.json"
    return IsolationSession(
        namespace=ns,
        backend="file",
        topology_label="single_node",
        restart_capable=True,
        multiprocess_capable=True,
        persistence_asserted=True,
        artifacts=[str(path)],
        worker_payload={"backend": "file", "path": str(path)},
        _tmp=tmp,
        _factory=lambda: FileLedgerStorage(path),
        _cleanup=lambda ids: None,
    )


def _sqlite_session(
    ns: VerificationNamespace, raw: dict[str, Any], workdir: Path
) -> IsolationSession:
    from mycelium.storage.sqlite_ledger import SqliteLedgerStorage

    tmp = Path(tempfile.mkdtemp(prefix="mycelium-verify-"))
    path = tmp / "ledger.db"
    table = "mycelium_verify_ledger"
    return IsolationSession(
        namespace=ns,
        backend="sqlite",
        topology_label="single_node",
        restart_capable=True,
        multiprocess_capable=True,
        persistence_asserted=True,
        artifacts=[str(path)],
        worker_payload={"backend": "sqlite", "path": str(path), "table": table},
        _tmp=tmp,
        _factory=lambda: SqliteLedgerStorage(path, table=table),
        _cleanup=lambda ids: None,
    )


def _postgres_session(
    ns: VerificationNamespace, raw: dict[str, Any], workdir: Path
) -> IsolationSession:
    from mycelium.storage.postgres_ledger import PostgresLedgerStorage

    try:
        dsn = resolve_storage_url(raw, url_key="dsn", alt_keys=("url",))
    except ValueError as exc:
        raise IsolationRefused(f"postgres ledger DSN unresolved: {exc}") from exc
    allow_temp = bool((raw.get("_verify") or {}).get("allow_temporary_schema"))
    table = str(raw.get("table", "mycelium_action_ledger"))
    if allow_temp:
        table = f"v{ns.run_id.replace('-', '')}"[:40]
        if not table[0].isalpha():
            table = "v" + table
        table = "".join(ch for ch in table.lower() if ch.isalnum() or ch == "_")

    def factory() -> LedgerStorage:
        return _PostgresPrefixedStorage(
            PostgresLedgerStorage(dsn, table=table),
            prefix=ns.prefix,
            dsn=dsn,
            table=table,
        )

    def cleanup(ids: list[str]) -> None:
        _postgres_delete_exact(dsn, table, ids)

    return IsolationSession(
        namespace=ns,
        backend="postgres",
        topology_label="distributed",
        restart_capable=True,
        multiprocess_capable=True,
        persistence_asserted=True,
        artifacts=[f"postgres table={table} prefix={ns.prefix}"],
        worker_payload={"backend": "postgres", "dsn": dsn, "table": table},
        _factory=factory,
        _cleanup=cleanup,
    )


def _postgres_delete_exact(dsn: str, table: str, ids: list[str]) -> None:
    if not ids:
        return
    try:
        import psycopg
        from psycopg import sql
    except ImportError as exc:
        raise IsolationRefused(
            "postgres cleanup requires psycopg; install mycelium-runtime[postgres]"
        ) from exc
    import re

    if not re.fullmatch(r"[a-z][a-z0-9_]*", table):
        raise IsolationRefused(f"refusing unverified postgres table name {table!r}")
    query = sql.SQL("DELETE FROM {} WHERE request_id = %s").format(sql.Identifier(table))
    with psycopg.connect(dsn) as conn:
        for request_id in ids:
            conn.execute(query, (request_id,))
        conn.commit()


class _PostgresPrefixedStorage:
    """Postgres access that never lists rows outside the verification prefix."""

    def __init__(self, inner: Any, *, prefix: str, dsn: str, table: str) -> None:
        self._inner = inner
        self._prefix = prefix
        self._dsn = dsn
        self._table = table

    def get(self, request_id: str) -> LedgerEntry | None:
        return self._inner.get(request_id)

    def set(self, entry: LedgerEntry) -> None:
        self._inner.set(entry)

    def try_claim_inflight(
        self,
        entry: LedgerEntry,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[str, LedgerEntry | None]:
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def try_transition(
        self,
        entry: LedgerEntry,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
        expected_fence: int | None = None,
        expected_effect_state: str | None = None,
    ) -> bool:
        return self._inner.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
            expected_fence=expected_fence,
            expected_effect_state=expected_effect_state,
        )

    def list_all(self) -> list[LedgerEntry]:
        try:
            import psycopg
            from psycopg import sql
        except ImportError as exc:
            raise IsolationRefused(
                "postgres list requires psycopg; install mycelium-runtime[postgres]"
            ) from exc
        import re

        if not re.fullmatch(r"[a-z][a-z0-9_]*", self._table):
            raise IsolationRefused(f"refusing unverified postgres table name {self._table!r}")
        query = sql.SQL("SELECT request_id FROM {} WHERE request_id LIKE %s").format(
            sql.Identifier(self._table)
        )
        ids: list[str] = []
        with psycopg.connect(self._dsn) as conn:
            for row in conn.execute(query, (self._prefix + "%",)):
                ids.append(str(row[0]))
        entries: list[LedgerEntry] = []
        for request_id in ids:
            if not request_id.startswith(self._prefix):
                raise IsolationRefused("postgres prefix query returned a foreign row")
            entry = self._inner.get(request_id)
            if entry is not None:
                entries.append(entry)
        return entries

    def resolve_request_id(self, effect_id: str) -> str | None:
        return self._inner.resolve_request_id(effect_id)

    def get_by_effect_id(self, effect_id: str) -> LedgerEntry | None:
        return self._inner.get_by_effect_id(effect_id)


def _redis_session(
    ns: VerificationNamespace, raw: dict[str, Any], workdir: Path
) -> IsolationSession:
    from mycelium.storage.redis_ledger import RedisLedgerStorage

    try:
        url = resolve_storage_url(raw, url_key="url")
    except ValueError as exc:
        raise IsolationRefused(f"redis ledger URL unresolved: {exc}") from exc
    prefix = f"{ns.prefix}action:"

    def factory() -> LedgerStorage:
        return RedisLedgerStorage(url, prefix=prefix, in_flight_ttl=None)

    def cleanup(ids: list[str]) -> None:
        _redis_delete_prefix(url, prefix, ids)

    return IsolationSession(
        namespace=ns,
        backend="redis",
        topology_label="distributed",
        restart_capable=True,
        multiprocess_capable=True,
        persistence_asserted=False,  # AOF remains operator-asserted
        artifacts=[f"redis prefix={prefix}"],
        worker_payload={"backend": "redis", "url": url, "prefix": prefix},
        _factory=factory,
        _cleanup=cleanup,
    )


def _redis_delete_prefix(url: str, prefix: str, ids: list[str]) -> None:
    try:
        import redis
    except ImportError as exc:
        raise IsolationRefused(
            "redis cleanup requires the redis package; install mycelium-runtime[redis]"
        ) from exc
    client = redis.Redis.from_url(url, decode_responses=True)
    base = prefix.rstrip(":")
    for request_id in ids:
        client.delete(f"{prefix}{request_id}")
        client.delete(f"{base}-tomb:{request_id}")


def establish_isolation(
    config: MyceliumConfig,
    *,
    workdir: Path | None = None,
    keep_artifacts: bool = False,
) -> IsolationSession:
    raw = _ledger_raw(config)
    storage_type = str(raw.get("storage", "memory"))
    verify_opts = dict(config.verify or {}) if getattr(config, "verify", None) else {}
    raw = dict(raw)
    raw["_verify"] = verify_opts
    run_id = str(uuid.uuid4())
    ns = VerificationNamespace(
        run_id=run_id,
        prefix=f"{VERIFY_PREFIX}{run_id}:",
        started_at=time.time(),
        backend=storage_type,
    )
    opener = _ADAPTERS.get(storage_type)
    if opener is None:
        raise IsolationRefused(
            f"unknown ledger storage {storage_type!r}; register a verification "
            "isolation adapter or use memory|file|sqlite|postgres|redis"
        )
    root = workdir if workdir is not None else Path(".")
    session: IsolationSession | None = None
    try:
        session = opener(ns, raw, root)
        session.prepare_artifacts()
        session.probe()
    except IsolationRefused as exc:
        artifacts = session.artifact_paths() if session is not None and keep_artifacts else []
        if session is not None:
            session.cleanup(keep_artifacts=keep_artifacts)
        raise IsolationRefused(str(exc), artifacts=artifacts) from exc
    except Exception as exc:
        artifacts = session.artifact_paths() if session is not None and keep_artifacts else []
        if session is not None:
            session.cleanup(keep_artifacts=keep_artifacts)
        raise IsolationRefused(
            f"could not establish isolated {storage_type} backend: {redact_secrets(str(exc))}",
            artifacts=artifacts,
        ) from exc
    return session


register_isolation_adapter("memory", _memory_session)
register_isolation_adapter("file", _file_session)
register_isolation_adapter("sqlite", _sqlite_session)
register_isolation_adapter("postgres", _postgres_session)
register_isolation_adapter("redis", _redis_session)


__all__ = [
    "DISTRIBUTED_BACKENDS",
    "FaultInjectingStorage",
    "IsolationGateStorage",
    "IsolationSession",
    "SINGLE_NODE_BACKENDS",
    "VERIFY_PREFIX",
    "VerificationAdapter",
    "VerificationNamespace",
    "establish_isolation",
    "register_isolation_adapter",
    "IsolationRefused",
]
