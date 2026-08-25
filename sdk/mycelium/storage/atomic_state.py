"""Namespaced atomic state shared by Mycelium's stateful guardrails."""

from __future__ import annotations

import copy
import json
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, Protocol, TypeVar
from urllib.parse import quote, unquote

from mycelium.storage.json_file import LockedJsonDictFile

T = TypeVar("T")
R = TypeVar("R")


class AtomicStateError(RuntimeError):
    """Raised when shared state cannot be read or updated safely."""


class AtomicStateContentionError(AtomicStateError):
    """Raised after repeated compare-and-swap conflicts."""


@dataclass(frozen=True)
class AtomicStateRecord:
    """A JSON state value and its backend-controlled revision."""

    value: dict[str, Any]
    version: int


class AtomicStateBackend(Protocol):
    """Minimal durable KV/CAS contract used by stateful guardrails."""

    def get(self, namespace: str, key: str) -> AtomicStateRecord | None: ...

    def create(self, namespace: str, key: str, value: dict[str, Any]) -> bool: ...

    def compare_and_swap(
        self,
        namespace: str,
        key: str,
        expected_version: int,
        value: dict[str, Any],
    ) -> bool: ...

    def delete(
        self,
        namespace: str,
        key: str,
        *,
        expected_version: int | None = None,
    ) -> bool: ...

    def scan(self, namespace: str) -> list[tuple[str, AtomicStateRecord]]: ...


def _copy_value(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TypeError("atomic state values must be dictionaries")
    return copy.deepcopy(value)


def _validate_part(label: str, value: str) -> str:
    normalized = str(value)
    if not normalized:
        raise ValueError(f"atomic state {label} must not be empty")
    return normalized


class InMemoryAtomicStateBackend:
    """Process-local reference backend with the full CAS contract."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], AtomicStateRecord] = {}
        self._lock = threading.RLock()

    def get(self, namespace: str, key: str) -> AtomicStateRecord | None:
        identity = (_validate_part("namespace", namespace), _validate_part("key", key))
        with self._lock:
            record = self._records.get(identity)
            if record is None:
                return None
            return AtomicStateRecord(_copy_value(record.value), record.version)

    def create(self, namespace: str, key: str, value: dict[str, Any]) -> bool:
        identity = (_validate_part("namespace", namespace), _validate_part("key", key))
        with self._lock:
            if identity in self._records:
                return False
            self._records[identity] = AtomicStateRecord(_copy_value(value), 1)
            return True

    def compare_and_swap(
        self,
        namespace: str,
        key: str,
        expected_version: int,
        value: dict[str, Any],
    ) -> bool:
        identity = (_validate_part("namespace", namespace), _validate_part("key", key))
        with self._lock:
            current = self._records.get(identity)
            if current is None or current.version != expected_version:
                return False
            self._records[identity] = AtomicStateRecord(
                _copy_value(value), current.version + 1
            )
            return True

    def delete(
        self,
        namespace: str,
        key: str,
        *,
        expected_version: int | None = None,
    ) -> bool:
        identity = (_validate_part("namespace", namespace), _validate_part("key", key))
        with self._lock:
            current = self._records.get(identity)
            if current is None:
                return False
            if expected_version is not None and current.version != expected_version:
                return False
            del self._records[identity]
            return True

    def scan(self, namespace: str) -> list[tuple[str, AtomicStateRecord]]:
        namespace = _validate_part("namespace", namespace)
        with self._lock:
            return sorted(
                (
                    key,
                    AtomicStateRecord(_copy_value(record.value), record.version),
                )
                for (record_namespace, key), record in self._records.items()
                if record_namespace == namespace
            )


class FileAtomicStateBackend:
    """Single-node JSON backend with cross-process locked CAS operations."""

    def __init__(self, path: str | Path) -> None:
        self._file = LockedJsonDictFile(path)

    @staticmethod
    def _identity(namespace: str, key: str) -> str:
        namespace = _validate_part("namespace", namespace)
        key = _validate_part("key", key)
        return f"{quote(namespace, safe='')}:{quote(key, safe='')}"

    @staticmethod
    def _record(raw: dict[str, Any]) -> AtomicStateRecord:
        value = raw.get("value")
        if not isinstance(value, dict):
            raise AtomicStateError("file atomic state record has a non-object value")
        version = int(raw.get("version") or 0)
        if version < 1:
            raise AtomicStateError("file atomic state record has an invalid version")
        return AtomicStateRecord(_copy_value(value), version)

    def get(self, namespace: str, key: str) -> AtomicStateRecord | None:
        identity = self._identity(namespace, key)

        def read(data: dict[str, dict[str, Any]]) -> AtomicStateRecord | None:
            raw = data.get(identity)
            return None if raw is None else self._record(raw)

        return self._file.read_modify_write_no_save(read)

    def create(self, namespace: str, key: str, value: dict[str, Any]) -> bool:
        identity = self._identity(namespace, key)

        def mutate(data: dict[str, dict[str, Any]]) -> bool:
            if identity in data:
                return False
            data[identity] = {
                "namespace": namespace,
                "key": key,
                "version": 1,
                "value": _copy_value(value),
            }
            return True

        return self._file.read_modify_write(mutate)

    def compare_and_swap(
        self,
        namespace: str,
        key: str,
        expected_version: int,
        value: dict[str, Any],
    ) -> bool:
        identity = self._identity(namespace, key)

        def mutate(data: dict[str, dict[str, Any]]) -> bool:
            raw = data.get(identity)
            if raw is None or int(raw.get("version") or 0) != expected_version:
                return False
            data[identity] = {
                "namespace": namespace,
                "key": key,
                "version": expected_version + 1,
                "value": _copy_value(value),
            }
            return True

        return self._file.read_modify_write(mutate)

    def delete(
        self,
        namespace: str,
        key: str,
        *,
        expected_version: int | None = None,
    ) -> bool:
        identity = self._identity(namespace, key)

        def mutate(data: dict[str, dict[str, Any]]) -> bool:
            raw = data.get(identity)
            if raw is None:
                return False
            if expected_version is not None and int(raw.get("version") or 0) != expected_version:
                return False
            del data[identity]
            return True

        return self._file.read_modify_write(mutate)

    def scan(self, namespace: str) -> list[tuple[str, AtomicStateRecord]]:
        namespace = _validate_part("namespace", namespace)

        def read(data: dict[str, dict[str, Any]]) -> list[tuple[str, AtomicStateRecord]]:
            records = []
            for raw in data.values():
                if raw.get("namespace") != namespace:
                    continue
                records.append((str(raw["key"]), self._record(raw)))
            return sorted(records, key=lambda item: item[0])

        return self._file.read_modify_write_no_save(read)


class RedisAtomicStateBackend:
    """Redis backend using WATCH/MULTI for revision-checked writes."""

    def __init__(self, url: str, *, prefix: str = "mycelium:state:") -> None:
        try:
            import redis
        except ImportError as exc:
            raise ImportError(
                "Redis storage requires the 'redis' package. "
                "Install with: pip install 'mycelium-runtime[redis]'"
            ) from exc
        self._redis_module = redis
        self._client = redis.Redis.from_url(url, decode_responses=True)
        self._prefix = str(prefix)

    def _key(self, namespace: str, key: str) -> str:
        namespace = _validate_part("namespace", namespace)
        key = _validate_part("key", key)
        return f"{self._prefix}{quote(namespace, safe='')}:{quote(key, safe='')}"

    @staticmethod
    def _decode(raw: str) -> AtomicStateRecord:
        try:
            payload = json.loads(raw)
            value = payload["value"]
            version = int(payload["version"])
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise AtomicStateError("Redis atomic state record is malformed") from exc
        if not isinstance(value, dict) or version < 1:
            raise AtomicStateError("Redis atomic state record is malformed")
        return AtomicStateRecord(_copy_value(value), version)

    @staticmethod
    def _encode(value: dict[str, Any], version: int) -> str:
        return json.dumps({"version": version, "value": _copy_value(value)}, default=str)

    def get(self, namespace: str, key: str) -> AtomicStateRecord | None:
        raw = self._client.get(self._key(namespace, key))
        return None if raw is None else self._decode(raw)

    def create(self, namespace: str, key: str, value: dict[str, Any]) -> bool:
        return bool(self._client.set(self._key(namespace, key), self._encode(value, 1), nx=True))

    def compare_and_swap(
        self,
        namespace: str,
        key: str,
        expected_version: int,
        value: dict[str, Any],
    ) -> bool:
        redis_key = self._key(namespace, key)
        try:
            with self._client.pipeline() as pipe:
                pipe.watch(redis_key)
                raw = pipe.get(redis_key)
                if raw is None or self._decode(raw).version != expected_version:
                    pipe.unwatch()
                    return False
                pipe.multi()
                pipe.set(redis_key, self._encode(value, expected_version + 1))
                pipe.execute()
                return True
        except self._redis_module.exceptions.WatchError:
            return False

    def delete(
        self,
        namespace: str,
        key: str,
        *,
        expected_version: int | None = None,
    ) -> bool:
        redis_key = self._key(namespace, key)
        if expected_version is None:
            return bool(self._client.delete(redis_key))
        try:
            with self._client.pipeline() as pipe:
                pipe.watch(redis_key)
                raw = pipe.get(redis_key)
                if raw is None or self._decode(raw).version != expected_version:
                    pipe.unwatch()
                    return False
                pipe.multi()
                pipe.delete(redis_key)
                pipe.execute()
                return True
        except self._redis_module.exceptions.WatchError:
            return False

    def scan(self, namespace: str) -> list[tuple[str, AtomicStateRecord]]:
        namespace = _validate_part("namespace", namespace)
        encoded_namespace = quote(namespace, safe="")
        key_prefix = f"{self._prefix}{encoded_namespace}:"
        records = []
        for redis_key in self._client.scan_iter(match=f"{key_prefix}*"):
            raw = self._client.get(redis_key)
            if raw is None:
                continue
            records.append((unquote(str(redis_key)[len(key_prefix) :]), self._decode(raw)))
        return sorted(records, key=lambda item: item[0])


_TABLE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class PostgresAtomicStateBackend:
    """Postgres JSONB backend with a revision checked in every update."""

    def __init__(self, dsn: str, *, table: str = "mycelium_state") -> None:
        try:
            import psycopg
            from psycopg import sql
        except ImportError as exc:
            raise ImportError(
                "Postgres storage requires the 'psycopg' package. "
                "Install with: pip install 'mycelium-runtime[postgres]'"
            ) from exc
        if not _TABLE_RE.fullmatch(table):
            raise ValueError(
                f"invalid Postgres table name {table!r}; "
                "use lowercase letters, digits, underscores"
            )
        self._psycopg = psycopg
        self._sql = sql
        self._dsn = str(dsn)
        self._table = table
        self._schema_ready = False

    def _table_id(self) -> Any:
        return self._sql.Identifier(self._table)

    def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        query = self._sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ("
            "namespace TEXT NOT NULL, state_key TEXT NOT NULL, "
            "version BIGINT NOT NULL, payload JSONB NOT NULL, "
            "updated_at DOUBLE PRECISION NOT NULL, "
            "PRIMARY KEY (namespace, state_key))"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            conn.execute(query)
            conn.commit()
        self._schema_ready = True

    @staticmethod
    def _payload(raw: Any) -> dict[str, Any]:
        value = raw if isinstance(raw, dict) else json.loads(raw)
        if not isinstance(value, dict):
            raise AtomicStateError("Postgres atomic state payload must be an object")
        return _copy_value(value)

    def get(self, namespace: str, key: str) -> AtomicStateRecord | None:
        namespace = _validate_part("namespace", namespace)
        key = _validate_part("key", key)
        self._ensure_schema()
        query = self._sql.SQL(
            "SELECT version, payload FROM {} WHERE namespace = %s AND state_key = %s"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            row = conn.execute(query, (namespace, key)).fetchone()
        if row is None:
            return None
        return AtomicStateRecord(self._payload(row[1]), int(row[0]))

    def create(self, namespace: str, key: str, value: dict[str, Any]) -> bool:
        namespace = _validate_part("namespace", namespace)
        key = _validate_part("key", key)
        self._ensure_schema()
        query = self._sql.SQL(
            "INSERT INTO {} (namespace, state_key, version, payload, updated_at) "
            "VALUES (%s, %s, 1, %s::jsonb, %s) "
            "ON CONFLICT (namespace, state_key) DO NOTHING"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            cursor = conn.execute(
                query, (namespace, key, json.dumps(_copy_value(value), default=str), time.time())
            )
            conn.commit()
            return cursor.rowcount == 1

    def compare_and_swap(
        self,
        namespace: str,
        key: str,
        expected_version: int,
        value: dict[str, Any],
    ) -> bool:
        namespace = _validate_part("namespace", namespace)
        key = _validate_part("key", key)
        self._ensure_schema()
        query = self._sql.SQL(
            "UPDATE {} SET version = version + 1, payload = %s::jsonb, updated_at = %s "
            "WHERE namespace = %s AND state_key = %s AND version = %s"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            cursor = conn.execute(
                query,
                (
                    json.dumps(_copy_value(value), default=str),
                    time.time(),
                    namespace,
                    key,
                    expected_version,
                ),
            )
            conn.commit()
            return cursor.rowcount == 1

    def delete(
        self,
        namespace: str,
        key: str,
        *,
        expected_version: int | None = None,
    ) -> bool:
        namespace = _validate_part("namespace", namespace)
        key = _validate_part("key", key)
        self._ensure_schema()
        if expected_version is None:
            query = self._sql.SQL(
                "DELETE FROM {} WHERE namespace = %s AND state_key = %s"
            ).format(self._table_id())
            params = (namespace, key)
        else:
            query = self._sql.SQL(
                "DELETE FROM {} WHERE namespace = %s AND state_key = %s AND version = %s"
            ).format(self._table_id())
            params = (namespace, key, expected_version)
        with self._psycopg.connect(self._dsn) as conn:
            cursor = conn.execute(query, params)
            conn.commit()
            return cursor.rowcount == 1

    def scan(self, namespace: str) -> list[tuple[str, AtomicStateRecord]]:
        namespace = _validate_part("namespace", namespace)
        self._ensure_schema()
        query = self._sql.SQL(
            "SELECT state_key, version, payload FROM {} "
            "WHERE namespace = %s ORDER BY state_key"
        ).format(self._table_id())
        with self._psycopg.connect(self._dsn) as conn:
            rows = conn.execute(query, (namespace,)).fetchall()
        return [
            (str(key), AtomicStateRecord(self._payload(payload), int(version)))
            for key, version, payload in rows
        ]


class NamespacedAtomicStorage(Generic[T]):
    """Typed adapter helper that turns backend CAS into safe object updates."""

    def __init__(
        self,
        backend: AtomicStateBackend,
        namespace: str,
        *,
        from_dict: Callable[[dict[str, Any]], T],
        to_dict: Callable[[T], dict[str, Any]],
        max_retries: int = 256,
    ) -> None:
        self.backend = backend
        self.namespace = _validate_part("namespace", namespace)
        self._from_dict = from_dict
        self._to_dict = to_dict
        self._max_retries = max_retries

    def get(self, key: str) -> T | None:
        record = self.backend.get(self.namespace, key)
        return None if record is None else self._from_dict(record.value)

    def create(self, key: str, value: T) -> bool:
        return self.backend.create(self.namespace, key, self._to_dict(value))

    def set(self, key: str, value: T) -> None:
        payload = self._to_dict(value)
        for attempt in range(self._max_retries):
            current = self.backend.get(self.namespace, key)
            if current is None:
                if self.backend.create(self.namespace, key, payload):
                    return
            elif self.backend.compare_and_swap(
                self.namespace, key, current.version, payload
            ):
                return
            time.sleep(min(0.0001 * (attempt + 1), 0.005))
        raise AtomicStateContentionError(
            f"atomic state update for {self.namespace!r}/{key!r} exceeded retry limit"
        )

    def update(
        self,
        key: str,
        *,
        initial: Callable[[], T],
        mutate: Callable[[T], R],
    ) -> R:
        """Run a pure mutation with optimistic CAS, retrying on contention."""

        for attempt in range(self._max_retries):
            current = self.backend.get(self.namespace, key)
            value = initial() if current is None else self._from_dict(current.value)
            result = mutate(value)
            payload = self._to_dict(value)
            written = (
                self.backend.create(self.namespace, key, payload)
                if current is None
                else self.backend.compare_and_swap(
                    self.namespace, key, current.version, payload
                )
            )
            if written:
                return result
            time.sleep(min(0.0001 * (attempt + 1), 0.005))
        raise AtomicStateContentionError(
            f"atomic state update for {self.namespace!r}/{key!r} exceeded retry limit"
        )

    def update_optional(
        self,
        key: str,
        mutate: Callable[[T | None], tuple[T, R]],
    ) -> R:
        """Atomically create or replace an object chosen by ``mutate``."""

        for attempt in range(self._max_retries):
            current = self.backend.get(self.namespace, key)
            existing = None if current is None else self._from_dict(current.value)
            value, result = mutate(existing)
            payload = self._to_dict(value)
            written = (
                self.backend.create(self.namespace, key, payload)
                if current is None
                else self.backend.compare_and_swap(
                    self.namespace, key, current.version, payload
                )
            )
            if written:
                return result
            time.sleep(min(0.0001 * (attempt + 1), 0.005))
        raise AtomicStateContentionError(
            f"atomic state update for {self.namespace!r}/{key!r} exceeded retry limit"
        )

    def list_all(self) -> list[T]:
        return [self._from_dict(record.value) for _, record in self.backend.scan(self.namespace)]


__all__ = [
    "AtomicStateBackend",
    "AtomicStateContentionError",
    "AtomicStateError",
    "AtomicStateRecord",
    "FileAtomicStateBackend",
    "InMemoryAtomicStateBackend",
    "NamespacedAtomicStorage",
    "PostgresAtomicStateBackend",
    "RedisAtomicStateBackend",
]
