"""Distributed outcome_emit backends: Postgres + Redis Streams.

Unit tests use fakeredis and an in-process Postgres stub so the default suite
never requires live services. Optional live gates reuse backend_gates helpers.
"""

from __future__ import annotations

import json
import os
import re
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from mycelium import (
    ConfigError,
    FileOutcomeStorage,
    InMemoryOutcomeStorage,
    OutcomeEmitError,
    OutcomeEmitter,
    OutcomeRow,
    PostgresOutcomeStorage,
    RedisOutcomeStorage,
    compute_dttr,
    load_config_from_string,
)
from mycelium.__main__ import main
from mycelium.outcome_emit import (
    EVENT_BODY_START,
    EVENT_RESOLUTION,
    GATE_ALLOW,
)
from mycelium.storage._helpers import redact_secrets


def _row(
    *,
    request_id: str = "req-1",
    event: str = EVENT_RESOLUTION,
    ts: float = 1.0,
    event_id: str | None = "evt-1",
    tool: str = "charge",
    run_id: str | None = "run-1",
    terminal_outcome: str | None = "COMPLETED",
) -> OutcomeRow:
    return OutcomeRow(
        ts=ts,
        agent_id="agent",
        tool=tool,
        request_id=request_id,
        event=event,
        gate=GATE_ALLOW,
        terminal_outcome=terminal_outcome,
        side_effect_class="keyed_mutate",
        tool_body_executed=event == EVENT_BODY_START,
        run_id=run_id,
        policy_version="2026.08.1",
        external_operation_ref="pi_123",
        resolution_reason="allow",
        parent_request_id="parent-1",
        handoff_id="handoff-1",
        event_id=event_id,
    )


# ---------------------------------------------------------------------------
# In-process Postgres stub (no live DB)
# ---------------------------------------------------------------------------


class _FakeSql:
    class Identifier:
        def __init__(self, name: str) -> None:
            self.name = name

    class SQL:
        def __init__(self, text: str) -> None:
            self.text = text

        def format(self, *parts: Any) -> _FakeSql.SQL:
            rendered = self.text
            for part in parts:
                token = part.name if isinstance(part, _FakeSql.Identifier) else str(part)
                rendered = rendered.replace("{}", token, 1)
            return _FakeSql.SQL(rendered)


class _FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or []

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _FakeConn:
    def __init__(self, store: dict[str, Any]) -> None:
        self._store = store

    def __enter__(self) -> _FakeConn:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, query: Any, params: tuple[Any, ...] | None = None) -> _FakeCursor:
        text = query.text if hasattr(query, "text") else str(query)
        with self._store["lock"]:
            if text.startswith("CREATE TABLE") or text.startswith("CREATE INDEX"):
                self._store["schema"] = True
                return _FakeCursor()
            if text.startswith("INSERT INTO"):
                assert params is not None
                event_id = params[0]
                payload = json.loads(params[6])
                if event_id not in self._store["by_id"]:
                    self._store["by_id"][event_id] = payload
                    self._store["order"].append(event_id)
                return _FakeCursor()
            if text.startswith("SELECT payload"):
                ordered = sorted(
                    self._store["by_id"].values(),
                    key=lambda row: (float(row["ts"]), row.get("event_id") or ""),
                )
                return _FakeCursor([(row,) for row in ordered])
        raise AssertionError(f"unexpected SQL: {text}")

    def commit(self) -> None:
        return None


class _FakePsycopg:
    def __init__(self, store: dict[str, Any]) -> None:
        self._store = store

    def connect(self, dsn: str) -> _FakeConn:
        self._store["dsns"].append(dsn)
        if self._store.get("fail_connect"):
            raise OSError(
                f'connection to server at "db.example" failed: '
                f"password authentication failed for user "
                f'"alice" dsn={dsn}'
            )
        if self._store.get("fail_write") and self._store.get("schema"):
            # Allow schema ensure, fail subsequent writes.
            conn = _FakeConn(self._store)

            def boom(query: Any, params: tuple[Any, ...] | None = None) -> _FakeCursor:
                text = query.text if hasattr(query, "text") else str(query)
                if text.startswith("INSERT"):
                    raise OSError(f"permission denied for table; dsn={dsn}")
                return _FakeConn.execute(conn, query, params)

            conn.execute = boom  # type: ignore[method-assign]
            return conn
        return _FakeConn(self._store)


def _install_fake_postgres(
    monkeypatch: pytest.MonkeyPatch,
    *,
    fail_connect: bool = False,
    fail_write: bool = False,
) -> dict[str, Any]:
    store: dict[str, Any] = {
        "by_id": {},
        "order": [],
        "lock": threading.Lock(),
        "schema": False,
        "dsns": [],
        "fail_connect": fail_connect,
        "fail_write": fail_write,
    }
    fake = _FakePsycopg(store)

    def _require() -> tuple[Any, Any]:
        return fake, _FakeSql

    monkeypatch.setattr(
        "mycelium.storage.postgres_outcome._require_psycopg",
        _require,
    )
    return store


def _fake_redis(monkeypatch: pytest.MonkeyPatch):
    fakeredis = pytest.importorskip("fakeredis")
    fake = fakeredis.FakeRedis(decode_responses=True)

    def from_url(url: str, **kwargs: object) -> object:
        return fake

    import redis

    monkeypatch.setattr(redis.Redis, "from_url", from_url)
    return fake


# ---------------------------------------------------------------------------
# Round-trip / ordering / dedupe
# ---------------------------------------------------------------------------


def test_postgres_outcome_round_trip_all_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch)
    storage = PostgresOutcomeStorage("postgresql://alice:s3cret@db/mycelium")
    original = _row()
    storage.append(original)
    restored = storage.list_all()
    assert len(restored) == 1
    assert restored[0] == original


def test_redis_outcome_round_trip_all_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_redis(monkeypatch)
    storage = RedisOutcomeStorage("redis://:s3cret@localhost/0")
    original = _row(event_id="redis-evt-1")
    storage.append(original)
    restored = storage.list_all()
    assert len(restored) == 1
    assert restored[0] == original


def test_postgres_retry_deduplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    store = _install_fake_postgres(monkeypatch)
    storage = PostgresOutcomeStorage("postgresql://localhost/mycelium")
    row = _row(event_id="same")
    storage.append(row)
    storage.append(row)
    assert len(store["by_id"]) == 1
    assert len(storage.list_all()) == 1


def test_redis_retry_deduplicates(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_redis(monkeypatch)
    storage = RedisOutcomeStorage("redis://localhost")
    row = _row(event_id="same-redis")
    storage.append(row)
    storage.append(row)
    assert len(storage.list_all()) == 1


def test_postgres_stable_retrieval_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch)
    storage = PostgresOutcomeStorage("postgresql://localhost/mycelium")
    storage.append(_row(event_id="b", ts=2.0, request_id="r2"))
    storage.append(_row(event_id="a", ts=1.0, request_id="r1"))
    storage.append(_row(event_id="c", ts=1.0, request_id="r1b"))
    ids = [row.event_id for row in storage.list_all()]
    assert ids == ["a", "c", "b"]


def test_redis_stable_retrieval_order(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_redis(monkeypatch)
    storage = RedisOutcomeStorage("redis://localhost", key_prefix="mycelium:outcomes")
    storage.append(_row(event_id="b", ts=2.0, request_id="r2"))
    storage.append(_row(event_id="a", ts=1.0, request_id="r1"))
    storage.append(_row(event_id="c", ts=1.0, request_id="r1b"))
    ids = [row.event_id for row in storage.list_all()]
    assert ids == ["a", "c", "b"]


def test_postgres_concurrent_writers(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_postgres(monkeypatch)
    storage = PostgresOutcomeStorage("postgresql://localhost/mycelium")
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def write(i: int) -> None:
        try:
            barrier.wait()
            storage.append(_row(event_id=f"evt-{i}", ts=float(i), request_id=f"r{i}"))
        except BaseException as exc:  # noqa: BLE001 - collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(storage.list_all()) == 8


def test_redis_concurrent_writers(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_redis(monkeypatch)
    storage = RedisOutcomeStorage("redis://localhost")
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def write(i: int) -> None:
        try:
            barrier.wait()
            storage.append(_row(event_id=f"evt-{i}", ts=float(i), request_id=f"r{i}"))
        except BaseException as exc:  # noqa: BLE001 - collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert len(storage.list_all()) == 8


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_production_accepts_postgres_outcome_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://alice:s3cret@db/mycelium")
    _install_fake_postgres(monkeypatch)
    cfg = load_config_from_string(
        """
profile: production
outcome_emit:
  storage: postgres
  url: ${DATABASE_URL}
  table: mycelium_outcomes
  on_failure: error
tools:
  ping: {}
"""
    )
    emitter = cfg.build_outcome_emitter()
    assert emitter is not None
    assert emitter.fail_closed
    assert isinstance(emitter.storage, PostgresOutcomeStorage)


def test_production_accepts_redis_with_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REDIS_URL", "redis://:s3cret@localhost/0")
    _fake_redis(monkeypatch)
    cfg = load_config_from_string(
        """
profile: production
outcome_emit:
  storage: redis
  url_env: REDIS_URL
  key_prefix: mycelium:outcomes
  persistence: required
  on_failure: error
tools:
  ping: {}
"""
    )
    emitter = cfg.build_outcome_emitter()
    assert isinstance(emitter.storage, RedisOutcomeStorage)


def test_production_rejects_redis_without_persistence_ack() -> None:
    with pytest.raises(ConfigError, match="persistence: required"):
        load_config_from_string(
            """
profile: production
outcome_emit:
  storage: redis
  url: redis://localhost
tools:
  ping: {}
"""
        )


def test_production_rejects_memory_and_unknown() -> None:
    with pytest.raises(ConfigError, match="memory storage"):
        load_config_from_string(
            "profile: production\noutcome_emit:\n  storage: memory\ntools:\n  ping: {}\n"
        )
    with pytest.raises(ConfigError, match="unknown outcome_emit storage"):
        load_config_from_string(
            "profile: production\noutcome_emit:\n  storage: s3\ntools:\n  ping: {}\n"
        )


def test_production_rejects_incomplete_postgres() -> None:
    with pytest.raises(ConfigError, match="incomplete"):
        load_config_from_string(
            """
profile: production
outcome_emit:
  storage: postgres
  table: mycelium_outcomes
tools:
  ping: {}
"""
        )


def test_production_rejects_incomplete_redis() -> None:
    with pytest.raises(ConfigError, match="incomplete"):
        load_config_from_string(
            """
profile: production
outcome_emit:
  storage: redis
  persistence: required
tools:
  ping: {}
"""
        )


def test_development_memory_and_file_unchanged(tmp_path: Path) -> None:
    mem = load_config_from_string(
        "outcome_emit:\n  storage: memory\ntools:\n  ping: {}\n"
    )
    assert isinstance(mem.build_outcome_emitter().storage, InMemoryOutcomeStorage)
    path = tmp_path / "o.jsonl"
    file_cfg = load_config_from_string(
        f"outcome_emit:\n  storage: file\n  path: {path}\ntools:\n  ping: {{}}\n"
    )
    assert isinstance(file_cfg.build_outcome_emitter().storage, FileOutcomeStorage)


def test_postgres_invalid_table_name_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch)
    with pytest.raises(ConfigError, match="invalid Postgres table name"):
        load_config_from_string(
            """
outcome_emit:
  storage: postgres
  dsn: postgresql://localhost/mycelium
  table: "outcomes;drop"
tools:
  ping: {}
"""
        ).build_outcome_emitter()


# ---------------------------------------------------------------------------
# Failures / credentials / semantics
# ---------------------------------------------------------------------------


def test_redact_secrets_strips_password_material() -> None:
    raw = "postgresql://alice:s3cret@db.example:5432/mycelium password=s3cret"
    cleaned = redact_secrets(raw)
    assert "s3cret" not in cleaned
    assert "***" in cleaned


def test_postgres_connection_error_redacts_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch, fail_connect=True)
    storage = PostgresOutcomeStorage(
        "postgresql://alice:s3cret@db.example/mycelium"
    )
    with pytest.raises(RuntimeError, match="Postgres outcome storage failed") as info:
        storage.append(_row())
    message = str(info.value)
    assert "s3cret" not in message
    assert "alice:s3cret" not in message


def test_redis_write_failure_surfaces_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _fake_redis(monkeypatch)
    storage = RedisOutcomeStorage("redis://:s3cret@localhost/0")

    def boom(*args: object, **kwargs: object) -> object:
        raise OSError(
            "READONLY You can't write against a read only replica "
            "redis://:s3cret@localhost/0"
        )

    monkeypatch.setattr(fake, "pipeline", boom)
    with pytest.raises(RuntimeError, match="Redis outcome storage failed") as info:
        storage.append(_row(event_id="e1"))
    assert "s3cret" not in str(info.value)


def test_production_emit_failure_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch, fail_write=True)
    monkeypatch.setenv("DATABASE_URL", "postgresql://alice:s3cret@db/mycelium")
    cfg = load_config_from_string(
        """
profile: production
outcome_emit:
  storage: postgres
  url_env: DATABASE_URL
tools:
  ping: {}
"""
    )
    emitter = cfg.build_outcome_emitter()
    assert emitter is not None
    with pytest.raises(OutcomeEmitError):
        emitter.emit_event(tool="t", request_id="r", event=EVENT_RESOLUTION)


def test_emit_failure_does_not_replace_tool_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mycelium import InMemoryLedgerStorage, SideEffectClass, ToolTransitionBinding, ledger_sync

    class _Boom:
        def append(self, row: OutcomeRow) -> None:
            if row.event == "body_fail":
                raise OSError("outcome down")
            self._rows.append(row)

        def __init__(self) -> None:
            self._rows: list[OutcomeRow] = []

        def list_all(self) -> list[OutcomeRow]:
            return list(self._rows)

    binding = ToolTransitionBinding.for_tool(
        agent_id="a",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )

    @ledger_sync(
        storage=InMemoryLedgerStorage(),
        transition_binding=binding,
        outcome_emitter=OutcomeEmitter(
            "a", storage=_Boom(), on_failure="error"
        ),
    )
    def explode(amount: int) -> str:
        raise RuntimeError("provider down")

    with pytest.raises(RuntimeError, match="provider down"):
        explode(1, request_id="charge:ORD-keep")


def test_dttr_compatible_with_postgres_and_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch)
    pg = PostgresOutcomeStorage("postgresql://localhost/mycelium")
    _fake_redis(monkeypatch)
    rd = RedisOutcomeStorage("redis://localhost")
    for storage in (pg, rd):
        storage.append(
            _row(event_id=f"{id(storage)}-1", ts=0.0, event=EVENT_RESOLUTION)
        )
        storage.append(
            replace(
                _row(
                    event_id=f"{id(storage)}-2",
                    ts=1.0,
                    event=EVENT_BODY_START,
                    terminal_outcome="IN_FLIGHT",
                ),
                tool_body_executed=True,
            )
        )
        report = compute_dttr(storage.list_all())
        assert report.transitions == 1
        assert report.dttr == 0.0


def test_cli_outcomes_dttr_reads_postgres_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    store = _install_fake_postgres(monkeypatch)
    cfg_path = tmp_path / "mycelium.yaml"
    cfg_path.write_text(
        """
outcome_emit:
  storage: postgres
  dsn: postgresql://localhost/mycelium
  table: mycelium_outcomes
""",
        encoding="utf-8",
    )
    # Seed via storage built the same way as CLI.
    from mycelium.config import MyceliumConfig

    storage = MyceliumConfig._build_outcome_storage(
        {"storage": "postgres", "dsn": "postgresql://localhost/mycelium"}
    )
    storage.append(_row(event_id="cli-1"))
    assert store["by_id"]
    assert main(["outcomes", "dttr", "-c", str(cfg_path), "--json"]) == 0


def test_file_and_memory_regression(tmp_path: Path) -> None:
    mem = InMemoryOutcomeStorage()
    mem.append(_row(event_id="m1"))
    assert len(mem.list_all()) == 1
    path = tmp_path / "outcomes.jsonl"
    file_storage = FileOutcomeStorage(path)
    file_storage.append(_row(event_id="f1"))
    file_storage.append(_row(event_id="f1"))  # file does not dedupe
    assert len(file_storage.list_all()) == 2


def test_emitter_mints_event_id_for_backends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_fake_postgres(monkeypatch)
    emitter = OutcomeEmitter(
        "a",
        storage=PostgresOutcomeStorage("postgresql://localhost/mycelium"),
    )
    emitter.emit_event(tool="t", request_id="r", event=EVENT_RESOLUTION)
    rows = emitter.storage.list_all()
    assert rows[0].event_id
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        rows[0].event_id or "",
    )


def test_malformed_row_rejected_by_from_dict() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        OutcomeRow.from_dict({"ts": 1.0, "tool": "t"})


# ---------------------------------------------------------------------------
# Optional live integration
# ---------------------------------------------------------------------------


def test_live_postgres_outcome_round_trip() -> None:
    from backend_gates import require_postgres_dsn_or_skip

    dsn = require_postgres_dsn_or_skip()
    table = "mycelium_test_outcomes"
    storage = PostgresOutcomeStorage(dsn, table=table)
    event_id = f"live-pg-{os.getpid()}"
    storage.append(_row(event_id=event_id, request_id=event_id))
    found = [row for row in storage.list_all() if row.event_id == event_id]
    assert len(found) == 1
    storage.append(_row(event_id=event_id, request_id=event_id))
    found_again = [row for row in storage.list_all() if row.event_id == event_id]
    assert len(found_again) == 1


def test_live_redis_outcome_round_trip() -> None:
    from backend_gates import require_redis_or_skip

    url = require_redis_or_skip()
    prefix = f"mycelium:test-outcomes:{os.getpid()}"
    storage = RedisOutcomeStorage(url, key_prefix=prefix)
    event_id = f"live-redis-{os.getpid()}"
    storage.append(_row(event_id=event_id, request_id=event_id))
    found = [row for row in storage.list_all() if row.event_id == event_id]
    assert len(found) == 1
    storage.append(_row(event_id=event_id, request_id=event_id))
    assert len([row for row in storage.list_all() if row.event_id == event_id]) == 1
