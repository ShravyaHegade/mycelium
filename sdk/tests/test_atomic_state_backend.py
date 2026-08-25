"""Conformance and guard-adapter tests for the unified state backend."""

from __future__ import annotations

import threading
import uuid
from pathlib import Path

import pytest

from mycelium import (
    AtomicAuditReceiptStorage,
    AtomicCompletionStorage,
    AtomicScopeGuardStorage,
    AuditReceiptError,
    AuditReceiptRecord,
    CompletionContract,
    ConfigError,
    FileAtomicStateBackend,
    InMemoryAtomicStateBackend,
    NamespacedAtomicStorage,
    PostgresAtomicStateBackend,
    RedisAtomicStateBackend,
    ScopeGrant,
    ScopeGuard,
    ScopeWidenRefusedError,
    load_config_from_string,
)


@pytest.fixture(params=["memory", "file"])
def backend(request: pytest.FixtureRequest, tmp_path: Path):
    if request.param == "memory":
        return InMemoryAtomicStateBackend()
    return FileAtomicStateBackend(tmp_path / "state.json")


def test_atomic_backend_contract_and_namespace_isolation(backend) -> None:
    assert backend.get("a", "key") is None
    assert backend.create("a", "key", {"value": 1})
    assert not backend.create("a", "key", {"value": 2})
    assert backend.create("b", "key", {"value": 9})

    record = backend.get("a", "key")
    assert record is not None
    assert record.version == 1
    assert record.value == {"value": 1}
    assert not backend.compare_and_swap("a", "key", 99, {"value": 2})
    assert backend.compare_and_swap("a", "key", 1, {"value": 2})
    assert backend.get("a", "key").version == 2
    assert backend.get("b", "key").value == {"value": 9}
    assert [key for key, _ in backend.scan("a")] == ["key"]
    assert not backend.delete("a", "key", expected_version=1)
    assert backend.delete("a", "key", expected_version=2)


def test_namespaced_update_does_not_lose_concurrent_increments(backend) -> None:
    storage = NamespacedAtomicStorage(
        backend,
        "counter",
        from_dict=dict,
        to_dict=dict,
    )

    def increment() -> None:
        for _ in range(50):
            storage.update(
                "shared",
                initial=lambda: {"count": 0},
                mutate=lambda value: value.update(count=value["count"] + 1),
            )

    threads = [threading.Thread(target=increment) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert storage.get("shared") == {"count": 200}


def test_completion_marks_from_multiple_workers_are_preserved() -> None:
    backend = InMemoryAtomicStateBackend()
    storage = AtomicCompletionStorage(backend)
    first = CompletionContract(storage, required=["a", "b"])
    second = CompletionContract(storage, required=["a", "b"])
    barrier = threading.Barrier(2)

    def mark(contract: CompletionContract, subtask: str) -> None:
        barrier.wait()
        contract.mark(subtask, "success", scope_key="run")

    threads = [
        threading.Thread(target=mark, args=(first, "a")),
        threading.Thread(target=mark, args=(second, "b")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    state = storage.get("run")
    assert state is not None
    assert set(state.marks) == {"a", "b"}


def test_scope_cannot_be_widened_through_shared_backend() -> None:
    backend = InMemoryAtomicStateBackend()
    storage = AtomicScopeGuardStorage(backend)
    first = ScopeGuard(storage)
    second = ScopeGuard(storage)
    first.bind("run", ScopeGrant(allowed_tools=frozenset({"read"})))
    with pytest.raises(ScopeWidenRefusedError):
        second.bind("run", ScopeGrant(allowed_tools=frozenset({"read", "write"})))


def test_audit_receipts_are_create_only() -> None:
    backend = InMemoryAtomicStateBackend()
    storage = AtomicAuditReceiptStorage(backend)
    receipt = AuditReceiptRecord(
        receipt_id="receipt-1",
        agent_id="agent",
        action="charge",
        action_kind="tool",
        request_id="request",
        inputs={},
        outputs={"ok": True},
        status="completed",
        timestamp=1.0,
        signature="sig",
    )
    storage.append(receipt)
    storage.append(receipt)
    changed = AuditReceiptRecord(**{**receipt.to_dict(), "signature": "different"})
    with pytest.raises(AuditReceiptError, match="different content"):
        storage.append(changed)


def test_global_state_backend_is_automatically_used_by_guards(tmp_path: Path) -> None:
    cfg = load_config_from_string(
        f"""
state_backend:
  storage: file
  path: {tmp_path / "shared-state.json"}
  namespace: app
loop_guard: {{}}
scope_guard:
  allowed_tools: [read]
completion:
  required: [done]
state_flush: {{}}
transition:
  agent_id: test
  policy_version: "1"
audit_receipt:
  signing_key: test-key
tools:
  read: {{}}
"""
    )

    assert type(cfg.build_loop_guard().storage).__name__ == "AtomicLoopGuardStorage"
    assert type(cfg.build_scope_guard().storage).__name__ == "AtomicScopeGuardStorage"
    assert type(cfg.build_completion_contract().storage).__name__ == "AtomicCompletionStorage"
    assert type(cfg.build_state_flush().storage).__name__ == "AtomicStateFlushStorage"
    assert type(cfg.build_audit_receipt().storage).__name__ == "AtomicAuditReceiptStorage"


def test_explicit_shared_storage_requires_global_backend() -> None:
    with pytest.raises(ConfigError, match="requires state_backend"):
        load_config_from_string(
            """
loop_guard:
  storage: shared
tools: {}
"""
        )


def test_redis_backend_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    fakeredis = pytest.importorskip("fakeredis")
    import redis

    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis.Redis, "from_url", lambda url, **kwargs: fake)
    backend = RedisAtomicStateBackend("redis://unused", prefix="test:state:")

    assert backend.create("scope", "run", {"allowed": ["read"]})
    assert not backend.compare_and_swap("scope", "run", 2, {"allowed": []})
    assert backend.compare_and_swap("scope", "run", 1, {"allowed": []})
    assert backend.get("scope", "run").value == {"allowed": []}
    assert backend.scan("scope")[0][0] == "run"


def test_live_redis_backend_contract() -> None:
    from backend_gates import require_redis_or_skip

    url = require_redis_or_skip()
    prefix = f"mycelium:test-state:{uuid.uuid4().hex}:"
    backend = RedisAtomicStateBackend(url, prefix=prefix)
    assert backend.create("scope", "run", {"value": 1})
    assert backend.compare_and_swap("scope", "run", 1, {"value": 2})
    assert backend.get("scope", "run").value == {"value": 2}
    assert backend.delete("scope", "run", expected_version=2)


def test_live_postgres_backend_contract() -> None:
    from backend_gates import require_postgres_dsn_or_skip

    dsn = require_postgres_dsn_or_skip()
    backend = PostgresAtomicStateBackend(dsn, table="mycelium_test_state")
    namespace = f"integration:{uuid.uuid4().hex}"
    assert backend.create(namespace, "run", {"value": 1})
    assert backend.compare_and_swap(namespace, "run", 1, {"value": 2})
    assert backend.get(namespace, "run").value == {"value": 2}
    assert backend.delete(namespace, "run", expected_version=2)
