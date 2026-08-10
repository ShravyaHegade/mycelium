"""TTL cache used by @protect and Session (ToolCache)."""

from __future__ import annotations

from unittest.mock import patch

from mycelium.cache import ToolCache, default_cache


def test_get_miss_returns_none() -> None:
    cache = ToolCache()
    assert cache.get("read_balance", None) is None


def test_set_get_roundtrip() -> None:
    cache = ToolCache()
    cache.set("read_balance", None, {"balance": 100}, ttl=60)
    assert cache.get("read_balance", None) == {"balance": 100}


def test_entity_id_scopes_keys() -> None:
    cache = ToolCache()
    cache.set("read_balance", "acct_1", {"balance": 100}, ttl=60)
    assert cache.get("read_balance", "acct_1") == {"balance": 100}
    assert cache.get("read_balance", "acct_2") is None
    assert cache.get("read_balance", None) is None


def test_same_tool_different_entity_are_isolated() -> None:
    cache = ToolCache()
    cache.set("read_balance", "acct_1", {"balance": 100}, ttl=60)
    cache.set("read_balance", "acct_2", {"balance": 200}, ttl=60)
    assert cache.get("read_balance", "acct_1") == {"balance": 100}
    assert cache.get("read_balance", "acct_2") == {"balance": 200}


def test_expired_entry_evicted_on_get() -> None:
    import time

    cache = ToolCache()
    now = time.monotonic()
    cache.set("read_balance", None, {"balance": 100}, ttl=60)
    with patch("mycelium.cache.time.monotonic", return_value=now + 1):
        assert cache.get("read_balance", None) == {"balance": 100}
    with patch("mycelium.cache.time.monotonic", return_value=now + 1000):
        assert cache.get("read_balance", None) is None
        assert cache.get("read_balance", None) is None


def test_clear_removes_entry() -> None:
    cache = ToolCache()
    cache.set("read_balance", "acct_1", {"balance": 100}, ttl=60)
    cache.clear("read_balance", "acct_1")
    assert cache.get("read_balance", "acct_1") is None
    cache.clear("read_balance", "acct_1")


def test_default_cache_is_shared_singleton() -> None:
    assert default_cache is default_cache
