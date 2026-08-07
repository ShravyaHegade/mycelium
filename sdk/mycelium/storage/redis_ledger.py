"""Redis-backed ledger storage with atomic SET NX claim semantics."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any, TypeVar

from mycelium.storage._helpers import ClaimOutcome

E = TypeVar("E")


def _require_redis() -> Any:
    try:
        import redis
    except ImportError as exc:
        raise ImportError(
            "Redis storage requires the 'redis' package. "
            "Install with: pip install 'mycelium-runtime[redis]'"
        ) from exc
    return redis


class RedisEntryStorage:
    """Generic Redis KV store for ledger entries keyed by request_id."""

    def __init__(
        self,
        url: str,
        *,
        prefix: str,
        from_dict: Callable[[dict[str, Any]], E],
        in_flight_ttl: float | None = 604800.0,
    ) -> None:
        redis = _require_redis()
        self._client = redis.Redis.from_url(url, decode_responses=True)
        self._prefix = prefix
        self._from_dict = from_dict
        self._in_flight_ttl = in_flight_ttl

    def _key(self, request_id: str) -> str:
        return f"{self._prefix}{request_id}"

    def get(self, request_id: str) -> E | None:
        raw = self._client.get(self._key(request_id))
        if raw is None:
            return None
        return self._from_dict(json.loads(raw))

    def set(self, entry: E) -> None:
        payload = json.dumps(entry.to_dict(), default=str)
        key = self._key(entry.request_id)
        if entry.status == "in-flight" and self._in_flight_ttl:
            self._client.set(key, payload, ex=int(self._in_flight_ttl))
        else:
            self._client.set(key, payload)
            if entry.status != "in-flight":
                self._client.persist(key)

    def try_claim_inflight(
        self,
        entry: E,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[ClaimOutcome, E | None]:
        from mycelium.storage._helpers import claim_inflight_outcome, with_lease

        key = self._key(entry.request_id)
        ttl = int(max(self._in_flight_ttl or lease_ttl or 0, lease_ttl * 4))

        for _ in range(32):
            existing_raw = self._client.get(key)
            if existing_raw is None:
                leased = with_lease(entry, now=time.time(), lease_ttl=lease_ttl)
                payload = json.dumps(leased.to_dict(), default=str)
                if ttl > 0:
                    claimed = self._client.set(key, payload, nx=True, ex=ttl)
                else:
                    claimed = self._client.set(key, payload, nx=True)
                if claimed:
                    return "claimed", None
                continue

            existing = self._from_dict(json.loads(existing_raw))
            now = time.time()
            outcome = claim_inflight_outcome(existing, now=now)
            if outcome == "completed":
                return "completed", existing
            if outcome == "in_flight":
                return "in_flight", existing

            reclaimed = self._try_reclaim(key, entry, ttl, lease_ttl)
            if reclaimed is not None:
                return reclaimed

        existing_raw = self._client.get(key)
        if existing_raw is None:
            return "claimed", None
        existing = self._from_dict(json.loads(existing_raw))
        if existing.status == "completed":
            return "completed", existing
        return "in_flight", existing

    def _try_reclaim(
        self,
        key: str,
        entry: E,
        ttl: int,
        lease_ttl: float,
    ) -> tuple[ClaimOutcome, E | None] | None:
        """CAS reclaim: only overwrite if the stored entry is still
        reclaimable per :func:`claim_inflight_outcome`. Returns ``None``
        when the watch fires (caller retries from the top)."""
        from redis.exceptions import WatchError

        from mycelium.storage._helpers import claim_inflight_outcome, with_lease

        now = time.time()
        leased = with_lease(entry, now=now, lease_ttl=lease_ttl)
        payload = json.dumps(leased.to_dict(), default=str)
        try:
            with self._client.pipeline(transaction=True) as pipe:
                pipe.watch(key)
                raw = pipe.get(key)
                if raw is None:
                    return ("claimed", None)
                current = self._from_dict(json.loads(raw))
                rerun = claim_inflight_outcome(current, now=time.time())
                if rerun != "claimed":
                    return rerun, current
                pipe.multi()
                if ttl > 0:
                    pipe.set(key, payload, ex=ttl)
                else:
                    pipe.set(key, payload)
                pipe.execute()
                return ("claimed", None)
        except WatchError:
            return None

    def try_transition(
        self,
        entry: E,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
    ) -> bool:
        from redis.exceptions import WatchError

        from mycelium.storage._helpers import lease_allows_renew

        key = self._key(entry.request_id)
        payload = json.dumps(entry.to_dict(), default=str)
        for _ in range(32):
            try:
                with self._client.pipeline(transaction=True) as pipe:
                    pipe.watch(key)
                    raw = pipe.get(key)
                    if raw is None:
                        return False
                    existing = json.loads(raw)
                    if existing.get("terminal_outcome") not in expected_terminal_outcomes:
                        return False
                    if expected_owner is not None and existing.get("owner") != expected_owner:
                        return False
                    if require_lease_held_at is not None and not lease_allows_renew(
                        existing.get("lease_until"),
                        now=require_lease_held_at,
                    ):
                        return False
                    pipe.multi()
                    pipe.set(key, payload)
                    pipe.execute()
                    return True
            except WatchError:
                continue
        return False

    def list_all(self) -> list[E]:
        pattern = f"{self._prefix}*"
        entries: list[E] = []
        for key in self._client.scan_iter(match=pattern):
            raw = self._client.get(key)
            if raw is not None:
                entries.append(self._from_dict(json.loads(raw)))
        return entries


class RedisLedgerStorage:
    """Redis storage for :class:`~mycelium.action_ledger.LedgerEntry`."""

    def __init__(
        self,
        url: str,
        *,
        prefix: str = "mycelium:action:",
        in_flight_ttl: float | None = 604800.0,
    ) -> None:
        from mycelium.action_ledger import LedgerEntry

        self._inner = RedisEntryStorage(
            url,
            prefix=prefix,
            from_dict=LedgerEntry.from_dict,
            in_flight_ttl=in_flight_ttl,
        )

    def get(self, request_id: str) -> Any:
        return self._inner.get(request_id)

    def set(self, entry: Any) -> None:
        self._inner.set(entry)

    def try_claim_inflight(
        self,
        entry: Any,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[ClaimOutcome, Any | None]:
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def list_all(self) -> list[Any]:
        return self._inner.list_all()

    def try_transition(
        self,
        entry: Any,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
    ) -> bool:
        return self._inner.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
        )


class RedisTaskLedgerStorage:
    """Redis storage for :class:`~mycelium.task_ledger.TaskLedgerEntry`."""

    def __init__(
        self,
        url: str,
        *,
        prefix: str = "mycelium:task:",
        in_flight_ttl: float | None = 604800.0,
    ) -> None:
        from mycelium.task_ledger import TaskLedgerEntry

        self._inner = RedisEntryStorage(
            url,
            prefix=prefix,
            from_dict=TaskLedgerEntry.from_dict,
            in_flight_ttl=in_flight_ttl,
        )

    def get(self, request_id: str) -> Any:
        return self._inner.get(request_id)

    def set(self, entry: Any) -> None:
        self._inner.set(entry)

    def try_claim_inflight(
        self,
        entry: Any,
        *,
        lease_ttl: float = 3600.0,
    ) -> tuple[ClaimOutcome, Any | None]:
        return self._inner.try_claim_inflight(entry, lease_ttl=lease_ttl)

    def try_transition(
        self,
        entry: Any,
        *,
        expected_terminal_outcomes: frozenset[str],
        expected_owner: str | None = None,
        require_lease_held_at: float | None = None,
    ) -> bool:
        return self._inner.try_transition(
            entry,
            expected_terminal_outcomes=expected_terminal_outcomes,
            expected_owner=expected_owner,
            require_lease_held_at=require_lease_held_at,
        )

    def list_all(self) -> list[Any]:
        return self._inner.list_all()
