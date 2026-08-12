"""Redis Streams-backed append-only outcome storage."""

from __future__ import annotations

import json
from typing import Any

from mycelium.outcome_emit import OutcomeRow, OutcomeStorage
from mycelium.storage._helpers import redact_secrets


def _require_redis() -> Any:
    try:
        import redis
    except ImportError as exc:
        raise ImportError(
            "Redis outcome storage requires the 'redis' package. "
            "Install with: pip install 'mycelium-runtime[redis]'"
        ) from exc
    return redis


class RedisOutcomeStorage(OutcomeStorage):
    """Append-only Redis Streams store for outcome rows.

    Uses a stream for concurrent distributed writers and a sibling dedup key
    per ``event_id`` so retried emissions do not create duplicate entries.
    Append uses a Redis ``WATCH``/``MULTI`` transaction so claim + ``XADD``
    are atomic without relying on process-local locks.

    **Persistence:** Mycelium cannot inspect the Redis server's durability
    settings. Production deployments must enable AOF (or an equivalently
    durable Redis configuration) and set ``outcome_emit.persistence:
    required`` as an explicit acknowledgement. Without that, Mycelium will
    not treat Redis outcome storage as production-durable.
    """

    def __init__(
        self,
        url: str,
        *,
        key_prefix: str = "mycelium:outcomes",
    ) -> None:
        redis = _require_redis()
        prefix = str(key_prefix).rstrip(":")
        if not prefix:
            raise ValueError("Redis outcome storage key_prefix must be non-empty")
        self._url = url
        self._prefix = prefix
        self._stream_key = f"{prefix}:stream"
        self._dedup_prefix = f"{prefix}:seen:"
        try:
            self._client = redis.Redis.from_url(url, decode_responses=True)
        except Exception as exc:
            raise self._wrap_error(exc, action="connect") from exc

    def _dedup_key(self, event_id: str) -> str:
        return f"{self._dedup_prefix}{event_id}"

    def _wrap_error(self, exc: BaseException, *, action: str) -> RuntimeError:
        detail = redact_secrets(str(exc))
        return RuntimeError(
            f"Redis outcome storage failed during {action} "
            f"(prefix={self._prefix!r}): {detail}. "
            "Check connectivity and that Redis Streams commands are available."
        )

    def append(self, row: OutcomeRow) -> None:
        if not row.event_id:
            raise ValueError(
                "RedisOutcomeStorage.append requires OutcomeRow.event_id "
                "(OutcomeEmitter mints one automatically)"
            )
        from redis.exceptions import WatchError

        payload = json.dumps(row.to_dict(), default=str)
        dedup_key = self._dedup_key(row.event_id)
        try:
            for _ in range(32):
                try:
                    with self._client.pipeline(transaction=True) as pipe:
                        pipe.watch(dedup_key)
                        if pipe.get(dedup_key) is not None:
                            return
                        pipe.multi()
                        pipe.set(dedup_key, "1")
                        pipe.xadd(
                            self._stream_key,
                            {
                                "event_id": row.event_id,
                                "ts": str(row.ts),
                                "payload": payload,
                            },
                        )
                        pipe.execute()
                        return
                except WatchError:
                    continue
            # Contended for too long — last check for idempotent success.
            if self._client.get(dedup_key) is not None:
                return
            raise RuntimeError(
                "Redis outcome append could not claim event_id after retries"
            )
        except Exception as exc:
            raise self._wrap_error(exc, action="append") from exc

    def list_all(self) -> list[OutcomeRow]:
        try:
            entries = self._client.xrange(self._stream_key)
        except Exception as exc:
            raise self._wrap_error(exc, action="list_all") from exc
        rows: list[OutcomeRow] = []
        for _entry_id, fields in entries:
            raw = fields.get("payload")
            if raw is None:
                continue
            try:
                rows.append(OutcomeRow.from_dict(json.loads(raw)))
            except Exception as exc:
                raise ValueError(
                    f"malformed Redis outcome payload under "
                    f"{self._stream_key!r}: {redact_secrets(str(exc))}"
                ) from exc
        rows.sort(
            key=lambda row: (
                float(row.ts),
                row.event_id or "",
            )
        )
        return rows


__all__ = ["RedisOutcomeStorage"]
