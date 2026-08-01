"""OutcomeEmitter: compact, append-only resolution telemetry + the DTTR metric.

Rows are flat and warehouse-friendly (one JSON object per line) and are
emitted only on resolution events — never per poll tick — so a small store
stays small. Emitters are fault-tolerant by design: an append failure is
logged and swallowed so telemetry can never break the tool path.

The Duplicate Tool Transition Rate (DTTR) is computed after the fact from
the rows: it is the number of *silent duplicate* tool-body executions divided
by the number of transitions that were long-running or redispatched. See
:func:`compute_dttr` for the exact definitions.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mycelium.storage.file_lock import PathFileLock

_logger = logging.getLogger(__name__)

# Discrete resolution events an emitter row can carry.
EVENT_RESOLUTION = "resolution"      # a claim/dispatch resolved to a gate
EVENT_BODY_START = "body_start"      # tool body began executing
EVENT_BODY_COMPLETE = "body_complete"  # tool body returned successfully
EVENT_BODY_FAIL = "body_fail"        # tool body raised; failure recorded
EVENT_RELEASE = "release"            # operator release recorded

# Coarse gate names attached to resolution events.
GATE_ALLOW = "ALLOW"
GATE_RETURN = "RETURN"
GATE_HARD_BLOCK = "HARD_BLOCK"
GATE_SOFT_BLOCK = "SOFT_BLOCK"
GATE_RELEASE = "RELEASE"


@dataclass(frozen=True)
class OutcomeRow:
    """One flat telemetry row describing a transition resolution event.

    ``tool_body_executed`` is True exactly on ``body_start`` rows — counting
    those rows per transition yields the tool-body execution count used by
    :func:`compute_dttr`. ``authorized_reexec`` is True only when that body
    run was authorized by a consumed ``NOT_EXECUTED`` verdict (reconciler or
    operator release), i.e. it is not a silent duplicate.
    """

    ts: float
    agent_id: str
    tool: str
    request_id: str
    event: str
    gate: str | None = None
    terminal_outcome: str | None = None
    side_effect_boundary: str | None = None
    side_effect_class: str | None = None
    tool_body_executed: bool = False
    dispatch_attempt: int | None = None
    authorized_reexec: bool = False
    owner: str | None = None
    error_class: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "agent_id": self.agent_id,
            "tool": self.tool,
            "request_id": self.request_id,
            "event": self.event,
            "gate": self.gate,
            "terminal_outcome": self.terminal_outcome,
            "side_effect_boundary": self.side_effect_boundary,
            "side_effect_class": self.side_effect_class,
            "tool_body_executed": self.tool_body_executed,
            "dispatch_attempt": self.dispatch_attempt,
            "authorized_reexec": self.authorized_reexec,
            "owner": self.owner,
            "error_class": self.error_class,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OutcomeRow:
        return cls(
            ts=float(data["ts"]),
            agent_id=str(data["agent_id"]),
            tool=str(data["tool"]),
            request_id=str(data["request_id"]),
            event=str(data["event"]),
            gate=data.get("gate"),
            terminal_outcome=data.get("terminal_outcome"),
            side_effect_boundary=data.get("side_effect_boundary"),
            side_effect_class=data.get("side_effect_class"),
            tool_body_executed=bool(data.get("tool_body_executed", False)),
            dispatch_attempt=data.get("dispatch_attempt"),
            authorized_reexec=bool(data.get("authorized_reexec", False)),
            owner=data.get("owner"),
            error_class=data.get("error_class"),
        )


class OutcomeStorage:
    def append(self, row: OutcomeRow) -> None:
        raise NotImplementedError

    def list_all(self) -> list[OutcomeRow]:
        raise NotImplementedError


class InMemoryOutcomeStorage(OutcomeStorage):
    def __init__(self) -> None:
        self._rows: list[OutcomeRow] = []

    def append(self, row: OutcomeRow) -> None:
        self._rows.append(row)

    def list_all(self) -> list[OutcomeRow]:
        return list(self._rows)


class FileOutcomeStorage(OutcomeStorage):
    """Append-only NDJSON outcome log with ``fcntl`` locking.

    Mirrors :class:`FileAuditReceiptStorage`; rows are one JSON object per
    line. Malformed lines are skipped with a warning so a torn write never
    breaks the reader.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = PathFileLock(self._path)

    def append(self, row: OutcomeRow) -> None:
        with self._lock.acquire():
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(row.to_dict(), default=str) + "\n")

    def list_all(self) -> list[OutcomeRow]:
        with self._lock.acquire():
            if not self._path.exists():
                return []
            rows: list[OutcomeRow] = []
            with self._path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(OutcomeRow.from_dict(json.loads(line)))
                    except Exception:
                        _logger.warning(
                            "skipping malformed outcome row: %s", line
                        )
            return rows


class OutcomeEmitter:
    """Fault-tolerant sink for :class:`OutcomeRow` telemetry.

    ``emit``/``emit_event`` never raise: a storage failure is logged and
    swallowed so emission can never break the tool path.
    """

    def __init__(
        self,
        agent_id: str,
        storage: OutcomeStorage | None = None,
    ) -> None:
        self.agent_id = agent_id
        self._storage = storage if storage is not None else InMemoryOutcomeStorage()

    @property
    def storage(self) -> OutcomeStorage:
        return self._storage

    def emit(self, row: OutcomeRow) -> None:
        try:
            self._storage.append(row)
        except Exception:
            _logger.exception(
                "outcome emitter storage failed for %s/%s",
                row.request_id,
                row.event,
            )

    def emit_event(
        self,
        *,
        tool: str,
        request_id: str,
        event: str,
        gate: str | None = None,
        terminal_outcome: str | None = None,
        side_effect_boundary: str | None = None,
        side_effect_class: str | None = None,
        tool_body_executed: bool = False,
        dispatch_attempt: int | None = None,
        authorized_reexec: bool = False,
        owner: str | None = None,
        error_class: str | None = None,
    ) -> None:
        self.emit(
            OutcomeRow(
                ts=time.time(),
                agent_id=self.agent_id,
                tool=tool,
                request_id=request_id,
                event=event,
                gate=gate,
                terminal_outcome=terminal_outcome,
                side_effect_boundary=side_effect_boundary,
                side_effect_class=side_effect_class,
                tool_body_executed=tool_body_executed,
                dispatch_attempt=dispatch_attempt,
                authorized_reexec=authorized_reexec,
                owner=owner,
                error_class=error_class,
            )
        )


@dataclass(frozen=True)
class TransitionDttr:
    """DTTR breakdown for a single transition (one request_id)."""

    request_id: str
    tool: str
    body_executions: int
    authorized_reexecs: int
    silent_duplicates: int
    resolution_events: int
    duration_seconds: float
    long_running_or_redispatched: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "tool": self.tool,
            "body_executions": self.body_executions,
            "authorized_reexecs": self.authorized_reexecs,
            "silent_duplicates": self.silent_duplicates,
            "resolution_events": self.resolution_events,
            "duration_seconds": round(self.duration_seconds, 3),
            "long_running_or_redispatched": self.long_running_or_redispatched,
        }


@dataclass(frozen=True)
class DttrReport:
    """Aggregate DTTR over a set of transitions."""

    dttr: float
    silent_duplicates: int
    long_running_or_redispatched: int
    transitions: int
    per_transition: tuple[TransitionDttr, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dttr": self.dttr,
            "silent_duplicates": self.silent_duplicates,
            "long_running_or_redispatched": self.long_running_or_redispatched,
            "transitions": self.transitions,
            "per_transition": [item.to_dict() for item in self.per_transition],
        }


def _coerce_row(raw: OutcomeRow | dict[str, Any]) -> OutcomeRow:
    if isinstance(raw, OutcomeRow):
        return raw
    return OutcomeRow.from_dict(raw)


def compute_dttr(
    rows: list[OutcomeRow | dict[str, Any]],
    *,
    long_running_after: float | None = None,
) -> DttrReport:
    """Compute the Duplicate Tool Transition Rate (DTTR) over *rows*.

    Definitions (a *transition* is every row sharing a ``request_id``):

    - A *silent duplicate* is a tool-body execution for a transition that had
      already executed at least once, without being authorized by a consumed
      ``NOT_EXECUTED`` verdict (reconciler ``NOT_EXECUTED`` or operator
      release verified ``not_executed``). The first execution is always
      authorized, and each consumed ``NOT_EXECUTED`` authorizes exactly one
      more.
    - A transition is *long-running or redispatched* when it saw >=2
      resolution events (framework dispatches that resolved to a gate) OR its
      wall-clock span exceeds *long_running_after* seconds.
    - ``DTTR = silent_duplicates / max(long_running_or_redispatched, 1)``;
      the target is 0.

    *long_running_after* of ``None`` disables the duration rule (only the
    ``>=2`` resolution rule counts). Pass ``long_running_after=lease_ttl`` to
    also treat transitions older than the execution lease as long-running.
    """
    normalized = [_coerce_row(row) for row in rows]
    by_transition: dict[str, list[OutcomeRow]] = {}
    for row in normalized:
        by_transition.setdefault(row.request_id, []).append(row)

    per_transition: list[TransitionDttr] = []
    silent_duplicates = 0
    long_or_redispatched = 0
    for request_id, group in by_transition.items():
        ordered = sorted(group, key=lambda row: row.ts)
        body_rows = [
            row
            for row in ordered
            if row.event == EVENT_BODY_START and row.tool_body_executed
        ]
        executions = len(body_rows)
        authorized = sum(1 for row in body_rows if row.authorized_reexec)
        allowed = 1 + authorized
        silent = max(0, executions - allowed)
        resolution_events = sum(
            1 for row in ordered if row.event == EVENT_RESOLUTION
        )
        duration = (
            ordered[-1].ts - ordered[0].ts if len(ordered) > 1 else 0.0
        )
        is_redispatched = resolution_events >= 2
        is_long_running = (
            long_running_after is not None and duration > long_running_after
        )
        marked = is_redispatched or is_long_running
        silent_duplicates += silent
        if marked:
            long_or_redispatched += 1
        per_transition.append(
            TransitionDttr(
                request_id=request_id,
                tool=body_rows[0].tool if body_rows else ordered[0].tool,
                body_executions=executions,
                authorized_reexecs=authorized,
                silent_duplicates=silent,
                resolution_events=resolution_events,
                duration_seconds=duration,
                long_running_or_redispatched=marked,
            )
        )

    per_transition.sort(
        key=lambda item: (not item.long_running_or_redispatched, -item.silent_duplicates)
    )
    dttr = silent_duplicates / max(long_or_redispatched, 1)
    return DttrReport(
        dttr=dttr,
        silent_duplicates=silent_duplicates,
        long_running_or_redispatched=long_or_redispatched,
        transitions=len(by_transition),
        per_transition=tuple(per_transition),
    )


def compute_dttr_from_storage(
    storage: OutcomeStorage,
    *,
    long_running_after: float | None = None,
) -> DttrReport:
    """Compute DTTR over every row currently held by *storage*."""
    return compute_dttr(
        storage.list_all(),
        long_running_after=long_running_after,
    )


__all__ = [
    "DttrReport",
    "EVENT_BODY_COMPLETE",
    "EVENT_BODY_FAIL",
    "EVENT_BODY_START",
    "EVENT_RELEASE",
    "EVENT_RESOLUTION",
    "FileOutcomeStorage",
    "GATE_ALLOW",
    "GATE_HARD_BLOCK",
    "GATE_RELEASE",
    "GATE_RETURN",
    "GATE_SOFT_BLOCK",
    "InMemoryOutcomeStorage",
    "OutcomeEmitter",
    "OutcomeRow",
    "OutcomeStorage",
    "TransitionDttr",
    "compute_dttr",
    "compute_dttr_from_storage",
]
