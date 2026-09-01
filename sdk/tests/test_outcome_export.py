"""Backend-neutral outcome metric projection and exporter tests."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import sys
from types import SimpleNamespace
from typing import Any

import pytest

from mycelium.outcome_emit import (
    EVENT_DECISION_DENIAL,
    EVENT_FENCE_REJECTION,
    EVENT_LEASE_RENEWAL_FAILURE,
    EVENT_RELEASE,
    GATE_HARD_BLOCK,
    InMemoryOutcomeStorage,
    OutcomeRow,
)
from mycelium.outcome_export import (
    METRIC_AMBIGUITY_AGE,
    METRIC_DECISION_DENIALS,
    METRIC_FENCE_REJECTIONS,
    METRIC_HARD_BLOCKS,
    METRIC_LEASE_RENEWAL_FAILURES,
    METRIC_OPERATOR_RELEASES,
    METRIC_RECOVERY_TIME,
    FanoutOutcomeStorage,
    OpenTelemetryOutcomeStorage,
    OutcomeMetricProjector,
    PrometheusOutcomeStorage,
    WebhookOutcomeStorage,
)


def _row(**changes: Any) -> OutcomeRow:
    values = {
        "ts": 10.0,
        "agent_id": "agent",
        "tool": "charge",
        "request_id": "req-1",
        "event": "resolution",
        "event_id": "evt-1",
    }
    values.update(changes)
    return OutcomeRow(**values)


def test_projector_produces_the_standard_metric_contract() -> None:
    projector = OutcomeMetricProjector()

    hard_block = projector.project(
        _row(gate=GATE_HARD_BLOCK, terminal_outcome="UNKNOWN")
    )
    assert {point.name for point in hard_block} == {
        METRIC_HARD_BLOCKS,
        METRIC_AMBIGUITY_AGE,
    }

    later_block = projector.project(
        _row(ts=17.5, event_id="evt-2", gate=GATE_HARD_BLOCK, terminal_outcome="UNKNOWN")
    )
    age = next(point for point in later_block if point.name == METRIC_AMBIGUITY_AGE)
    assert age.value == 7.5

    released = projector.project(
        _row(ts=20.0, event=EVENT_RELEASE, gate="RELEASE", terminal_outcome="UNKNOWN")
    )
    assert {point.name for point in released} == {
        METRIC_OPERATOR_RELEASES,
        METRIC_RECOVERY_TIME,
    }
    recovery = next(point for point in released if point.name == METRIC_RECOVERY_TIME)
    assert recovery.value == 10.0

    operational = (
        (EVENT_FENCE_REJECTION, METRIC_FENCE_REJECTIONS),
        (EVENT_LEASE_RENEWAL_FAILURE, METRIC_LEASE_RENEWAL_FAILURES),
        (EVENT_DECISION_DENIAL, METRIC_DECISION_DENIALS),
    )
    for event, expected in operational:
        assert [point.name for point in projector.project(_row(event=event))] == [expected]


def test_metric_attributes_exclude_high_cardinality_fields() -> None:
    points = OutcomeMetricProjector().project(
        _row(
            event=EVENT_DECISION_DENIAL,
            run_id="run-secret",
            owner="ops@example.com",
            resolution_reason="free form",
        )
    )
    assert points[0].attributes == {
        "agent_id": "agent",
        "tool": "charge",
        "terminal_outcome": "",
    }


class _FakeInstrument:
    def __init__(self) -> None:
        self.calls: list[tuple[str, float, dict[str, str]]] = []

    def add(self, value: float, *, attributes: dict[str, str]) -> None:
        self.calls.append(("add", value, attributes))

    def record(self, value: float, *, attributes: dict[str, str]) -> None:
        self.calls.append(("record", value, attributes))


class _FakeMeter:
    def __init__(self) -> None:
        self.instruments: dict[str, _FakeInstrument] = {}

    def create_counter(self, name: str, **_: Any) -> _FakeInstrument:
        return self.instruments.setdefault(name, _FakeInstrument())

    def create_histogram(self, name: str, **_: Any) -> _FakeInstrument:
        return self.instruments.setdefault(name, _FakeInstrument())


def test_opentelemetry_storage_uses_application_meter() -> None:
    meter = _FakeMeter()
    storage = OpenTelemetryOutcomeStorage(meter)
    storage.append(_row(event=EVENT_FENCE_REJECTION))
    assert meter.instruments[METRIC_FENCE_REJECTIONS].calls[0][0] == "add"


def test_prometheus_storage_uses_registry_and_labels(monkeypatch: Any) -> None:
    created: dict[str, Any] = {}

    class Metric:
        def __init__(self, name: str, *_: Any, **kwargs: Any) -> None:
            self.name = name
            self.registry = kwargs["registry"]
            self.values: list[float] = []
            created[name] = self

        def labels(self, *_: str) -> Metric:
            return self

        def inc(self, value: float) -> None:
            self.values.append(value)

        def observe(self, value: float) -> None:
            self.values.append(value)

    registry = object()
    fake = SimpleNamespace(REGISTRY=object(), Counter=Metric, Histogram=Metric)
    monkeypatch.setitem(sys.modules, "prometheus_client", fake)
    storage = PrometheusOutcomeStorage(registry=registry)
    storage.append(_row(event=EVENT_LEASE_RENEWAL_FAILURE))
    metric = created[METRIC_LEASE_RENEWAL_FAILURES.replace(".", "_")]
    assert metric.registry is registry
    assert metric.values == [1.0]


def test_webhook_posts_versioned_signed_envelope(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    class Response:
        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_: Any) -> None:
            return None

        def read(self, _: int) -> bytes:
            return b""

    def open_request(request: Any, *, timeout: float) -> Response:
        captured["request"] = request
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr("mycelium.outcome_export.urlopen", open_request)
    storage = WebhookOutcomeStorage(
        "https://events.example.test/outcomes",
        secret="signing-key",
        timeout=2.5,
    )
    storage.append(_row(event=EVENT_DECISION_DENIAL))

    request = captured["request"]
    body = request.data
    payload = json.loads(body)
    assert payload["schema"] == "mycelium.outcome.v1"
    assert payload["id"] == "evt-1"
    assert payload["metrics"][0]["name"] == METRIC_DECISION_DENIALS
    expected = hmac.new(b"signing-key", body, hashlib.sha256).hexdigest()
    assert request.headers["X-mycelium-signature"] == f"sha256={expected}"
    assert captured["timeout"] == 2.5


@pytest.mark.parametrize(
    "timeout", [None, "5", 0, -1, math.nan, math.inf, -math.inf, True, False]
)
def test_webhook_rejects_non_finite_non_positive_and_boolean_timeouts(timeout: Any) -> None:
    with pytest.raises(ValueError, match="finite positive"):
        WebhookOutcomeStorage("https://events.example.test/outcomes", timeout=timeout)


@pytest.mark.parametrize("timeout", [0.001, 2, 2.5])
def test_webhook_accepts_positive_finite_timeouts(timeout: float) -> None:
    storage = WebhookOutcomeStorage("https://events.example.test/outcomes", timeout=timeout)
    assert storage._timeout == timeout


def test_fanout_writes_all_sinks_and_reads_primary() -> None:
    primary = InMemoryOutcomeStorage()
    secondary = InMemoryOutcomeStorage()
    fanout = FanoutOutcomeStorage(primary, secondary)
    row = _row()
    fanout.append(row)
    assert fanout.list_all() == [row]
    assert secondary.list_all() == [row]
