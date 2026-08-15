"""Secret-in-args: raw credentials never reach claim, evidence, or artifacts."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from mycelium.action_ledger import InMemoryLedgerStorage, ledger_sync
from mycelium.secret_protection import (
    REDACTED_MARKER,
    SecretArgsPolicy,
    SecretInArgsError,
    apply_secret_args,
    declare_secret_fields,
    register_secret_resolver,
    reset_secret_protection_state,
)
from mycelium.transition import TransitionScope, execution_scope
from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import synthetic_binding

# Synthetic only — never a real credential. Failure text must not echo it.
_SYNTHETIC = "sk_test_MyceliumVerifyFakeSecretAF010xx"
_FRAGMENTS = (
    "sk_test_MyceliumVerify",
    "MyceliumVerifyFakeSecretAF010",
)
_REF = "secret://stripe/verify/api-key"


def _contains_secret(text: str) -> bool:
    if _SYNTHETIC in text:
        return True
    return any(fragment in text for fragment in _FRAGMENTS)


def _scan_payload(value: Any) -> bool:
    try:
        text = json.dumps(value, default=str)
    except TypeError:
        text = repr(value)
    return _contains_secret(text)


def _scan_paths(paths: list[str]) -> bool:
    for raw in paths:
        path = Path(raw)
        try:
            if path.is_file():
                if _contains_secret(path.read_text(encoding="utf-8", errors="replace")):
                    return True
            elif path.is_dir():
                for child in path.rglob("*"):
                    if child.is_file() and _contains_secret(
                        child.read_text(encoding="utf-8", errors="replace")
                    ):
                        return True
        except OSError:
            continue
    return False


class _RecordingStorage(InMemoryLedgerStorage):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
        self.events.append("claim")
        return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    def set(self, entry) -> None:
        if _scan_payload(entry.to_dict() if hasattr(entry, "to_dict") else entry):
            raise RuntimeError("storage refused a payload that contained a secret")
        return super().set(entry)


class _BoomStorage(InMemoryLedgerStorage):
    def set(self, entry) -> None:
        raise RuntimeError(f"persist failed args={entry.args!r} kwargs={entry.kwargs!r}")


class _BoomEmitter:
    fail_closed = True

    def emit_event(self, **payload: Any) -> None:
        raise RuntimeError(f"emit failed {payload!r}")

    def emit(self, row: Any) -> None:
        payload = row.to_dict() if hasattr(row, "to_dict") else row
        raise RuntimeError(f"emit failed {payload!r}")


def _wrap(
    storage,
    *,
    policy: SecretArgsPolicy,
    events: list[str],
    seen: list[Any],
    fail_with_secret: bool = False,
    outcome_emitter: Any = None,
):
    binding = synthetic_binding()

    @declare_secret_fields("api_key")
    def verify_charge(
        amount: int,
        api_key: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        events.append("body")
        seen.append(api_key)
        if fail_with_secret:
            raise RuntimeError(f"provider rejected {_SYNTHETIC}")
        return {"charged": True, "amount": amount, "payload": payload}

    ledgered = ledger_sync(
        storage=storage,
        transition_binding=binding,
        lease_ttl=30.0,
        lease_renew_interval=0,
        poll_interval=0.02,
        poll_timeout=5.0,
        outcome_emitter=outcome_emitter,
    )(verify_charge)
    return apply_secret_args(
        ledgered,
        policy,
        tool_name="verify_charge",
        secret_fields=["api_key"],
        consequential=True,
    )


@verify_scenario("secret-in-args")
def run_secret_in_args(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    artifact = iso.artifact_file("secret-in-args-")
    dump = Path(iso.artifact_file("secret-in-args-dump-"))
    failures: list[str] = []
    decisions: list[str] = []
    attempts = 0
    bodies = 0
    reset_secret_protection_state()

    policy_error = SecretArgsPolicy(enabled=True, policy="error")
    policy_warn = SecretArgsPolicy(enabled=True, policy="warn")
    events: list[str] = []
    seen: list[Any] = []
    resolver_calls: list[str] = []

    def resolver(reference: str) -> str:
        resolver_calls.append(reference)
        events.append("resolve")
        return _SYNTHETIC

    register_secret_resolver(resolver)

    # 1. Raw secret is blocked before claim / body / side effect.
    recording = _RecordingStorage(events)
    rid_raw = iso.track(iso.namespace.request_id("secret-in-args", "raw"))
    attempts += 1
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = _wrap(recording, policy=policy_error, events=events, seen=seen)
        blocked = False
        try:
            tool(1, api_key=_SYNTHETIC, request_id=rid_raw)
        except SecretInArgsError as exc:
            blocked = True
            if _contains_secret(str(exc)):
                failures.append("SecretInArgsError echoed the synthetic secret")
        except Exception:  # noqa: BLE001
            failures.append("raw secret raised an unexpected exception type")
    if not blocked:
        failures.append("raw secret was not blocked")
    if "body" in events or "claim" in recording.events:
        failures.append("raw secret reached claim or tool body")
    elif recording.get(rid_raw) is not None:
        failures.append("raw secret created a ledger claim")
    else:
        decisions.append("raw-secret:blocked-before-claim")

    # 2. Nested secret is detected.
    events.clear()
    rid_nested = iso.track(iso.namespace.request_id("secret-in-args", "nested"))
    attempts += 1
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = _wrap(recording, policy=policy_error, events=events, seen=seen)
        nested_blocked = False
        try:
            tool(
                1,
                payload={"nested": {"password": _SYNTHETIC}},
                request_id=rid_nested,
            )
        except SecretInArgsError:
            nested_blocked = True
        except Exception:  # noqa: BLE001
            failures.append("nested secret raised an unexpected exception type")
    if not nested_blocked or "body" in events or recording.get(rid_nested) is not None:
        failures.append("nested secret was not blocked before claim")
    else:
        decisions.append("nested-secret:blocked")

    # 3. secret:// reference is allowed; resolver runs only at execution.
    events.clear()
    seen.clear()
    resolver_calls.clear()
    recording.events.clear()
    rid_ref = iso.track(iso.namespace.request_id("secret-in-args", "ref"))
    attempts += 1
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = _wrap(recording, policy=policy_error, events=events, seen=seen)
        result = tool(2, api_key=_REF, request_id=rid_ref)
    bodies += 1
    if resolver_calls != [_REF]:
        failures.append("resolver was not invoked exactly once with the reference")
    if events[:2] != ["claim", "resolve"] or "body" not in events:
        failures.append("resolver did not run after claim and before the body")
    if seen != [_SYNTHETIC]:
        failures.append("tool body did not receive the resolved value")
    if result.get("charged") is not True:
        failures.append("reference call did not complete")
    entry = recording.get(rid_ref)
    if entry is None:
        failures.append("reference call did not persist a ledger row")
    elif _scan_payload(entry.to_dict()):
        failures.append("resolved secret leaked into the ledger row")
    elif _REF not in json.dumps(entry.kwargs, default=str) and _REF not in json.dumps(
        entry.args, default=str
    ):
        failures.append("ledger row lost the secret:// reference")
    else:
        decisions.append("reference:resolved-after-claim")

    # 4. Storage / emission failures must not leak the secret (warn path).
    boom = _BoomStorage()
    rid_boom = iso.track(iso.namespace.request_id("secret-in-args", "boom"))
    attempts += 1
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = _wrap(boom, policy=policy_warn, events=events, seen=seen)
        try:
            tool(1, api_key=_SYNTHETIC, request_id=rid_boom)
            failures.append("warn-policy storage failure did not raise")
        except Exception as exc:  # noqa: BLE001
            if _contains_secret(str(exc)):
                failures.append("storage failure leaked the synthetic secret")
            else:
                decisions.append("storage-failure:sanitized")

    rid_emit = iso.track(iso.namespace.request_id("secret-in-args", "emit"))
    attempts += 1
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = _wrap(
            iso.open_storage(),
            policy=policy_warn,
            events=events,
            seen=seen,
            outcome_emitter=_BoomEmitter(),
        )
        try:
            tool(1, api_key=_SYNTHETIC, request_id=rid_emit)
            failures.append("warn-policy emission failure did not raise")
        except Exception as exc:  # noqa: BLE001
            if _contains_secret(str(exc)):
                failures.append("emission failure leaked the synthetic secret")
            else:
                decisions.append("emission-failure:sanitized")

    events.clear()
    rid_exc = iso.track(iso.namespace.request_id("secret-in-args", "exc"))
    attempts += 1
    with execution_scope(TransitionScope(thread_id="verify", run_id="verify")):
        tool = _wrap(
            iso.open_storage(),
            policy=policy_error,
            events=events,
            seen=seen,
            fail_with_secret=True,
        )
        try:
            tool(1, api_key=_REF, request_id=rid_exc)
            failures.append("provider exception was swallowed")
        except SecretInArgsError:
            failures.append("protection replaced the provider exception")
        except Exception as exc:  # noqa: BLE001
            if _contains_secret(str(exc)):
                failures.append("provider exception leaked the secret")
            else:
                decisions.append("provider-exception:sanitized")

    # 5. Search every generated artifact and namespaced ledger row.
    dump.write_text(
        json.dumps(
            {
                "decisions": decisions,
                "marker": REDACTED_MARKER,
                "reference": _REF,
                "entries": [
                    item.to_dict()
                    for item in (*recording.list_all(), *iso.open_storage().list_all())
                ],
            },
            default=str,
        ),
        encoding="utf-8",
    )
    artifact_paths = [artifact, str(dump), *iso.artifact_paths()]
    if _scan_paths(artifact_paths):
        failures.append("synthetic secret found in generated artifacts")
    for item in (*recording.list_all(), *iso.open_storage().list_all()):
        if _scan_payload(item.to_dict()):
            failures.append("synthetic secret found in a ledger row")
            break
    if any(not iso.namespace.owns(item) for item in iso.tracked_ids):
        failures.append("tracked request_id escaped the verification namespace")
    else:
        decisions.append("cleanup:namespace-safe")

    reset_secret_protection_state()
    status = (
        VerificationStatus.PASS if not failures else VerificationStatus.FAIL
    )
    return VerificationEvidence(
        scenario="secret-in-args",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=attempts,
        body_executions=bodies,
        ledger_decisions=decisions,
        duration=time.time() - started,
        expected_behavior=(
            "Raw secrets are blocked before claim; secret:// refs resolve only "
            "at execution; resolved values never appear in evidence or artifacts."
        ),
        observed_behavior="; ".join(failures or decisions),
        artifacts=artifact_paths,
        limitations=[
            "Host logs and third-party provider SDKs are not_verifiable.",
            "Synthetic credentials only; no real provider was contacted.",
        ],
        status=status,
        summary=(
            "Secret-in-args blocked raw credentials and kept references opaque"
            if not failures
            else "Secret-in-args leaked or failed to block a synthetic credential"
        ),
        remediation=(
            ""
            if not failures
            else "Pass secret:// references; keep secret_args.policy: error."
        ),
    )
