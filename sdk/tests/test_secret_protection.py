"""AF-010 secret-in-args: scanner, policies, resolver, and evidence paths."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import multiprocessing as mp
import threading
from dataclasses import dataclass
from typing import Any

import pytest

from mycelium import (
    ConfigError,
    InMemoryLedgerStorage,
    SecretInArgsError,
    declare_secret_fields,
    is_secret_reference,
    ledger_sync,
    load_config_from_string,
    register_secret_hmac_key,
    register_secret_resolver,
    resolve_secret_reference,
    sanitize_secrets,
    scan_secrets,
)
from mycelium.action_ledger import ARGS_DRIFT_HARD
from mycelium.audit_receipt import AuditReceiptEmitter, InMemoryAuditReceiptStorage
from mycelium.doctor.types import DoctorStatus
from mycelium.outcome_emit import InMemoryOutcomeStorage, OutcomeEmitter
from mycelium.secret_protection import (
    REDACTED_MARKER,
    SecretArgsPolicy,
    apply_secret_args,
    fingerprint_args,
    reset_active_secret_policy,
    reset_secret_protection_state,
    sanitize_exception,
    sanitize_for_evidence,
    sanitize_text,
    secret_hmac_digest,
    set_active_secret_policy,
)
from mycelium.transition import (
    SideEffectClass,
    ToolTransitionBinding,
    TransitionScope,
    args_fingerprint,
    execution_scope,
)

# Fake material only. Assertions never interpolate these into messages.
_FAKE = "sk_test_MyceliumUnitFakeSecretAF010xx"
_FAKE_PASS = "mycelium-unit-fake-password"
_FAKE_TOKEN = "mycelium-unit-fake-bearer-token"
_REF = "secret://stripe/unit/api-key"
_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJteWNlbGl1bS11bml0In0.fakesigpad"
_PEM = (
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIFakePrivateKeyMaterialForTestsOnly\n"
    "-----END PRIVATE KEY-----"
)
_ENTROPY = "n7Kq2Vx9Lm4Pz8Rw1Yt6Hs3Bd0Cf5Gj-Ae_Q9uW2"


def _assert_absent(haystack: Any) -> None:
    text = haystack if isinstance(haystack, str) else json.dumps(haystack, default=str)
    if _FAKE in text or _FAKE_PASS in text or _FAKE_TOKEN in text:
        raise AssertionError("synthetic secret leaked into an observed value")


def _assert_present(haystack: Any, value: str) -> None:
    text = haystack if isinstance(haystack, str) else json.dumps(haystack, default=str)
    if value not in text:
        raise AssertionError("expected identifier was missing")


@pytest.fixture(autouse=True)
def _reset_secrets() -> None:
    reset_secret_protection_state()
    yield
    reset_secret_protection_state()


def _policy(name: str = "error", **kwargs: Any) -> SecretArgsPolicy:
    return SecretArgsPolicy(enabled=True, policy=name, **kwargs)


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="t",
        policy_version="test",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def test_nested_containers_and_models() -> None:
    @dataclass
    class Payload:
        password: str
        nested: dict[str, Any]

    class Model:
        def model_dump(self) -> dict[str, Any]:
            return {"api_key": _FAKE, "ok": True}

    findings = scan_secrets(
        {
            "outer": [
                {"token": _FAKE_TOKEN},
                Payload(password=_FAKE_PASS, nested={"cookie": "a=b"}),
            ],
            "model": Model(),
        }
    )
    kinds = {item.kind for item in findings}
    assert "sensitive_field" in kinds
    assert all(item.field != _FAKE for item in findings)
    safe = sanitize_secrets(
        {"outer": [{"token": _FAKE_TOKEN}], "password": _FAKE_PASS}
    )
    _assert_absent(safe)
    assert safe["outer"][0]["token"] == REDACTED_MARKER


def test_sensitive_field_names() -> None:
    for name in (
        "api_key",
        "api_secret",
        "token",
        "access_token",
        "refresh_token",
        "password",
        "passwd",
        "authorization",
        "cookie",
        "client_secret",
        "private_key",
        "signing_key",
        "webhook_secret",
    ):
        found = scan_secrets({name: "x"})
        assert found, name
        assert found[0].kind == "sensitive_field"


def test_authorization_headers_and_urls() -> None:
    findings = scan_secrets(
        {
            "authorization": f"Bearer {_FAKE_TOKEN}",
            "page": f"https://user:{_FAKE_PASS}@example.com/v1",
            "link": f"https://api.example.com/?access_token={_FAKE_TOKEN}",
        }
    )
    kinds = {item.kind for item in findings}
    assert "sensitive_field" in kinds or "authorization" in kinds
    assert "url_credential" in kinds
    safe = sanitize_secrets(
        {
            "authorization": f"Bearer {_FAKE_TOKEN}",
            "page": f"https://user:{_FAKE_PASS}@example.com/v1",
        }
    )
    _assert_absent(safe)


def test_private_key_and_jwt() -> None:
    kinds = {item.kind for item in scan_secrets({"pem": _PEM, "jwt": _JWT})}
    assert "private_key" in kinds
    assert "jwt" in kinds
    safe = sanitize_secrets({"pem": _PEM, "jwt": _JWT})
    assert _PEM not in json.dumps(safe)
    assert _JWT not in json.dumps(safe)


def test_entropy_false_positives() -> None:
    payload = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "hex": "aabbccddeeff00112233445566778899",
        "token_count": 12,
        "note": "a" * 48,
    }
    assert scan_secrets(payload) == []
    session = scan_secrets({"session_token": _ENTROPY})
    assert session and session[0].kind == "entropy"


def test_caller_arguments_are_immutable() -> None:
    payload = {"api_key": _FAKE, "nested": [_FAKE_PASS]}
    original = copy.deepcopy(payload)
    scan_secrets(payload)
    sanitize_secrets(payload)
    assert payload == original


def test_error_policy_blocks_before_claim_and_side_effects() -> None:
    storage = InMemoryLedgerStorage()
    ran: list[str] = []

    @declare_secret_fields("api_key")
    def charge(api_key: str) -> str:
        ran.append("body")
        return "ok"

    wrapped = apply_secret_args(
        ledger_sync(storage=storage, transition_binding=_binding())(charge),
        _policy("error"),
        tool_name="charge",
        secret_fields=["api_key"],
        consequential=True,
    )
    with execution_scope(TransitionScope(thread_id="t", run_id="r")):
        with pytest.raises(SecretInArgsError) as caught:
            wrapped(api_key=_FAKE, request_id="rid-raw")
    _assert_absent(str(caught.value))
    assert ran == []
    assert storage.get("rid-raw") is None


def test_redact_policy_fails_closed_for_consequential_secrets() -> None:
    def echo(api_key: str) -> str:
        return api_key

    wrapped = apply_secret_args(
        echo,
        _policy("redact"),
        tool_name="echo",
        secret_fields=["api_key"],
        consequential=True,
    )
    with pytest.raises(SecretInArgsError):
        wrapped(api_key=_FAKE)


def test_redact_policy_safe_copy_for_read_only_entropy() -> None:
    def echo(session_token: str) -> str:
        return session_token

    wrapped = apply_secret_args(
        echo,
        _policy("redact"),
        tool_name="echo",
        consequential=False,
    )
    assert wrapped(session_token=_ENTROPY) == REDACTED_MARKER


def test_warn_policy_runs_but_sanitizes_evidence() -> None:
    storage = InMemoryLedgerStorage()
    receipts = InMemoryAuditReceiptStorage()
    outcomes = InMemoryOutcomeStorage()

    def charge(api_key: str) -> dict[str, str]:
        return {"status": "ok"}

    wrapped = apply_secret_args(
        ledger_sync(
            storage=storage,
            transition_binding=_binding(),
            audit_emitter=AuditReceiptEmitter(
                agent_id="t",
                signing_key="unit-test-signing-key",
                storage=receipts,
            ),
            outcome_emitter=OutcomeEmitter(agent_id="t", storage=outcomes),
        )(charge),
        _policy("warn"),
        tool_name="charge",
        consequential=True,
    )
    with execution_scope(TransitionScope(thread_id="t", run_id="r")):
        assert wrapped(api_key=_FAKE, request_id="rid-warn") == {"status": "ok"}
    entry = storage.get("rid-warn")
    assert entry is not None
    _assert_absent(entry.to_dict())
    _assert_absent([item.to_dict() for item in receipts.list_all()])
    _assert_absent([item.to_dict() for item in outcomes.list_all()])


def test_reference_resolution_timing_and_sanitized_resolver_error() -> None:
    events: list[str] = []

    class TimedStorage(InMemoryLedgerStorage):
        def try_claim_inflight(self, entry, *, lease_ttl: float = 3600.0):
            events.append("claim")
            return super().try_claim_inflight(entry, lease_ttl=lease_ttl)

    def resolver(reference: str) -> str:
        events.append("resolve")
        if reference.endswith("missing"):
            raise RuntimeError(f"missing {_FAKE}")
        return _FAKE

    register_secret_resolver(resolver)
    seen: list[str] = []

    @declare_secret_fields("api_key")
    def charge(api_key: str) -> str:
        events.append("body")
        seen.append(api_key)
        return "ok"

    wrapped = apply_secret_args(
        ledger_sync(storage=TimedStorage(), transition_binding=_binding())(charge),
        _policy("error"),
        tool_name="charge",
        secret_fields=["api_key"],
        consequential=True,
    )
    with execution_scope(TransitionScope(thread_id="t", run_id="r")):
        wrapped(api_key=_REF, request_id="rid-ref")
    assert events[:3] == ["claim", "resolve", "body"]
    assert seen == [_FAKE]
    with pytest.raises(SecretInArgsError) as caught:
        resolve_secret_reference("secret://stripe/unit/missing")
    _assert_absent(str(caught.value))


def test_async_parity_and_disabled_behavior() -> None:
    async def echo(api_key: str) -> str:
        return api_key

    blocked = apply_secret_args(
        echo, _policy("error"), tool_name="echo", consequential=True
    )
    with pytest.raises(SecretInArgsError):
        asyncio.run(blocked(api_key=_FAKE))

    omitted = SecretArgsPolicy(enabled=False)
    passthrough = apply_secret_args(echo, omitted, tool_name="echo")
    assert asyncio.run(passthrough(api_key=_FAKE)) == _FAKE


def test_fingerprints_preserve_identity_without_raw_hashes() -> None:
    args = (1,)
    kwargs = {"q": "plain"}
    assert fingerprint_args(args, kwargs) == args_fingerprint(args, kwargs)
    token = set_active_secret_policy(_policy("error"))
    try:
        secret_fp = fingerprint_args((), {"api_key": _FAKE})
        other_fp = fingerprint_args((), {"api_key": "other-fake"})
        assert secret_fp == other_fp
        assert secret_fp == fingerprint_args((), {"api_key": REDACTED_MARKER})
    finally:
        reset_active_secret_policy(token)


def test_hmac_correlation_never_stores_raw_value() -> None:
    register_secret_hmac_key(b"unit-hmac-key")
    digest = secret_hmac_digest(_FAKE)
    assert digest.startswith(f"{REDACTED_MARKER}:hmac:")
    _assert_absent(digest)


def test_logging_and_cli_text_are_redacted(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger("mycelium.secret_protection")
    apply_secret_args(lambda: None, _policy("warn"), tool_name="noop")
    with caplog.at_level(logging.WARNING, logger="mycelium"):
        logger.warning("token=%s", _FAKE)
    _assert_absent(caplog.text)
    _assert_absent(sanitize_text(f"password={_FAKE_PASS}"))


def test_doctor_and_verify_render_redact() -> None:
    from mycelium.doctor.render import render_json as doctor_json
    from mycelium.doctor.types import DoctorCheck, DoctorReport
    from mycelium.verify.render import render_json as verify_json
    from mycelium.verify.types import VerificationEvidence, VerificationReport, VerificationStatus

    report = DoctorReport(
        overall_status=DoctorStatus.PASS,
        profile="development",
        checks=[
            DoctorCheck(
                id="x",
                category="Secret-in-args",
                status=DoctorStatus.PASS,
                summary=f"ok {_FAKE}",
            )
        ],
    )
    _assert_absent(doctor_json(report))
    evidence = VerificationEvidence(
        scenario="secret-in-args",
        backend="memory",
        namespace="mycelium:verify:x:",
        summary=f"ok {_FAKE}",
        status=VerificationStatus.PASS,
    )
    vreport = VerificationReport(
        overall_status=VerificationStatus.PASS,
        config_path="x.yaml",
        profile="development",
        topology=None,
        backend="memory",
        scenarios=[evidence],
    )
    _assert_absent(verify_json(vreport))


def test_production_rejects_weaker_secret_policy() -> None:
    with pytest.raises(ConfigError, match="secret_args.policy"):
        load_config_from_string(
            """
profile: production
action_ledger:
  storage: sqlite
  path: ./ledger.db
  tools: [charge]
outcome_emit:
  storage: file
  path: ./outcomes.jsonl
secret_args:
  enabled: true
  policy: warn
tools:
  charge:
    side_effect_class: non_idempotent_mutate
"""
        )


def test_omitted_and_invalid_config() -> None:
    cfg = load_config_from_string("tools:\n  search:\n    side_effect_class: read\n")
    assert cfg.secret_args is None
    assert cfg.secret_args_applies("search") is False
    with pytest.raises(ConfigError, match="unsupported"):
        load_config_from_string("secret_args:\n  hmac_key: nope\n")
    with pytest.raises(ConfigError, match="policy"):
        load_config_from_string("secret_args:\n  policy: drop\n")
    with pytest.raises(ConfigError, match="allow_fields"):
        load_config_from_string("secret_args:\n  allow_fields: [1]\n")


def test_thread_and_process_scan() -> None:
    results: list[int] = []

    def worker() -> None:
        results.append(len(scan_secrets({"password": "x"})))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for item in threads:
        item.start()
    for item in threads:
        item.join()
    assert results == [1] * 8
    with mp.get_context("spawn").Pool(1) as pool:
        count = pool.apply(_child_scan, ({"api_key": "x"},))
    assert count == 1


def _child_scan(payload: dict[str, Any]) -> int:
    from mycelium.secret_protection import scan_secrets as scan

    return len(scan(payload))


def test_public_helpers() -> None:
    assert is_secret_reference(_REF)
    assert not is_secret_reference("https://example.com")
    register_secret_resolver(lambda ref: "resolved")
    assert resolve_secret_reference(_REF) == "resolved"
    safe = sanitize_for_evidence({"api_key": _FAKE})
    _assert_absent(safe)
    exc = sanitize_exception(RuntimeError(f"boom {_FAKE}"))
    _assert_absent(str(exc))
    assert isinstance(exc, RuntimeError)


def test_args_drift_unchanged_without_secrets() -> None:
    storage = InMemoryLedgerStorage()
    calls = {"n": 0}

    def search(q: str) -> str:
        calls["n"] += 1
        return q

    wrapped = ledger_sync(
        storage=storage,
        transition_binding=ToolTransitionBinding.for_tool(
            agent_id="t",
            policy_version="test",
            side_effect_class=SideEffectClass.READ,
        ),
        on_args_drift=ARGS_DRIFT_HARD,
    )(search)
    with execution_scope(TransitionScope(thread_id="t", run_id="r")):
        assert wrapped(q="one", request_id="rid-plain") == "one"
        assert wrapped(q="one", request_id="rid-plain") == "one"
    assert calls["n"] == 1
