"""AF-010 Secret-in-args: keep credentials out of the tool boundary.

Fail-closed pre-execution blocking is the primary protection. Redaction of
persisted and emitted representations is defense-in-depth. Mycelium cannot
sanitize logs created inside arbitrary application or provider code.
"""

from __future__ import annotations

import functools
import hashlib
import hmac
import inspect
import logging
import math
import re
import threading
import warnings
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass, fields, is_dataclass, replace
from typing import Any, ParamSpec, Protocol, TypeVar
from urllib.parse import urlsplit, urlunsplit

P = ParamSpec("P")
R = TypeVar("R")

REDACTED_MARKER = "[REDACTED]"
SECRET_REF_PREFIX = "secret://"

POLICY_ERROR = "error"
POLICY_REDACT = "redact"
POLICY_WARN = "warn"
SECRET_ARGS_POLICIES = frozenset({POLICY_ERROR, POLICY_REDACT, POLICY_WARN})

SENSITIVE_FIELD_NAMES = frozenset(
    {
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
    }
)

_HEADER_ALIASES = frozenset({"authorization", "cookie", "x-api-key", "x_api_key"})
_WEAK_NAME_PARTS = ("secret", "token", "passwd", "password", "apikey", "api_key")

_JWT_RE = re.compile(r"^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")
_PEM_RE = re.compile(
    r"-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"
)
_AWS_KEY_RE = re.compile(r"\bAKIA[0-9A-Z]{16}\b")
_STRIPE_RE = re.compile(r"\b(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{16,}\b")
_GITHUB_RE = re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}\b")
_GITHUB_PAT_RE = re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b")
_SLACK_RE = re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+(\S+)")
_BASIC_RE = re.compile(r"(?i)\bBasic\s+([A-Za-z0-9+/=_-]{8,})")
_URL_USERINFO_RE = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^/\s@]+)@"
)
_URL_TOKEN_RE = re.compile(
    r"(?i)([?&](?:access_token|refresh_token|id_token|api_key|token|key)=)([^&\s#]+)"
)
_PASSWORD_ASSIGN_RE = re.compile(
    r"(?i)\b(password|passwd|pwd|client_secret|api_secret)\s*=\s*([^\s\"']+)"
)
_SECRET_REF_RE = re.compile(
    r"^secret://[A-Za-z0-9._~-]+(?:/[A-Za-z0-9._~-]+)+$"
)
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_HEX_RE = re.compile(r"^[0-9a-f]{16,}$", re.IGNORECASE)

_logger = logging.getLogger("mycelium.secret_protection")
_policy_var: ContextVar[Any] = ContextVar("mycelium_secret_args_policy", default=None)
_resolver_lock = threading.RLock()
_resolver: SecretResolver | None = None
_hmac_key: bytes | None = None
_log_filter_installed = False
_SECRET_FIELDS_ATTR = "_mycelium_secret_fields"


class SecretResolver(Protocol):
    """Host-registered mapping from ``secret://…`` references to values."""

    def __call__(self, reference: str) -> str: ...


class SecretInArgsError(Exception):
    """Raised when a raw secret is present in tool arguments.

    The message and attributes never include the original secret value.
    """

    def __init__(
        self,
        message: str,
        *,
        tool: str | None = None,
        paths: tuple[str, ...] = (),
        kinds: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.tool = tool
        self.paths = paths
        self.kinds = kinds


@dataclass(frozen=True)
class SecretFinding:
    """A detected secret location. Never stores the original value."""

    path: str
    kind: str
    field: str | None = None


@dataclass(frozen=True)
class SecretArgsPolicy:
    """Validated secret-in-args policy (not a storage constructor argument)."""

    enabled: bool = False
    policy: str = POLICY_ERROR
    allow_fields: frozenset[str] = frozenset()
    allow_tools: frozenset[str] = frozenset()
    entropy_detection: bool = True
    secret_fields: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.policy not in SECRET_ARGS_POLICIES:
            raise ValueError(
                f"secret_args.policy must be one of {sorted(SECRET_ARGS_POLICIES)}, "
                f"got {self.policy!r}"
            )


def is_secret_reference(value: Any) -> bool:
    """True when *value* is an opaque ``secret://…`` identifier."""
    return isinstance(value, str) and bool(_SECRET_REF_RE.fullmatch(value))


def register_secret_resolver(resolver: SecretResolver | None) -> None:
    """Register the process-wide secret-reference resolver.

    Applications must register one explicitly. Mycelium does not invent a
    default environment-variable resolver.
    """
    with _resolver_lock:
        global _resolver
        _resolver = resolver


def registered_secret_resolver() -> SecretResolver | None:
    with _resolver_lock:
        return _resolver


def register_secret_hmac_key(key: bytes | str | None) -> None:
    """Host-owned key material for optional correlation HMAC digests."""
    with _resolver_lock:
        global _hmac_key
        if key is None:
            _hmac_key = None
            return
        _hmac_key = key.encode("utf-8") if isinstance(key, str) else bytes(key)


def secret_hmac_digest(value: str, *, key: bytes | None = None) -> str:
    """Keyed HMAC of a secret. Never use an unsalted hash for correlation."""
    material = key if key is not None else _hmac_key
    if not material:
        raise SecretInArgsError(
            "HMAC correlation requires host-owned key material; "
            "call register_secret_hmac_key first"
        )
    digest = hmac.new(material, value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{REDACTED_MARKER}:hmac:{digest[:16]}"


def resolve_secret_reference(reference: str) -> str:
    """Resolve a ``secret://…`` reference via the registered resolver.

    Failures never include the resolved value. Missing resolvers and invalid
    references raise :class:`SecretInArgsError`.
    """
    if not is_secret_reference(reference):
        raise SecretInArgsError("not a secret reference")
    resolver = registered_secret_resolver()
    if resolver is None:
        raise SecretInArgsError(
            "no secret resolver is registered; "
            "call register_secret_resolver before resolving references"
        )
    try:
        value = resolver(reference)
    except SecretInArgsError:
        raise
    except Exception as exc:
        raise SecretInArgsError(
            f"secret resolver failed ({type(exc).__name__})"
        ) from None
    if not isinstance(value, str) or not value:
        raise SecretInArgsError("secret resolver returned an empty value")
    return value


def declare_secret_fields(*names: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Mark tool parameters that may hold ``secret://…`` references."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        setattr(func, _SECRET_FIELDS_ATTR, tuple(_normalize_field(n) for n in names))
        return func

    return decorator


def secret_fields_for(func: Callable[..., Any] | None) -> frozenset[str]:
    if func is None:
        return frozenset()
    seen: set[int] = set()
    current: Any = func
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        raw = getattr(current, _SECRET_FIELDS_ATTR, ())
        if raw:
            return frozenset(str(item) for item in raw)
        current = getattr(current, "__wrapped__", None)
    return frozenset()


def get_active_secret_policy() -> SecretArgsPolicy | None:
    return _policy_var.get()


def set_active_secret_policy(
    policy: SecretArgsPolicy | None,
) -> Any:
    return _policy_var.set(policy)


def reset_active_secret_policy(token: Any) -> None:
    _policy_var.reset(token)


def reset_secret_protection_state() -> None:
    """Test helper: clear resolver, HMAC key, and active policy."""
    register_secret_resolver(None)
    register_secret_hmac_key(None)
    _policy_var.set(None)


def _normalize_field(name: str) -> str:
    return str(name).strip().lower().replace("-", "_")


def _field_allowed(name: str | None, allow_fields: frozenset[str]) -> bool:
    if not name:
        return False
    return _normalize_field(name) in allow_fields


def _is_sensitive_field(name: str | None) -> bool:
    if not name:
        return False
    normalized = _normalize_field(name)
    if normalized in SENSITIVE_FIELD_NAMES or normalized in _HEADER_ALIASES:
        return True
    leaf = normalized.rsplit(".", 1)[-1]
    return leaf in SENSITIVE_FIELD_NAMES or leaf in _HEADER_ALIASES


def _shannon_entropy(text: str) -> float:
    if not text:
        return 0.0
    counts: dict[str, int] = {}
    for char in text:
        counts[char] = counts.get(char, 0) + 1
    length = len(text)
    return -sum((n / length) * math.log2(n / length) for n in counts.values())


def _looks_like_high_entropy_token(text: str) -> bool:
    if len(text) < 40 or " " in text or "\n" in text:
        return False
    if _UUID_RE.fullmatch(text) or _HEX_RE.fullmatch(text):
        return False
    if text.isalpha() or text.isdigit():
        return False
    if not re.fullmatch(r"[A-Za-z0-9/+=._~-]+", text):
        return False
    return _shannon_entropy(text) >= 4.5


def _string_kind(
    text: str,
    *,
    field: str | None,
    entropy_detection: bool,
) -> str | None:
    if is_secret_reference(text):
        return None
    if _PEM_RE.search(text):
        return "private_key"
    if _JWT_RE.fullmatch(text.strip()):
        return "jwt"
    if _AWS_KEY_RE.search(text) or _STRIPE_RE.search(text):
        return "credential_format"
    if _GITHUB_RE.search(text) or _GITHUB_PAT_RE.search(text) or _SLACK_RE.search(text):
        return "credential_format"
    if _BEARER_RE.search(text) or _BASIC_RE.search(text):
        return "authorization"
    if _URL_USERINFO_RE.search(text) or _URL_TOKEN_RE.search(text):
        return "url_credential"
    if _PASSWORD_ASSIGN_RE.search(text):
        return "password_assignment"
    if _is_sensitive_field(field) and text.strip():
        return "sensitive_field"
    if entropy_detection and _looks_like_high_entropy_token(text):
        if field and any(part in _normalize_field(field) for part in _WEAK_NAME_PARTS):
            return "entropy"
    return None


def scan_secrets(
    value: Any,
    *,
    entropy_detection: bool = True,
    allow_fields: frozenset[str] | Sequence[str] = (),
    _path: str = "$",
    _field: str | None = None,
    _seen: set[int] | None = None,
) -> list[SecretFinding]:
    """Recursively find secrets. Findings never include original values."""
    allowed = (
        allow_fields
        if isinstance(allow_fields, frozenset)
        else frozenset(_normalize_field(item) for item in allow_fields)
    )
    seen = _seen if _seen is not None else set()
    findings: list[SecretFinding] = []

    if _field_allowed(_field, allowed) or _field_allowed(_path.rsplit(".", 1)[-1], allowed):
        return findings

    obj_id = id(value)
    if obj_id in seen:
        return findings
    if isinstance(value, (dict, list, tuple)) or is_dataclass(value):
        seen.add(obj_id)

    if isinstance(value, str):
        kind = _string_kind(value, field=_field, entropy_detection=entropy_detection)
        if kind is not None:
            findings.append(SecretFinding(path=_path, kind=kind, field=_field))
        return findings

    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            return findings
        return scan_secrets(
            decoded,
            entropy_detection=entropy_detection,
            allow_fields=allowed,
            _path=_path,
            _field=_field,
            _seen=seen,
        )

    if isinstance(value, Mapping):
        for key, item in value.items():
            key_name = str(key)
            child_field = _normalize_field(key_name)
            child_path = f"{_path}.{key_name}"
            if isinstance(key, str):
                key_kind = _string_kind(
                    key, field=None, entropy_detection=entropy_detection
                )
                if key_kind is not None:
                    findings.append(
                        SecretFinding(path=f"{_path}[{key_name}]", kind=key_kind)
                    )
            if _is_sensitive_field(child_field) and not _field_allowed(child_field, allowed):
                if isinstance(item, str) and is_secret_reference(item):
                    continue
                if item not in (None, ""):
                    findings.append(
                        SecretFinding(
                            path=child_path,
                            kind="sensitive_field",
                            field=child_field,
                        )
                    )
                    continue
            findings.extend(
                scan_secrets(
                    item,
                    entropy_detection=entropy_detection,
                    allow_fields=allowed,
                    _path=child_path,
                    _field=child_field,
                    _seen=seen,
                )
            )
        return findings

    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            findings.extend(
                scan_secrets(
                    item,
                    entropy_detection=entropy_detection,
                    allow_fields=allowed,
                    _path=f"{_path}[{index}]",
                    _field=_field,
                    _seen=seen,
                )
            )
        return findings

    if is_dataclass(value) and not isinstance(value, type):
        payload = {item.name: getattr(value, item.name) for item in fields(value)}
        return scan_secrets(
            payload,
            entropy_detection=entropy_detection,
            allow_fields=allowed,
            _path=_path,
            _field=_field,
            _seen=seen,
        )

    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            payload = dump()
        except Exception:  # noqa: BLE001 — treat as opaque
            return findings
        return scan_secrets(
            payload,
            entropy_detection=entropy_detection,
            allow_fields=allowed,
            _path=_path,
            _field=_field,
            _seen=seen,
        )
    return findings


def _redact_string(text: str, *, hmac_key: bytes | None) -> str:
    if is_secret_reference(text):
        return text
    redacted = text
    redacted = _PEM_RE.sub(REDACTED_MARKER, redacted)
    redacted = _URL_USERINFO_RE.sub(r"\1***@", redacted)
    redacted = _URL_TOKEN_RE.sub(rf"\1{REDACTED_MARKER}", redacted)
    redacted = _PASSWORD_ASSIGN_RE.sub(rf"\1={REDACTED_MARKER}", redacted)
    redacted = _BEARER_RE.sub(f"Bearer {REDACTED_MARKER}", redacted)
    redacted = _BASIC_RE.sub(f"Basic {REDACTED_MARKER}", redacted)
    redacted = _STRIPE_RE.sub(REDACTED_MARKER, redacted)
    redacted = _AWS_KEY_RE.sub(REDACTED_MARKER, redacted)
    redacted = _GITHUB_PAT_RE.sub(REDACTED_MARKER, redacted)
    redacted = _GITHUB_RE.sub(REDACTED_MARKER, redacted)
    redacted = _SLACK_RE.sub(REDACTED_MARKER, redacted)
    if _JWT_RE.fullmatch(redacted.strip()):
        redacted = REDACTED_MARKER
    if redacted == text and hmac_key is not None and _looks_like_high_entropy_token(text):
        return secret_hmac_digest(text, key=hmac_key)
    if redacted == text and (
        _PEM_RE.search(text)
        or _JWT_RE.fullmatch(text.strip())
        or _URL_USERINFO_RE.search(text)
    ):
        return REDACTED_MARKER
    return redacted


def _sanitize_url(text: str) -> str:
    try:
        parts = urlsplit(text)
    except ValueError:
        return _redact_string(text, hmac_key=None)
    if parts.username or parts.password:
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        netloc = f"***@{host}" if host else "***"
        text = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    return _redact_string(text, hmac_key=None)


def sanitize_secrets(
    value: Any,
    *,
    hmac_key: bytes | None = None,
    entropy_detection: bool = True,
    allow_fields: frozenset[str] | Sequence[str] = (),
    _field: str | None = None,
    _seen: dict[int, Any] | None = None,
) -> Any:
    """Return a copy with detected secrets replaced by ``[REDACTED]``.

    Never stores or returns the original secret. ``secret://`` references are
    preserved. Caller-owned containers are not mutated.
    """
    allowed = (
        allow_fields
        if isinstance(allow_fields, frozenset)
        else frozenset(_normalize_field(item) for item in allow_fields)
    )
    seen = _seen if _seen is not None else {}
    if _field_allowed(_field, allowed):
        return value

    obj_id = id(value)
    if obj_id in seen:
        return seen[obj_id]

    if isinstance(value, str):
        if is_secret_reference(value):
            return value
        if _is_sensitive_field(_field) and value.strip():
            if hmac_key is not None:
                return secret_hmac_digest(value, key=hmac_key)
            return REDACTED_MARKER
        if "://" in value:
            return _sanitize_url(value)
        kind = _string_kind(value, field=_field, entropy_detection=entropy_detection)
        if kind is None:
            return value
        if hmac_key is not None:
            return secret_hmac_digest(value, key=hmac_key)
        redacted = _redact_string(value, hmac_key=hmac_key)
        return redacted if redacted != value else REDACTED_MARKER

    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8")
        except UnicodeDecodeError:
            return value
        sanitized = sanitize_secrets(
            decoded,
            hmac_key=hmac_key,
            entropy_detection=entropy_detection,
            allow_fields=allowed,
            _field=_field,
            _seen=seen,
        )
        if sanitized == decoded:
            return value
        return REDACTED_MARKER.encode("utf-8")

    if isinstance(value, Mapping):
        out: dict[Any, Any] = {}
        seen[obj_id] = out
        for key, item in value.items():
            key_name = str(key)
            child_field = _normalize_field(key_name)
            safe_key: Any = key
            if isinstance(key, str):
                key_kind = _string_kind(
                    key, field=None, entropy_detection=entropy_detection
                )
                if key_kind is not None:
                    safe_key = REDACTED_MARKER
            out[safe_key] = sanitize_secrets(
                item,
                hmac_key=hmac_key,
                entropy_detection=entropy_detection,
                allow_fields=allowed,
                _field=child_field,
                _seen=seen,
            )
        return out

    if isinstance(value, list):
        out_list: list[Any] = []
        seen[obj_id] = out_list
        for item in value:
            out_list.append(
                sanitize_secrets(
                    item,
                    hmac_key=hmac_key,
                    entropy_detection=entropy_detection,
                    allow_fields=allowed,
                    _field=_field,
                    _seen=seen,
                )
            )
        return out_list

    if isinstance(value, tuple):
        return tuple(
            sanitize_secrets(
                item,
                hmac_key=hmac_key,
                entropy_detection=entropy_detection,
                allow_fields=allowed,
                _field=_field,
                _seen=seen,
            )
            for item in value
        )

    if is_dataclass(value) and not isinstance(value, type):
        payload = {
            item.name: sanitize_secrets(
                getattr(value, item.name),
                hmac_key=hmac_key,
                entropy_detection=entropy_detection,
                allow_fields=allowed,
                _field=_normalize_field(item.name),
                _seen=seen,
            )
            for item in fields(value)
        }
        try:
            return replace(value, **payload)
        except Exception:  # noqa: BLE001
            return payload

    dump = getattr(value, "model_dump", None)
    if callable(dump):
        try:
            payload = dump()
        except Exception:  # noqa: BLE001
            return value
        return sanitize_secrets(
            payload,
            hmac_key=hmac_key,
            entropy_detection=entropy_detection,
            allow_fields=allowed,
            _field=_field,
            _seen=seen,
        )
    return value


def sanitize_text(text: str) -> str:
    """Sanitize a free-form string (logs, exceptions, CLI, YAML/JSON)."""
    policy = get_active_secret_policy()
    entropy = policy.entropy_detection if policy is not None else False
    result = sanitize_secrets(text, entropy_detection=entropy)
    return result if isinstance(result, str) else REDACTED_MARKER


def sanitize_for_evidence(value: Any) -> Any:
    """Sanitize a value for any persisted or emitted representation."""
    policy = get_active_secret_policy()
    if policy is None or not policy.enabled:
        return sanitize_secrets(value, entropy_detection=False)
    return sanitize_secrets(
        value,
        hmac_key=_hmac_key,
        entropy_detection=policy.entropy_detection,
        allow_fields=policy.allow_fields,
    )


def sanitize_exception(exc: BaseException) -> BaseException:
    """Preserve exception type; expose only a sanitized message."""
    original = str(exc)
    message = sanitize_text(original)
    if message == original:
        return exc
    try:
        safe = type(exc)(message)
    except Exception:  # noqa: BLE001
        safe = RuntimeError(f"{type(exc).__name__}: {message}")
    return safe


def fingerprint_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """Argument fingerprint that never unsalted-hashes raw secrets."""
    from mycelium.transition import args_fingerprint

    policy = get_active_secret_policy()
    if policy is None or not policy.enabled:
        return args_fingerprint(args, kwargs)
    safe_args, safe_kwargs = sanitize_secrets(
        (args, dict(kwargs)),
        hmac_key=_hmac_key,
        entropy_detection=policy.entropy_detection,
        allow_fields=policy.allow_fields,
    )
    return args_fingerprint(tuple(safe_args), dict(safe_kwargs))


def redact_is_safe(
    findings: Sequence[SecretFinding],
    *,
    secret_fields: frozenset[str],
    consequential: bool,
) -> bool:
    """True only when redacting cannot change required tool semantics."""
    for finding in findings:
        field = finding.field
        if field and field in secret_fields:
            return False
        if finding.kind in {
            "sensitive_field",
            "credential_format",
            "authorization",
            "private_key",
            "jwt",
        }:
            return False
        if consequential and finding.kind != "entropy":
            return False
    return True


def enforce_secret_args(
    tool: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    policy: SecretArgsPolicy,
    secret_fields: frozenset[str] = frozenset(),
    consequential: bool = False,
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Scan arguments and apply the configured policy.

    Does not mutate the caller's original containers. Returns the args that
    inner layers should see (original, or a redacted copy when safe).
    """
    if not policy.enabled or tool in policy.allow_tools:
        return args, kwargs
    findings = scan_secrets(
        {"args": args, "kwargs": kwargs},
        entropy_detection=policy.entropy_detection,
        allow_fields=policy.allow_fields,
    )
    raw = [item for item in findings if item.kind != "reference"]
    if not raw:
        return args, kwargs

    paths = tuple(item.path for item in raw)
    kinds = tuple(sorted({item.kind for item in raw}))
    message = (
        f"SecretInArgsError: tool {tool!r} received raw secret material "
        f"at {', '.join(paths)} ({', '.join(kinds)}). "
        "Pass secret:// references instead of credentials."
    )

    if policy.policy == POLICY_ERROR:
        raise SecretInArgsError(message, tool=tool, paths=paths, kinds=kinds)

    if policy.policy == POLICY_REDACT:
        if not redact_is_safe(
            raw, secret_fields=secret_fields, consequential=consequential
        ):
            raise SecretInArgsError(message, tool=tool, paths=paths, kinds=kinds)
        safe = sanitize_secrets(
            {"args": args, "kwargs": kwargs},
            hmac_key=_hmac_key,
            entropy_detection=policy.entropy_detection,
            allow_fields=policy.allow_fields,
        )
        return tuple(safe["args"]), dict(safe["kwargs"])

    warnings.warn(message, stacklevel=3)
    _logger.warning("%s", message)
    return args, kwargs


def resolve_declared_secret_fields(
    func: Callable[..., Any] | None,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    extra_fields: frozenset[str] = frozenset(),
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """Resolve ``secret://`` refs on declared fields only, on a copy."""
    declared = secret_fields_for(func) | extra_fields
    if not declared:
        return args, kwargs
    merged = dict(kwargs)
    names: list[str] = []
    if func is not None:
        try:
            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            names = list(bound.arguments)
            for name, value in bound.arguments.items():
                if name not in merged and name not in {"args", "kwargs"}:
                    merged[name] = value
        except (TypeError, ValueError):
            names = []

    changed = False
    resolved_kwargs = dict(kwargs)
    resolved_args = list(args)
    for field_name in declared:
        if field_name not in merged:
            continue
        value = merged[field_name]
        if not is_secret_reference(value):
            continue
        resolved = resolve_secret_reference(value)
        changed = True
        if field_name in resolved_kwargs:
            resolved_kwargs[field_name] = resolved
        elif field_name in names:
            index = names.index(field_name)
            if index < len(resolved_args):
                resolved_args[index] = resolved
            else:
                resolved_kwargs[field_name] = resolved
        else:
            resolved_kwargs[field_name] = resolved
    if not changed:
        return args, kwargs
    return tuple(resolved_args), resolved_kwargs


def _mark_secret_args(func: Callable[..., Any]) -> None:
    func._mycelium_secret_args = True  # type: ignore[attr-defined]


def apply_secret_args(
    func: Callable[..., Any],
    policy: SecretArgsPolicy,
    *,
    tool_name: str | None = None,
    secret_fields: frozenset[str] | Sequence[str] = (),
    consequential: bool = False,
) -> Callable[..., Any]:
    """Wrap *func* so secrets are scanned before any inner guard or claim."""
    name = tool_name or getattr(func, "__name__", "tool")
    declared = (
        secret_fields
        if isinstance(secret_fields, frozenset)
        else frozenset(_normalize_field(item) for item in secret_fields)
    )
    effective = replace(policy, secret_fields=policy.secret_fields | declared)
    if declared:
        setattr(func, _SECRET_FIELDS_ATTR, tuple(declared))
    _ensure_log_filter()

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            call_args, call_kwargs = enforce_secret_args(
                name,
                args,
                kwargs,
                policy=effective,
                secret_fields=secret_fields_for(func) | declared,
                consequential=consequential,
            )
            token = set_active_secret_policy(effective)
            try:
                ledgered = getattr(func, "_mycelium_ledger", False) or getattr(
                    func, "_mycelium_task_ledger", False
                )
                if not ledgered:
                    call_args, call_kwargs = resolve_declared_secret_fields(
                        func,
                        call_args,
                        call_kwargs,
                        extra_fields=secret_fields_for(func) | declared,
                    )
                return await func(*call_args, **call_kwargs)
            except SecretInArgsError:
                raise
            except Exception as exc:
                safe = sanitize_exception(exc)
                if safe is exc:
                    raise
                raise safe from None
            finally:
                reset_active_secret_policy(token)

        _mark_secret_args(async_wrapper)
        return async_wrapper

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        call_args, call_kwargs = enforce_secret_args(
            name,
            args,
            kwargs,
            policy=effective,
            secret_fields=secret_fields_for(func) | declared,
            consequential=consequential,
        )
        token = set_active_secret_policy(effective)
        try:
            ledgered = getattr(func, "_mycelium_ledger", False) or getattr(
                func, "_mycelium_task_ledger", False
            )
            if not ledgered:
                call_args, call_kwargs = resolve_declared_secret_fields(
                    func,
                    call_args,
                    call_kwargs,
                    extra_fields=secret_fields_for(func) | declared,
                )
            return func(*call_args, **call_kwargs)
        except SecretInArgsError:
            raise
        except Exception as exc:
            safe = sanitize_exception(exc)
            if safe is exc:
                raise
            raise safe from None
        finally:
            reset_active_secret_policy(token)

    _mark_secret_args(sync_wrapper)
    return sync_wrapper


class _SecretLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
            record.msg = sanitize_text(rendered)
            record.args = ()
        except Exception:  # noqa: BLE001 — logging must never raise
            record.msg = REDACTED_MARKER
            record.args = ()
        return True


def _ensure_log_filter() -> None:
    global _log_filter_installed
    if _log_filter_installed:
        return
    filt = _SecretLogFilter()
    logging.getLogger("mycelium").addFilter(filt)
    for name, logger in logging.Logger.manager.loggerDict.items():
        if isinstance(logger, logging.Logger) and name.startswith("mycelium"):
            logger.addFilter(filt)
    _log_filter_installed = True


__all__ = [
    "POLICY_ERROR",
    "POLICY_REDACT",
    "POLICY_WARN",
    "REDACTED_MARKER",
    "SECRET_ARGS_POLICIES",
    "SECRET_REF_PREFIX",
    "SENSITIVE_FIELD_NAMES",
    "SecretArgsPolicy",
    "SecretFinding",
    "SecretInArgsError",
    "SecretResolver",
    "apply_secret_args",
    "declare_secret_fields",
    "enforce_secret_args",
    "fingerprint_args",
    "get_active_secret_policy",
    "is_secret_reference",
    "register_secret_hmac_key",
    "register_secret_resolver",
    "registered_secret_resolver",
    "reset_secret_protection_state",
    "resolve_declared_secret_fields",
    "resolve_secret_reference",
    "sanitize_exception",
    "sanitize_for_evidence",
    "sanitize_secrets",
    "sanitize_text",
    "scan_secrets",
    "secret_fields_for",
    "secret_hmac_digest",
    "set_active_secret_policy",
]
