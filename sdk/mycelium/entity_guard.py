"""Destination-policy guard: writes may only reach host-authorized entities.

A consequential write can carry sensitive payload, but it may cross only into
a destination the host listed. Unknown, missing, malformed, or dynamic
destinations fail closed before ledger claim. The model cannot mutate the
allowlist. Policies live in host YAML / ``EntityGuardPolicy`` — never in
prompt text.
"""

from __future__ import annotations

import functools
import inspect
import ipaddress
from collections.abc import Callable, Mapping, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field, replace
from email.utils import getaddresses, parseaddr
from typing import Any, ParamSpec, TypeVar
from urllib.parse import unquote, urlsplit, urlunsplit

from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import LEDGER_KWARG_KEYS

P = ParamSpec("P")
R = TypeVar("R")

DEST_EMAIL = "email"
DEST_HTTPS_URL = "https_url"
DEST_ENTITY_ID = "entity_id"
DEST_HOST = "host"
DEST_TYPES = frozenset({DEST_EMAIL, DEST_HTTPS_URL, DEST_ENTITY_ID, DEST_HOST})

MISSING_POLICY_ERROR = "error"
MISSING_POLICY_WARN = "warn"
MISSING_POLICIES = frozenset({MISSING_POLICY_ERROR, MISSING_POLICY_WARN})

REASON_MISSING = "missing"
REASON_MALFORMED = "malformed"
REASON_DYNAMIC = "dynamic"
REASON_NOT_ALLOWED = "not_allowed"
REASON_UNDECLARED = "undeclared"

PAYLOAD_OMITTED = "[PAYLOAD_OMITTED]"

_EMAIL_FIELD_NAMES = frozenset(
    {
        "to",
        "cc",
        "bcc",
        "recipient",
        "recipients",
        "from",
        "sender",
        "reply_to",
        "reply-to",
        "email",
        "emails",
    }
)
_URL_FIELD_NAMES = frozenset(
    {
        "url",
        "uri",
        "href",
        "webhook",
        "webhook_url",
        "callback",
        "callback_url",
        "redirect",
        "redirect_url",
        "redirect_uri",
        "endpoint",
        "host",
    }
)
_ID_FIELD_NAMES = frozenset(
    {
        "project_id",
        "bucket",
        "account",
        "destination",
        "workspace_id",
        "org_id",
        "team_id",
        "channel_id",
    }
)
_PAYLOAD_FIELD_NAMES = frozenset(
    {
        "body",
        "content",
        "text",
        "html",
        "message",
        "payload",
        "file",
        "data",
        "attachments",
        "subject",
        "title",
        "description",
        "markdown",
        "bytes",
    }
)
_HOMOGLYPH_DOTS = ("\u3002", "\uff0e", "\uff61", "\u2024")
_DYNAMIC_MARKERS = ("{", "}", "{{", "}}", "${", "%s", "%(")
_MISSING = object()

_policy_var: ContextVar[EntityGuardPolicy | None] = ContextVar(
    "mycelium_entity_guard_policy", default=None
)
_decision_var: ContextVar[EntityDecision | None] = ContextVar(
    "mycelium_entity_guard_decision", default=None
)


class EntityGuardError(ToolBoundaryError):
    """Destination missing, malformed, dynamic, undeclared, or not allowed."""

    def __init__(
        self,
        message: str,
        *,
        tool: str,
        reason: str,
        dest_class: str | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(
            message,
            violation="entity_guard",
            tool_name=tool,
            llm_message=(
                f"Destination policy blocked {tool!r}: {message}. "
                "Use a host-authorized recipient, host, or entity id. "
                "The tool body was not executed."
            ),
            field=path,
            expected=dest_class,
            recovery_hint=(
                "Pass a destination the host listed in entity_guard.tools. "
                "The model cannot add allowlist entries."
            ),
        )
        self.tool = tool
        self.reason = reason
        self.dest_class = dest_class
        self.path = path


@dataclass(frozen=True)
class DestinationAllow:
    addresses: frozenset[str] = frozenset()
    domains: frozenset[str] = frozenset()
    hosts: frozenset[str] = frozenset()
    values: frozenset[str] = frozenset()

    def is_empty(self) -> bool:
        return not (self.addresses or self.domains or self.hosts or self.values)


@dataclass(frozen=True)
class DestinationSpec:
    path: str
    dest_type: str
    allow: DestinationAllow = field(default_factory=DestinationAllow)
    required: bool = True
    reject_redirects: bool = True

    def deny_if_present(self) -> bool:
        return self.allow.is_empty()


@dataclass(frozen=True)
class ToolDestinationPolicy:
    destinations: tuple[DestinationSpec, ...]


@dataclass(frozen=True)
class EntityGuardPolicy:
    """Host-owned destination policy. The model cannot add allowlist entries."""

    enabled: bool = True
    missing_policy: str = MISSING_POLICY_ERROR
    policy_version: str = "unspecified"
    tools: dict[str, ToolDestinationPolicy] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.missing_policy not in MISSING_POLICIES:
            raise ValueError(
                "entity_guard.missing_policy must be one of "
                f"{sorted(MISSING_POLICIES)}, got {self.missing_policy!r}"
            )


@dataclass(frozen=True)
class ApprovedDestination:
    path: str
    dest_class: str
    entity: str


@dataclass(frozen=True)
class EntityDecision:
    tool: str
    destinations: tuple[ApprovedDestination, ...]
    policy_version: str
    decision: str
    reason: str | None = None

    def to_evidence(self) -> dict[str, Any]:
        return {
            "tool": self.tool,
            "destinations": [
                {
                    "class": item.dest_class,
                    "entity": item.entity,
                    "path": item.path,
                }
                for item in self.destinations
            ],
            "policy_version": self.policy_version,
            "decision": self.decision,
            **({"reason": self.reason} if self.reason else {}),
        }


def get_active_entity_policy() -> EntityGuardPolicy | None:
    return _policy_var.get()


def set_active_entity_policy(policy: EntityGuardPolicy | None) -> Token[EntityGuardPolicy | None]:
    return _policy_var.set(policy)


def reset_active_entity_policy(token: Token[EntityGuardPolicy | None]) -> None:
    _policy_var.reset(token)


def get_active_entity_decision() -> EntityDecision | None:
    return _decision_var.get()


def reset_entity_guard_state() -> None:
    _policy_var.set(None)
    _decision_var.set(None)


def _is_dynamic(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    if any(marker in stripped for marker in _DYNAMIC_MARKERS):
        return True
    if stripped in {"*", "?", "..."}:
        return True
    return False


def _fully_unquote(value: str, *, rounds: int = 4) -> str:
    current = value
    for _ in range(rounds):
        nxt = unquote(current)
        if nxt == current:
            return current
        current = nxt
    return current


def _idna_host(host: str) -> str:
    cleaned = host.strip(".").lower()
    if not cleaned:
        raise ValueError("empty host")
    return cleaned.encode("idna").decode("ascii")


def canonicalize_email(raw: Any, *, tool: str = "tool", path: str = "recipient") -> str:
    if not isinstance(raw, str):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_EMAIL,
            path=path,
        )
    text = raw.strip()
    if not text:
        raise EntityGuardError(
            "destination is missing",
            tool=tool,
            reason=REASON_MISSING,
            dest_class=DEST_EMAIL,
            path=path,
        )
    if _is_dynamic(text):
        raise EntityGuardError(
            "destination is dynamic",
            tool=tool,
            reason=REASON_DYNAMIC,
            dest_class=DEST_EMAIL,
            path=path,
        )
    _name, address = parseaddr(text)
    if not address or "@" not in address or address.count("@") != 1:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_EMAIL,
            path=path,
        )
    local, domain = address.rsplit("@", 1)
    if not local or not domain:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_EMAIL,
            path=path,
        )
    try:
        host = _idna_host(domain)
    except (UnicodeError, ValueError) as exc:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_EMAIL,
            path=path,
        ) from exc
    return f"{local.lower()}@{host}"


def _emails_from_value(raw: Any, *, tool: str, path: str) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return ()
        if "," in text or ";" in text:
            pairs = getaddresses([text])
            return tuple(
                canonicalize_email(
                    f"{name} <{addr}>" if name else addr, tool=tool, path=path
                )
                for name, addr in pairs
                if addr
            )
        return (canonicalize_email(text, tool=tool, path=path),)
    if isinstance(raw, (list, tuple)):
        found: list[str] = []
        for item in raw:
            found.extend(_emails_from_value(item, tool=tool, path=path))
        return tuple(found)
    if isinstance(raw, Mapping):
        found = []
        for key, item in raw.items():
            if str(key).lower() in _EMAIL_FIELD_NAMES:
                found.extend(_emails_from_value(item, tool=tool, path=f"{path}.{key}"))
        if found:
            return tuple(found)
    raise EntityGuardError(
        "destination is malformed",
        tool=tool,
        reason=REASON_MALFORMED,
        dest_class=DEST_EMAIL,
        path=path,
    )


def _has_embedded_absolute_url(*parts: str) -> bool:
    for part in parts:
        lowered = unquote(part or "").lower()
        if "https://" in lowered or "http://" in lowered:
            return True
    return False


def canonicalize_https_url(
    raw: Any,
    *,
    tool: str = "tool",
    path: str = "url",
    reject_redirects: bool = True,
) -> str:
    if not isinstance(raw, str):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    text = raw.strip()
    if not text:
        raise EntityGuardError(
            "destination is missing",
            tool=tool,
            reason=REASON_MISSING,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    if _is_dynamic(text):
        raise EntityGuardError(
            "destination is dynamic",
            tool=tool,
            reason=REASON_DYNAMIC,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    if "\\" in text or any(ch.isspace() for ch in text):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    if any(dot in text for dot in _HOMOGLYPH_DOTS):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    decoded = _fully_unquote(text)
    parts = urlsplit(decoded)
    if parts.scheme.lower() != "https":
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    if parts.username is not None or parts.password is not None:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    host = parts.hostname
    if not host:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    try:
        host = _idna_host(host)
    except (UnicodeError, ValueError) as exc:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        ) from exc
    if reject_redirects and _has_embedded_absolute_url(
        parts.path, parts.query, parts.fragment
    ):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        )
    try:
        port = parts.port
    except ValueError as exc:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HTTPS_URL,
            path=path,
        ) from exc
    netloc = host if port in (None, 443) else f"{host}:{port}"
    path_part = parts.path or "/"
    return urlunsplit(("https", netloc, path_part, parts.query, ""))


def canonicalize_host(raw: Any, *, tool: str = "tool", path: str = "host") -> str:
    if isinstance(raw, str) and raw.strip().lower().startswith("https://"):
        url = canonicalize_https_url(raw, tool=tool, path=path, reject_redirects=True)
        host = urlsplit(url).hostname
        if not host:
            raise EntityGuardError(
                "destination is malformed",
                tool=tool,
                reason=REASON_MALFORMED,
                dest_class=DEST_HOST,
                path=path,
            )
        return host
    if not isinstance(raw, str):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HOST,
            path=path,
        )
    text = raw.strip()
    if not text:
        raise EntityGuardError(
            "destination is missing",
            tool=tool,
            reason=REASON_MISSING,
            dest_class=DEST_HOST,
            path=path,
        )
    if _is_dynamic(text) or "/" in text or "\\" in text or "@" in text:
        raise EntityGuardError(
            "destination is malformed" if "/" in text or "@" in text else "destination is dynamic",
            tool=tool,
            reason=REASON_DYNAMIC if _is_dynamic(text) else REASON_MALFORMED,
            dest_class=DEST_HOST,
            path=path,
        )
    try:
        return _idna_host(text.split(":")[0])
    except (UnicodeError, ValueError) as exc:
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_HOST,
            path=path,
        ) from exc


def canonicalize_entity_id(raw: Any, *, tool: str = "tool", path: str = "id") -> str:
    if raw is None:
        raise EntityGuardError(
            "destination is missing",
            tool=tool,
            reason=REASON_MISSING,
            dest_class=DEST_ENTITY_ID,
            path=path,
        )
    if not isinstance(raw, (str, int)):
        raise EntityGuardError(
            "destination is malformed",
            tool=tool,
            reason=REASON_MALFORMED,
            dest_class=DEST_ENTITY_ID,
            path=path,
        )
    text = str(raw).strip()
    if not text:
        raise EntityGuardError(
            "destination is missing",
            tool=tool,
            reason=REASON_MISSING,
            dest_class=DEST_ENTITY_ID,
            path=path,
        )
    if _is_dynamic(text):
        raise EntityGuardError(
            "destination is dynamic",
            tool=tool,
            reason=REASON_DYNAMIC,
            dest_class=DEST_ENTITY_ID,
            path=path,
        )
    return text


def _host_from_url(url: str) -> str:
    host = urlsplit(url).hostname
    return host or ""


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


def _email_allowed(canonical: str, allow: DestinationAllow) -> bool:
    if canonical in allow.addresses:
        return True
    domain = canonical.rsplit("@", 1)[-1]
    return domain in allow.domains


def _host_allowed(host: str, allow: DestinationAllow) -> bool:
    if host in allow.hosts:
        return True
    if _is_ip(host):
        return host in allow.hosts
    return False


def _entity_allowed(canonical: str, allow: DestinationAllow) -> bool:
    allowed = {item.casefold() for item in allow.values}
    return canonical.casefold() in allowed


def _lookup_path(mapping: Mapping[str, Any], path: str) -> Any:
    current: Any = mapping
    for part in path.split("."):
        if isinstance(current, Mapping) and part in current:
            current = current[part]
        else:
            return _MISSING
    return current


def _set_path(mapping: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    current: Any = mapping
    for part in parts[:-1]:
        nxt = current.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            current[part] = nxt
        current = nxt
    current[parts[-1]] = value


def _split_bookkeeping(
    func: Callable[..., Any], kwargs: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    extra = {key: value for key, value in kwargs.items() if key in LEDGER_KWARG_KEYS}
    known = {key: value for key, value in kwargs.items() if key not in LEDGER_KWARG_KEYS}
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        return known, extra
    if any(
        param.kind is inspect.Parameter.VAR_KEYWORD
        for param in signature.parameters.values()
    ):
        return known, extra
    names = set(signature.parameters)
    for key, value in list(known.items()):
        if key not in names:
            extra[key] = value
            del known[key]
    return known, extra


def _bound_mapping(
    func: Callable[..., Any], args: tuple[Any, ...], kwargs: dict[str, Any]
) -> dict[str, Any]:
    try:
        signature = inspect.signature(func)
        bound = signature.bind_partial(*args, **kwargs)
        return dict(bound.arguments)
    except (TypeError, ValueError):
        return dict(kwargs)


def _rebuild_call(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    mapping: dict[str, Any],
    extra: dict[str, Any],
) -> tuple[tuple[Any, ...], dict[str, Any]]:
    new_args = list(args)
    new_kwargs = dict(extra)
    try:
        signature = inspect.signature(func)
    except (TypeError, ValueError):
        new_kwargs.update(mapping)
        return tuple(new_args), new_kwargs
    positional = [
        param
        for param in signature.parameters.values()
        if param.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ]
    for index, param in enumerate(positional):
        if param.name in mapping and index < len(new_args):
            new_args[index] = mapping[param.name]
        elif param.name in mapping:
            new_kwargs[param.name] = mapping[param.name]
    for key, value in mapping.items():
        if key not in {param.name for param in positional}:
            new_kwargs[key] = value
    return tuple(new_args), new_kwargs


def _raise(
    *,
    tool: str,
    reason: str,
    dest_class: str | None,
    path: str | None,
    missing_policy: str,
) -> None:
    if reason == REASON_MISSING and missing_policy == MISSING_POLICY_WARN:
        # Still fail closed: warn is not a license to execute.
        pass
    messages = {
        REASON_MISSING: "destination is missing",
        REASON_MALFORMED: "destination is malformed",
        REASON_DYNAMIC: "destination is dynamic",
        REASON_NOT_ALLOWED: "destination is not allowed",
        REASON_UNDECLARED: "undeclared destination",
    }
    raise EntityGuardError(
        messages.get(reason, "destination is not allowed"),
        tool=tool,
        reason=reason,
        dest_class=dest_class,
        path=path,
    )


def _canonicalize_values(
    spec: DestinationSpec, raw: Any, *, tool: str
) -> tuple[str, ...]:
    if spec.dest_type == DEST_EMAIL:
        return _emails_from_value(raw, tool=tool, path=spec.path)
    if spec.dest_type == DEST_HTTPS_URL:
        if isinstance(raw, (list, tuple)):
            return tuple(
                canonicalize_https_url(
                    item,
                    tool=tool,
                    path=spec.path,
                    reject_redirects=spec.reject_redirects,
                )
                for item in raw
            )
        return (
            canonicalize_https_url(
                raw,
                tool=tool,
                path=spec.path,
                reject_redirects=spec.reject_redirects,
            ),
        )
    if spec.dest_type == DEST_HOST:
        if isinstance(raw, (list, tuple)):
            return tuple(canonicalize_host(item, tool=tool, path=spec.path) for item in raw)
        return (canonicalize_host(raw, tool=tool, path=spec.path),)
    if isinstance(raw, (list, tuple)):
        return tuple(canonicalize_entity_id(item, tool=tool, path=spec.path) for item in raw)
    return (canonicalize_entity_id(raw, tool=tool, path=spec.path),)


def _value_allowed(spec: DestinationSpec, canonical: str) -> bool:
    if spec.dest_type == DEST_EMAIL:
        return _email_allowed(canonical, spec.allow)
    if spec.dest_type in {DEST_HTTPS_URL, DEST_HOST}:
        host = _host_from_url(canonical) if spec.dest_type == DEST_HTTPS_URL else canonical
        return _host_allowed(host, spec.allow)
    return _entity_allowed(canonical, spec.allow)


def _field_kind(name: str) -> str | None:
    key = name.lower()
    if key in _EMAIL_FIELD_NAMES:
        return DEST_EMAIL
    if key in _URL_FIELD_NAMES:
        return DEST_HTTPS_URL if key != "host" else DEST_HOST
    if key in _ID_FIELD_NAMES:
        return DEST_ENTITY_ID
    return None


def _walk_undeclared(
    value: Any,
    *,
    prefix: str,
    covered: set[str],
    payload: bool,
) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if payload:
        return found
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            path = f"{prefix}.{name}" if prefix else name
            kind = _field_kind(name)
            in_payload = name.lower() in _PAYLOAD_FIELD_NAMES
            if kind and path not in covered and not in_payload:
                if item not in (None, "", [], {}):
                    found.append((path, kind))
            found.extend(
                _walk_undeclared(
                    item, prefix=path, covered=covered, payload=in_payload
                )
            )
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found.extend(
                _walk_undeclared(
                    item,
                    prefix=f"{prefix}[{index}]",
                    covered=covered,
                    payload=payload,
                )
            )
    return found


def enforce_entity_guard(
    tool: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    policy: EntityGuardPolicy,
    func: Callable[..., Any] | None = None,
) -> tuple[tuple[Any, ...], dict[str, Any], EntityDecision]:
    """Canonicalize and authorize destinations. Fail closed on any violation."""
    tool_policy = policy.tools.get(tool)
    if tool_policy is None:
        decision = EntityDecision(
            tool=tool,
            destinations=(),
            policy_version=policy.policy_version,
            decision="allow",
        )
        _decision_var.set(decision)
        return args, kwargs, decision

    tool_kwargs, extra = (
        _split_bookkeeping(func, kwargs) if func is not None else (dict(kwargs), {})
    )
    mapping = (
        _bound_mapping(func, args, tool_kwargs) if func is not None else dict(tool_kwargs)
    )
    approved: list[ApprovedDestination] = []
    covered: set[str] = set()

    for spec in tool_policy.destinations:
        covered.add(spec.path)
        raw = _lookup_path(mapping, spec.path)
        empty = raw is _MISSING or raw in (None, "", [], ())
        if empty:
            if spec.deny_if_present() or not spec.required:
                continue
            _raise(
                tool=tool,
                reason=REASON_MISSING,
                dest_class=spec.dest_type,
                path=spec.path,
                missing_policy=policy.missing_policy,
            )
        values = _canonicalize_values(spec, raw, tool=tool)
        if not values:
            if spec.deny_if_present() or not spec.required:
                continue
            _raise(
                tool=tool,
                reason=REASON_MISSING,
                dest_class=spec.dest_type,
                path=spec.path,
                missing_policy=policy.missing_policy,
            )
        if spec.deny_if_present():
            _raise(
                tool=tool,
                reason=REASON_NOT_ALLOWED,
                dest_class=spec.dest_type,
                path=spec.path,
                missing_policy=policy.missing_policy,
            )
        rewritten: list[str] = []
        for canonical in values:
            if not _value_allowed(spec, canonical):
                _raise(
                    tool=tool,
                    reason=REASON_NOT_ALLOWED,
                    dest_class=spec.dest_type,
                    path=spec.path,
                    missing_policy=policy.missing_policy,
                )
            entity = (
                _host_from_url(canonical)
                if spec.dest_type == DEST_HTTPS_URL
                else canonical
            )
            approved.append(
                ApprovedDestination(
                    path=spec.path, dest_class=spec.dest_type, entity=entity
                )
            )
            rewritten.append(canonical)
        if isinstance(raw, (list, tuple)):
            _set_path(mapping, spec.path, rewritten)
        elif spec.dest_type == DEST_EMAIL and isinstance(raw, Mapping):
            pass
        else:
            _set_path(mapping, spec.path, rewritten[0] if len(rewritten) == 1 else rewritten)

    for path, kind in _walk_undeclared(mapping, prefix="", covered=covered, payload=False):
        _raise(
            tool=tool,
            reason=REASON_UNDECLARED,
            dest_class=kind,
            path=path,
            missing_policy=policy.missing_policy,
        )

    decision = EntityDecision(
        tool=tool,
        destinations=tuple(approved),
        policy_version=policy.policy_version,
        decision="allow",
    )
    _decision_var.set(decision)
    if func is not None:
        return (*_rebuild_call(func, args, kwargs, mapping, extra), decision)
    merged = dict(mapping)
    merged.update(extra)
    return args, merged, decision


def sanitize_entity_evidence(
    args: Sequence[Any],
    kwargs: Mapping[str, Any],
    policy: EntityGuardPolicy | None = None,
) -> tuple[list[Any], dict[str, Any]]:
    """Keep tool / approved entity IDs; drop sensitive payload."""
    del policy
    decision = get_active_entity_decision()
    dest_by_path = {item.path: item.entity for item in decision.destinations} if decision else {}

    def scrub(value: Any, *, name: str | None = None, payload: bool = False) -> Any:
        key = (name or "").lower()
        if key in _PAYLOAD_FIELD_NAMES or payload:
            return PAYLOAD_OMITTED
        if name and name in dest_by_path:
            return dest_by_path[name]
        if isinstance(value, Mapping):
            return {
                str(k): scrub(
                    v,
                    name=str(k),
                    payload=str(k).lower() in _PAYLOAD_FIELD_NAMES,
                )
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [scrub(item, payload=payload) for item in value]
        if isinstance(value, tuple):
            return [scrub(item, payload=payload) for item in value]
        return value

    safe_kwargs = scrub(dict(kwargs)) if isinstance(kwargs, Mapping) else {}
    assert isinstance(safe_kwargs, dict)
    return [scrub(item) for item in args], safe_kwargs


def destination_fingerprint(decision: EntityDecision | None) -> tuple[str, ...]:
    if decision is None:
        return ()
    return tuple(sorted(f"{item.dest_class}:{item.entity}" for item in decision.destinations))


def apply_entity_guard(
    func: Callable[P, R],
    policy: EntityGuardPolicy,
    *,
    tool_name: str | None = None,
) -> Callable[P, R]:
    """Wrap *func* so destinations are authorized before any inner guard or claim."""
    name = tool_name or getattr(func, "__name__", "tool")

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            token = set_active_entity_policy(policy)
            try:
                call_args, call_kwargs, _decision = enforce_entity_guard(
                    name, args, kwargs, policy=policy, func=func
                )
                return await func(*call_args, **call_kwargs)
            except EntityGuardError as exc:
                _decision_var.set(
                    EntityDecision(
                        tool=name,
                        destinations=(),
                        policy_version=policy.policy_version,
                        decision="deny",
                        reason=exc.reason,
                    )
                )
                raise
            finally:
                reset_active_entity_policy(token)

        async_wrapper._mycelium_entity_guard = True  # type: ignore[attr-defined]
        return async_wrapper  # type: ignore[return-value]

    @functools.wraps(func)
    def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
        token = set_active_entity_policy(policy)
        try:
            call_args, call_kwargs, _decision = enforce_entity_guard(
                name, args, kwargs, policy=policy, func=func
            )
            return func(*call_args, **call_kwargs)
        except EntityGuardError as exc:
            _decision_var.set(
                EntityDecision(
                    tool=name,
                    destinations=(),
                    policy_version=policy.policy_version,
                    decision="deny",
                    reason=exc.reason,
                )
            )
            raise
        finally:
            reset_active_entity_policy(token)

    sync_wrapper._mycelium_entity_guard = True  # type: ignore[attr-defined]
    return sync_wrapper  # type: ignore[return-value]


def entity_guard_policy_for_tool(
    policy: EntityGuardPolicy, tool_name: str
) -> EntityGuardPolicy:
    tool = policy.tools.get(tool_name)
    if tool is None:
        return replace(policy, tools={})
    return replace(policy, tools={tool_name: tool})


__all__ = [
    "DEST_EMAIL",
    "DEST_ENTITY_ID",
    "DEST_HOST",
    "DEST_HTTPS_URL",
    "DEST_TYPES",
    "MISSING_POLICIES",
    "MISSING_POLICY_ERROR",
    "MISSING_POLICY_WARN",
    "PAYLOAD_OMITTED",
    "ApprovedDestination",
    "DestinationAllow",
    "DestinationSpec",
    "EntityDecision",
    "EntityGuardError",
    "EntityGuardPolicy",
    "ToolDestinationPolicy",
    "apply_entity_guard",
    "canonicalize_email",
    "canonicalize_entity_id",
    "canonicalize_host",
    "canonicalize_https_url",
    "destination_fingerprint",
    "enforce_entity_guard",
    "entity_guard_policy_for_tool",
    "get_active_entity_decision",
    "get_active_entity_policy",
    "reset_active_entity_policy",
    "reset_entity_guard_state",
    "sanitize_entity_evidence",
    "set_active_entity_policy",
]
