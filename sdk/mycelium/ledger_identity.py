"""Pure call-identity helpers for the action ledger."""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING

from mycelium.transition import (
    LEDGER_KWARG_KEYS,
    SideEffectClass,
    args_fingerprint,
    get_active_execution_scope,
    resolve_scope,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from typing import Any

    from mycelium.ledger_model import LedgerEntry
    from mycelium.transition import ToolTransitionBinding


def _bind_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    """Store a serializable snapshot of the call arguments."""
    return {
        "args": list(args),
        "kwargs": dict(kwargs),
    }


def _drop_ledger_keys(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Remove Mycelium bookkeeping keys before calling the actual tool."""
    return {k: v for k, v in kwargs.items() if k not in LEDGER_KWARG_KEYS}


def _identity_scopes_differ(
    existing: LedgerEntry,
    kwargs: dict[str, Any],
    binding: ToolTransitionBinding | None,
) -> bool:
    """True when incoming scope does not match the stored claim's scope."""
    scope_from = binding.scope_from if binding is not None else {}
    incoming = resolve_scope(scope_from=scope_from, kwargs=kwargs)
    stored = _scope_from_stored_kwargs(dict(existing.kwargs), scope_from)
    return (
        incoming.thread_id != stored[0]
        or incoming.run_id != stored[1]
        or incoming.node != stored[2]
    )


def _scope_from_stored_kwargs(
    kwargs: dict[str, Any],
    scope_from: dict[str, str],
) -> tuple[str, str, str]:
    """Read scope from a stored claim only — not the current execution_scope."""
    resolved = {"thread_id": "", "run_id": "", "node": ""}
    for field_name, source in scope_from.items():
        if field_name not in resolved:
            continue
        if source in kwargs and kwargs[source]:
            resolved[field_name] = str(kwargs[source])
    for field_name in resolved:
        if field_name in kwargs and kwargs[field_name]:
            resolved[field_name] = str(kwargs[field_name])
    return (resolved["thread_id"], resolved["run_id"], resolved["node"])


def _args_drift_exclude_keys(
    binding: ToolTransitionBinding | None,
) -> frozenset[str]:
    """Keys omitted from args-drift fingerprints (provider-key gate owns these)."""
    if binding is None or binding.provider_idempotency_key_param is None:
        return frozenset()
    return frozenset({binding.provider_idempotency_key_param})


def _evidence_value(value: Any) -> Any:
    from mycelium.destructive_confirm import (
        get_active_destructive_policy,
        sanitize_destructive_evidence,
    )
    from mycelium.entity_guard import get_active_entity_policy, sanitize_entity_evidence
    from mycelium.secret_protection import get_active_secret_policy, sanitize_for_evidence

    result = value
    policy = get_active_secret_policy()
    if policy is not None and policy.enabled:
        result = sanitize_for_evidence(result)
    if get_active_entity_policy() is not None:
        if isinstance(result, dict):
            _args, scrubbed = sanitize_entity_evidence((), result)
            del _args
            result = scrubbed
    if get_active_destructive_policy() is not None and isinstance(result, dict):
        _args, scrubbed = sanitize_destructive_evidence((), result)
        del _args
        return scrubbed
    return result


def _evidence_args(args: Any, kwargs: Any) -> tuple[list[Any], dict[str, Any]]:
    from mycelium.destructive_confirm import (
        get_active_destructive_policy,
        sanitize_destructive_evidence,
    )
    from mycelium.entity_guard import get_active_entity_policy, sanitize_entity_evidence
    from mycelium.secret_protection import get_active_secret_policy, sanitize_secrets

    out_args: list[Any] = list(args)
    out_kwargs: dict[str, Any] = dict(kwargs)
    policy = get_active_secret_policy()
    if policy is not None and policy.enabled:
        safe = sanitize_secrets(
            {"args": out_args, "kwargs": out_kwargs},
            entropy_detection=policy.entropy_detection,
            allow_fields=policy.allow_fields,
        )
        out_args, out_kwargs = list(safe["args"]), dict(safe["kwargs"])
    if get_active_entity_policy() is not None:
        out_args, out_kwargs = sanitize_entity_evidence(out_args, out_kwargs)
    if get_active_destructive_policy() is not None:
        out_args, out_kwargs = sanitize_destructive_evidence(out_args, out_kwargs)
    return out_args, out_kwargs


def _evidence_error(error: BaseException) -> str:
    from mycelium.secret_protection import (
        get_active_secret_policy,
        sanitize_exception,
        sanitize_text,
    )

    policy = get_active_secret_policy()
    if policy is None or not policy.enabled:
        return f"{type(error).__name__}: {error}"
    safe = sanitize_exception(error)
    return sanitize_text(f"{type(safe).__name__}: {safe}")


def _args_drift_fingerprint(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    exclude: frozenset[str],
) -> str:
    from mycelium.secret_protection import fingerprint_args, get_active_secret_policy

    filtered = (
        kwargs
        if not exclude
        else {key: value for key, value in kwargs.items() if key not in exclude}
    )
    policy = get_active_secret_policy()
    digest = (
        fingerprint_args(args, filtered)
        if policy is not None and policy.enabled
        else args_fingerprint(args, filtered)
    )
    from mycelium.entity_guard import destination_fingerprint, get_active_entity_decision

    dests = destination_fingerprint(get_active_entity_decision())
    from mycelium.destructive_confirm import (
        destructive_fingerprint,
        get_active_destructive_decision,
        get_active_destructive_policy,
        sanitize_destructive_evidence,
    )

    if get_active_destructive_policy() is not None:
        scrubbed_args, scrubbed_kwargs = sanitize_destructive_evidence(args, filtered)
        digest = (
            fingerprint_args(tuple(scrubbed_args), scrubbed_kwargs)
            if policy is not None and policy.enabled
            else args_fingerprint(tuple(scrubbed_args), scrubbed_kwargs)
        )
    destructive = destructive_fingerprint(get_active_destructive_decision())
    from mycelium.use_time_currency import (
        get_pending_use_time_facts,
        use_time_fingerprint,
    )

    currency = use_time_fingerprint(get_pending_use_time_facts())
    extra = tuple(dests) + tuple(destructive) + tuple(currency)
    if not extra:
        return digest
    import hashlib

    return hashlib.sha256(f"{digest}|{'|'.join(extra)}".encode()).hexdigest()


def _args_drift_scope_key(kwargs: dict[str, Any]) -> str | None:
    """Return ``run_id`` or fallback ``thread_id`` for args-drift isolation."""
    scope = get_active_execution_scope()
    run_id = kwargs.get("run_id") or (scope.run_id if scope else None)
    if run_id:
        return str(run_id)
    thread_id = kwargs.get("thread_id") or (scope.thread_id if scope else None)
    if thread_id:
        return str(thread_id)
    return None


def _args_drift_scopes_match(incoming: str | None, stored: str | None) -> bool:
    """True when both sides share a scope, or both are unscoped (legacy)."""
    if incoming is None and stored is None:
        return True
    if incoming is None or stored is None:
        return False
    return incoming == stored


def _canonical_call_mapping(
    func: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
) -> dict[str, Any]:
    mapping = dict(kwargs)
    try:
        bound = inspect.signature(func).bind_partial(*args, **kwargs)
        bound.apply_defaults()
        mapping.update(bound.arguments)
    except (TypeError, ValueError):
        pass
    return mapping


def _use_boundary_call_mapping(
    func: Callable[..., Any],
    args: tuple[Any, ...],
    clean_kwargs: Mapping[str, Any],
    dispatch_kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Canonical tool args for use-boundary validation.

    Authorize-time ``request_id`` is retained for call comparisons / validators.
    Configured request bindings at USE compare against the current trusted
    ``dispatch_scope`` (see use_time_currency._current_context_ids), not this
    frozen authorize-time copy.
    """
    mapping = _canonical_call_mapping(func, args, clean_kwargs)
    request_id = dispatch_kwargs.get("request_id")
    if isinstance(request_id, str) and request_id.strip():
        mapping["request_id"] = request_id
    return mapping


def _claim_kwargs(kwargs: dict[str, Any], clean_kwargs: dict[str, Any]) -> dict[str, Any]:
    """Kwargs for claim: tool args plus optional bookkeeping pass-through.

    ``state_ref`` / ``decision_id`` / handoff ids / dispatch and scope ids are
    bookkeeping (excluded from the tool body and ``args_fingerprint``) but must
    still reach ``_new_inflight_entry`` for audit and the opt-in args-drift gate
    (same dispatch ticket + different args, scoped by ``run_id`` / ``thread_id``).
    """
    claim_kwargs = dict(clean_kwargs)
    for key in (
        "decision_id",
        "state_ref",
        "parent_request_id",
        "handoff_id",
        "request_id",
        "tool_call_id",
        "thread_id",
        "run_id",
        "node",
    ):
        if key in kwargs and kwargs[key] is not None:
            claim_kwargs[key] = kwargs[key]
    scope = get_active_execution_scope()
    if scope is not None:
        for key, value in (
            ("thread_id", scope.thread_id),
            ("run_id", scope.run_id),
            ("node", scope.node),
        ):
            if key not in claim_kwargs and value:
                claim_kwargs[key] = value
    return claim_kwargs


def _is_read_only_binding(
    transition_binding: ToolTransitionBinding | None,
) -> bool:
    return (
        transition_binding is not None
        and transition_binding.side_effect_class == SideEffectClass.READ
    )
