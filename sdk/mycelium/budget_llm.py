"""Auto-wire ``BudgetGuard.check("llm")`` around model turns.

Tools already get ``@budget_guard``. Pure LLM turns used to require a manual
``guard.check("llm")`` before every call — easy to forget, so the budget
slept while the agent burned money. This module wraps the **callable** the
host already uses for one model turn (framework glue differs; provider does
not).

- Raw loop: ``@budget_llm(guard)`` / ``wrap_llm_callable``
- LangGraph / LangChain chat models: ``instrument_langgraph_llm``
- CrewAI LLM objects: ``instrument_crewai_llm``
- Duck-typed entry: ``instrument_llm``
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Awaitable, Callable, Iterator
from typing import Any, ParamSpec, TypeVar

from mycelium.budget_guard import KIND_LLM, BudgetGuard

P = ParamSpec("P")
R = TypeVar("R")

_MARK = "_mycelium_budget_llm"


def extract_token_usage(result: Any) -> tuple[int | None, int | None]:
    """Best-effort ``(tokens_in, tokens_out)`` from common framework shapes.

    Understands LangChain ``AIMessage.usage_metadata``, legacy
    ``response_metadata["token_usage"]``, plain dicts, and objects with
    ``usage`` / ``usage_metadata``. Returns ``(None, None)`` when unknown —
    never invents USD (host price tables stay out of Mycelium).
    """
    if result is None:
        return None, None

    if isinstance(result, (list, tuple)):
        for item in reversed(result):
            tokens_in, tokens_out = extract_token_usage(item)
            if tokens_in is not None or tokens_out is not None:
                return tokens_in, tokens_out
        return None, None

    usage = getattr(result, "usage_metadata", None)
    parsed = _usage_mapping(usage)
    if parsed != (None, None):
        return parsed

    meta = getattr(result, "response_metadata", None)
    if isinstance(meta, dict):
        parsed = _usage_mapping(meta.get("token_usage") or meta.get("usage"))
        if parsed != (None, None):
            return parsed

    if isinstance(result, dict):
        parsed = _usage_mapping(
            result.get("usage_metadata")
            or result.get("token_usage")
            or result.get("usage")
        )
        if parsed != (None, None):
            return parsed

    parsed = _usage_mapping(getattr(result, "usage", None))
    if parsed != (None, None):
        return parsed

    return None, None


def _usage_mapping(usage: Any) -> tuple[int | None, int | None]:
    if usage is None:
        return None, None
    if not isinstance(usage, dict):
        tokens_in = getattr(usage, "input_tokens", None)
        if tokens_in is None:
            tokens_in = getattr(usage, "prompt_tokens", None)
        tokens_out = getattr(usage, "output_tokens", None)
        if tokens_out is None:
            tokens_out = getattr(usage, "completion_tokens", None)
        return _as_optional_int(tokens_in), _as_optional_int(tokens_out)

    tokens_in = usage.get("input_tokens", usage.get("prompt_tokens"))
    tokens_out = usage.get("output_tokens", usage.get("completion_tokens"))
    if tokens_in is None and tokens_out is None and "total_tokens" in usage:
        return _as_optional_int(usage.get("total_tokens")), None
    return _as_optional_int(tokens_in), _as_optional_int(tokens_out)


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _maybe_record_usage(
    guard: BudgetGuard,
    result: Any,
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> None:
    if not record_usage:
        return
    tokens_in, tokens_out = extract_token_usage(result)
    if tokens_in is None and tokens_out is None:
        return
    guard.record_usage(
        scope_key=scope_key,
        kwargs=kwargs,
        tokens_in=tokens_in or 0,
        tokens_out=tokens_out or 0,
    )


def wrap_llm_callable(
    guard: BudgetGuard,
    call: Callable[P, R],
    *,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Callable[P, R]:
    """Wrap one model-turn callable with ``check("llm")`` (+ optional usage)."""
    if getattr(call, _MARK, False):
        return call

    if inspect.iscoroutinefunction(call):

        @functools.wraps(call)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            guard.check(KIND_LLM, scope_key=scope_key, kwargs=dict(kwargs))
            result = await call(*args, **kwargs)
            _maybe_record_usage(
                guard,
                result,
                scope_key=scope_key,
                kwargs=dict(kwargs),
                record_usage=record_usage,
            )
            return result

        wrapper: Callable[P, R] = async_wrapper  # type: ignore[assignment]
    else:

        @functools.wraps(call)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            guard.check(KIND_LLM, scope_key=scope_key, kwargs=dict(kwargs))
            result = call(*args, **kwargs)
            if inspect.isawaitable(result):
                return _await_and_record(
                    guard,
                    result,
                    scope_key=scope_key,
                    kwargs=dict(kwargs),
                    record_usage=record_usage,
                )
            _maybe_record_usage(
                guard,
                result,
                scope_key=scope_key,
                kwargs=dict(kwargs),
                record_usage=record_usage,
            )
            return result

        wrapper = sync_wrapper  # type: ignore[assignment]

    setattr(wrapper, _MARK, True)
    return wrapper


async def _await_and_record(
    guard: BudgetGuard,
    result: Awaitable[Any],
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> Any:
    value = await result
    _maybe_record_usage(
        guard,
        value,
        scope_key=scope_key,
        kwargs=kwargs,
        record_usage=record_usage,
    )
    return value


def budget_llm(
    guard: BudgetGuard,
    *,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator: ``check("llm")`` before the wrapped model-turn function."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        return wrap_llm_callable(
            guard,
            func,
            scope_key=scope_key,
            record_usage=record_usage,
        )

    return decorator


def _wrap_method(
    guard: BudgetGuard,
    owner: Any,
    name: str,
    *,
    scope_key: str | None,
    record_usage: bool,
) -> None:
    original = getattr(owner, name, None)
    if original is None or not callable(original):
        return
    if getattr(original, _MARK, False):
        return
    # Bound methods: wrap the underlying function with a bound-style wrapper.
    wrapped = wrap_llm_callable(
        guard,
        original,
        scope_key=scope_key,
        record_usage=record_usage,
    )
    setattr(owner, name, wrapped)


def _wrap_stream_method(
    guard: BudgetGuard,
    owner: Any,
    name: str,
    *,
    scope_key: str | None,
    record_usage: bool,
) -> None:
    original = getattr(owner, name, None)
    if original is None or not callable(original):
        return
    if getattr(original, _MARK, False):
        return

    if inspect.iscoroutinefunction(original):

        @functools.wraps(original)
        async def async_stream(*args: Any, **kwargs: Any) -> Any:
            guard.check(KIND_LLM, scope_key=scope_key, kwargs=dict(kwargs))
            stream = original(*args, **kwargs)
            if hasattr(stream, "__aiter__"):
                return _arecord_stream(
                    guard,
                    stream,
                    scope_key=scope_key,
                    kwargs=dict(kwargs),
                    record_usage=record_usage,
                )
            return stream

        setattr(async_stream, _MARK, True)
        setattr(owner, name, async_stream)
        return

    @functools.wraps(original)
    def sync_stream(*args: Any, **kwargs: Any) -> Any:
        guard.check(KIND_LLM, scope_key=scope_key, kwargs=dict(kwargs))
        stream = original(*args, **kwargs)
        if hasattr(stream, "__iter__") and not isinstance(stream, (str, bytes)):
            return _record_stream(
                guard,
                stream,
                scope_key=scope_key,
                kwargs=dict(kwargs),
                record_usage=record_usage,
            )
        return stream

    setattr(sync_stream, _MARK, True)
    setattr(owner, name, sync_stream)


def _record_stream(
    guard: BudgetGuard,
    stream: Any,
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> Iterator[Any]:
    last: Any = None
    for chunk in stream:
        last = chunk
        yield chunk
    _maybe_record_usage(
        guard,
        last,
        scope_key=scope_key,
        kwargs=kwargs,
        record_usage=record_usage,
    )


async def _arecord_stream(
    guard: BudgetGuard,
    stream: Any,
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> Any:
    last: Any = None
    async for chunk in stream:
        last = chunk
        yield chunk
    _maybe_record_usage(
        guard,
        last,
        scope_key=scope_key,
        kwargs=kwargs,
        record_usage=record_usage,
    )


def instrument_langgraph_llm(
    model: Any,
    guard: BudgetGuard,
    *,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Any:
    """Wrap a LangGraph/LangChain chat model so each turn hits the budget.

    Patches ``invoke`` / ``ainvoke`` / ``batch`` / ``abatch`` / ``stream`` /
    ``astream`` in place. Idempotent. Does not import LangChain — duck-typed.
    """
    if getattr(model, _MARK, False):
        return model
    for name in ("invoke", "ainvoke", "batch", "abatch"):
        _wrap_method(
            guard,
            model,
            name,
            scope_key=scope_key,
            record_usage=record_usage,
        )
    for name in ("stream", "astream"):
        _wrap_stream_method(
            guard,
            model,
            name,
            scope_key=scope_key,
            record_usage=record_usage,
        )
    setattr(model, _MARK, True)
    return model


def instrument_crewai_llm(
    llm: Any,
    guard: BudgetGuard,
    *,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Any:
    """Wrap a CrewAI-style LLM (``.call`` / ``.acall``) with budget checks.

    Duck-typed — CrewAI is not a Mycelium dependency. Idempotent.
    """
    if getattr(llm, _MARK, False):
        return llm
    for name in ("call", "acall", "invoke", "ainvoke"):
        _wrap_method(
            guard,
            llm,
            name,
            scope_key=scope_key,
            record_usage=record_usage,
        )
    setattr(llm, _MARK, True)
    return llm


def instrument_llm(
    target: Any,
    guard: BudgetGuard,
    *,
    framework: str | None = None,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Any:
    """Dispatch to the right framework glue (or wrap a plain callable).

    ``framework``: ``"langgraph"`` | ``"crewai"`` | ``"callable"`` | ``None``
    (auto-detect from duck type).
    """
    kind = framework
    if kind is None:
        kind = _detect_framework(target)
    if kind == "langgraph":
        return instrument_langgraph_llm(
            target,
            guard,
            scope_key=scope_key,
            record_usage=record_usage,
        )
    if kind == "crewai":
        return instrument_crewai_llm(
            target,
            guard,
            scope_key=scope_key,
            record_usage=record_usage,
        )
    if kind == "callable":
        if not callable(target):
            raise TypeError(
                "instrument_llm(..., framework='callable') requires a callable"
            )
        return wrap_llm_callable(
            guard,
            target,
            scope_key=scope_key,
            record_usage=record_usage,
        )
    raise ValueError(
        f"framework must be one of 'langgraph', 'crewai', 'callable', or None; "
        f"got {framework!r}"
    )


def _detect_framework(target: Any) -> str:
    if callable(target) and not hasattr(target, "invoke") and not hasattr(target, "call"):
        return "callable"
    # Chat models expose invoke; CrewAI LLM historically exposes call.
    if hasattr(target, "invoke") or hasattr(target, "ainvoke"):
        return "langgraph"
    if hasattr(target, "call") or hasattr(target, "acall"):
        return "crewai"
    if callable(target):
        return "callable"
    raise TypeError(
        "cannot auto-detect LLM framework for "
        f"{type(target).__name__}; pass framework='langgraph'|'crewai'|'callable'"
    )


__all__ = [
    "budget_llm",
    "extract_token_usage",
    "instrument_crewai_llm",
    "instrument_langgraph_llm",
    "instrument_llm",
    "wrap_llm_callable",
]
