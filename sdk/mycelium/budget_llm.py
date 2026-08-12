"""Auto-wire ``BudgetGuard.check("llm")`` around model turns.

Tools already get ``@budget_guard``. When ``budget:`` is enabled, supported
frameworks (LangGraph / LangChain chat models) are patched automatically so
hosts only configure YAML limits. Manual ``check("llm")`` /
``record_usage()`` remain the custom-provider fallback.

- Automatic: LangChain ``BaseChatModel`` invoke / stream (config load)
- Raw loop: ``@budget_llm(guard)`` / ``wrap_llm_callable``
- Explicit wrap: ``instrument_langgraph_llm`` / ``instrument_crewai_llm``
- Duck-typed entry: ``instrument_llm``
"""

from __future__ import annotations

import contextvars
import functools
import inspect
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, ParamSpec, TypeVar

from mycelium.budget_guard import KIND_LLM, BudgetGuard
from mycelium.loop_guard import resolve_run_id
from mycelium.transition import TransitionScope, execution_scope

P = ParamSpec("P")
R = TypeVar("R")

_MARK = "_mycelium_budget_llm"
_LLM_ADAPTER_LANGGRAPH = "langgraph"

_active_budget_guard: BudgetGuard | None = None
_registered_llm_adapters: dict[str, LlmBudgetAdapter] = {}
_cost_resolvers: list[Callable[..., float | None]] = []
_llm_turn_active: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "mycelium_llm_turn_active", default=False
)
_unwired_llm_warned = False


@dataclass(frozen=True)
class LlmBudgetAdapter:
    """Custom or built-in LLM budget adapter.

    Official LangGraph/LangChain wiring is installed automatically.
    Register one of these only for an unsupported provider.
    """

    name: str
    measures_tokens: bool = True
    measures_cost: bool = False
    extract_usage: Callable[[Any], tuple[int | None, int | None]] | None = None
    resolve_cost: Callable[..., float | None] | None = None
    pre_call: Callable[..., None] | None = None
    finalize_stream: Callable[..., Any] | None = None

    def __post_init__(self) -> None:
        if self.measures_cost and self.resolve_cost is None:
            raise ValueError(
                f"LLM budget adapter {self.name!r} sets measures_cost=True "
                "but provides no resolve_cost. Mycelium never invents prices."
            )


def get_active_budget_guard() -> BudgetGuard | None:
    return _active_budget_guard


def set_active_budget_guard(guard: BudgetGuard | None) -> None:
    global _active_budget_guard
    _active_budget_guard = guard


def registered_llm_budget_adapters() -> frozenset[str]:
    return frozenset(_registered_llm_adapters)


def register_llm_budget_adapter(adapter: LlmBudgetAdapter) -> None:
    """Declare a custom LLM adapter before ``load_config`` (unsupported providers)."""
    name = str(adapter.name).strip()
    if not name:
        raise ValueError("LLM budget adapter name must be non-empty")
    _registered_llm_adapters[name] = adapter


def register_llm_cost_resolver(resolver: Callable[..., float | None]) -> None:
    """Host price table: ``(tokens_in, tokens_out, model=None) -> usd | None``."""
    _cost_resolvers.append(resolver)


def reset_llm_budget_state() -> None:
    """Clear process-wide automatic LLM budget state (tests)."""
    global _active_budget_guard, _unwired_llm_warned
    _active_budget_guard = None
    _registered_llm_adapters.clear()
    _cost_resolvers.clear()
    _unwired_llm_warned = False


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

    gen = getattr(result, "generation_info", None)
    if isinstance(gen, dict):
        parsed = _usage_mapping(gen.get("token_usage") or gen.get("usage"))
        if parsed != (None, None):
            return parsed

    llm_output = getattr(result, "llm_output", None)
    if isinstance(llm_output, dict):
        parsed = _usage_mapping(llm_output.get("token_usage") or llm_output.get("usage"))
        if parsed != (None, None):
            return parsed

    return None, None


def extract_model_identity(result: Any) -> tuple[str | None, str | None]:
    """Best-effort ``(provider, model)`` from common response shapes."""
    if result is None:
        return None, None
    if isinstance(result, (list, tuple)):
        for item in reversed(result):
            provider, model = extract_model_identity(item)
            if provider or model:
                return provider, model
        return None, None

    meta = getattr(result, "response_metadata", None)
    if not isinstance(meta, dict) and isinstance(result, dict):
        meta = result.get("response_metadata") or result
    if not isinstance(meta, dict):
        meta = {}
    model = (
        meta.get("model_name")
        or meta.get("model")
        or meta.get("ls_model_name")
        or getattr(result, "model", None)
        or getattr(result, "model_name", None)
    )
    provider = (
        meta.get("ls_provider")
        or meta.get("provider")
        or getattr(result, "provider", None)
    )
    return (
        str(provider) if provider else None,
        str(model) if model else None,
    )


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


def _adapter_extract_usage(result: Any) -> tuple[int | None, int | None]:
    for adapter in _registered_llm_adapters.values():
        if adapter.extract_usage is None:
            continue
        parsed = adapter.extract_usage(result)
        if parsed != (None, None):
            return parsed
    return extract_token_usage(result)


def _resolve_cost(
    tokens_in: int,
    tokens_out: int,
    model: str | None,
) -> float | None:
    for adapter in _registered_llm_adapters.values():
        if adapter.resolve_cost is None:
            continue
        usd = adapter.resolve_cost(tokens_in, tokens_out, model)
        if usd is not None:
            return float(usd)
    for resolver in _cost_resolvers:
        usd = resolver(tokens_in, tokens_out, model)
        if usd is not None:
            return float(usd)
    return None


def _maybe_record_usage(
    guard: BudgetGuard,
    result: Any,
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
    missing_if_empty: bool = True,
) -> None:
    if not record_usage:
        return
    tokens_in, tokens_out = _adapter_extract_usage(result)
    provider, model = extract_model_identity(result)
    if tokens_in is None and tokens_out is None:
        if missing_if_empty:
            guard.note_missing_usage(scope_key=scope_key, kwargs=kwargs)
        return
    usd = _resolve_cost(tokens_in or 0, tokens_out or 0, model)
    guard.record_usage(
        scope_key=scope_key,
        kwargs=kwargs,
        tokens_in=tokens_in or 0,
        tokens_out=tokens_out or 0,
        usd=usd,
        model=model,
        provider=provider,
    )


def _call_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    cfg = kwargs.get("config")
    configurable: dict[str, Any] = {}
    if isinstance(cfg, dict):
        raw = cfg.get("configurable")
        if isinstance(raw, dict):
            configurable = raw
        run_id = kwargs.get("run_id") or cfg.get("run_id") or configurable.get("run_id")
        if run_id:
            merged = dict(kwargs)
            merged["run_id"] = run_id
            return merged
    return dict(kwargs)


@contextmanager
def _llm_execution_scope(kwargs: dict[str, Any]) -> Iterator[None]:
    run_id = resolve_run_id(kwargs=kwargs)
    if run_id is None:
        yield
        return
    with execution_scope(TransitionScope(run_id=str(run_id), node="llm")):
        yield


def _pre_call_hooks(kwargs: dict[str, Any]) -> None:
    for adapter in _registered_llm_adapters.values():
        if adapter.pre_call is not None:
            adapter.pre_call(kwargs)


def _record_from_exception(
    guard: BudgetGuard,
    exc: BaseException,
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> None:
    if not record_usage:
        return
    tokens_in, tokens_out = _adapter_extract_usage(exc)
    if tokens_in is None and tokens_out is None:
        return
    provider, model = extract_model_identity(exc)
    usd = _resolve_cost(tokens_in or 0, tokens_out or 0, model)
    try:
        guard.record_usage(
            scope_key=scope_key,
            kwargs=kwargs,
            tokens_in=tokens_in or 0,
            tokens_out=tokens_out or 0,
            usd=usd,
            model=model,
            provider=provider,
        )
    except Exception:
        # Never hide the provider error behind accounting.
        return


def _account_after_provider_failure(
    guard: BudgetGuard,
    exc: BaseException,
    *,
    chunks: list[Any] | None = None,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> None:
    """Record known usage or mark it unknown. Never raise over ``exc``."""
    if not record_usage:
        return
    try:
        if _adapter_extract_usage(exc) != (None, None):
            _record_from_exception(
                guard,
                exc,
                scope_key=scope_key,
                kwargs=kwargs,
                record_usage=record_usage,
            )
            return
        if chunks:
            _record_aggregated_usage(
                guard,
                chunks=chunks,
                completed=False,
                scope_key=scope_key,
                kwargs=kwargs,
                record_usage=record_usage,
            )
            return
        guard.note_missing_usage(
            scope_key=scope_key,
            kwargs=kwargs,
            reason="provider call failed before usage metadata arrived",
        )
    except Exception:
        return


def wrap_llm_callable(
    guard: BudgetGuard,
    call: Callable[P, R],
    *,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Callable[P, R]:
    """Wrap one model-turn callable with ``check("llm")`` (+ optional usage)."""
    marked = getattr(call, _MARK, False) or getattr(
        getattr(call, "__func__", None), _MARK, False
    )
    if marked:
        return call

    if inspect.iscoroutinefunction(call):

        @functools.wraps(call)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            call_kwargs = _call_kwargs(dict(kwargs))
            with _llm_execution_scope(call_kwargs):
                if _llm_turn_active.get():
                    return await call(*args, **kwargs)
                token = _llm_turn_active.set(True)
                try:
                    _pre_call_hooks(call_kwargs)
                    guard.check(KIND_LLM, scope_key=scope_key, kwargs=call_kwargs)
                    try:
                        result = await call(*args, **kwargs)
                    except BaseException as exc:
                        _account_after_provider_failure(
                            guard,
                            exc,
                            scope_key=scope_key,
                            kwargs=call_kwargs,
                            record_usage=record_usage,
                        )
                        raise
                    _maybe_record_usage(
                        guard,
                        result,
                        scope_key=scope_key,
                        kwargs=call_kwargs,
                        record_usage=record_usage,
                    )
                    return result
                finally:
                    _llm_turn_active.reset(token)

        wrapper: Callable[P, R] = async_wrapper  # type: ignore[assignment]
    else:

        @functools.wraps(call)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            call_kwargs = _call_kwargs(dict(kwargs))
            with _llm_execution_scope(call_kwargs):
                if _llm_turn_active.get():
                    return call(*args, **kwargs)
                token = _llm_turn_active.set(True)
                try:
                    _pre_call_hooks(call_kwargs)
                    guard.check(KIND_LLM, scope_key=scope_key, kwargs=call_kwargs)
                    try:
                        result = call(*args, **kwargs)
                    except BaseException as exc:
                        _account_after_provider_failure(
                            guard,
                            exc,
                            scope_key=scope_key,
                            kwargs=call_kwargs,
                            record_usage=record_usage,
                        )
                        raise
                    if inspect.isawaitable(result):
                        return _await_and_record(
                            guard,
                            result,
                            scope_key=scope_key,
                            kwargs=call_kwargs,
                            record_usage=record_usage,
                        )
                    _maybe_record_usage(
                        guard,
                        result,
                        scope_key=scope_key,
                        kwargs=call_kwargs,
                        record_usage=record_usage,
                    )
                    return result
                finally:
                    _llm_turn_active.reset(token)

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
    try:
        value = await result
    except BaseException as exc:
        _account_after_provider_failure(
            guard,
            exc,
            scope_key=scope_key,
            kwargs=kwargs,
            record_usage=record_usage,
        )
        raise
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


def _is_marked(fn: Any) -> bool:
    if fn is None:
        return False
    if getattr(fn, _MARK, False):
        return True
    return bool(getattr(getattr(fn, "__func__", None), _MARK, False))


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
    if _is_marked(original):
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
    if _is_marked(original):
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


def _merge_chunk_usage(
    seen: list[tuple[int | None, int | None]],
) -> tuple[int | None, int | None]:
    """Prefer last cumulative total; fall back to summing deltas."""
    nonempty = [pair for pair in seen if pair != (None, None)]
    if not nonempty:
        return None, None
    last_in, last_out = nonempty[-1]
    if len(nonempty) == 1:
        return last_in, last_out
    last_total = (last_in or 0) + (last_out or 0)
    earlier_max = max((a or 0) + (b or 0) for a, b in nonempty[:-1])
    if last_total >= earlier_max:
        return last_in, last_out
    return (
        sum(pair[0] or 0 for pair in nonempty),
        sum(pair[1] or 0 for pair in nonempty),
    )


def _record_aggregated_usage(
    guard: BudgetGuard,
    *,
    chunks: list[Any],
    completed: bool,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> None:
    if not record_usage:
        return
    for adapter in _registered_llm_adapters.values():
        if adapter.finalize_stream is not None:
            adapter.finalize_stream(chunks, completed=completed)
    seen = [_adapter_extract_usage(chunk) for chunk in chunks]
    tokens_in, tokens_out = _merge_chunk_usage(seen)
    last = chunks[-1] if chunks else None
    if tokens_in is None and tokens_out is None and last is not None:
        tokens_in, tokens_out = _adapter_extract_usage(last)
    if tokens_in is None and tokens_out is None:
        reason = (
            "stream closed before usage metadata arrived"
            if not completed
            else "no token usage in the provider/framework response"
        )
        guard.note_missing_usage(
            scope_key=scope_key, kwargs=kwargs, reason=reason
        )
        return
    provider, model = extract_model_identity(last)
    usd = _resolve_cost(tokens_in or 0, tokens_out or 0, model)
    guard.record_usage(
        scope_key=scope_key,
        kwargs=kwargs,
        tokens_in=tokens_in or 0,
        tokens_out=tokens_out or 0,
        usd=usd,
        model=model,
        provider=provider,
    )


def _record_stream(
    guard: BudgetGuard,
    stream: Any,
    *,
    scope_key: str | None,
    kwargs: dict[str, Any] | None,
    record_usage: bool,
) -> Iterator[Any]:
    chunks: list[Any] = []
    completed = False
    try:
        for chunk in stream:
            chunks.append(chunk)
            yield chunk
        completed = True
    except BaseException as exc:
        _account_after_provider_failure(
            guard,
            exc,
            chunks=chunks,
            scope_key=scope_key,
            kwargs=kwargs,
            record_usage=record_usage,
        )
        raise
    _record_aggregated_usage(
        guard,
        chunks=chunks,
        completed=completed,
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
    chunks: list[Any] = []
    completed = False
    try:
        async for chunk in stream:
            chunks.append(chunk)
            yield chunk
        completed = True
    except BaseException as exc:
        _account_after_provider_failure(
            guard,
            exc,
            chunks=chunks,
            scope_key=scope_key,
            kwargs=kwargs,
            record_usage=record_usage,
        )
        raise
    _record_aggregated_usage(
        guard,
        chunks=chunks,
        completed=completed,
        scope_key=scope_key,
        kwargs=kwargs,
        record_usage=record_usage,
    )


def install_langgraph_llm_budget() -> bool:
    """Patch LangChain ``BaseChatModel`` once so invoke/stream hit BudgetGuard.

    Returns True when a real model boundary is installed. Idempotent.
    """
    try:
        from langchain_core.language_models.chat_models import BaseChatModel
    except ImportError:
        return False

    if _is_marked(BaseChatModel.invoke):
        return True

    def _make_invoke(orig: Any) -> Any:
        if inspect.iscoroutinefunction(orig):

            @functools.wraps(orig)
            async def async_invoke(self: Any, *args: Any, **kwargs: Any) -> Any:
                guard = get_active_budget_guard()
                if guard is None or _llm_turn_active.get():
                    return await orig(self, *args, **kwargs)
                wrapped = wrap_llm_callable(guard, orig)
                return await wrapped(self, *args, **kwargs)

            setattr(async_invoke, _MARK, True)
            return async_invoke

        @functools.wraps(orig)
        def invoke_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            guard = get_active_budget_guard()
            if guard is None or _llm_turn_active.get():
                return orig(self, *args, **kwargs)
            wrapped = wrap_llm_callable(guard, orig)
            return wrapped(self, *args, **kwargs)

        setattr(invoke_wrapper, _MARK, True)
        return invoke_wrapper

    def _make_stream(orig: Any) -> Any:
        if inspect.iscoroutinefunction(orig):

            @functools.wraps(orig)
            async def async_stream(self: Any, *args: Any, **kwargs: Any) -> Any:
                guard = get_active_budget_guard()
                if guard is None or _llm_turn_active.get():
                    return orig(self, *args, **kwargs)
                call_kwargs = _call_kwargs(kwargs)
                with _llm_execution_scope(call_kwargs):
                    token = _llm_turn_active.set(True)
                    try:
                        _pre_call_hooks(call_kwargs)
                        guard.check(KIND_LLM, kwargs=call_kwargs)
                        stream = orig(self, *args, **kwargs)
                        if hasattr(stream, "__aiter__"):
                            return _arecord_stream(
                                guard,
                                stream,
                                scope_key=None,
                                kwargs=call_kwargs,
                                record_usage=True,
                            )
                        return stream
                    finally:
                        _llm_turn_active.reset(token)

            setattr(async_stream, _MARK, True)
            return async_stream

        @functools.wraps(orig)
        def stream_wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            guard = get_active_budget_guard()
            if guard is None or _llm_turn_active.get():
                return orig(self, *args, **kwargs)
            call_kwargs = _call_kwargs(kwargs)
            with _llm_execution_scope(call_kwargs):
                token = _llm_turn_active.set(True)
                try:
                    _pre_call_hooks(call_kwargs)
                    guard.check(KIND_LLM, kwargs=call_kwargs)
                    stream = orig(self, *args, **kwargs)
                    if hasattr(stream, "__aiter__"):
                        return _arecord_stream(
                            guard,
                            stream,
                            scope_key=None,
                            kwargs=call_kwargs,
                            record_usage=True,
                        )
                    if hasattr(stream, "__iter__") and not isinstance(
                        stream, (str, bytes)
                    ):
                        return _record_stream(
                            guard,
                            stream,
                            scope_key=None,
                            kwargs=call_kwargs,
                            record_usage=True,
                        )
                    return stream
                finally:
                    _llm_turn_active.reset(token)

        setattr(stream_wrapper, _MARK, True)
        return stream_wrapper

    for name in ("invoke", "ainvoke", "batch", "abatch"):
        original = getattr(BaseChatModel, name, None)
        if original is None or not callable(original) or _is_marked(original):
            continue
        setattr(BaseChatModel, name, _make_invoke(original))

    for name in ("stream", "astream"):
        original = getattr(BaseChatModel, name, None)
        if original is None or not callable(original) or _is_marked(original):
            continue
        setattr(BaseChatModel, name, _make_stream(original))

    return True


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
    "LlmBudgetAdapter",
    "budget_llm",
    "extract_model_identity",
    "extract_token_usage",
    "get_active_budget_guard",
    "install_langgraph_llm_budget",
    "instrument_crewai_llm",
    "instrument_langgraph_llm",
    "instrument_llm",
    "register_llm_budget_adapter",
    "register_llm_cost_resolver",
    "registered_llm_budget_adapters",
    "reset_llm_budget_state",
    "set_active_budget_guard",
    "wrap_llm_callable",
]
