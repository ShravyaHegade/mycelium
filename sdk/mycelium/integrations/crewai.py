"""CrewAI runtime adapters (optional; CrewAI is not a core dependency).

CrewAI does not expose its provider tool-call id to public tool hooks. The
adapter therefore builds a conservative logical dispatch identity from the
stable crew/run/task/agent context, tool name, and canonical tool arguments.
Applications still own business identity: production consequential tools must
use Mycelium's normal ``request_id`` / ``request_id_from`` contract.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
from collections.abc import Callable, Mapping
from contextlib import ExitStack
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, TypeVar

from mycelium.transition import (
    TransitionScope,
    canonical_json,
    dispatch_scope,
    execution_scope,
    get_active_execution_scope,
)

R = TypeVar("R")

_RUNTIME_WRAPPED_MARK = "_mycelium_crewai_runtime_wrapped"
_TOOL_WRAPPED_MARK = "_mycelium_crewai_integration"


class CrewAIIntegrationError(RuntimeError):
    """Raised when the optional CrewAI adapter cannot be applied safely."""


@dataclass(frozen=True)
class _CrewAIOptions:
    run_id_from: str | None = None


_active_options: _CrewAIOptions | None = None
_tool_scope_stacks: ContextVar[tuple[ExitStack, ...]] = ContextVar(
    "mycelium_crewai_tool_scope_stacks",
    default=(),
)


def _set_active_crewai_integration(
    *,
    enabled: bool,
    run_id_from: str | None = None,
) -> None:
    """Set process-wide CrewAI options for the latest activated config."""
    global _active_options
    _active_options = _CrewAIOptions(run_id_from=run_id_from) if enabled else None


def _object_identity(value: Any, *fields: str) -> str:
    for field in fields:
        candidate = getattr(value, field, None)
        if candidate is not None and str(candidate).strip():
            return str(candidate).strip()
    return type(value).__qualname__


def _kickoff_inputs(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    value = kwargs.get("inputs", args[0] if args else None)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise CrewAIIntegrationError("CrewAI kickoff inputs must be a mapping")
    return dict(value)


def _crew_scope(
    crew: Any,
    inputs: dict[str, Any],
    options: _CrewAIOptions,
) -> TransitionScope:
    crew_key = _object_identity(crew, "key", "name", "id")
    if options.run_id_from is not None:
        field = options.run_id_from
        value = inputs.get(field)
        if value is None or not str(value).strip():
            raise CrewAIIntegrationError(
                f"CrewAI kickoff inputs are missing the configured stable run id "
                f"field {field!r}; the crew was not started"
            )
        run_id = str(value).strip()
    else:
        digest = hashlib.sha256(
            canonical_json({"crew": crew_key, "inputs": inputs}).encode()
        ).hexdigest()
        run_id = f"crewai-run:{digest}"
    return TransitionScope(
        thread_id=f"crewai:{crew_key}",
        run_id=run_id,
        node="crew",
    )


def _tool_runtime_metadata(context: Any) -> tuple[str, TransitionScope]:
    base = get_active_execution_scope() or TransitionScope()
    task = _object_identity(getattr(context, "task", None), "key", "name", "id")
    agent = _object_identity(
        getattr(context, "agent", None), "key", "role", "id"
    )
    tool_name = str(getattr(context, "tool_name", "") or "unknown-tool")
    raw_input = getattr(context, "tool_input", {})
    tool_input = dict(raw_input) if isinstance(raw_input, Mapping) else {"value": raw_input}
    payload = {
        "thread_id": base.thread_id,
        "run_id": base.run_id,
        "task": task,
        "agent": agent,
        "tool": tool_name,
        "tool_input": tool_input,
    }
    digest = hashlib.sha256(canonical_json(payload).encode()).hexdigest()
    return (
        f"crewai:{digest}",
        TransitionScope(
            thread_id=base.thread_id,
            run_id=base.run_id,
            node=f"task:{task}:agent:{agent}",
            destructive_grants=base.destructive_grants,
        ),
    )


def _enter_tool_scope(context: Any) -> None:
    dispatch_id, scope = _tool_runtime_metadata(context)
    stack = ExitStack()
    stack.enter_context(execution_scope(scope))
    stack.enter_context(dispatch_scope(dispatch_id))
    _tool_scope_stacks.set((*_tool_scope_stacks.get(), stack))


def _exit_tool_scope(_context: Any) -> None:
    stacks = _tool_scope_stacks.get()
    if not stacks:
        return
    stack = stacks[-1]
    _tool_scope_stacks.set(stacks[:-1])
    stack.close()


def _gate_terminal(context: Any, hook_aborted: type[BaseException]) -> None:
    if getattr(context, "status", "completed") != "completed":
        return
    from mycelium.completion_contract import get_active_completion_contract

    contract = get_active_completion_contract()
    if contract is None:
        return
    scope = get_active_execution_scope()
    scope_key = None
    if scope is not None:
        scope_key = scope.run_id or scope.thread_id or None
    try:
        contract.complete_run(scope_key=scope_key)
    except Exception as exc:
        # CrewAI deliberately swallows ordinary hook exceptions. HookAborted is
        # its public fail-closed signal, so translate every terminal-check
        # failure, including storage errors, into that signal.
        raise hook_aborted(str(exc), source="mycelium") from exc


def _scoped_hooks(
    *,
    interception_point: Any,
    hook_aborted: type[BaseException],
) -> dict[Any, list[Callable[[Any], None]]]:
    return {
        interception_point.PRE_TOOL_CALL: [_enter_tool_scope],
        interception_point.POST_TOOL_CALL: [_exit_tool_scope],
        interception_point.EXECUTION_END: [
            functools.partial(_gate_terminal, hook_aborted=hook_aborted)
        ],
    }


def _run_with_crewai_context(
    crew: Any,
    original: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    scoped_hooks: Callable[..., Any],
    interception_point: Any,
    hook_aborted: type[BaseException],
) -> R:
    options = _active_options
    if options is None:
        return original(crew, *args, **kwargs)
    scope = _crew_scope(crew, _kickoff_inputs(args, kwargs), options)
    token = _tool_scope_stacks.set(())
    try:
        hooks = _scoped_hooks(
            interception_point=interception_point,
            hook_aborted=hook_aborted,
        )
        with execution_scope(scope), scoped_hooks(hooks):
            return original(crew, *args, **kwargs)
    finally:
        for stack in reversed(_tool_scope_stacks.get()):
            stack.close()
        _tool_scope_stacks.reset(token)


async def _arun_with_crewai_context(
    crew: Any,
    original: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    scoped_hooks: Callable[..., Any],
    interception_point: Any,
    hook_aborted: type[BaseException],
) -> Any:
    options = _active_options
    if options is None:
        return await original(crew, *args, **kwargs)
    scope = _crew_scope(crew, _kickoff_inputs(args, kwargs), options)
    token = _tool_scope_stacks.set(())
    try:
        hooks = _scoped_hooks(
            interception_point=interception_point,
            hook_aborted=hook_aborted,
        )
        with execution_scope(scope), scoped_hooks(hooks):
            return await original(crew, *args, **kwargs)
    finally:
        for stack in reversed(_tool_scope_stacks.get()):
            stack.close()
        _tool_scope_stacks.reset(token)


def install_crewai_runtime(*, run_id_from: str | None = None) -> bool:
    """Install CrewAI identity and completion hooks once.

    ``run_id_from`` names a stable key in ``Crew.kickoff(inputs=...)``. When
    omitted, development runs derive a deterministic run id from the crew and
    complete input mapping. That convenience does not replace the normal
    host-owned ``request_id`` requirement for consequential production tools.

    Returns ``False`` when CrewAI is not installed. Incompatible CrewAI hook
    APIs raise :class:`CrewAIIntegrationError` instead of silently running
    without protection.
    """
    _set_active_crewai_integration(enabled=True, run_id_from=run_id_from)
    try:
        from crewai import Crew
        from crewai.hooks.dispatch import (
            HookAborted,
            InterceptionPoint,
            scoped_hooks,
        )
    except ImportError:
        return False

    kickoff = getattr(Crew, "kickoff", None)
    if not callable(kickoff):
        raise CrewAIIntegrationError("CrewAI integration requires Crew.kickoff")
    if not getattr(kickoff, _RUNTIME_WRAPPED_MARK, False):
        original_kickoff = kickoff

        @functools.wraps(original_kickoff)
        def wrapped_kickoff(self: Any, *args: Any, **kwargs: Any) -> Any:
            return _run_with_crewai_context(
                self,
                original_kickoff,
                args,
                kwargs,
                scoped_hooks=scoped_hooks,
                interception_point=InterceptionPoint,
                hook_aborted=HookAborted,
            )

        setattr(wrapped_kickoff, _RUNTIME_WRAPPED_MARK, True)
        Crew.kickoff = wrapped_kickoff

    akickoff = getattr(Crew, "akickoff", None)
    if callable(akickoff) and not getattr(akickoff, _RUNTIME_WRAPPED_MARK, False):
        original_akickoff = akickoff

        @functools.wraps(original_akickoff)
        async def wrapped_akickoff(self: Any, *args: Any, **kwargs: Any) -> Any:
            return await _arun_with_crewai_context(
                self,
                original_akickoff,
                args,
                kwargs,
                scoped_hooks=scoped_hooks,
                interception_point=InterceptionPoint,
                hook_aborted=HookAborted,
            )

        setattr(wrapped_akickoff, _RUNTIME_WRAPPED_MARK, True)
        Crew.akickoff = wrapped_akickoff

    from mycelium.completion_contract import register_terminal_adapter

    register_terminal_adapter("crewai")
    return True


def instrument_crewai_tool(
    func: Callable[..., R],
    *,
    run_id_from: str | None = None,
) -> Callable[..., R]:
    """Enable CrewAI runtime identity around a Mycelium-protected callable.

    The callable's visible signature is unchanged. CrewAI lifecycle hooks put
    framework metadata in Mycelium context variables before the callable runs.
    Applying this function twice is idempotent.
    """
    if getattr(func, _TOOL_WRAPPED_MARK, False):
        return func
    if not install_crewai_runtime(run_id_from=run_id_from):
        raise CrewAIIntegrationError(
            "CrewAI integration is enabled but CrewAI is not installed; "
            "install 'mycelium-runtime[crewai]'"
        )

    if inspect.iscoroutinefunction(func):

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await func(*args, **kwargs)

        wrapper: Callable[..., R] = async_wrapper
    else:

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        wrapper = sync_wrapper
    setattr(wrapper, _TOOL_WRAPPED_MARK, True)
    return wrapper


def instrument_crewai_llm(
    llm: Any,
    guard: Any,
    *,
    scope_key: str | None = None,
    record_usage: bool = True,
) -> Any:
    """Wrap a CrewAI-style LLM so each ``call`` / ``acall`` hits the budget.

    Duck-typed; see ``mycelium.budget_llm.instrument_crewai_llm``.
    """
    from mycelium.budget_llm import instrument_crewai_llm as _instrument

    return _instrument(
        llm,
        guard,
        scope_key=scope_key,
        record_usage=record_usage,
    )


__all__ = [
    "CrewAIIntegrationError",
    "install_crewai_runtime",
    "instrument_crewai_llm",
    "instrument_crewai_tool",
]
