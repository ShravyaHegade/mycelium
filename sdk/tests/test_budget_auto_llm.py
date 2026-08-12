"""Automatic LLM budget enforcement (LangChain/LangGraph + production)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from mycelium import (
    PROFILE_PRODUCTION,
    BudgetAccountingError,
    ConfigError,
    LedgerHardBlockError,
    load_config_from_string,
    register_llm_budget_adapter,
    register_llm_cost_resolver,
    reset_llm_budget_state,
    wrap_llm_callable,
)
from mycelium.budget_guard import (
    KIND_LLM,
    BudgetGuard,
    InMemoryBudgetGuardStorage,
)
from mycelium.budget_llm import (
    LlmBudgetAdapter,
    get_active_budget_guard,
    instrument_langgraph_llm,
)
from mycelium.transition import TransitionScope, execution_scope


@pytest.fixture(autouse=True)
def _reset_llm_budget() -> None:
    reset_llm_budget_state()
    yield
    reset_llm_budget_state()


def _scope(run_id: str = "llm-auto") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="agent")


def _budget_yaml(
    *,
    production: bool = False,
    tokens: bool = True,
    cost: bool = False,
    missing: str | None = None,
    extra: str = "",
    langgraph: bool = True,
) -> str:
    profile = "profile: production\n" if production else ""
    meters = "  max_steps: 30\n"
    if tokens:
        meters += "  max_tokens: 100000\n"
    if cost:
        meters += "  max_cost_usd: 10\n"
    policy = (
        f"  missing_usage_policy: {missing}\n" if missing is not None else ""
    )
    integration = (
        "integrations:\n  langgraph:\n    enabled: true\n" if langgraph else ""
    )
    outcomes = (
        "outcome_emit:\n  storage: file\n  path: ./mycelium-test-outcomes.jsonl\n"
        if production
        else ""
    )
    return f"""
{profile}budget:
  storage: memory
{meters}{policy}{extra}{integration}{outcomes}
"""


class _CountingChat(BaseChatModel):
    """Minimal chat model so BaseChatModel.invoke is the real boundary."""

    def __init__(self, sink: dict[str, int], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sink = sink

    @property
    def _llm_type(self) -> str:
        return "counting-fake"

    def _generate(
        self,
        messages: list[Any],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        self._sink["n"] = self._sink.get("n", 0) + 1
        msg = AIMessage(
            content="ok",
            usage_metadata={
                "input_tokens": 4,
                "output_tokens": 6,
                "total_tokens": 10,
            },
            response_metadata={"model_name": "fake-mini", "ls_provider": "fake"},
        )
        return ChatResult(generations=[ChatGeneration(message=msg)])


def test_sync_model_checks_before_and_records_after() -> None:
    cfg = load_config_from_string(_budget_yaml())
    assert cfg.llm_budget_wired
    guard = cfg.build_budget_guard()
    assert guard is not None
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    with execution_scope(_scope("sync")):
        out = model.invoke("hi")
        assert out.content == "ok"
        assert sink["n"] == 1
        state = guard.get_state("sync")
        assert state is not None
        assert state.steps == 1
        assert state.tokens == 10
        assert state.last_model == "fake-mini"


async def test_async_matches_sync() -> None:
    cfg = load_config_from_string(_budget_yaml())
    guard = cfg.build_budget_guard()
    assert guard is not None
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    with execution_scope(_scope("async")):
        out = await model.ainvoke("hi")
        assert out.content == "ok"
        state = guard.get_state("async")
        assert state is not None
        assert state.tokens == 10
        assert state.steps == 1


def test_streaming_aggregates_and_records_once() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(), max_tokens=1000, max_steps=5
    )

    class StreamModel:
        def stream(self, prompt: str) -> Any:
            yield SimpleNamespace(content="a", usage_metadata=None)
            yield SimpleNamespace(
                content="b",
                usage_metadata={"input_tokens": 3, "output_tokens": 5},
            )

    model = instrument_langgraph_llm(StreamModel(), guard)
    with execution_scope(_scope("stream")):
        chunks = list(model.stream("hi"))
        assert len(chunks) == 2
        state = guard.get_state("stream")
        assert state is not None
        assert state.tokens == 8
        assert state.steps == 1


def test_provider_retries_do_not_double_count() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(), max_tokens=1000, max_steps=5
    )
    inner_calls = {"n": 0}

    def provider(prompt: str) -> dict[str, Any]:
        inner_calls["n"] += 1
        if inner_calls["n"] == 1:
            # Framework-level retry stays inside one wrapped invoke.
            inner_calls["n"] += 1
        return {"usage": {"input_tokens": 2, "output_tokens": 2}}

    wrapped = wrap_llm_callable(guard, provider)
    with execution_scope(_scope("retry")):
        wrapped("hi")
        state = guard.get_state("retry")
        assert state is not None
        assert state.tokens == 4
        assert state.steps == 1
        assert inner_calls["n"] == 2


def test_exhausted_budget_prevents_provider_call() -> None:
    load_config_from_string(
        """
budget:
  storage: memory
  max_steps: 1
  warn_at: 1.0
"""
    )
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    with execution_scope(_scope("exh")):
        model.invoke("one")
        with pytest.raises(LedgerHardBlockError):
            model.invoke("two")
    assert sink["n"] == 1


def test_missing_usage_warns_once() -> None:
    cfg = load_config_from_string(
        """
budget:
  storage: memory
  max_tokens: 1000
  missing_usage_policy: warn
  on_missing_meter: "off"
"""
    )
    guard = cfg.build_budget_guard()
    assert guard is not None

    def bare(prompt: str) -> str:
        return prompt

    wrapped = wrap_llm_callable(guard, bare)
    with execution_scope(_scope("miss-warn")):
        with pytest.warns(UserWarning, match="no measurable usage"):
            assert wrapped("a") == "a"
        # Second call does not warn again.
        assert wrapped("b") == "b"
        state = guard.get_state("miss-warn")
        assert state is not None
        assert state.tokens == 0


def test_missing_usage_error_blocks_later_calls() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(),
        max_tokens=1000,
        max_steps=10,
        missing_usage_policy="error",
    )

    def bare(prompt: str) -> str:
        return prompt

    wrapped = wrap_llm_callable(guard, bare)
    with execution_scope(_scope("miss-err")):
        with pytest.raises(BudgetAccountingError, match="never invented"):
            wrapped("a")
        with pytest.raises(BudgetAccountingError, match="unknown LLM usage"):
            wrapped("b")


def test_different_runs_have_isolated_budgets() -> None:
    cfg = load_config_from_string(_budget_yaml())
    guard = cfg.build_budget_guard()
    assert guard is not None
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    with execution_scope(_scope("run-a")):
        model.invoke("a")
    with execution_scope(_scope("run-b")):
        model.invoke("b")
    assert guard.get_state("run-a") is not None
    assert guard.get_state("run-b") is not None
    assert guard.get_state("run-a").tokens == 10
    assert guard.get_state("run-b").tokens == 10


def test_same_run_retries_share_accounting() -> None:
    cfg = load_config_from_string(_budget_yaml())
    guard = cfg.build_budget_guard()
    assert guard is not None
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    with execution_scope(_scope("shared")):
        model.invoke("a")
    with execution_scope(_scope("shared")):
        model.invoke("b")
    state = guard.get_state("shared")
    assert state is not None
    assert state.steps == 2
    assert state.tokens == 20


def test_instrumentation_is_idempotent() -> None:
    cfg = load_config_from_string(_budget_yaml())
    cfg2 = load_config_from_string(_budget_yaml())
    assert cfg.llm_budget_wired
    assert cfg2.llm_budget_wired
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    again = instrument_langgraph_llm(model, cfg2.build_budget_guard())
    assert again is model
    with execution_scope(_scope("once")):
        model.invoke("hi")
    guard = cfg2.build_budget_guard()
    assert guard is not None
    state = guard.get_state("once")
    assert state is not None
    assert state.steps == 1
    assert state.tokens == 10


def test_missing_usage_does_not_replace_provider_or_stream_error() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(),
        max_tokens=1000,
        max_steps=5,
        missing_usage_policy="error",
    )

    def fail(prompt: str) -> str:
        raise RuntimeError("provider down")

    wrapped = wrap_llm_callable(guard, fail)
    with execution_scope(_scope("hide-sync")):
        with pytest.raises(RuntimeError, match="provider down"):
            wrapped("hi")
        state = guard.get_state("hide-sync")
        assert state is not None
        assert state.usage_unknown

    class StreamBoom:
        def stream(self, prompt: str) -> Any:
            yield SimpleNamespace(content="partial")
            raise RuntimeError("stream down")

    model = instrument_langgraph_llm(StreamBoom(), guard)
    with execution_scope(_scope("hide-stream")):
        with pytest.raises(RuntimeError, match="stream down"):
            list(model.stream("hi"))
        streamed = guard.get_state("hide-stream")
        assert streamed is not None
        assert streamed.usage_unknown


def test_measures_cost_requires_resolve_cost() -> None:
    with pytest.raises(ValueError, match="resolve_cost"):
        LlmBudgetAdapter(name="priced", measures_cost=True)


def test_provider_exceptions_remain_visible() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(), max_tokens=1000, max_steps=5
    )

    class Boom(Exception):
        usage_metadata = {"input_tokens": 1, "output_tokens": 1}

    def fail(prompt: str) -> str:
        raise Boom("provider down")

    wrapped = wrap_llm_callable(guard, fail)
    with execution_scope(_scope("boom")):
        with pytest.raises(Boom, match="provider down"):
            wrapped("hi")
        state = guard.get_state("boom")
        assert state is not None
        assert state.tokens == 2


def test_production_fails_without_llm_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    budget_llm_mod = importlib.import_module("mycelium.budget_llm")
    monkeypatch.setattr(budget_llm_mod, "install_langgraph_llm_budget", lambda: False)
    with pytest.raises(ConfigError, match="LLM adapter"):
        load_config_from_string(_budget_yaml(production=True))


def test_production_fails_when_langgraph_present_but_not_selected() -> None:
    with pytest.raises(ConfigError, match="explicitly selected") as exc:
        load_config_from_string(_budget_yaml(production=True, langgraph=False))
    assert "integrations.langgraph.enabled" in str(exc.value)
    assert "Having LangGraph installed is not enough" in str(exc.value)


def test_production_rejects_unenforceable_cost_limit() -> None:
    with pytest.raises(ConfigError, match="cost resolver"):
        load_config_from_string(_budget_yaml(production=True, cost=True))


def test_production_accepts_cost_resolver() -> None:
    register_llm_cost_resolver(lambda tin, tout, model=None: 0.01)
    cfg = load_config_from_string(_budget_yaml(production=True, cost=True))
    assert cfg.profile == PROFILE_PRODUCTION
    assert cfg.llm_budget_wired


def test_production_rejects_explicit_missing_usage_warn() -> None:
    with pytest.raises(ConfigError, match="missing_usage_policy"):
        load_config_from_string(
            _budget_yaml(production=True, missing="warn")
        )


def test_manual_apis_remain_compatible() -> None:
    guard = BudgetGuard(
        InMemoryBudgetGuardStorage(), max_steps=2, warn_at=1.0
    )
    with execution_scope(_scope("manual")):
        guard.check(KIND_LLM)
        guard.record_usage(tokens_in=1, tokens_out=1, usd=0.0)
        state = guard.get_state("manual")
        assert state is not None
        assert state.tokens == 2


def test_existing_config_without_budget_unchanged() -> None:
    cfg = load_config_from_string("tools:\n  ping: {}")
    assert cfg.budget is None
    assert not cfg.llm_budget_wired
    assert get_active_budget_guard() is None
    sink: dict[str, int] = {}
    model = _CountingChat(sink)
    out = model.invoke("hi")
    assert out.content == "ok"
    assert sink["n"] == 1


def test_custom_adapter_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    import importlib

    budget_llm_mod = importlib.import_module("mycelium.budget_llm")
    monkeypatch.setattr(budget_llm_mod, "install_langgraph_llm_budget", lambda: False)
    register_llm_budget_adapter(
        LlmBudgetAdapter(name="custom", measures_tokens=True, measures_cost=False)
    )
    cfg = load_config_from_string(_budget_yaml(production=True, langgraph=False))
    assert "custom" in cfg._llm_adapters
    assert not cfg.langgraph_enabled
