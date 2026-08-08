"""Budget LLM auto-wiring: check("llm") without a manual per-turn hook."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mycelium import (
    BudgetGuard,
    InMemoryBudgetGuardStorage,
    LedgerHardBlockError,
    budget_llm,
    extract_token_usage,
    instrument_crewai_llm,
    instrument_langgraph_llm,
    instrument_llm,
    load_config_from_string,
    wrap_llm_callable,
)
from mycelium.transition import TransitionScope, execution_scope


def _scope(run_id: str = "llm-run") -> TransitionScope:
    return TransitionScope(thread_id="t1", run_id=run_id, node="agent")


def test_extract_token_usage_from_langchain_shapes() -> None:
    msg = SimpleNamespace(
        usage_metadata={"input_tokens": 10, "output_tokens": 4},
        response_metadata={},
    )
    assert extract_token_usage(msg) == (10, 4)

    legacy = SimpleNamespace(
        usage_metadata=None,
        response_metadata={"token_usage": {"prompt_tokens": 3, "completion_tokens": 2}},
    )
    assert extract_token_usage(legacy) == (3, 2)

    assert extract_token_usage([msg]) == (10, 4)
    assert extract_token_usage({"usage": {"input_tokens": 1, "output_tokens": 1}}) == (
        1,
        1,
    )


def test_wrap_llm_callable_checks_and_records() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=3, max_tokens=100)

    def call_model(prompt: str) -> dict:
        return {
            "text": prompt,
            "usage": {"input_tokens": 5, "output_tokens": 7},
        }

    wrapped = wrap_llm_callable(guard, call_model)
    with execution_scope(_scope()):
        assert wrapped("hi")["text"] == "hi"
        state = guard.get_state("llm-run")
        assert state is not None
        assert state.steps == 1
        assert state.tokens == 12


def test_budget_llm_decorator_hard_blocks() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=2, warn_at=1.0)

    @budget_llm(guard)
    def call_model(n: int) -> int:
        return n

    with execution_scope(_scope("dec")):
        assert call_model(1) == 1
        assert call_model(2) == 2
        with pytest.raises(LedgerHardBlockError):
            call_model(3)


def test_instrument_langgraph_llm_invoke() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=2, warn_at=1.0)

    class FakeChatModel:
        def __init__(self) -> None:
            self.calls = 0

        def invoke(self, messages: list[str], **kwargs: object) -> SimpleNamespace:
            self.calls += 1
            return SimpleNamespace(
                content=messages[-1],
                usage_metadata={"input_tokens": 2, "output_tokens": 3},
            )

        async def ainvoke(
            self, messages: list[str], **kwargs: object
        ) -> SimpleNamespace:
            return self.invoke(messages, **kwargs)

    model = instrument_langgraph_llm(FakeChatModel(), guard)
    with execution_scope(_scope("lg")):
        assert model.invoke(["a"]).content == "a"
        assert model.calls == 1
        state = guard.get_state("lg")
        assert state is not None
        assert state.tokens == 5
        model.invoke(["b"])
        with pytest.raises(LedgerHardBlockError):
            model.invoke(["c"])
        assert model.calls == 2  # third invoke blocked before body


@pytest.mark.asyncio
async def test_instrument_langgraph_llm_ainvoke() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=1, warn_at=1.0)

    class FakeChatModel:
        async def ainvoke(self, messages: list[str], **kwargs: object) -> str:
            return messages[-1]

    model = instrument_langgraph_llm(FakeChatModel(), guard, record_usage=False)
    with execution_scope(_scope("async-lg")):
        assert await model.ainvoke(["x"]) == "x"
        with pytest.raises(LedgerHardBlockError):
            await model.ainvoke(["y"])


def test_instrument_crewai_llm_call() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=2, warn_at=1.0)

    class FakeCrewLLM:
        def __init__(self) -> None:
            self.calls = 0

        def call(self, messages: list[dict[str, str]], **kwargs: object) -> str:
            self.calls += 1
            return "ok"

        async def acall(
            self, messages: list[dict[str, str]], **kwargs: object
        ) -> str:
            return self.call(messages, **kwargs)

    llm = instrument_crewai_llm(FakeCrewLLM(), guard, record_usage=False)
    with execution_scope(_scope("crew")):
        assert llm.call([{"role": "user", "content": "hi"}]) == "ok"
        llm.call([{"role": "user", "content": "again"}])
        with pytest.raises(LedgerHardBlockError):
            llm.call([{"role": "user", "content": "nope"}])
        assert llm.calls == 2


def test_instrument_llm_autodetect_and_idempotent() -> None:
    guard = BudgetGuard(InMemoryBudgetGuardStorage(), max_steps=5, warn_at=1.0)

    class FakeChatModel:
        def invoke(self, x: str) -> str:
            return x

    model = FakeChatModel()
    once = instrument_llm(model, guard)
    twice = instrument_llm(once, guard)
    assert once is twice is model

    @budget_llm(guard)
    def raw(x: str) -> str:
        return x

    again = wrap_llm_callable(guard, raw)
    assert again is raw

    with execution_scope(_scope("auto")):
        assert model.invoke("z") == "z"
        assert raw("z") == "z"


def test_config_instrument_llm_requires_budget() -> None:
    bare = load_config_from_string(
        """
transition:
  agent_id: a
  policy_version: "1"
"""
    )
    with pytest.raises(Exception, match="budget"):
        bare.instrument_llm(lambda: None, framework="callable")

    cfg = load_config_from_string(
        """
transition:
  agent_id: a
  policy_version: "1"
budget:
  storage: memory
  max_steps: 2
  warn_at: 1.0
"""
    )

    def call_model() -> str:
        return "hi"

    wrapped = cfg.instrument_llm(call_model, framework="callable", record_usage=False)
    with execution_scope(_scope("cfg")):
        assert wrapped() == "hi"
        wrapped()
        with pytest.raises(LedgerHardBlockError):
            wrapped()
