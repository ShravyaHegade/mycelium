"""Automatic CompletionContract terminal wiring (LangGraph END + production)."""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType
from typing import Any, TypedDict
from unittest.mock import patch

import pytest
from langgraph.graph import END, START, StateGraph

from mycelium import (
    PROFILE_PRODUCTION,
    CompletionRefusedError,
    ConfigError,
    gate_graph_end,
    load_config_from_string,
    register_terminal_adapter,
    reset_completion_terminal_state,
    wrap_final_message,
)
from mycelium.completion_contract import (
    COMPLETION_WRAPPED_MARK,
    CompletionContract,
    get_active_completion_contract,
)
from mycelium.config import _load_config_for_preflight
from mycelium.transition import TransitionScope, execution_scope


class _State(TypedDict):
    messages: list[str]
    hops: list[str]


@pytest.fixture(autouse=True)
def _reset_terminal_state() -> None:
    reset_completion_terminal_state()
    yield
    reset_completion_terminal_state()


def _completion_yaml(
    *,
    production: bool = False,
    extra: str = "",
    langgraph: bool = True,
) -> str:
    profile = "profile: production\n" if production else ""
    integration = (
        "integrations:\n  langgraph:\n    enabled: true\n" if langgraph else ""
    )
    outcomes = (
        "outcome_emit:\n  storage: file\n  path: ./mycelium-test-outcomes.jsonl\n"
        if production
        else ""
    )
    return f"""
{profile}completion:
  storage: memory
  required:
    - id: charge_customer
  optional:
    - id: send_receipt
{extra}{integration}{outcomes}
"""


def _graph():
    builder = StateGraph(_State)

    def work(state: _State) -> _State:
        return {"messages": state["messages"], "hops": [*state["hops"], "work"]}

    def finish(state: _State) -> _State:
        return {
            "messages": [*state["messages"], "done"],
            "hops": [*state["hops"], "finish"],
        }

    builder.add_node("work", work)
    builder.add_node("finish", finish)
    builder.add_edge(START, "work")
    builder.add_edge("work", "finish")
    builder.add_edge("finish", END)
    return builder.compile()


def _run_config(run_id: str = "run-auto") -> dict[str, Any]:
    return {"configurable": {"thread_id": "t1", "run_id": run_id}, "run_id": run_id}


def test_langgraph_end_automatically_protected() -> None:
    cfg = load_config_from_string(_completion_yaml())
    assert cfg.completion_terminal_wired
    graph = _graph()
    with pytest.raises(CompletionRefusedError) as exc:
        graph.invoke({"messages": ["hi"], "hops": []}, _run_config())
    assert exc.value.pending_required == ["charge_customer"]


def test_final_response_blocked_until_required_marked() -> None:
    cfg = load_config_from_string(_completion_yaml())
    contract = cfg.build_completion_contract()
    assert contract is not None
    graph = _graph()
    runtime = _run_config("run-mark")
    with pytest.raises(CompletionRefusedError):
        graph.invoke({"messages": ["hi"], "hops": []}, runtime)
    contract.mark("charge_customer", "success", scope_key="run-mark")
    result = graph.invoke({"messages": ["hi"], "hops": []}, runtime)
    assert result["messages"][-1] == "done"


def test_optional_unfinished_warns_but_allows() -> None:
    cfg = load_config_from_string(_completion_yaml())
    contract = cfg.build_completion_contract()
    assert contract is not None
    contract.mark("charge_customer", "success", scope_key="run-opt")
    graph = _graph()
    with pytest.warns(UserWarning, match="optional"):
        result = graph.invoke(
            {"messages": ["hi"], "hops": []}, _run_config("run-opt")
        )
    assert result["messages"][-1] == "done"


@pytest.mark.asyncio
async def test_sync_and_async_terminal_paths_match() -> None:
    cfg = load_config_from_string(_completion_yaml())
    contract = cfg.build_completion_contract()
    assert contract is not None
    graph = _graph()
    sync_cfg = _run_config("run-sync")
    async_cfg = _run_config("run-async")
    with pytest.raises(CompletionRefusedError) as sync_exc:
        graph.invoke({"messages": ["hi"], "hops": []}, sync_cfg)
    with pytest.raises(CompletionRefusedError) as async_exc:
        await graph.ainvoke({"messages": ["hi"], "hops": []}, async_cfg)
    assert sync_exc.value.pending_required == async_exc.value.pending_required
    contract.mark("charge_customer", "success", scope_key="run-sync")
    contract.mark("charge_customer", "success", scope_key="run-async")
    sync_out = graph.invoke({"messages": ["hi"], "hops": []}, sync_cfg)
    async_out = await graph.ainvoke({"messages": ["hi"], "hops": []}, async_cfg)
    assert sync_out["messages"][-1] == async_out["messages"][-1] == "done"


def _three_node_graph(order: list[str]):
    builder = StateGraph(_State)

    def work(state: _State) -> _State:
        order.append("work")
        return {"messages": state["messages"], "hops": [*state["hops"], "work"]}

    def mid(state: _State) -> _State:
        order.append("mid")
        return {"messages": state["messages"], "hops": [*state["hops"], "mid"]}

    def finish(state: _State) -> _State:
        order.append("finish")
        return {
            "messages": [*state["messages"], "done"],
            "hops": [*state["hops"], "finish"],
        }

    builder.add_node("work", work)
    builder.add_node("mid", mid)
    builder.add_node("finish", finish)
    builder.add_edge(START, "work")
    builder.add_edge("work", "mid")
    builder.add_edge("mid", "finish")
    builder.add_edge("finish", END)
    return builder.compile()


def _chunk_has_node(chunk: Any, node: str) -> bool:
    if isinstance(chunk, dict) and node in chunk:
        return True
    if isinstance(chunk, dict) and node in chunk.get("hops", []):
        return True
    return False


def test_stream_yields_chunks_before_graph_finishes() -> None:
    cfg = load_config_from_string(_completion_yaml())
    contract = cfg.build_completion_contract()
    assert contract is not None
    contract.mark("charge_customer", "success", scope_key="run-stream")
    order: list[str] = []
    graph = _three_node_graph(order)
    with pytest.warns(UserWarning, match="optional"):
        for _chunk in graph.stream(
            {"messages": ["hi"], "hops": []}, _run_config("run-stream")
        ):
            order.append("yield")
    assert order.index("yield") < order.index("finish")
    assert order.count("yield") >= 2


@pytest.mark.asyncio
async def test_astream_yields_chunks_before_graph_finishes() -> None:
    cfg = load_config_from_string(_completion_yaml())
    contract = cfg.build_completion_contract()
    assert contract is not None
    contract.mark("charge_customer", "success", scope_key="run-astream")
    order: list[str] = []
    graph = _three_node_graph(order)
    with pytest.warns(UserWarning, match="optional"):
        async for _chunk in graph.astream(
            {"messages": ["hi"], "hops": []}, _run_config("run-astream")
        ):
            order.append("yield")
    assert order.index("yield") < order.index("finish")
    assert order.count("yield") >= 2


def test_stream_withholds_terminal_chunk_on_refuse() -> None:
    cfg = load_config_from_string(_completion_yaml())
    assert cfg.build_completion_contract() is not None
    graph = _graph()
    seen: list[Any] = []
    with pytest.raises(CompletionRefusedError):
        for chunk in graph.stream(
            {"messages": ["hi"], "hops": []}, _run_config("run-hold")
        ):
            seen.append(chunk)
    assert seen
    assert not any(_chunk_has_node(chunk, "finish") for chunk in seen)


@pytest.mark.asyncio
async def test_astream_withholds_terminal_chunk_on_refuse() -> None:
    cfg = load_config_from_string(_completion_yaml())
    assert cfg.build_completion_contract() is not None
    graph = _graph()
    seen: list[Any] = []
    with pytest.raises(CompletionRefusedError):
        async for chunk in graph.astream(
            {"messages": ["hi"], "hops": []}, _run_config("run-ahold")
        ):
            seen.append(chunk)
    assert seen
    assert not any(_chunk_has_node(chunk, "finish") for chunk in seen)


def test_automatic_wiring_happens_exactly_once() -> None:
    checks: list[str] = []
    original = CompletionContract.check_terminal

    def _spy(self: CompletionContract, *args: Any, **kwargs: Any) -> Any:
        checks.append(self.required[0])
        return original(self, *args, **kwargs)

    cfg = load_config_from_string(_completion_yaml())
    cfg2 = load_config_from_string(_completion_yaml())
    assert cfg.completion_terminal_wired
    assert cfg2.completion_terminal_wired
    contract = cfg2.build_completion_contract()
    assert contract is not None
    contract.mark("charge_customer", "success", scope_key="run-once")
    graph = _graph()
    with patch.object(CompletionContract, "check_terminal", _spy):
        graph.invoke({"messages": ["hi"], "hops": []}, _run_config("run-once"))
    assert checks == ["charge_customer"]


def test_intermediate_nodes_are_unaffected() -> None:
    cfg = load_config_from_string(_completion_yaml())
    assert cfg.build_completion_contract() is not None
    hops: list[str] = []

    builder = StateGraph(_State)

    def work(state: _State) -> _State:
        hops.append("work")
        return {"messages": state["messages"], "hops": [*state["hops"], "work"]}

    def finish(state: _State) -> _State:
        hops.append("finish")
        return {"messages": state["messages"], "hops": [*state["hops"], "finish"]}

    builder.add_node("work", work)
    builder.add_node("finish", finish)
    builder.add_edge(START, "work")
    builder.add_edge("work", "finish")
    builder.add_edge("finish", END)
    graph = builder.compile()
    with pytest.raises(CompletionRefusedError):
        graph.invoke({"messages": ["hi"], "hops": []}, _run_config("run-hops"))
    assert hops == ["work", "finish"]


def test_production_loads_when_langgraph_adapter_installs() -> None:
    cfg = load_config_from_string(_completion_yaml(production=True))
    assert cfg.profile == PROFILE_PRODUCTION
    assert cfg.langgraph_enabled
    assert cfg.completion_terminal_wired
    graph = _graph()
    with pytest.raises(CompletionRefusedError):
        graph.invoke({"messages": ["hi"], "hops": []}, _run_config("prod-end"))


def test_production_fails_when_langgraph_present_but_not_selected() -> None:
    with pytest.raises(ConfigError, match="explicitly selected") as exc:
        load_config_from_string(_completion_yaml(production=True, langgraph=False))
    assert "integrations.langgraph.enabled" in str(exc.value)
    assert "Having LangGraph installed is not enough" in str(exc.value)


def test_production_startup_fails_when_no_adapter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "mycelium.config.install_langgraph_completion_terminal",
        lambda: False,
    )
    with pytest.raises(ConfigError, match="LangGraph") as exc:
        load_config_from_string(_completion_yaml(production=True))
    assert "wrap_final_message" in str(exc.value)
    assert "gate_graph_end" in str(exc.value)
    assert PROFILE_PRODUCTION in str(exc.value)


def test_development_warns_when_automatic_wiring_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "mycelium.config.install_langgraph_completion_terminal",
        lambda: False,
    )
    with pytest.warns(UserWarning, match="no terminal adapter"):
        cfg = load_config_from_string(_completion_yaml())
    assert not cfg.completion_terminal_wired
    assert get_active_completion_contract() is not None


def test_manual_adapters_still_work_for_custom_frameworks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "mycelium.config.install_langgraph_completion_terminal",
        lambda: False,
    )
    with pytest.warns(UserWarning, match="no terminal adapter"):
        cfg = load_config_from_string(_completion_yaml())
    contract = cfg.build_completion_contract()
    assert contract is not None
    emitted: list[str] = []

    def emit(msg: str) -> str:
        emitted.append(msg)
        return msg

    finalize = wrap_final_message(contract, emit)
    with execution_scope(TransitionScope(thread_id="t", run_id="custom", node="end")):
        with pytest.raises(CompletionRefusedError):
            finalize("nope")
        assert emitted == []
        contract.mark("charge_customer", "success")
        with pytest.warns(UserWarning, match="optional"):
            assert finalize("ok") == "ok"
        assert emitted == ["ok"]
        result = gate_graph_end(contract)
        assert result is not None
        assert result.verdict == "allow_with_warnings"


def test_production_accepts_registered_manual_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "mycelium.config.install_langgraph_completion_terminal",
        lambda: False,
    )
    register_terminal_adapter("final_message")
    cfg = load_config_from_string(_completion_yaml(production=True, langgraph=False))
    assert cfg.completion_terminal_wired
    assert cfg.profile == PROFILE_PRODUCTION
    assert not cfg.langgraph_enabled


def test_production_loads_configured_custom_adapter_installer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("test_mycelium_completion_adapter")
    calls: list[str] = []

    def install() -> None:
        calls.append("install")
        register_terminal_adapter("custom-final-message")

    module.install = install  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)

    cfg = load_config_from_string(
        _completion_yaml(
            production=True,
            langgraph=False,
            extra="  adapter_installer: test_mycelium_completion_adapter:install\n",
        )
    )

    assert calls == ["install"]
    assert cfg.completion_terminal_wired
    assert cfg._terminal_adapters == frozenset({"custom-final-message"})


def test_preflight_config_does_not_import_custom_adapter(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    module = ModuleType("test_mycelium_preflight_adapter")
    calls: list[str] = []

    def install() -> None:
        calls.append("install")
        register_terminal_adapter("custom-final-message")

    module.install = install  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, module.__name__, module)

    config_path = tmp_path / "mycelium.yaml"
    config_path.write_text(
        _completion_yaml(
            production=True,
            langgraph=False,
            extra="  adapter_installer: test_mycelium_preflight_adapter:install\n",
        ),
        encoding="utf-8",
    )
    cfg = _load_config_for_preflight(config_path)

    assert calls == []
    assert not cfg.completion_terminal_wired


def test_existing_config_without_completion_unchanged() -> None:
    cfg = load_config_from_string(
        """
tools:
  ping: {}
"""
    )
    assert cfg.completion is None
    assert not cfg.completion_terminal_wired
    assert get_active_completion_contract() is None
    graph = _graph()
    result = graph.invoke({"messages": ["hi"], "hops": []}, _run_config("plain"))
    assert result["messages"][-1] == "done"


async def test_wrap_final_message_is_idempotent_and_async() -> None:
    import inspect

    contract = CompletionContract(required=["charge_customer"])

    def emit(msg: str) -> str:
        return msg

    once = wrap_final_message(contract, emit)
    twice = wrap_final_message(contract, once)
    assert twice is once
    assert getattr(once, COMPLETION_WRAPPED_MARK)

    async def aemit(msg: str) -> str:
        return msg

    awrapped = wrap_final_message(contract, aemit)
    assert getattr(awrapped, COMPLETION_WRAPPED_MARK)
    assert inspect.iscoroutinefunction(awrapped)
    with execution_scope(
        TransitionScope(thread_id="t", run_id="async-wrap", node="end")
    ):
        with pytest.raises(CompletionRefusedError):
            await awrapped("nope")
        contract.mark("charge_customer", "success")
        assert await awrapped("ok") == "ok"
