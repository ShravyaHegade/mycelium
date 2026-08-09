"""Thin handoff identity: parent_request_id / handoff_id audit glue."""

from __future__ import annotations

from mycelium import (
    HandoffLink,
    InMemoryLedgerStorage,
    SideEffectClass,
    TerminalOutcome,
    ToolTransitionBinding,
    TransitionScope,
    execution_scope,
    get_active_handoff,
    handoff_scope,
    ledger_sync,
)


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="demo",
        policy_version="1",
        side_effect_class=SideEffectClass.NON_IDEMPOTENT_MUTATE,
    )


def test_handoff_scope_sets_active_link() -> None:
    assert get_active_handoff() is None
    with handoff_scope("parent-1", handoff_id="h-9") as link:
        assert link == HandoffLink(parent_request_id="parent-1", handoff_id="h-9")
        assert get_active_handoff() == link
    assert get_active_handoff() is None


def test_nested_handoff_scope_restores_parent() -> None:
    with handoff_scope("outer"):
        assert get_active_handoff() is not None
        assert get_active_handoff().parent_request_id == "outer"
        with handoff_scope("inner", handoff_id="nested"):
            assert get_active_handoff().parent_request_id == "inner"
            assert get_active_handoff().handoff_id == "nested"
        assert get_active_handoff().parent_request_id == "outer"
        assert get_active_handoff().handoff_id is None


def test_child_claim_inherits_handoff_scope() -> None:
    storage = InMemoryLedgerStorage()
    binding = _binding()

    @ledger_sync(storage=storage, transition_binding=binding)
    def spawn_subagent(task: str) -> dict[str, str]:
        return {"spawned": task}

    @ledger_sync(storage=storage, transition_binding=binding)
    def charge(amount: float) -> dict[str, float]:
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        spawn_subagent(task="pay", tool_call_id="spawn-1")
        parent_id = storage.list_all()[0].request_id

        with handoff_scope(parent_id, handoff_id="handoff-pay"):
            charge(amount=10.0, tool_call_id="charge-1")

    entries = {e.tool: e for e in storage.list_all()}
    assert entries["spawn_subagent"].parent_request_id is None
    child = entries["charge"]
    assert child.parent_request_id == parent_id
    assert child.handoff_id == "handoff-pay"
    assert child.resolved_terminal_outcome() == TerminalOutcome.COMPLETED


def test_explicit_kwargs_override_handoff_scope() -> None:
    storage = InMemoryLedgerStorage()
    binding = _binding()

    @ledger_sync(storage=storage, transition_binding=binding)
    def charge(amount: float) -> dict[str, float]:
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        with handoff_scope("from-scope"):
            charge(
                amount=1.0,
                tool_call_id="c1",
                parent_request_id="from-kwarg",
                handoff_id="h-kw",
            )

    entry = storage.list_all()[0]
    assert entry.parent_request_id == "from-kwarg"
    assert entry.handoff_id == "h-kw"


def test_list_transitions_filters_by_parent() -> None:
    from mycelium import ActionLedger

    storage = InMemoryLedgerStorage()
    ledger = ActionLedger(storage=storage)
    binding = _binding()

    @ledger_sync(storage=storage, transition_binding=binding)
    def charge(amount: float) -> dict[str, float]:
        return {"charged": amount}

    with execution_scope(TransitionScope(thread_id="t1", run_id="r1")):
        with handoff_scope("parent-a"):
            charge(amount=1.0, tool_call_id="a1")
        with handoff_scope("parent-b"):
            charge(amount=2.0, tool_call_id="b1")

    kids_a = ledger.list_transitions(parent_request_id="parent-a")
    kids_b = ledger.list_transitions(parent_request_id="parent-b")
    assert len(kids_a) == 1
    assert kids_a[0].parent_request_id == "parent-a"
    assert kids_a[0].kwargs.get("amount") == 1.0 or kids_a[0].args == [1.0]
    assert len(kids_b) == 1
    assert kids_b[0].parent_request_id == "parent-b"
    assert kids_b[0].kwargs.get("amount") == 2.0 or kids_b[0].args == [2.0]


def test_entry_round_trips_handoff_fields() -> None:
    from mycelium import LedgerEntry

    entry = LedgerEntry(
        request_id="r1",
        tool="charge",
        args=[],
        kwargs={},
        status="completed",
        terminal_outcome=TerminalOutcome.COMPLETED.value,
        parent_request_id="parent-9",
        handoff_id="h-1",
    )
    restored = LedgerEntry.from_dict(entry.to_dict())
    assert restored.parent_request_id == "parent-9"
    assert restored.handoff_id == "h-1"


def test_legacy_entry_missing_handoff_fields() -> None:
    from mycelium import LedgerEntry

    restored = LedgerEntry.from_dict(
        {
            "request_id": "r1",
            "tool": "charge",
            "args": [],
            "kwargs": {},
            "status": "completed",
            "terminal_outcome": TerminalOutcome.COMPLETED.value,
        }
    )
    assert restored.parent_request_id is None
    assert restored.handoff_id is None
