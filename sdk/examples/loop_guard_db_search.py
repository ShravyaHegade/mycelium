"""AF-003 demo: database search thrash across distinct tool_call_ids.

Run from the sdk directory (with the package installed / PYTHONPATH set)::

    python examples/loop_guard_db_search.py

Shows soft (ToolBoundaryError) at read N=5, then hard (LedgerHardBlockError),
then operator ``clear`` / ``allow-once``.
"""

from __future__ import annotations

from mycelium.action_ledger import LedgerHardBlockError
from mycelium.loop_guard import (
    VERIFIED_ALLOW_ONCE,
    VERIFIED_CLEAR,
    InMemoryLoopGuardStorage,
    LoopGuard,
    loop_guard_sync,
)
from mycelium.tool_boundary import ToolBoundaryError
from mycelium.transition import SideEffectClass, TransitionScope, execution_scope


def main() -> None:
    guard = LoopGuard(InMemoryLoopGuardStorage())
    calls = {"n": 0}

    @loop_guard_sync(
        guard,
        tool_name="db_search",
        side_effect_class=SideEffectClass.READ,
    )
    def db_search(query: str, *, tool_call_id: str) -> list[str]:
        calls["n"] += 1
        return [f"row for {query}"]

    scope = TransitionScope(thread_id="demo", run_id="demo-search-1", node="tools")
    print("=== AF-003 database search thrash ===\n")

    with execution_scope(scope):
        for i in range(4):
            rows = db_search(query="users where active=1", tool_call_id=f"call_{i}")
            print(f"  dispatch call_{i}: ok ({rows[0]}); bodies={calls['n']}")

        try:
            db_search(query="users where active=1", tool_call_id="call_4")
        except ToolBoundaryError as exc:
            print(f"\nSOFT: {exc.llm_message}")

        try:
            db_search(query="users where active=1", tool_call_id="call_5")
        except LedgerHardBlockError as exc:
            print(f"HARD: {exc}")

        print("\nOperator: allow-once")
        guard.release(
            "demo-search-1",
            verified=VERIFIED_ALLOW_ONCE,
            by="ops@example.com",
            reason="confirmed one more identical search is intentional",
        )
        rows = db_search(query="users where active=1", tool_call_id="call_6")
        print(f"  allow-once ran: {rows[0]}; bodies={calls['n']}")

        print("\n(re-arm will soft/hard again on further identical thrash)")
        print("Operator alternative: --verified clear to wipe counters, or abort-run")
        _ = VERIFIED_CLEAR  # documented sibling


if __name__ == "__main__":
    main()
