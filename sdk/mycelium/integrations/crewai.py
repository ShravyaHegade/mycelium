"""CrewAI adapters (optional — CrewAI is not a Mycelium dependency)."""

from __future__ import annotations

from typing import Any


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


__all__ = ["instrument_crewai_llm"]
