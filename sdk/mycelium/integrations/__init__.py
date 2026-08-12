"""Optional framework adapters for Mycelium."""

from mycelium.integrations.crewai import instrument_crewai_llm
from mycelium.integrations.langgraph import (
    LangGraphIntegrationError,
    completion_gate_end,
    install_langgraph_completion_terminal,
    instrument_langgraph_llm,
    instrument_langgraph_tool,
)

__all__ = [
    "LangGraphIntegrationError",
    "completion_gate_end",
    "install_langgraph_completion_terminal",
    "instrument_crewai_llm",
    "instrument_langgraph_llm",
    "instrument_langgraph_tool",
]
