"""Optional framework adapters for Mycelium."""

from mycelium.integrations.crewai import (
    CrewAIIntegrationError,
    install_crewai_runtime,
    instrument_crewai_llm,
    instrument_crewai_tool,
)
from mycelium.integrations.langgraph import (
    LangGraphIntegrationError,
    completion_gate_end,
    install_langgraph_completion_terminal,
    instrument_langgraph_llm,
    instrument_langgraph_tool,
)

__all__ = [
    "CrewAIIntegrationError",
    "LangGraphIntegrationError",
    "completion_gate_end",
    "install_crewai_runtime",
    "install_langgraph_completion_terminal",
    "instrument_crewai_llm",
    "instrument_crewai_tool",
    "instrument_langgraph_llm",
    "instrument_langgraph_tool",
]
