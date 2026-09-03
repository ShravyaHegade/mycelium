"""Command-line implementation modules for Mycelium.

Parser code is loaded only when a caller requests it; importing the namespace
itself does not initialize the runtime or command handlers.
"""

from __future__ import annotations

__all__ = ["build_parser", "dispatch"]


def __getattr__(name: str):
    if name in __all__:
        from mycelium.cli import parser

        value = getattr(parser, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
