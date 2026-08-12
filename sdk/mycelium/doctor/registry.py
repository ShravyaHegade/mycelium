"""Extensible check registry for ``mycelium doctor``."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from mycelium.config import MyceliumConfig
    from mycelium.doctor.types import DoctorCheck


@dataclass(frozen=True)
class DoctorContext:
    """Shared inputs for every registered check."""

    config: MyceliumConfig
    connectivity: bool
    timeout_seconds: float
    verbose: bool


DoctorCheckFn = Callable[[DoctorContext], Iterable["DoctorCheck"]]

_REGISTRY: list[tuple[str, DoctorCheckFn]] = []


def register_check(check_id: str, fn: DoctorCheckFn) -> DoctorCheckFn:
    """Register a doctor check. Safe to call from integration modules."""
    _REGISTRY.append((check_id, fn))
    return fn


def doctor_check(check_id: str) -> Callable[[DoctorCheckFn], DoctorCheckFn]:
    def decorator(fn: DoctorCheckFn) -> DoctorCheckFn:
        return register_check(check_id, fn)

    return decorator


def iter_registered_checks() -> list[tuple[str, DoctorCheckFn]]:
    return list(_REGISTRY)


def clear_registry_for_tests() -> None:
    """Test helper — clear dynamically registered checks."""
    _REGISTRY.clear()


__all__ = [
    "DoctorCheckFn",
    "DoctorContext",
    "clear_registry_for_tests",
    "doctor_check",
    "iter_registered_checks",
    "register_check",
]
