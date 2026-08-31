"""Standardized declarative tool contracts and their runtime validation."""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from mycelium.config_schema import ToolContractModel
from mycelium.tool_boundary import ToolBoundaryError

_TYPE_NAMES = {"string", "integer", "number", "boolean", "array", "object", "null"}
# Public ergonomic name; the config schema model remains the single source of truth.
ToolContract = ToolContractModel


def _type_from_spec(spec: str | dict[str, Any], path: str) -> Any:
    if isinstance(spec, str):
        if spec not in _TYPE_NAMES:
            raise ValueError(
                f"{path}: unsupported type {spec!r}; expected one of {sorted(_TYPE_NAMES)}"
            )
        return {
            "string": str,
            "integer": int,
            "number": float,
            "boolean": bool,
            "array": list[Any],
            "object": dict[str, Any],
            "null": type(None),
        }[spec]
    if not isinstance(spec, dict):
        raise ValueError(f"{path}: expected a type name or mapping, got {type(spec).__name__}")
    type_name = spec.get("type")
    if type_name not in _TYPE_NAMES:
        raise ValueError(
            f"{path}.type: unsupported type {type_name!r}; expected one of {sorted(_TYPE_NAMES)}"
        )
    if type_name == "array":
        return list[_type_from_spec(spec.get("items", "object"), f"{path}.items")]
    if type_name == "object" and spec.get("properties"):
        fields = {
            n: (_type_from_spec(v, f"{path}.properties.{n}"), ...)
            for n, v in spec["properties"].items()
        }
        return create_model(
            "ContractObject", __config__=ConfigDict(extra="forbid", strict=True), **fields
        )
    return _type_from_spec(type_name, path)


def _contract_model(contract: ToolContractModel, *, output: bool = False) -> type[BaseModel] | None:
    specs = (
        contract.output_schema
        if output
        else {
            n: contract.argument_types.get(n, "object")
            for n in (*contract.required_args, *contract.optional_args)
        }
    )
    if not specs:
        return None
    fields = {}
    for name, spec in specs.items():
        py_type = _type_from_spec(spec, name)
        fields[name] = (
            (py_type, ...) if output or name in contract.required_args else (py_type | None, None)
        )
    return create_model(
        "ToolContractOutput" if output else "ToolContractInput",
        __config__=ConfigDict(extra="forbid", strict=True),
        **fields,
    )


def validate_contract_definition(contract: ToolContractModel, *, tool_name: str) -> None:
    try:
        _contract_model(contract)
        _contract_model(contract, output=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"tool {tool_name!r} contract: {exc}") from exc


def _boundary(
    tool: str,
    violation: str,
    message: str,
    *,
    field: str | None = None,
    expected: str | None = None,
    actual: Any = None,
    no_retry: bool = False,
) -> ToolBoundaryError:
    error = ToolBoundaryError(
        message,
        violation=violation,
        tool_name=tool,
        llm_message=message,
        field=field,
        expected=expected,
        actual=actual,
    )
    error.no_retry = no_retry  # type: ignore[attr-defined]
    return error


def validate_contract_call(
    tool: str, contract: ToolContractModel, kwargs: dict[str, Any]
) -> dict[str, Any]:
    values = dict(kwargs)
    operation = values.pop("operation", None)
    capability = values.pop("capability", None)
    if operation is None and len(contract.operations) == 1:
        operation = contract.operations[0]
    if contract.operations and operation not in contract.operations:
        raise _boundary(
            tool,
            "unsupported_operation",
            f"Tool {tool!r} does not support operation {operation!r}; "
            f"expected one of {contract.operations}.",
            field="operation",
            expected="one of " + ", ".join(contract.operations),
            actual=operation,
        )
    if contract.capabilities and capability is not None and capability not in contract.capabilities:
        raise _boundary(
            tool,
            "unsupported_capability",
            f"Tool {tool!r} does not declare capability {capability!r}; "
            f"expected one of {contract.capabilities}.",
            field="capability",
            expected="one of " + ", ".join(contract.capabilities),
            actual=capability,
        )
    model = _contract_model(contract)
    if model is None:
        return values
    try:
        model.model_validate(values)
    except ValidationError as exc:
        first = exc.errors()[0]
        field = ".".join(str(x) for x in first.get("loc", ())) or None
        msg = str(first.get("msg", "valid value"))
        raise _boundary(
            tool,
            "contract_validation_failed",
            f"Tool {tool!r} contract validation failed for field {field!r}: "
            f"{msg}; actual={values.get(field)!r}.",
            field=field,
            expected=msg,
            actual=values.get(field),
        ) from exc
    return values


def apply_tool_contract(func: Any, contract: ToolContractModel, *, tool_name: str) -> Any:
    def contract_kwargs(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        validation_kwargs = {
            k: v for k, v in kwargs.items() if k not in {"operation", "capability"}
        }
        if args:
            try:
                bound = inspect.signature(func).bind_partial(*args, **validation_kwargs)
                return dict(bound.arguments)
            except TypeError:
                return dict(kwargs)
        return dict(kwargs)

    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            validate_contract_call(tool_name, contract, contract_kwargs(args, kwargs))
            call_kwargs = {k: v for k, v in kwargs.items() if k not in {"operation", "capability"}}
            return _validate_output(tool_name, contract, await func(*args, **call_kwargs))
    else:

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            validate_contract_call(tool_name, contract, contract_kwargs(args, kwargs))
            call_kwargs = {k: v for k, v in kwargs.items() if k not in {"operation", "capability"}}
            return _validate_output(tool_name, contract, func(*args, **call_kwargs))

    return wrapper


def _validate_output(tool: str, contract: ToolContractModel, result: Any) -> Any:
    model = _contract_model(contract, output=True)
    if model is None:
        return result
    try:
        model.model_validate(result)
    except ValidationError as exc:
        first = exc.errors()[0]
        field = ".".join(str(x) for x in first.get("loc", ())) or None
        msg = str(first.get("msg"))
        raise _boundary(
            tool,
            "output_validation_failed",
            f"Tool {tool!r} returned invalid contract output for field {field!r}: "
            f"{msg}; actual={result!r}.",
            field=field,
            expected=msg,
            actual=result,
            no_retry=True,
        ) from exc
    return result


__all__ = [
    "ToolContract",
    "ToolContractModel",
    "apply_tool_contract",
    "validate_contract_call",
    "validate_contract_definition",
]
