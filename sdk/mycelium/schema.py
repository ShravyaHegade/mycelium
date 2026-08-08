"""Internal schema helpers: converts user field specs to Pydantic models."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, create_model

FieldSpec = dict[str, Any]
ToolSchema = dict[str, FieldSpec]

_SCALAR_TYPES: dict[str, type] = {
    "string": str,
    "integer": int,
    "number": float,
    "boolean": bool,
}


class SchemaBuildError(ValueError):
    """Raised when a tool field spec cannot be compiled into a Pydantic type."""


def _compile_type(spec: FieldSpec, *, path: str) -> Any:
    """Compile a JSON-schema-ish field/item spec into a Python type annotation.

    Supported:
    - string / integer / number / boolean → scalars
    - array → requires ``items``; recursive item type → ``list[...]``
    - object → ``dict[str, Any]``, or ``dict[str, T]`` when
      ``additional_properties`` is a type spec

    Unknown type names raise :class:`SchemaBuildError` at build time (no silent
    fallback to ``str``). Nested ``properties`` models are out of v1.
    """
    if not isinstance(spec, dict):
        raise SchemaBuildError(f"{path}: type spec must be a dict, got {type(spec).__name__}")

    type_name = str(spec.get("type", "string"))

    if type_name in _SCALAR_TYPES:
        return _SCALAR_TYPES[type_name]

    if type_name == "array":
        items = spec.get("items")
        if not isinstance(items, dict):
            raise SchemaBuildError(
                f"{path}: array requires items: {{type: ...}}"
            )
        item_type = _compile_type(items, path=f"{path}.items")
        return list[item_type]  # type: ignore[valid-type]

    if type_name == "object":
        additional = spec.get("additional_properties")
        if additional is None:
            return dict[str, Any]
        if not isinstance(additional, dict):
            raise SchemaBuildError(
                f"{path}: additional_properties must be a type spec dict"
            )
        value_type = _compile_type(
            additional, path=f"{path}.additional_properties"
        )
        return dict[str, value_type]  # type: ignore[valid-type]

    raise SchemaBuildError(f"{path}: unknown type {type_name!r}")


def _field_constraints(name: str, spec: FieldSpec) -> dict[str, Any]:
    """Collect Pydantic Field kwargs; constraints are type-aware."""
    type_name = str(spec.get("type", "string"))
    constraints: dict[str, Any] = {}

    if pattern := spec.get("pattern"):
        if type_name != "string":
            raise SchemaBuildError(
                f"{name}: pattern is only valid on string fields, not {type_name!r}"
            )
        constraints["pattern"] = pattern

    if (min_length := spec.get("min_length")) is not None:
        if type_name not in {"string", "array"}:
            raise SchemaBuildError(
                f"{name}: min_length is only valid on string or array fields, "
                f"not {type_name!r}"
            )
        constraints["min_length"] = min_length

    if (max_length := spec.get("max_length")) is not None:
        if type_name not in {"string", "array"}:
            raise SchemaBuildError(
                f"{name}: max_length is only valid on string or array fields, "
                f"not {type_name!r}"
            )
        constraints["max_length"] = max_length

    return constraints


def fields_to_model(fields: ToolSchema, *, model_name: str) -> type[BaseModel]:
    """Build a Pydantic model from a user-facing field spec dict."""
    if not fields:
        raise ValueError("schema must define at least one field")

    model_fields: dict[str, Any] = {}
    for name, spec in fields.items():
        if not isinstance(spec, dict):
            raise SchemaBuildError(
                f"{name}: field spec must be a dict, got {type(spec).__name__}"
            )
        py_type = _compile_type(spec, path=name)
        constraints = _field_constraints(name, spec)

        if spec.get("required", True):
            model_fields[name] = (py_type, Field(**constraints))
        else:
            model_fields[name] = (py_type | None, Field(default=None, **constraints))

    return create_model(model_name, **model_fields)  # type: ignore[call-overload]
