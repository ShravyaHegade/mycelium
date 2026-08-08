"""Schema compiler: array/object types + hard-error on unknown names (Honey)."""

import pytest
from pydantic import ValidationError

from mycelium.schema import SchemaBuildError, fields_to_model


def test_array_of_integers_accepts_and_rejects() -> None:
    model = fields_to_model(
        {"ids": {"type": "array", "items": {"type": "integer"}, "required": True}},
        model_name="IdsInput",
    )
    assert model.model_validate({"ids": [1, 2, 3]}).ids == [1, 2, 3]
    with pytest.raises(ValidationError):
        model.model_validate({"ids": ["a", "b"]})


def test_array_of_objects_accepts_dicts() -> None:
    model = fields_to_model(
        {
            "items": {
                "type": "array",
                "items": {"type": "object"},
                "required": True,
            }
        },
        model_name="ObjListInput",
    )
    payload = {"items": [{"id": 1}, {"id": 2, "name": "x"}]}
    assert model.model_validate(payload).items == payload["items"]


def test_object_field_accepts_dict() -> None:
    model = fields_to_model(
        {"input": {"type": "object", "required": True}},
        model_name="ObjInput",
    )
    assert model.model_validate({"input": {"ids": [1, 2, 3]}}).input == {
        "ids": [1, 2, 3]
    }


def test_object_with_additional_properties_value_type() -> None:
    model = fields_to_model(
        {
            "scores": {
                "type": "object",
                "additional_properties": {"type": "number"},
                "required": True,
            }
        },
        model_name="ScoresInput",
    )
    assert model.model_validate({"scores": {"a": 1.5, "b": 2}}).scores == {
        "a": 1.5,
        "b": 2.0,
    }
    with pytest.raises(ValidationError):
        model.model_validate({"scores": {"a": "nope"}})


def test_nested_array_of_arrays() -> None:
    model = fields_to_model(
        {
            "matrix": {
                "type": "array",
                "items": {"type": "array", "items": {"type": "integer"}},
                "required": True,
            }
        },
        model_name="MatrixInput",
    )
    assert model.model_validate({"matrix": [[1, 2], [3]]}).matrix == [[1, 2], [3]]


def test_unknown_type_raises_at_build_time() -> None:
    with pytest.raises(SchemaBuildError, match="unknown type 'sting'"):
        fields_to_model(
            {"name": {"type": "sting", "required": True}},
            model_name="BadInput",
        )


def test_array_without_items_raises() -> None:
    with pytest.raises(SchemaBuildError, match="array requires items"):
        fields_to_model(
            {"ids": {"type": "array", "required": True}},
            model_name="BadArray",
        )


def test_pattern_rejected_on_non_string() -> None:
    with pytest.raises(SchemaBuildError, match="pattern is only valid on string"):
        fields_to_model(
            {"ids": {"type": "integer", "pattern": r"^\d+$", "required": True}},
            model_name="BadPattern",
        )


def test_min_length_on_array() -> None:
    model = fields_to_model(
        {
            "ids": {
                "type": "array",
                "items": {"type": "integer"},
                "min_length": 1,
                "required": True,
            }
        },
        model_name="MinIds",
    )
    assert model.model_validate({"ids": [1]}).ids == [1]
    with pytest.raises(ValidationError):
        model.model_validate({"ids": []})


def test_scalars_still_work() -> None:
    model = fields_to_model(
        {
            "name": {"type": "string", "required": True},
            "count": {"type": "integer", "required": True},
            "price": {"type": "number", "required": False},
            "ok": {"type": "boolean", "required": True},
        },
        model_name="Scalars",
    )
    row = model.model_validate(
        {"name": "a", "count": 2, "price": None, "ok": True}
    )
    assert row.name == "a"
    assert row.count == 2
    assert row.price is None
    assert row.ok is True
