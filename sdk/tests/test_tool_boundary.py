import pytest

from mycelium import ToolBoundaryError, bounded, bounded_sync

FETCH_CUSTOMER_SCHEMA = {
    "customer_id": {
        "type": "string",
        "required": True,
        "min_length": 1,
        "pattern": r"^c\d+$",
    },
}

CUSTOMER_RECORD_SCHEMA = {
    "customer_id": {"type": "string", "required": True},
    "name": {"type": "string", "required": True},
}

DELETE_FILE_SCHEMA = {
    "path": {"type": "string", "required": True},
}


async def test_bounded_accepts_valid_input() -> None:
    calls = 0

    @bounded(schema=FETCH_CUSTOMER_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        nonlocal calls
        calls += 1
        return {"customer_id": customer_id}

    result = await fetch_customer(customer_id="c1")

    assert result == {"customer_id": "c1"}
    assert calls == 1


async def test_bounded_raises_on_missing_field() -> None:
    calls = 0

    @bounded(schema=FETCH_CUSTOMER_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        nonlocal calls
        calls += 1
        return {"customer_id": customer_id}

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer()

    assert exc.value.violation == "missing_required_field"
    assert exc.value.field == "customer_id"
    assert exc.value.tool_name == "fetch_customer"
    assert "customer_id" in exc.value.llm_message
    assert calls == 0


async def test_bounded_raises_on_null_value() -> None:
    calls = 0

    @bounded(schema=FETCH_CUSTOMER_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        nonlocal calls
        calls += 1
        return {"customer_id": customer_id}

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer(customer_id=None)

    assert exc.value.violation in {"type_mismatch", "string_type"}
    assert "null" in exc.value.llm_message.lower() or exc.value.actual == "null"
    assert calls == 0


async def test_bounded_raises_on_pattern_mismatch() -> None:
    @bounded(schema=FETCH_CUSTOMER_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        return {"customer_id": customer_id}

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer(customer_id="alice")

    assert exc.value.violation == "pattern_mismatch"
    assert "alice" in (exc.value.actual or "")


def test_bounded_sync_validates_before_call() -> None:
    calls = 0

    @bounded_sync(schema=FETCH_CUSTOMER_SCHEMA)
    def fetch_customer(customer_id: str) -> dict:
        nonlocal calls
        calls += 1
        return {"customer_id": customer_id}

    with pytest.raises(ToolBoundaryError):
        fetch_customer(customer_id="bad")

    assert calls == 0
    assert fetch_customer(customer_id="c99") == {"customer_id": "c99"}
    assert calls == 1


async def test_llm_message_is_actionable() -> None:
    @bounded(schema=FETCH_CUSTOMER_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        return {}

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer(customer_id=None)

    msg = exc.value.llm_message
    assert "fetch_customer" in msg
    assert "customer_id" in msg
    assert "Expected:" in msg


async def test_scope_gate_blocks_disallowed_path() -> None:
    calls = 0

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=["/workspace/src/"])
    async def delete_file(path: str) -> dict:
        nonlocal calls
        calls += 1
        return {"deleted": path}

    with pytest.raises(ToolBoundaryError) as exc:
        await delete_file(path="/.git/config")

    assert exc.value.violation == "scope_path"
    assert calls == 0

    await delete_file(path="/workspace/src/foo.py")
    assert calls == 1


async def test_entity_pattern_scope_gate() -> None:
    @bounded(
        schema=FETCH_CUSTOMER_SCHEMA,
        entity_param="customer_id",
        entity_pattern=r"^c\d+$",
    )
    async def fetch_customer(customer_id: str) -> dict:
        return {"customer_id": customer_id}

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer(customer_id="alice")

    assert exc.value.violation in {"scope_entity_pattern", "pattern_mismatch"}


async def test_output_validation_blocks_bad_return() -> None:
    calls = 0

    @bounded(schema=FETCH_CUSTOMER_SCHEMA, output_schema=CUSTOMER_RECORD_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        nonlocal calls
        calls += 1
        return []

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer(customer_id="c1")

    assert exc.value.violation == "output_validation_failed"
    assert calls == 1


# --- Honey / langchain#39150 shapes: array + object + bind failures ---

TOOL_USE_IDS_SCHEMA = {
    "ids": {
        "type": "array",
        "items": {"type": "integer"},
        "required": True,
    },
}

TOOL_USE_INPUT_SCHEMA = {
    "input": {"type": "object", "required": True},
}


async def test_bounded_accepts_array_of_integers() -> None:
    """Honey shape: {"type": "tool_use", "input": {"ids": [1,2,3]}}."""
    calls = 0

    @bounded(schema=TOOL_USE_IDS_SCHEMA)
    async def tool_use(ids: list[int]) -> dict:
        nonlocal calls
        calls += 1
        return {"ids": ids}

    assert await tool_use(ids=[1, 2, 3]) == {"ids": [1, 2, 3]}
    assert calls == 1


async def test_bounded_rejects_array_item_type_mismatch() -> None:
    calls = 0

    @bounded(schema=TOOL_USE_IDS_SCHEMA)
    async def tool_use(ids: list[int]) -> dict:
        nonlocal calls
        calls += 1
        return {"ids": ids}

    with pytest.raises(ToolBoundaryError) as exc:
        await tool_use(ids=["a", "b"])

    assert calls == 0
    assert exc.value.tool_name == "tool_use"
    assert exc.value.field is not None and exc.value.field.startswith("ids")


async def test_bounded_accepts_object_field() -> None:
    @bounded(schema=TOOL_USE_INPUT_SCHEMA)
    async def tool_use(input: dict) -> dict:
        return input

    payload = {"ids": [1, 2, 3]}
    assert await tool_use(input=payload) == payload


async def test_bounded_accepts_array_of_objects() -> None:
    schema = {
        "rows": {
            "type": "array",
            "items": {"type": "object"},
            "required": True,
        }
    }

    @bounded(schema=schema)
    async def batch_write(rows: list[dict]) -> dict:
        return {"n": len(rows)}

    assert await batch_write(rows=[{"a": 1}, {"b": 2}]) == {"n": 2}


async def test_unknown_schema_type_raises_at_decoration_time() -> None:
    from mycelium.schema import SchemaBuildError

    with pytest.raises(SchemaBuildError, match="unknown type 'sting'"):

        @bounded(schema={"name": {"type": "sting", "required": True}})
        async def bad_tool(name: str) -> dict:
            return {"name": name}


async def test_undeclared_kwarg_is_tool_boundary_error() -> None:
    calls = 0

    @bounded(schema=FETCH_CUSTOMER_SCHEMA)
    async def fetch_customer(customer_id: str) -> dict:
        nonlocal calls
        calls += 1
        return {"customer_id": customer_id}

    with pytest.raises(ToolBoundaryError) as exc:
        await fetch_customer(customer_id="c1", ids=[1, 2, 3])

    assert isinstance(exc.value, ToolBoundaryError)
    assert not isinstance(exc.value, TypeError)
    assert exc.value.violation == "unexpected_kwarg"
    assert exc.value.field == "ids"
    assert "ids" in exc.value.llm_message
    assert calls == 0


async def test_required_missing_is_tool_boundary_error_not_type_error() -> None:
    calls = 0

    @bounded(schema=TOOL_USE_IDS_SCHEMA)
    async def tool_use(ids: list[int]) -> dict:
        nonlocal calls
        calls += 1
        return {"ids": ids}

    with pytest.raises(ToolBoundaryError) as exc:
        await tool_use()

    assert exc.value.violation == "missing_required_field"
    assert exc.value.field == "ids"
    assert calls == 0
