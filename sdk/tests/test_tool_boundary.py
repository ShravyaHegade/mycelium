from pathlib import Path

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


async def test_scope_gate_blocks_dotdot_prefix_bypass() -> None:
    """``startswith`` alone lets ``/allowed/../../etc/passwd`` through; normpath must not."""
    calls = 0

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=["/workspace/src/"])
    async def delete_file(path: str) -> dict:
        nonlocal calls
        calls += 1
        return {"deleted": path}

    with pytest.raises(ToolBoundaryError) as exc:
        await delete_file(path="/workspace/src/../../etc/passwd")

    assert exc.value.violation == "scope_path"
    assert calls == 0


async def test_scope_gate_blocks_sibling_prefix_bypass() -> None:
    """``/workspace_evil`` must not match allowlist ``/workspace``."""
    calls = 0

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=["/workspace"])
    async def delete_file(path: str) -> dict:
        nonlocal calls
        calls += 1
        return {"deleted": path}

    with pytest.raises(ToolBoundaryError) as exc:
        await delete_file(path="/workspace_evil/secret")

    assert exc.value.violation == "scope_path"
    assert calls == 0

    await delete_file(path="/workspace/ok.txt")
    assert calls == 1


async def test_scope_gate_blocks_directory_symlink_escape(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()
    target = outside / "target.txt"
    target.write_text("keep")
    (allowed / "escape").symlink_to(outside, target_is_directory=True)
    calls = 0

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def delete_file(path: str) -> dict[str, str]:
        nonlocal calls
        calls += 1
        Path(path).unlink()
        return {"deleted": path}

    with pytest.raises(ToolBoundaryError) as exc:
        await delete_file(path=str(allowed / "escape" / "target.txt"))

    assert exc.value.violation == "scope_path"
    assert calls == 0
    assert target.read_text() == "keep"


async def test_scope_gate_blocks_missing_target_through_symlink_escape(
    tmp_path: Path,
) -> None:
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()
    (allowed / "escape").symlink_to(outside, target_is_directory=True)
    calls = 0

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def create_file(path: str) -> dict[str, str]:
        nonlocal calls
        calls += 1
        Path(path).write_text("created")
        return {"created": path}

    target = outside / "new.txt"
    with pytest.raises(ToolBoundaryError) as exc:
        await create_file(path=str(allowed / "escape" / "new.txt"))

    assert exc.value.violation == "scope_path"
    assert calls == 0
    assert not target.exists()


def test_scope_gate_blocks_file_symlink_escape(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside"
    allowed.mkdir()
    outside.mkdir()
    target = outside / "target.txt"
    target.write_text("private")
    link = allowed / "target.txt"
    link.symlink_to(target)
    calls = 0

    @bounded_sync(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    def read_file(path: str) -> str:
        nonlocal calls
        calls += 1
        return Path(path).read_text()

    with pytest.raises(ToolBoundaryError) as exc:
        read_file(path=str(link))

    assert exc.value.violation == "scope_path"
    assert calls == 0


async def test_scope_gate_allows_symlink_resolving_inside_root(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    nested = allowed / "nested"
    nested.mkdir(parents=True)
    target = nested / "target.txt"
    target.write_text("safe")
    (allowed / "alias").symlink_to(nested, target_is_directory=True)

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def read_file(path: str) -> str:
        return Path(path).read_text()

    assert await read_file(path=str(allowed / "alias" / "target.txt")) == "safe"


async def test_scope_gate_allows_missing_descendant_inside_root(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    candidate = allowed / "missing" / "new.txt"

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def prepare_file(path: str) -> dict[str, str]:
        return {"path": path}

    assert await prepare_file(path=str(candidate)) == {"path": str(candidate)}


async def test_scope_gate_allows_relative_root_and_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    allowed = Path("allowed")
    allowed.mkdir()
    target = allowed / "target.txt"
    target.write_text("safe")

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def read_file(path: str) -> str:
        return Path(path).read_text()

    assert await read_file(path=str(target)) == "safe"


async def test_scope_gate_allows_allowed_root_that_is_a_symlink(tmp_path: Path) -> None:
    storage = tmp_path / "storage"
    storage.mkdir()
    target = storage / "target.txt"
    target.write_text("safe")
    allowed = tmp_path / "allowed"
    allowed.symlink_to(storage, target_is_directory=True)

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def read_file(path: str) -> str:
        return Path(path).read_text()

    assert await read_file(path=str(allowed / "target.txt")) == "safe"


async def test_scope_gate_fails_closed_on_symlink_loop(tmp_path: Path) -> None:
    allowed = tmp_path / "allowed"
    allowed.mkdir()
    (allowed / "loop-a").symlink_to(allowed / "loop-b")
    (allowed / "loop-b").symlink_to(allowed / "loop-a")
    calls = 0

    @bounded(schema=DELETE_FILE_SCHEMA, allowed_paths=[str(allowed)])
    async def read_file(path: str) -> str:
        nonlocal calls
        calls += 1
        return Path(path).read_text()

    with pytest.raises(ToolBoundaryError) as exc:
        await read_file(path=str(allowed / "loop-a" / "target.txt"))

    assert exc.value.violation == "scope_path"
    assert calls == 0


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
