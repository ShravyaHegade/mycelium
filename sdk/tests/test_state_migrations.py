"""Unified guard-state migration API and CLI coverage."""

from __future__ import annotations

from pathlib import Path

import pytest

from mycelium import (
    CompletionRunState,
    FileAtomicStateBackend,
    FileCompletionStorage,
    FileLoopGuardStorage,
    LoopRunState,
    StateMigrationError,
    apply_state_migration,
    load_config,
    plan_state_migration,
)
from mycelium.__main__ import main


def _config(tmp_path: Path) -> Path:
    path = tmp_path / "mycelium.yaml"
    path.write_text(
        f"""
state_backend:
  storage: file
  path: {tmp_path / "shared.json"}
  namespace: test
loop_guard:
  storage: file
  path: {tmp_path / "loops.json"}
completion:
  storage: file
  path: {tmp_path / "completion.json"}
  required: [done]
tools: {{}}
""",
        encoding="utf-8",
    )
    return path


def test_plan_apply_and_repeat_state_migration(tmp_path: Path) -> None:
    FileLoopGuardStorage(tmp_path / "loops.json").set(
        LoopRunState(scope_key="run-loop", streak=2)
    )
    FileCompletionStorage(tmp_path / "completion.json").set(
        CompletionRunState(scope_key="run-completion", required=["done"])
    )
    cfg = load_config(_config(tmp_path))

    plan = plan_state_migration(cfg)
    assert plan.total_records == 2
    assert plan.pending_records == 2
    assert plan.can_apply

    result = apply_state_migration(cfg)
    assert result.migrated_records == 2
    assert result.unchanged_records == 0

    again = apply_state_migration(cfg)
    assert again.migrated_records == 0
    assert again.unchanged_records == 2
    backend = FileAtomicStateBackend(tmp_path / "shared.json")
    assert backend.get("test:loop_guard", "run-loop") is not None
    assert backend.get("test:completion", "run-completion") is not None
    assert FileLoopGuardStorage(tmp_path / "loops.json").get("run-loop") is not None


def test_state_migration_cli_plan_and_apply(tmp_path: Path, capsys) -> None:
    FileLoopGuardStorage(tmp_path / "loops.json").set(LoopRunState(scope_key="run"))
    config = _config(tmp_path)

    assert main(["state", "migrate", "--plan", "-c", str(config)]) == 0
    assert "migrate=1" in capsys.readouterr().out
    assert main(["state", "migrate", "--apply", "-c", str(config)]) == 0
    assert "migrated=1" in capsys.readouterr().out


def test_state_migration_refuses_different_destination_record(tmp_path: Path) -> None:
    FileLoopGuardStorage(tmp_path / "loops.json").set(
        LoopRunState(scope_key="run", streak=1)
    )
    config = _config(tmp_path)
    cfg = load_config(config)
    backend = FileAtomicStateBackend(tmp_path / "shared.json")
    assert backend.create(
        "test:loop_guard",
        "run",
        LoopRunState(scope_key="run", streak=99).to_dict(),
    )

    plan = plan_state_migration(cfg)
    assert plan.conflicting_records == 1
    assert not plan.can_apply
    with pytest.raises(StateMigrationError, match="conflicting"):
        apply_state_migration(cfg)
