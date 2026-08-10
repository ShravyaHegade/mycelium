"""Adoption example: LangGraph + Redis + receipts + crash (requires Redis)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("langgraph")

from backend_gates import require_redis_or_skip

EXAMPLE = (
    Path(__file__).resolve().parents[1]
    / "examples"
    / "langgraph_redis_crash"
    / "run.py"
)
SDK_ROOT = Path(__file__).resolve().parents[1]


def test_example_run_py_main_exits_zero() -> None:
    url = require_redis_or_skip()
    env = {
        **os.environ,
        "MYCELIUM_REDIS_URL": url,
        "MYCELIUM_SIGNING_KEY": "demo-signing-key",
        "PYTHONPATH": os.pathsep.join(
            [str(SDK_ROOT), os.environ.get("PYTHONPATH", "")]
        ).rstrip(os.pathsep),
    }
    proc = subprocess.run(
        [sys.executable, str(EXAMPLE)],
        cwd=str(SDK_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stdout + "\n" + proc.stderr
    assert "redispatch RETURN" in proc.stdout
    assert "crash HARD_BLOCK" in proc.stdout
