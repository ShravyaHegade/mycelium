"""Thin compatibility entry point for python -m mycelium."""

from __future__ import annotations

# Preserve historical imports from mycelium.__main__ while implementations live
# in lifecycle-focused CLI modules.
# ruff: noqa: F401
import argparse
import json
import os
import shutil
import sys
from importlib import resources
from pathlib import Path
from typing import Any

from mycelium.cli.commands import (
    _ENV_ADAPTER_REPORT_SIGNING_KEY,
    _ENV_LEDGER_FILE,
    _ENV_OUTCOME_FILE,
    _ENV_POSTGRES_DSN,
    _ENV_REDIS_URL,
    _ENV_SQLITE_PATH,
    _TEMPLATE_FULL,
    _TEMPLATE_MINIMAL,
    _TEMPLATE_QUICKSTART,
    cmd_budget_release,
    cmd_budget_status,
    cmd_completion_mark,
    cmd_completion_status,
    cmd_config_docs,
    cmd_config_example,
    cmd_config_schema,
    cmd_demo,
    cmd_doctor,
    cmd_init,
    cmd_loops_release,
    cmd_loops_status,
    cmd_outcomes_dttr,
    cmd_providers_verify,
    cmd_providers_verify_report,
    cmd_run,
    cmd_scope_bind,
    cmd_scope_status,
    cmd_skills_install,
    cmd_verify,
)
from mycelium.cli.parser import build_parser, dispatch, run_cli
from mycelium.cli_migrations import cmd_migrate, cmd_state_migrate
from mycelium.cli_transitions import (
    _add_operator_storage_args,
    cmd_transitions_export,
    cmd_transitions_list,
    cmd_transitions_mark_dead,
    cmd_transitions_prune,
    cmd_transitions_release,
    cmd_transitions_show,
)
from mycelium.transition import TerminalOutcome


def main(argv: list[str] | None = None) -> int:
    return run_cli(argv)


if __name__ == "__main__":
    raise SystemExit(main())
