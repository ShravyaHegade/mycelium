"""Ledger and unified-state migration CLI commands."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from mycelium.cli_transitions import _operator_storages


def cmd_migrate(args: argparse.Namespace) -> int:
    """Plan or apply explicit ActionLedger schema migrations."""
    from mycelium.action_ledger import LedgerSchemaVersionError
    from mycelium.config import ConfigError
    from mycelium.ledger_migrations import (
        LedgerMigrationError,
        apply_ledger_migration,
        plan_ledger_migration,
    )

    try:
        storages = _operator_storages(args)
        plans = [
            plan_ledger_migration(storage, target_version=args.target_version)
            for storage in storages
        ]
    except (
        ConfigError,
        LedgerMigrationError,
        LedgerSchemaVersionError,
        OSError,
        ValueError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    payload: dict[str, Any] = {
        "mode": "plan" if args.plan else "apply",
        "target_version": args.target_version,
        "backends": [
            {"backend": index, **plan.to_dict()} for index, plan in enumerate(plans, start=1)
        ],
    }
    unsupported = any(not plan.can_apply for plan in plans)

    if args.plan:
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"Ledger migration plan: target schema {args.target_version}")
            for index, plan in enumerate(plans, start=1):
                versions = (
                    ", ".join(
                        f"v{version}={count}"
                        for version, count in sorted(plan.version_counts.items())
                    )
                    or "empty"
                )
                print(
                    f"backend {index}: total={plan.total_entries} "
                    f"migrate={plan.pending_entries} current={plan.current_entries} "
                    f"active={plan.active_pending_entries} ({versions})"
                )
                if plan.unsupported_versions:
                    print(f"  unsupported versions: {list(plan.unsupported_versions)}")
            print("No ledger rows were changed.")
        return 1 if unsupported else 0

    if unsupported:
        print("error: migration refused because unsupported schema versions exist", file=sys.stderr)
        return 1

    try:
        results = [
            apply_ledger_migration(
                storage,
                target_version=args.target_version,
                allow_active=bool(args.allow_active),
            )
            for storage in storages
        ]
    except (LedgerMigrationError, LedgerSchemaVersionError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    payload["backends"] = [
        {"backend": index, **result.to_dict()} for index, result in enumerate(results, start=1)
    ]
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for index, result in enumerate(results, start=1):
            print(
                f"backend {index}: migrated={result.migrated_entries} "
                f"unchanged={result.unchanged_entries} schema={result.target_version}"
            )
        print("Ledger migration complete. Run 'mycelium migrate --plan' to verify.")
    return 0


def cmd_state_migrate(args: argparse.Namespace) -> int:
    """Plan or copy legacy guard state into ``state_backend``."""

    from mycelium.config import ConfigError, load_config
    from mycelium.state_migrations import (
        StateMigrationError,
        apply_state_migration,
        plan_state_migration,
    )

    try:
        cfg = load_config(args.config)
        plan = plan_state_migration(cfg)
    except (ConfigError, StateMigrationError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.plan:
        payload = {"mode": "plan", **plan.to_dict()}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(
                "Shared-state migration plan: "
                f"total={plan.total_records} migrate={plan.pending_records} "
                f"unchanged={plan.unchanged_records} conflicts={plan.conflicting_records}"
            )
            for feature, count in sorted(plan.feature_counts.items()):
                print(f"  {feature}: {count}")
            print("No state records were changed.")
        return 1 if not plan.can_apply else 0

    if not plan.can_apply:
        print("error: migration refused because destination conflicts exist", file=sys.stderr)
        return 1
    try:
        result = apply_state_migration(cfg)
    except (StateMigrationError, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    payload = {"mode": "apply", **result.to_dict()}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"Shared-state migration complete: migrated={result.migrated_records} "
            f"unchanged={result.unchanged_records}"
        )
        print("Switch migrated feature storage to 'shared', then run mycelium doctor.")
    return 0


