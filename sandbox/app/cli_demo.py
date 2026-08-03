"""Run the user's graph like ``mycelium run --config … -- python agent_app.py``."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any

from app.models import Injector, PlanStep, ToolSpec
from app.yaml_builder import tools_from_plan


def _agent_source(plan: list[PlanStep], injector: Injector) -> str:
    outcomes = {
        s.tool_call_id: (s.outcome or "success") for s in plan
    }
    # Compact JSON only — multiline dumps break textwrap.dedent indent stripping.
    plan_json = json.dumps(
        [s.model_dump(exclude_none=True) for s in plan],
        separators=(",", ":"),
    )
    outcomes_json = json.dumps(outcomes, separators=(",", ":"))
    return textwrap.dedent(
        f'''\
        """Sandbox-generated agent — plain python vs mycelium run."""

        from __future__ import annotations

        import json
        import threading
        import time

        PLAN = json.loads("""{plan_json}""")
        OUTCOMES_BY_CALL = json.loads("""{outcomes_json}""")
        INJECTOR = {injector!r}
        EXECUTIONS: dict[str, int] = {{}}


        def _count(name: str) -> int:
            EXECUTIONS[name] = EXECUTIONS.get(name, 0) + 1
            return EXECUTIONS[name]


        def _outcome(tool_call_id: str) -> str:
            return OUTCOMES_BY_CALL.get(tool_call_id, "success")


        def _timeout_raise(tool_call_id: str, message: str) -> None:
            try:
                from mycelium import record_external_operation, side_effect
            except ImportError:
                raise RuntimeError(message)
            with side_effect():
                record_external_operation(f"pi_sandbox_{{tool_call_id}}")
                raise RuntimeError(message)


        def _tool(
            name: str,
            *,
            fail_msg: str,
            timeout_msg: str | None = None,
            result: dict,
            tool_call_id: str,
            **printed: object,
        ) -> dict:
            n = _count(name)
            outcome = _outcome(tool_call_id)
            extras = " ".join(f"{{k}}={{v!r}}" for k, v in printed.items())
            print(
                f"[tool] {{name}} n={{n}} {{extras}} outcome={{outcome}} id={{tool_call_id}}",
                flush=True,
            )
            if outcome in ("fail_before", "custom_fail"):
                raise RuntimeError(fail_msg)
            if outcome == "timeout_after_effect" and timeout_msg:
                _timeout_raise(tool_call_id, timeout_msg)
            if outcome == "slow":
                time.sleep(0.35)
            return result


        def charge(amount: float = 10.0, tool_call_id: str = "call_1") -> dict:
            return _tool(
                "charge",
                fail_msg="charge failed before effect",
                timeout_msg="provider timeout after charge accepted",
                result={{"charged": amount}},
                tool_call_id=tool_call_id,
                amount=amount,
            )


        def refund(amount: float = 10.0, tool_call_id: str = "call_1") -> dict:
            return _tool(
                "refund",
                fail_msg="refund failed before effect",
                timeout_msg="provider timeout after refund accepted",
                result={{"refunded": amount}},
                tool_call_id=tool_call_id,
                amount=amount,
            )


        def send_email(to: str = "user@example.com", tool_call_id: str = "call_1") -> dict:
            return _tool(
                "send_email",
                fail_msg="send_email failed before effect",
                timeout_msg="smtp timeout after accept",
                result={{"sent": True, "to": to}},
                tool_call_id=tool_call_id,
                to=to,
            )


        def ship_order(order_id: str = "ord_1001", tool_call_id: str = "call_1") -> dict:
            return _tool(
                "ship_order",
                fail_msg="ship failed before warehouse commit",
                timeout_msg="warehouse timeout after label printed",
                result={{"shipped": True, "order_id": order_id}},
                tool_call_id=tool_call_id,
                order_id=order_id,
            )


        def create_ticket(
            subject: str = "Need help with my order",
            tool_call_id: str = "call_1",
        ) -> dict:
            return _tool(
                "create_ticket",
                fail_msg="ticket create failed before write",
                timeout_msg="helpdesk timeout after ticket created",
                result={{"ticket": "T-1001", "subject": subject}},
                tool_call_id=tool_call_id,
                subject=subject,
            )


        def post_slack(channel: str = "#ops", tool_call_id: str = "call_1") -> dict:
            return _tool(
                "post_slack",
                fail_msg="slack post failed before send",
                timeout_msg="slack timeout after message accepted",
                result={{"posted": True, "channel": channel}},
                tool_call_id=tool_call_id,
                channel=channel,
            )


        def search_docs(query: str = "billing", tool_call_id: str = "call_1") -> dict:
            return _tool(
                "search_docs",
                fail_msg="search failed",
                result={{"query": query, "hits": EXECUTIONS.get("search_docs", 1)}},
                tool_call_id=tool_call_id,
                query=query,
            )


        def lookup_order(order_id: str = "ord_1001", tool_call_id: str = "call_1") -> dict:
            return _tool(
                "lookup_order",
                fail_msg="lookup failed",
                result={{"order_id": order_id, "status": "paid"}},
                tool_call_id=tool_call_id,
                order_id=order_id,
            )


        def set_status(
            order_id: str = "ord_1001",
            status: str = "processing",
            tool_call_id: str = "call_1",
        ) -> dict:
            return _tool(
                "set_status",
                fail_msg="status update failed before write",
                timeout_msg="db timeout after status write",
                result={{"order_id": order_id, "status": status}},
                tool_call_id=tool_call_id,
                order_id=order_id,
                status=status,
            )


        def charge_keyed(
            amount: float = 10.0,
            idempotency_key: str = "pay_key_1",
            tool_call_id: str = "call_1",
        ) -> dict:
            return _tool(
                "charge_keyed",
                fail_msg="keyed charge failed before effect",
                timeout_msg="provider timeout after keyed charge accepted",
                result={{"charged": amount, "idempotency_key": idempotency_key}},
                tool_call_id=tool_call_id,
                amount=amount,
                idempotency_key=idempotency_key,
            )


        def delete_account(
            account_id: str = "acct_1",
            tool_call_id: str = "call_1",
        ) -> dict:
            return _tool(
                "delete_account",
                fail_msg="delete failed before purge",
                timeout_msg="timeout after account purge started",
                result={{"deleted": True, "account_id": account_id}},
                tool_call_id=tool_call_id,
                account_id=account_id,
            )


        # Resolve via globals() at call time so ``mycelium run`` wrappers
        # (setattr on this module) are picked up — do not cache raw callables.
        def _kwargs(step: dict) -> dict:
            tool = step["tool"]
            out = {{"tool_call_id": step.get("tool_call_id", "call_1")}}
            if tool in ("charge", "refund"):
                out["amount"] = step.get("amount", 10.0)
            elif tool == "charge_keyed":
                out["amount"] = step.get("amount", 10.0)
                out["idempotency_key"] = step.get("idempotency_key", "pay_key_1")
            elif tool == "send_email":
                out["to"] = step.get("to", "user@example.com")
            elif tool == "search_docs":
                out["query"] = step.get("query", "billing")
            elif tool in ("ship_order", "lookup_order"):
                out["order_id"] = step.get("order_id", "ord_1001")
            elif tool == "set_status":
                out["order_id"] = step.get("order_id", "ord_1001")
                out["status"] = step.get("status", "processing")
            elif tool == "create_ticket":
                out["subject"] = step.get("subject", "Need help with my order")
            elif tool == "post_slack":
                out["channel"] = step.get("channel", "#ops")
            elif tool == "delete_account":
                out["account_id"] = step.get("account_id", "acct_1")
            return out


        def _invoke(step: dict) -> None:
            globals()[step["tool"]](**_kwargs(step))


        def main() -> int:
            print("=== sandbox agent start ===", flush=True)
            print(f"injector={{INJECTOR!r}} steps={{len(PLAN)}}", flush=True)
            if not PLAN:
                print("empty plan", flush=True)
                return 0

            if INJECTOR == "redispatch":
                for step in PLAN:
                    _invoke(step)
                    print(
                        f"[agent] redispatch {{step['tool']}} same tool_call_id",
                        flush=True,
                    )
                    _invoke(step)
            elif INJECTOR == "peer_slow":
                for step in PLAN:
                    step = dict(step)
                    OUTCOMES_BY_CALL[step["tool_call_id"]] = "slow"

                    def owner(s: dict = step) -> None:
                        try:
                            _invoke(s)
                        except BaseException as exc:  # noqa: BLE001
                            print(f"[agent] owner error: {{exc}}", flush=True)

                    def peer(s: dict = step) -> None:
                        time.sleep(0.05)
                        try:
                            _invoke(s)
                            print(
                                f"[agent] peer also invoked {{s['tool']}}",
                                flush=True,
                            )
                        except BaseException as exc:  # noqa: BLE001
                            print(
                                f"[agent] peer: {{type(exc).__name__}}: {{exc}}",
                                flush=True,
                            )

                    t1 = threading.Thread(target=owner)
                    t2 = threading.Thread(target=peer)
                    t1.start(); t2.start()
                    t1.join(timeout=3); t2.join(timeout=3)
            elif INJECTOR == "crash_hard_block":
                for step in PLAN:
                    step = dict(step)
                    OUTCOMES_BY_CALL[step["tool_call_id"]] = "timeout_after_effect"
                    try:
                        _invoke(step)
                    except Exception as exc:
                        print(
                            f"[agent] first crash {{step['tool']}}: {{exc}}",
                            flush=True,
                        )
                    print(
                        f"[agent] blind retry after crash {{step['tool']}}",
                        flush=True,
                    )
                    try:
                        _invoke(step)
                        print("[agent] retry ran again (unguarded)", flush=True)
                    except Exception as exc:
                        print(
                            f"[agent] retry: {{type(exc).__name__}}: {{exc}}",
                            flush=True,
                        )
            else:
                for step in PLAN:
                    try:
                        _invoke(step)
                    except Exception as exc:
                        print(f"[agent] step error: {{exc}}", flush=True)
                        break

            print(f"=== executions {{json.dumps(EXECUTIONS)}} ===", flush=True)
            print("=== sandbox agent done ===", flush=True)
            return 0


        if __name__ == "__main__":
            raise SystemExit(main())
        '''
    )


def _yaml_for_cli(
    tools: list[ToolSpec],
    *,
    agent_id: str,
    policy_version: str,
) -> str:
    names = sorted({t.name for t in tools})
    class_by = {t.name: t.side_effect_class for t in tools}
    lines = [
        "transition:",
        f"  agent_id: {agent_id}",
        f'  policy_version: "{policy_version}"',
        "",
        "action_ledger:",
        "  storage: memory",
        "  tools:",
    ]
    for name in names:
        lines.append(f"    - {name}")
    lines += ["", "tools:"]
    for name in names:
        lines.append(f"  {name}:")
        lines.append(f"    callable: agent_app:{name}")
        lines.append(f"    side_effect_class: {class_by[name]}")
    lines.append("")
    return "\n".join(lines)


def _run_cmd(
    cmd: list[str], *, cwd: Path, env: dict[str, str], timeout: float = 25.0
) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ok": False,
            "exit_code": -1,
            "stdout": "",
            "stderr": "timeout",
            "command": cmd,
        }
    return {
        "ok": proc.returncode == 0,
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "command": cmd,
    }


def run_mycelium_cli_demo(
    plan: list[PlanStep],
    *,
    injector: Injector = "none",
    agent_id: str = "sandbox-agent",
    policy_version: str = "2026.08.1",
    tools: list[ToolSpec] | None = None,
) -> dict[str, Any]:
    """Write temp agent + YAML; run plain python vs ``python -m mycelium run``."""
    if not plan:
        plan = [PlanStep(tool="charge", tool_call_id="call_1", outcome="success")]
    tools = tools_from_plan(plan, tools)

    with tempfile.TemporaryDirectory(prefix="mycelium-sandbox-") as tmp:
        root = Path(tmp)
        agent_path = root / "agent_app.py"
        runner_path = root / "run_agent.py"
        yaml_path = root / "mycelium.yaml"
        agent_path.write_text(_agent_source(plan, injector), encoding="utf-8")
        # Entry must ``import agent_app`` (not ``python -m agent_app`` / script path):
        # ``-m`` loads a separate ``__main__`` module, so YAML wraps on
        # ``agent_app:charge`` would not apply to the code that runs.
        runner_path.write_text(
            textwrap.dedent(
                """\
                import agent_app

                if __name__ == "__main__":
                    raise SystemExit(agent_app.main())
                """
            ),
            encoding="utf-8",
        )
        yaml_text = _yaml_for_cli(
            tools, agent_id=agent_id, policy_version=policy_version
        )
        yaml_path.write_text(yaml_text, encoding="utf-8")

        env = os.environ.copy()
        py_path = [str(root)]
        existing = env.get("PYTHONPATH")
        if existing:
            py_path.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(py_path)

        without_cmd = [sys.executable, str(runner_path)]
        with_cmd = [
            sys.executable,
            "-m",
            "mycelium",
            "run",
            "--config",
            str(yaml_path),
            "--",
            sys.executable,
            str(runner_path),
        ]

        without = _run_cmd(without_cmd, cwd=root, env=env)
        with_m = _run_cmd(with_cmd, cwd=root, env=env)

        return {
            "yaml": yaml_text,
            "agent_filename": "agent_app.py",
            "agent_preview": agent_path.read_text(encoding="utf-8")[:5000],
            "without": {
                **without,
                "display_command": "python run_agent.py",
            },
            "with_mycelium": {
                **with_m,
                "display_command": (
                    "mycelium run --config mycelium.yaml -- python run_agent.py"
                ),
            },
        }
