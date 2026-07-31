"""Run the full Mycelium feature demo (bundled proofs + transition envelope)."""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Literal

from mycelium.proofs.feature_demo import (
    prove_hard_block,
    prove_lease_auto_renew,
    prove_operator_release,
    prove_read_unknown_safe_retry,
    prove_reconcile_completed,
    prove_repair_gate,
)
from mycelium.proofs.langgraph_7417 import (
    load_fixture,
    prove_ledger_deduplication,
    reproduce_baseline_duplicate,
)

# Pacing for ``mycelium demo --slow`` (screen recordings / narration).
_SLOW_LINE_PAUSE = 0.85
_SLOW_SECTION_PAUSE = 2.8
_SLOW_EXEC_PAUSE = 1.2

_SectionKind = Literal["danger", "ok", "info"]


def _ascii_safe(text: Any) -> str:
    """Render ``text`` so it cannot crash a non-UTF-8 console (e.g. Windows).

    Windows consoles (cp1252) raise ``UnicodeEncodeError`` on characters like
    em dashes or ``->`` arrows. Demo output is ASCII-only so the tour runs
    identically everywhere; any unexpected non-ASCII is replaced rather than
    crashing the demo.
    """
    return str(text).encode("ascii", errors="replace").decode("ascii")


def _color_enabled(stream: Any = None) -> bool:
    """True when ANSI colors should be used (TTY, not NO_COLOR)."""
    if os.environ.get("NO_COLOR"):
        return False
    if os.environ.get("FORCE_COLOR"):
        return True
    target = stream if stream is not None else sys.stdout
    return bool(getattr(target, "isatty", lambda: False)())


class _Style:
    """Tiny ANSI helper — no third-party deps."""

    def __init__(self, *, enabled: bool) -> None:
        self.enabled = enabled

    def _wrap(self, code: str, text: str) -> str:
        if not self.enabled:
            return text
        return f"\033[{code}m{text}\033[0m"

    def bold(self, text: str) -> str:
        return self._wrap("1", text)

    def dim(self, text: str) -> str:
        return self._wrap("2", text)

    def red(self, text: str) -> str:
        return self._wrap("31", text)

    def green(self, text: str) -> str:
        return self._wrap("32", text)

    def yellow(self, text: str) -> str:
        return self._wrap("33", text)

    def cyan(self, text: str) -> str:
        return self._wrap("36", text)

    def bold_red(self, text: str) -> str:
        return self._wrap("1;31", text)

    def bold_green(self, text: str) -> str:
        return self._wrap("1;32", text)

    def bold_cyan(self, text: str) -> str:
        return self._wrap("1;36", text)

    def bold_yellow(self, text: str) -> str:
        return self._wrap("1;33", text)


class _Pace:
    """Print helpers with optional pauses and colors for video recording."""

    def __init__(self, *, slow: bool = False, color: bool | None = None) -> None:
        self.slow = slow
        enabled = _color_enabled() if color is None else color
        self.style = _Style(enabled=enabled)

    def _pause(self, seconds: float) -> None:
        if self.slow and seconds > 0:
            time.sleep(seconds)

    def _print(self, msg: str, *, file: Any = None) -> None:
        print(_ascii_safe(msg), file=file, flush=True)

    def say(self, msg: str = "", *, pause: float | None = None) -> None:
        self._print(msg)
        if pause is None:
            pause = _SLOW_LINE_PAUSE if self.slow else 0.0
        self._pause(pause)

    def title(self, msg: str) -> None:
        self.say(self.style.bold_cyan(msg))

    def muted(self, msg: str) -> None:
        self.say(self.style.dim(msg))

    def section(self, title: str, *, kind: _SectionKind = "info") -> None:
        self._pause(_SLOW_SECTION_PAUSE if self.slow else 0.0)
        self._print("")
        if kind == "danger":
            painted = self.style.bold_red(title)
        elif kind == "ok":
            painted = self.style.bold_green(title)
        else:
            painted = self.style.bold_cyan(title)
        self._print(painted)
        self._print(self.style.dim("-" * len(title)))
        self._pause(_SLOW_LINE_PAUSE if self.slow else 0.0)

    def execute(self, tool_name: str, record: dict[str, Any]) -> None:
        self._pause(_SLOW_EXEC_PAUSE if self.slow else 0.0)
        tag = self.style.bold_yellow("[EXECUTING]")
        self._print(f"  {tag} {tool_name}({record!r})")
        self._pause(_SLOW_LINE_PAUSE if self.slow else 0.0)

    def metric(self, label: str, value: Any, *, emphasize: bool = False) -> None:
        painted = self.style.bold(str(value)) if emphasize else str(value)
        self.say(f"{label}{painted}")

    def passed(self, msg: str) -> None:
        self._print(f"{self.style.bold_green('PASS')}: {msg}")
        self._pause((_SLOW_LINE_PAUSE + 0.6) if self.slow else 0.0)

    def bug(self, msg: str) -> None:
        """Highlight the intentional failure case (without Mycelium)."""
        self._print(f"{self.style.bold_red('BUG')}: {msg}")
        self._pause((_SLOW_LINE_PAUSE + 0.6) if self.slow else 0.0)

    def failed(self, msg: str) -> None:
        self._print(
            f"{self.style.bold_red('FAIL')}: {msg}",
            file=sys.stderr,
        )


def run_demo(*, redis: bool = False, slow: bool = False) -> int:
    """Run baseline + guarded feature proofs. Returns exit code.

    When ``redis=True``, also runs the two-worker real-Redis Cloud-style proof
    (requires a reachable Redis; see ``MYCELIUM_TEST_REDIS_URL``).

    When ``slow=True``, pauses between lines/sections for screen recording.

    Colors are on when stdout is a TTY (disable with ``NO_COLOR=1``).
    """
    pace = _Pace(slow=slow)
    fixture = load_fixture()
    scenario = fixture["scenario"]
    total = 9 if redis else 8

    pace.title("Mycelium feature demo")
    pace.muted(
        "Pitch: unified reliability at the tool boundary - "
        "transition envelope, class-aware gates, lease, repair, "
        "reconcile, and operator release."
    )
    pace.muted("This tour runs in ONE process (in-memory ledger). The real risk")
    pace.muted("is cross-process redispatch: run `mycelium demo --redis` for the")
    pace.muted("two-worker Cloud-style proof (requires Redis).")
    pace.muted(f"Fixture: {fixture['id']}")
    pace.muted(f"Source:  {fixture['source_url']}")
    pace.muted(f"Pattern: {fixture['pattern']}")

    pace.section(f"[1/{total}] Without Mycelium: unguarded redispatch", kind="danger")
    pace.say(
        f"Simulating redispatch of {scenario['tool_name']!r} "
        f"(runtime={scenario['runtime']!r}, no transition envelope)"
    )
    baseline = reproduce_baseline_duplicate(fixture, on_execute=pace.execute)
    pace.metric("Unguarded executions: ", len(baseline), emphasize=True)
    if len(baseline) == 2:
        pace.bug("duplicate side effect reproduced (this is the bug)")
    else:
        pace.failed(f"expected 2 executions, got {len(baseline)}")
        return 1

    pace.section(f"[2/{total}] Transition envelope: redispatch runs once", kind="ok")
    pace.say(
        f"Same scenario with transition + side_effect_class=non_idempotent_mutate, "
        f"tool_call_id={scenario['tool_call_id']!r}"
    )
    try:
        result = prove_ledger_deduplication(fixture, on_execute=pace.execute)
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.metric("Ledgered executions: ", len(result["executions"]), emphasize=True)
    pace.say(f"r1 == r2:   {result['r1'] == result['r2']}")
    pace.say(f"side_effect_class: {result['side_effect_class']}")
    pace.passed("redispatch resolved existing transition; side effect ran once")

    pace.section(f"[3/{total}] Lease auto-renew: peer stays on POLL", kind="ok")
    pace.say("Long ledgered tool extends lease_until while running;")
    pace.say("a redispatched peer must POLL, not reclaim mid-flight.")
    try:
        lease = prove_lease_auto_renew()
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.say(f"Peer lease_validity: {lease['lease_validity']}")
    pace.say(f"Peer gate:           {lease['peer_gate']}")
    pace.passed("auto-renew kept lease HELD; peer gated to POLL")

    pace.section(f"[4/{total}] REPAIR gate: heal incomplete durable record", kind="ok")
    pace.say("Corrupt idempotency_key on a completed transition, then redispatch.")
    try:
        repair = prove_repair_gate()
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.metric("Tool body executions: ", repair["executions"], emphasize=True)
    pace.say(f"Repaired key present: {bool(repair['repaired_idempotency_key'])}")
    pace.passed("REPAIR healed the record; body did not run twice")

    pace.section(f"[5/{total}] Reconcile: provider proves COMPLETED", kind="ok")
    pace.say("Ambiguous mutate recorded external_operation_ref; reconciler says done.")
    try:
        recon = prove_reconcile_completed()
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.metric("Tool body executions: ", recon["executions"], emphasize=True)
    pace.say(f"Reconcile calls:      {recon['reconcile_calls']}")
    pace.say(f"Returned result:      {recon['result']}")
    pace.passed("reconcile returned provider result without a second side effect")

    pace.section(f"[6/{total}] Class-aware READ: UNKNOWN may safely retry", kind="info")
    pace.say("Reads can re-execute on UNKNOWN; mutating tools hard-block instead.")
    try:
        read = prove_read_unknown_safe_retry()
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.say(f"side_effect_class: {read['class']}")
    pace.metric("Tool body executions after UNKNOWN: ", read["executions"], emphasize=True)
    pace.passed("READ UNKNOWN re-executed once (safe by class)")

    pace.section(f"[7/{total}] HARD_BLOCK: ambiguous mutate cannot re-execute", kind="danger")
    pace.say("Payment accepted by provider, then timeout - outcome is ambiguous.")
    pace.say("Redispatch must NOT run the side effect again: gate = HARD_BLOCK.")
    try:
        hardblock = prove_hard_block()
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.metric("Redispatch gate:     ", hardblock["gate"], emphasize=True)
    pace.say(f"Raised:               {hardblock['raised']}")
    pace.say(f"Terminal outcome:     {hardblock['terminal_outcome']}")
    pace.metric("Tool body executions: ", hardblock["executions"], emphasize=True)
    pace.passed("ambiguous mutate hard-blocked; no blind re-execution")

    pace.section(f"[8/{total}] Operator release: unblock stuck mutate", kind="ok")
    pace.say("Same hard-block recovered: operator verifies not_executed,")
    pace.say("then exactly one re-execution, then RETURN.")
    try:
        release = prove_operator_release()
    except AssertionError as exc:
        pace.failed(str(exc))
        return 1
    pace.metric(
        "Tool body executions: ",
        f"{release['executions']} (fail + one re-exec)",
        emphasize=True,
    )
    pace.say(f"Release verified: {release['operator_resolution_applied']}")
    pace.say(f"Final outcome:    {release['final_outcome']}")
    pace.passed("operator release recovered the stuck transition")

    if redis:
        from mycelium.proofs.langgraph_7417_redis import (
            ENV_REDIS_URL,
            prove_two_worker_redis_redispatch,
            redis_reachable,
            resolve_redis_url,
        )

        pace.section(f"[9/{total}] Cloud-style: 2 workers + real Redis", kind="ok")
        url = resolve_redis_url()
        pace.say(f"Redis URL: {url} (override with {ENV_REDIS_URL})")
        if not redis_reachable(url):
            pace.failed(f"Redis not reachable at {url!r}")
            return 1
        try:
            multi = prove_two_worker_redis_redispatch(url=url)
        except (AssertionError, RuntimeError) as exc:
            pace.failed(str(exc))
            return 1
        pace.say(f"Workers:     {multi['workers']}")
        pace.metric("Ledgered executions (2 workers): ", multi["executions"], emphasize=True)
        pace.say(f"request_id:  {multi['request_id']}")
        pace.passed("second worker polled; side effect ran once on shared Redis ledger")

    pace.section("Use in your agent", kind="info")
    pace.say("pip install mycelium-runtime")
    pace.say("pip install 'mycelium-runtime[redis]'  # multi-worker / Cloud-style")
    pace.say("mycelium init")
    pace.say("mycelium demo --slow         # this tour, paced for recording")
    pace.say("mycelium demo --redis        # optional 2-worker Redis proof")
    pace.say("mycelium transitions list --stuck   # operator triage")
    pace.say()
    pace.say("from mycelium import load_config")
    pace.say()
    pace.say('config = load_config("mycelium.yaml")')
    pace.say()
    pace.say("@config.apply")
    pace.say(f"def {scenario['tool_name']}(task: str, duration_seconds: int) -> dict:")
    pace.say("    return run_slow_subagent(task)")
    pace.say()
    pace.say("# Pass tool_call_id from LangGraph on each invocation")
    if slow:
        pace.say()
        pace.title("Demo complete.")
        pace._pause(1.5)
    return 0
