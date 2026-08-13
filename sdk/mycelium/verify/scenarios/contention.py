"""Contention: exactly one synthetic body under simultaneous workers."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path

from mycelium.verify.registry import ScenarioContext, verify_scenario
from mycelium.verify.types import VerificationEvidence, VerificationStatus
from mycelium.verify.workers import (
    contention_round_failure,
    contention_worker,
    count_executions,
    join_workers,
    spawn_workers,
    terminate_owned,
)


@verify_scenario("contention")
def run_contention(ctx: ScenarioContext) -> VerificationEvidence:
    started = time.time()
    iso = ctx.isolation
    if not iso.multiprocess_capable:
        return VerificationEvidence(
            scenario="contention",
            backend=iso.backend,
            namespace=iso.namespace.prefix,
            duration=time.time() - started,
            expected_behavior="exactly one body execution per round across real processes",
            observed_behavior="backend cannot share state across processes",
            limitations=["memory is process-local; multiprocess contention not proven"],
            status=VerificationStatus.SKIP,
            summary="Contention skipped (backend is not multiprocess-capable)",
            remediation="Use file/sqlite/postgres/redis to empirically verify contention.",
        )

    if not iso.worker_payload or iso.worker_payload.get("backend") == "memory":
        return VerificationEvidence(
            scenario="contention",
            backend=iso.backend,
            namespace=iso.namespace.prefix,
            duration=time.time() - started,
            expected_behavior="exactly one body execution per round across real processes",
            observed_behavior="no worker payload for a shared backend",
            status=VerificationStatus.SKIP,
            summary="Contention skipped (no isolated shared backend payload)",
            remediation="Configure file/sqlite/postgres/redis storage.",
        )

    workers = max(2, min(int(ctx.workers), 8))
    rounds = max(1, min(int(ctx.rounds), 20))
    work = Path(tempfile.mkdtemp(prefix="mycelium-verify-contention-"))
    max_exec = 0
    round_fail = False
    fail_reason = ""
    observed_rounds = 0

    spec = {
        **iso.worker_payload,
        "run_id": iso.namespace.run_id,
        "prefix_ns": iso.namespace.prefix,
    }
    try:
        for round_i in range(rounds):
            request_id = iso.track(iso.namespace.request_id("contention", f"r{round_i}"))
            exec_file = str(work / f"exec-{round_i}.txt")
            out_file = str(work / f"out-{round_i}.txt")
            err_file = str(work / f"err-{round_i}.txt")
            ready_file = str(work / f"ready-{round_i}.txt")
            barrier_file = str(work / f"barrier-{round_i}.txt")
            payloads = [
                {
                    **spec,
                    "request_id": request_id,
                    "exec_file": exec_file,
                    "out_file": out_file,
                    "err_file": err_file,
                    "ready_file": ready_file,
                    "barrier_file": barrier_file,
                    "poll_timeout": min(ctx.timeout_seconds, 10.0),
                }
                for _ in range(workers)
            ]
            procs = spawn_workers(contention_worker, payloads)
            ctx.owned_procs.extend(procs)
            deadline = time.time() + min(ctx.timeout_seconds, 8.0)
            while time.time() < deadline:
                if os.path.exists(ready_file):
                    lines = Path(ready_file).read_text(encoding="utf-8").splitlines()
                    if len(lines) >= workers:
                        break
                time.sleep(0.02)
            Path(barrier_file).write_text("go\n", encoding="utf-8")
            join_workers(procs, timeout=ctx.timeout_seconds)
            executions = count_executions(exec_file)
            max_exec = max(max_exec, executions)
            observed_rounds += 1
            reason = contention_round_failure(
                procs,
                executions=executions,
                out_file=out_file,
                err_file=err_file,
                workers=workers,
                ready_file=ready_file,
            )
            if reason is not None:
                round_fail = True
                fail_reason = f"round {round_i}: {reason}"
                break
    finally:
        terminate_owned([p for p in ctx.owned_procs if p.is_alive()])

    ok = not round_fail and observed_rounds == rounds and max_exec == 1
    limitations: list[str] = []
    if iso.backend in {"file", "sqlite"}:
        limitations.append("single-node verification only")
    if iso.backend == "redis" and not iso.persistence_asserted:
        limitations.append("Redis persistence remains operator-asserted")
    return VerificationEvidence(
        scenario="contention",
        backend=iso.backend,
        namespace=iso.namespace.prefix,
        attempts=rounds * workers,
        body_executions=max_exec,
        duration=time.time() - started,
        expected_behavior="exactly one body execution in every round",
        observed_behavior=(
            fail_reason
            or (
                f"max body_executions={max_exec} over {observed_rounds}/{rounds} "
                f"round(s), workers={workers}"
            )
        ),
        artifacts=[str(work)],
        limitations=limitations,
        status=VerificationStatus.PASS if ok else VerificationStatus.FAIL,
        summary=(
            f"one winner across {rounds} rounds"
            if ok
            else fail_reason or f"duplicate execution under contention (max={max_exec})"
        ),
        remediation=(
            "" if ok else "Use an atomic shared backend (postgres/redis/file lock) for claims."
        ),
    )
