# Mycelium

[![PyPI version](https://img.shields.io/pypi/v/mycelium-runtime.svg)](https://pypi.org/project/mycelium-runtime/)
[![Python](https://img.shields.io/pypi/pyversions/mycelium-runtime.svg)](https://pypi.org/project/mycelium-runtime/)
[![Downloads](https://static.pepy.tech/badge/mycelium-runtime)](https://pepy.tech/project/mycelium-runtime)

**The reliability layer for AI agents.**

Wrong answers are recoverable. Wrong actions are expensive. Mycelium sits between your agent loop and its tools and prevents the runtime failures that make production agents unsafe — duplicate charges, infinite tool loops, bad args, stale context, early “done,” scope creep.

Not recovery after. Not tracing or dashboards. Prevention at the tool boundary.

*Early but API-stable: breaking changes only at major versions. The catalog
grows; the promise stays. Current package version is on
[PyPI](https://pypi.org/project/mycelium-runtime/) — do not hardcode it here.*

Early design-partner use: **live outbound-email lane** (Week 1: 25 ledgered sends, 0 duplicates). Not a public logo; the interactive sandbox is separate.

## The promise — failure-mode catalog (AF-00N)

The taxonomy **is** the product story. Each ID is a real failure class from production GitHub issues across LangChain, LangGraph, CrewAI, and related stacks. Mycelium ships a guard (or roadmap module) per class:

| ID | Failure mode | Promise |
|----|--------------|---------|
| **AF-002** | Observability black hole | **Any tool, any provider:** prove run-or-not and enforce at-most-once — ledger, lease, reconcile, operator release |
| **AF-003** | Infinite reasoning loops | Same tool+args under new call ids soft- then hard-block until an operator releases |
| **AF-004** | Tool misuse | Invalid args and out-of-scope tools are blocked before they run |
| **AF-006** | Context corruption | Stale or broken tool/history context is caught before the next turn |
| **AF-007** | Premature termination | Required checklist items must complete before the run can declare success |
| **AF-008** | Cascading permission | The run tool allowlist freezes; mid-run / handoff widen is refused |
| **AF-010** | Secret-in-args | Raw credentials are blocked before claim; pass `secret://` references |
| **AF-011** | Destructive confirm | Tool permission is not object authorization; host-issued grants only |
| AF-001 / AF-005 / AF-009 | Hallucination · goal misalignment · injection | Roadmap / judgment tier — not claimed as deterministic SDK guards yet |

Full definitions (shipped vs roadmap): [sdk/docs/FAILURE_MODE_CATALOG.md](sdk/docs/FAILURE_MODE_CATALOG.md).

**The SDK is the distribution; the catalog is the company.**

## Who it's for

Developers running **agents with side-effect tools** in production (payments, emails, API writes, long subagent calls) on **LangGraph, CrewAI, or a plain Python loop**.

Python 3.10+. Framework-agnostic. Drop in via YAML + `mycelium run`, or decorators.

## How it works

Mycelium wraps tool calls after the LLM returns `tool_calls` and returns a verdict: run, return a stored result, wait, ask the provider what happened, or hard-stop.

**Core — AF-002** (`mycelium init` / `mycelium run`):

- Durable **execution ledger** around mutating tools: claim before the side effect, record terminal state, **do not redispatch unless the previous attempt is proven safe**.
  - **Flagship (AF-002):** any tool, any provider — record a handle, ask the provider what happened, enforce **at-most-once**. Shipped adapters (e.g. Gmail sent-log) are demos of that contract, not the product story.
  - **Effect identity and state for classified transitions:** deterministic,
    destination-aware `effect_id`; unified durable `EffectState`
    (`INTENDED → ATTEMPTING → COMMITTED | ABORTED | UNKNOWN`) with legacy-row
    resolution.
  - **Fenced effect commit:** every claim increments a durable fence; decision,
    boundary, heartbeat, provider-reference, completion, failure,
    reconciliation, and operator-resolution writes reject stale fences.
    Registered pure decision predicates are evaluated once and recorded
    atomically with `INTENDED → ATTEMPTING`.
  - **Recovery capability:** `ToolCapability` distinguishes `idempotent`,
    `queryable`, and `blind`. Ambiguous blind effects park; queryable effects
    require a class-appropriate probe (`keyed_mutate` provider key or a
    `Reconciler`; non-idempotent mutation requires a reconciler). Dishonest
    declarations fail closed.
  - **Reads:** poll in-flight, reclaim expired leases, soft-block ambiguous `UNKNOWN`
  - **Mutations:** hard-block ambiguity; **provider reconcile** when a lookup can prove ran-or-not; **operator release** when a human must verify (`mycelium transitions release`)
  - `mycelium verify --scenario simulation` sweeps crash boundaries and proves
    the at-most-one-COMMITTED invariant plus stale-fence rejection on durable
    backends.
  - Crash windows, worker death, lease auto-renew, CAS/atomicity, and fail-closed storage — see [sdk/README.md](sdk/README.md#resolution-gates)
  - LangGraph Cloud redispatches long tools around **~180s**; the ledger guards that window ([langgraph#7417](https://github.com/langchain-ai/langgraph/issues/7417))

**Opt-in guards** (configure or call explicitly):

- **AF-003 Infinite loops** — `loop_guard:` · soft → hard → `mycelium loops release`
- **AF-004 Tool misuse** — `@bounded` / registry · block bad args (incl. `array` / `object`) and out-of-scope tools / paths
- **Budget / runaway spend (unnumbered)** — `budget:` · ceilings + `@budget_guard` / `instrument_llm` (auto `check("llm")`) · `mycelium budget release`
- **AF-010 Secret-in-args** — `secret_args:` · block raw credentials before claim · pass `secret://` references · `mycelium verify --scenario secret-in-args`
- **Entity / destination guard (unnumbered)** — `entity_guard:` · write tools declare the destination argument · host allowlist · fail closed before claim · `mycelium verify --scenario entity-guard`
- **AF-011 Destructive confirm** — `destructive_confirm:` · host-issued object grant · fail closed before claim · `mycelium verify --scenario destructive-confirm`
- **Authority-window expiry** — `authority_window:` · re-check time-bounded authority at use before the side-effect boundary · `mycelium verify --scenario authority-window`
- **Use-time currency (AF-012)** — `use_time_currency:` · revalidate decide-time facts at use · `mycelium verify --scenario use-time-currency`
- **AF-006 Context corruption** — `@protect` / Session · optional message/history validation
- **AF-007 Premature termination** — `completion:` host checklist · refuse or warn-and-allow
- **AF-008 Scope escalation** — `scope_guard:` freeze allowlist · re-check every step
- **AF-002 Args drift** — default `on_args_drift: soft` (same call id, different args refused; `hard` / `off` opt-in)
- **State authority** — refuse decisions from superseded checkpoints before claim
- **DTTR telemetry** — opt-in `OutcomeEmitter` so the no-double-execute guarantee is observable

Budget step accounting is automatic in `check()` and `@budget_guard`.
Use `record_usage()` for observed token/USD usage without `steps`; combining
`record_usage(steps=...)` with the default step meter warns because it counts
the step twice.

`budget.max_steps` counts each budget-guarded tool invocation and instrumented
LLM turn—not business outcomes or high-level workflow stages. Include normal
failure, retry, and cleanup calls when sizing it, and enforce business limits
such as candidates, attempts, or successful submissions separately.

Implementation detail (envelope field stack, gate matrix, payment identity): [sdk/README.md](sdk/README.md#transition-envelope-fields). Failure & threat model: [sdk/docs/FAILURE_AND_THREAT_MODEL.md](sdk/docs/FAILURE_AND_THREAT_MODEL.md).

Not Langfuse. Use both if you want traces and guards. Not an approvals inbox, hosted observability, on-chain audit trail, or agent framework — [What Mycelium does not do](sdk/README.md#what-mycelium-does-not-do).

## Use it

### Let your coding agent wire it

The PyPI package bundles the official
[`mycelium-setup`](.agents/skills/mycelium-setup/SKILL.md) skill. Install it
offline into the current project's agent catalog, then simply ask your coding
agent: “Set up Mycelium in this project.” The agent inventories tools,
classifies side effects, adds dependencies, creates or merges `mycelium.yaml`,
wires the actual runtime boundary, adds tests, and runs Doctor/Verify.

```bash
pip install mycelium-runtime
mycelium skills install                    # ./.agents/skills/mycelium-setup
mycelium skills install --target ~/.codex/skills  # optional user catalog
```

The workflow is intentionally fail-closed: it will not invent production
secrets, business request identities, provider permissions, or authorization.
It completes everything supported by repository evidence and reports only the
specific host-owned input that genuinely cannot be derived.

```bash
pip install mycelium-runtime
pip install 'mycelium-runtime[langgraph]'  # automatic LangGraph runtime IDs
pip install 'mycelium-runtime[redis]'      # multi-worker / cloud ledger
pip install 'mycelium-runtime[postgres]'   # Postgres ledger backend
mycelium demo --slow       # feature tour, paced for screen recording
mycelium demo              # same tour, fast
mycelium demo --redis      # optional Cloud-style two-worker Redis proof (#7417)
mycelium init              # on-ramp: transition + one ledgered tool → mycelium.yaml
mycelium init --full       # reference: all guards (fill TODOs; not the default)
mycelium init --minimal    # smaller multi-guard scaffold
```

`mycelium demo --redis` runs two OS processes against a real Redis ledger — Worker B redispatches while A is in-flight; B polls and returns A's result. Needs Redis (`MYCELIUM_TEST_REDIS_URL` or `redis://127.0.0.1:6379/15`) and `pip install 'mycelium-runtime[redis]'`.

`mycelium init` is the real start path (duplicate-tool fix). Use `--full` when you want every section documented in one file.

```yaml
# after: mycelium init
integrations:
  langgraph:
    enabled: true

transition:
  agent_id: my-agent
  policy_version: "2026.07.1"
  # lease_ttl: 3600
  # lease_renew_interval: 1200   # default = lease_ttl/3; 0 disables auto-renew

action_ledger:
  storage: file
  path: ./mycelium-ledger.json
  unclassified_policy: strict   # warn (default) or strict
  tools: [my_side_effect_tool]

tools:
  my_side_effect_tool:
    callable: my_app.tools:my_side_effect_tool
    side_effect_class: non_idempotent_mutate
```

Launch your existing Python application without adding decorators:

```bash
mycelium run --config mycelium.yaml -- python -m my_app
```

`mycelium run` validates and wraps every configured callable before the
application starts. It preserves the child process's arguments, working
directory, signals, and exit code. The command accepts the current Python
interpreter only.

The direct/decorator library default for an unclassified tool is still
`unclassified_policy: warn`. Under `profile: production`, an omitted
`action_ledger.unclassified_policy` now defaults to `strict`, so failed
unclassified retries hard-block; an explicit `warn` remains a compatibility
choice. Consequential production tools should declare `side_effect_class`.

Explicit instrumentation remains supported when you prefer code-level control:

```python
from mycelium import load_config

config = load_config("mycelium.yaml")

@config.apply
def my_side_effect_tool(...) -> dict:
    ...
```

Without YAML, prefer the ledger decorators (`@ledger` / `@ledger_sync` for tools;
`@task_ledger` / `@task_ledger_sync` for coarser task-level idempotency). Same transition
envelope and gates — see [sdk/README.md](sdk/README.md#what-ledger--ledger_sync-do)
and [task-level idempotency](sdk/README.md#quickstart-task-level-idempotency).
If you own the tool runner and need explicit claim → execute → complete
(PROCEED/SKIP-style), see
[Manual integration](sdk/README.md#manual-integration-claim--execute--complete)
— same ledger; no YAML switch.

Do not combine standalone guard decorators with command mode on the same
function. Fully configured `@config.apply` wrappers are detected and skipped.
Keep callable modules import-safe: registrations performed inside a target
module while that module is still importing cannot be retroactively replaced.

With the optional LangGraph integration, `ToolNode` / `create_agent` injects
`ToolRuntime`; Mycelium automatically maps its `tool_call_id`, thread, run, and
node into transition identity. Explicit IDs still override captured values.
Custom tool executors can continue passing `tool_call_id` manually. Redispatch
resolves the existing transition: read tools poll/soft-block; mutating tools
hard-block or reconcile against the provider when you record
`external_operation_ref`.

Zero-ops single-node durable ledger: YAML `storage: sqlite` + `path:` (stdlib;
no extra install). Multi-worker / cloud: `pip install 'mycelium-runtime[redis]'`
or `'mycelium-runtime[postgres]'`. See the
[handbook](https://mycelium-labs.github.io/).

## Docs

- **Handbook:** https://mycelium-labs.github.io/ ([website repo](https://github.com/mycelium-labs/mycelium-labs.github.io))
- **Try in 5 minutes:** https://mycelium-labs.github.io/try.html
- **Sandbox demo:** [mycelium-labs/mycelium-labs.github.io/sandbox](https://github.com/mycelium-labs/mycelium-labs.github.io/tree/main/sandbox)
- **Full API reference:** [sdk/README.md](sdk/README.md)
- **Doctor vs Verify:** `mycelium doctor` inspects configuration; `mycelium verify` runs synthetic failure scenarios. Neither proves a real provider is correct.
- **Release policy & checklist:** [sdk/docs/RELEASE.md](sdk/docs/RELEASE.md) (batch; calm over velocity)
- **Security policy & private reporting:** [SECURITY.md](SECURITY.md)
- **PyPI:** https://pypi.org/project/mycelium-runtime/

## Release process

**Batch. Calm over velocity.** A reliability layer is trusted by infrequent,
coherent PyPI cuts — not by many versions per day. Full policy + **pre-release
checklist:** [sdk/docs/RELEASE.md](sdk/docs/RELEASE.md).

**Default workflow:** merge feature/fix/docs PRs to `main` **without** bumping
the version. When a batch is ready (≈ weekly or slower), open one release PR
that bumps `sdk/pyproject.toml` + `CHANGELOG.md` and passes the checklist.
Same-day multiple publishes are forbidden except critical hotfixes.

### One-time setup

Create a GitHub Personal Access Token with `contents: write` scope on this repo and add it as a repository secret named `RELEASE_PAT` at **Settings → Secrets and variables → Actions**. This is required because the tag push uses the PAT (instead of `GITHUB_TOKEN`) so that `publish.yml`'s `on: push: tags: v*` trigger fires — `GITHUB_TOKEN`-pushed tags cannot trigger other workflows.

### Per-release steps

1. Land work on `main` via PRs (no version bump required on each PR).
2. When batching a cut: open a **release PR** — bump `sdk/pyproject.toml`, add
   `## X.Y.Z (date)` to `CHANGELOG.md`, sync user-facing version lines, complete
   the [pre-release checklist](sdk/docs/RELEASE.md#before-you-bump-the-version-checklist).
3. CI (pytest + ruff on Python 3.10–3.13) must pass on that PR.
4. Merge the release PR. On push to `main`, automation:
   - Reads the version from `sdk/pyproject.toml`.
   - Checks whether tag `v{version}` already exists — if it does, exits quietly (doc-only or non-version merges release nothing).
   - Runs the SDK tests and ruff (Python 3.12) as a safety gate before tagging.
   - Creates an annotated tag `v{version}` and pushes it (via PAT so the tag-push triggers the publish workflow).
   - Creates a GitHub Release with notes extracted from the matching `CHANGELOG.md` section (falls back to auto-generated notes if extraction finds nothing).
   - The tag push triggers [publish.yml](.github/workflows/publish.yml) (`on: push: tags: v*`) which builds and uploads to PyPI via trusted publishing.

**Manual escape hatch:** pushing a `v*` tag or triggering `workflow_dispatch` on the publish workflow still works — the existing manual path is unchanged. If the automation fails, publish manually by running `git push origin v{version}` locally after merging.

## License

MIT. See [LICENSE](LICENSE).
