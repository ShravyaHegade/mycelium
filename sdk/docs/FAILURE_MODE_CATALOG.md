# Failure-mode catalog (AF-001…AF-009)

Stable IDs used across the SDK README, handbook, and changelogs. Sourced from
a GitHub-issue corpus across LangChain, LangGraph, CrewAI, AutoGen, Cline,
OpenHands, and related stacks.

| ID | Failure mode | Mycelium surface |
|----|--------------|------------------|
| AF-001 | Hallucination cascade | (judgment / evals; not in SDK) |
| AF-002 | Observability black hole | Transition envelope · ledgers · reconcile · receipts |
| AF-003 | Infinite reasoning loops | `loop_guard:` / `@loop_guard` |
| AF-004 | Tool misuse | `@bounded` · `ToolRegistry` · `ToolRunner` |
| AF-005 | Goal misalignment | (judgment / evals; not in SDK) |
| AF-006 | Context corruption | `@protect` · `Session` · `MessageValidator` · `HistoryGuard` |
| AF-007 | Premature termination | `completion:` / `complete_run` |
| AF-008 | Cascading permission | `scope_guard:` / `@scope_guard` |
| AF-009 | Instruction injection | (MCP gateway revisit; not in SDK) |

---

## AF-001 Hallucination cascade

**Class:** Decision validity / judgment

**One line:** Agent confidently acts on fabricated facts; errors compound across tool calls.

The agent invents or misstates facts, then treats those fabrications as ground
truth for later tool calls. Wrong lookup → wrong write → wrong confirmation.

**What users hit:** unverified tool returns treated as truth; fabricated IDs /
amounts / paths into mutating tools; plans that never re-ground against source
context.

**Why not core yet:** needs a judge model (precision/recall tradeoffs). Outside
the deterministic `ALLOW` / `BLOCK` chassis.

---

## AF-002 Observability black hole

**Class:** Execution / audit

**One line:** Consequential actions leave no durable, trustworthy record:
retries double-run; crashes leave unknown commit.

This names the *failure class*, not a tracing product. Mycelium ships
**prevention guards** (ledgers, leases, gates, optional signed receipts), not
spans/dashboards.

**What users hit:** framework redispatch doubles a payment/email/write; crash
mid-action with unknown commit; logs that cannot stand as auditor-verifiable
proof.

**Guards:** transition envelope · `ActionLedger` / `TaskLedger` · reconcile ·
`StateFlush` · audit receipts · opt-in `on_args_drift` (identity-conflict).
See the [SDK README](../README.md) and
[failure & threat model](FAILURE_AND_THREAT_MODEL.md).

---

## AF-003 Infinite reasoning loops

**Class:** Run-level reliability

**One line:** Same action pattern across distinct dispatches; no progress,
burns tokens / duplicates effects.

The ledger dedupes redispatches of the *same* `tool_call_id`. AF-003 is when
the model mints a **new** id each turn with the same tool + args (new
transition key every time).

**What users hit:** tight tool+args loops; “try again” with no strategy change;
token burn; repeated real side effects for mutating tools.

**Guard:** `loop_guard:` action-hash streak → soft `ToolBoundaryError` then
hard `LedgerHardBlockError`; operator `mycelium loops release`.

---

## AF-004 Tool misuse

**Class:** Safety / tool boundary

**One line:** Tool calls with invalid inputs or outside intended scope; silent
failure or wrong side effects.

**What users hit:** missing/wrong-typed args; path or entity outside allowed
scope; bad return shapes; tools not on the allowlist; no recovery path for the
LLM.

**Guards:** `@bounded` / `bounded_sync` (input/output/entity/path) ·
`ToolRegistry` · `ToolRunner`.

---

## AF-005 Goal misalignment

**Class:** Decision validity / judgment

**One line:** Optimizes a proxy objective, not user intent.

The agent closes the ticket / finishes a checklist / maximizes tool success
while missing the user's actual goal. Wrong success still looks “done.”

**What users hit:** proxy metrics over declared intent; objective drift;
“success” against the wrong condition.

**Why not core yet:** judge-model / evals territory (same product-tier caveat
as AF-001). AF-007 only gates an *explicit* host checklist, not open-ended goals.

---

## AF-006 Context corruption

**Class:** Context

**One line:** Stale, truncated, or poisoned context → false picture of the world.

**What users hit:** stale tool/environment state; broken transcripts (orphan
tool results, duplicate ids, bad roles); oversized history silently truncated;
cross-request cache bleed.

**Guards:** `@protect` / `Session` · `MessageValidator` · `HistoryGuard`.

---

## AF-007 Premature termination

**Class:** Run-level reliability

**One line:** Stops early and presents partial work as complete.

**What users hit:** early stop with incomplete checklists; missing required
side effects before “success”; declared subtasks never resolved.

**Guard:** `completion:` host `required` / `optional` checklist; unmarked
required → refuse (`CompletionRefusedError`); unmarked optional → warn and
allow. Entry points: `complete_run()`, LangGraph END, `wrap_final_message`.

---

## AF-008 Cascading permission

**Class:** Safety / scope

**One line:** Narrow grant escalates transitively beyond intent.

**What users hit:** allowlist drift across a long run; handoff / subagent
inherits broader tools than the parent grant.

**Guard:** `scope_guard:` freeze the run tool allowlist (from `registry` /
`tools:`) and re-check every step. Mid-run widen →
`ToolBoundaryError` (`scope_escalation_tool`). Entity/path/output stay on
`@bounded` (AF-004).

---

## AF-009 Instruction injection

**Class:** Safety / content

**One line:** Untrusted content hijacks instructions.

External data (tool output, retrieved docs, pasted text) contains instructions
that override the system/developer prompt. The agent follows the injected
policy.

**What users hit:** poisoned tool returns or RAG chunks; “ignore previous
instructions” payloads; instruction/data boundary collapse in context.

**Why not core yet:** revisit at the **MCP gateway**, where taint isolation is
mechanically enforceable. Complementary to dedicated prompt-injection products.

---

## How to read the IDs

- **AF-00N** = failure *class* from the corpus taxonomy, not a PyPI version.
- Feature docs use the ID in headings (e.g. “Loop guard (AF-003)”) so you can
  jump from a changelog line to the definition here.
- Ledger-core guarantees (AF-002 prevention) are spelled out in
  [FAILURE_AND_THREAT_MODEL.md](FAILURE_AND_THREAT_MODEL.md); other AF modules
  are optional guards documented in the SDK README.
