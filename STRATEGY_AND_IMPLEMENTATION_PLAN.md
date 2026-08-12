# Mycelium: Current State, Strategy, and Implementation Plan

Status: proposed execution plan, August 2026

This document translates Mycelium's existing product philosophy, failure-mode
catalog, shipped SDK, threat model, and local roadmap into a single recommended
execution plan. It separates what exists today from what should happen next.

It is not a release announcement. Items described as proposed or planned are
not shipped product claims.

## Executive recommendation

Mycelium's technical core is already strong enough to pursue production trust.
The next milestone should not be another broad set of guards. It should be a
repeatable path from installation to a verified production deployment on one
consequential workflow.

The recommended order is:

1. Make production readiness mechanically verifiable.
2. Put one public consequential workload through that path.
3. Make reconciler implementations independently testable.
4. Simplify the default adoption path around AF-002.
5. Export durable, machine-readable verdicts without becoming a dashboard.
6. Use a deliberately thin MCP gateway to extend distribution beyond Python.
7. Enter decision-level and hosted work only after production pull.

The central product milestone remains one public production user running
payment or a comparably consequential tool through Mycelium. That result is
more valuable than roadmap completeness or a new package version.

## Product identity and philosophy

Mycelium is the reliability layer for AI agents. It sits between an agent loop
and its tools and gives consequential transitions a deterministic verdict.

The core question is:

> Is this transition safe, and can we prove what happened?

The same question expands through four scopes:

1. **Tool-call level:** may this call execute, and did it execute?
2. **Run level:** is the run bounded, permitted, and making valid progress?
3. **Decision level:** is the state or belief behind the action still valid?
4. **Fleet level:** can an organization audit and resolve consequential actions
  across agents and systems?

The following principles should remain hard constraints:

- **Prevention before observation.** Mycelium prevents unsafe execution at the
tool boundary. It complements tracing products rather than replacing them.
- **Fail closed under ambiguity.** A consequential action must not be blindly
re-executed when the previous outcome cannot be proven.
- **Deterministic guards.** SDK guards should produce explainable ALLOW, RETURN,
POLL, REPAIR, SOFT_BLOCK, or HARD_BLOCK decisions. Judge-model behavior is a
separate product tier.
- **The taxonomy is the product story.** AF-00N describes the failures users
recognize; the SDK is how the protection is distributed.
- **Expansion is earned.** New rings should be pulled by production users, not
added to make the catalog look complete.
- **Trust over release velocity.** Reliability is demonstrated by guarantees,  
proofs, and production use—not version count.

## Current state

### Product surface

Mycelium currently presents a framework-agnostic Python runtime for developers
running agents with side-effecting tools, including payments, email, API
writes, and long-running subagent calls. Users can integrate through YAML and
`mycelium run`, decorators, or an explicit claim/execute/complete flow.

AF-002 is the flagship promise: any tool, any provider—prove run-or-not and
enforce at-most-once. Additional deterministic surfaces cover:

- AF-003 infinite action loops;
- AF-004 invalid or out-of-scope tool use;
- AF-006 broken, stale, or corrupted context;
- AF-007 premature terminal completion;
- AF-008 mid-run permission widening;
- time, step, token, and cost budgets;
- stale state authority and argument drift.

AF-001 hallucination, AF-005 goal misalignment, and AF-009 instruction
injection are not claimed as deterministic SDK guards.

### Shipped core

The SDK includes:

- durable transition handling across memory, file, SQLite, Redis, and Postgres;
- atomic first claim, CAS transitions, owner fencing, leases, renewal, and
worker-death evidence;
- terminal outcomes and durable side-effect-boundary progression;
- ALLOW, RETURN, POLL, REPAIR, SOFT_BLOCK, and HARD_BLOCK resolution paths;
- provider reconciliation and operator release;
- provider idempotency-key enforcement and key-validity windows;
- external-operation references and signed audit receipts;
- compact outcome emission and Duplicate Tool Transition Rate computation;
- LangGraph runtime identity integration and two-worker Redis proofs;
- command, decorator, and manual integration paths.



### Verification posture

The repository has a strong verification story:

- the local SDK suite currently passes 821 tests, with six optional
environment-dependent tests skipped in the reviewed environment;
- Ruff passes;
- CI covers Python 3.10 through 3.13;
- CI provisions Redis and Postgres and requires the concurrency proofs to run;
- process-kill, multi-process, storage-outage, property, payment-provider, and
redispatch-race tests exercise the flagship guarantee;
- the failure and threat model maps documented guarantees to concrete tests.

This is sufficient technical evidence to move from core construction to
production adoption. It does not by itself establish market trust.

### Product maturity

The main unresolved milestone is external production proof. Early live email
use is valuable, but it is not yet a public production reference on a payment
or equivalently consequential path. The project therefore has technical
validation without full market validation.

## Gap analysis



### 1. Installation does not prove protection

Several safety properties can remain unwired or degrade to warnings:

- unclassified tools may use the warning policy;
- memory storage is allowed for side-effecting tools;
- missing stable identity may prevent useful deduplication;
- loop and scope guards can skip when run identity is absent;
- completion protection only runs through a wired terminal adapter;
- budget enforcement depends on LLM-boundary and usage wiring;
- outcome emission is optional.

Each choice preserves compatibility, but together they allow an installation
to appear protected without satisfying the flagship production guarantee.

### 2. The first integration asks users to understand too much

Correct configuration may require knowledge of transition identity,
side-effect class, spendability, retry permission, boundary markers, provider
keys, reconciler semantics, storage durability, worker-death policy, and
framework scope propagation.

Those concepts are appropriate in the implementation and reference docs. The
default adoption path should hide most of them behind a production-safe
profile and an explicit readiness report.

### 3. Transition identity remains partly caller-controlled

The runtime can enforce identity consistency but cannot prove that callers
minted a stable, server-authoritative identity. Mutable or irrelevant
arguments can alter fingerprints, missing IDs weaken deduplication, and
client-minted payment keys remain an application risk.

Thin handoff identity establishes audit causation, not authority. Thick crypto
identity is not required now, but stable-identity policy needs a narrow,
enforceable production surface.

### 4. Reconciler correctness is trusted by contract

A reconciler that incorrectly reports NOT_EXECUTED can authorize one duplicate
execution. Provider indexing lag and ambiguous matching make this a practical
risk. The abstraction is sound, but third-party reconciler implementations
need a reusable conformance suite.

### 5. Safe failure still requires operational maturity

The CLI supports listing, inspection, death marking, and operator release.
Production teams additionally need alert ownership, evidence requirements,
reason codes, provider-record links, response expectations, and a rehearsed
incident path. A parked action is safe, but it is not operationally complete
until another engineer can resolve it correctly.

### 6. Verdict telemetry is useful but not yet a full integration contract

OutcomeEmitter and DTTR provide a good base. The export surface should gain
stable run and parent correlation, policy/config versions, failure reason
codes, terminal run outcomes, and straightforward warehouse or OpenTelemetry
delivery. Mycelium should emit these facts, not build a tracing dashboard.

### 7. The public story risks feature dilution

The core differentiation is AF-002 at-most-once execution under crash,
redispatch, and ambiguity. Presenting every guard at equal weight can make the
project look like a general guardrails bundle. Secondary guards should support
the flagship story rather than compete with it on the first screen.

### 8. Roadmap state can drift from shipped state

Planning notes have at times continued to list already-shipped work as missing.
For a trust product, current-state accuracy is part of product discipline.
Release state, public claims, local plans, and implementation status should be
reconciled during each coherent release batch.

### 9. The proposed MCP gateway v1 is too broad

The current scope combines two transports, tools, resources, ledger tenancy,
identity derivation, storage selection, a verdict pipeline, and a new approval
experience. That is strategically attractive but too much for a first
distribution slice. It also risks entering approval-inbox territory that the
project currently treats as a non-goal.

## Recommended implementation plan



### Phase 0 — Align claims and plans

Objective: establish one accurate source of current state before new work.

Deliverables:

- reconcile roadmap checklists with the current package and changelog;
- remove already-shipped items from active blocker lists;
- ensure README, SDK README, catalog, threat model, and handbook agree on what
is guaranteed, optional, or deferred;
- keep AF-002 as the first-screen promise;
- define measurable production-readiness criteria.

Exit criteria:

- no known shipped capability remains labeled unimplemented;
- no roadmap capability is phrased as a shipped guarantee;
- a reviewer can identify the flagship promise and its limitations in under
five minutes.



### Phase 1 — Production readiness validator

Objective: turn configuration safety into a deterministic verdict.

Recommended commands:

```text
mycelium doctor --config mycelium.yaml
mycelium doctor --config mycelium.yaml --production
mycelium verify --config mycelium.yaml --scenario redispatch
```

`doctor --production` should fail when a consequential deployment has:

- memory storage;
- missing stable request identity;
- unclassified tools or warning-only classification;
- missing provider-key policy where keyed mutation requires it;
- no reconciler or documented operator path for ambiguous mutations;
- death-signal protection disabled without an explicit acceptance;
- missing runtime scope required by configured guards;
- completion, budget, or scope protection configured but not instrumented;
- no durable verdict/receipt output;
- a configuration path where a claimed guarantee silently skips.

The output should be a capability report, for example:

```text
At-most-once execution: PROVEN
Stable identity:        PROVEN
Crash ambiguity:        HARD_BLOCK + OPERATOR PATH
Multi-worker storage:   PROVEN
Verdict telemetry:      MISSING
Production readiness:   FAIL
```

The verifier should run an actual contention or redispatch scenario against
the configured backend and produce a machine-readable result.

Exit criteria:

- unsafe production configuration cannot receive a passing report;
- the report distinguishes configured, instrumented, and empirically verified
properties;
- CI can run the verifier for a partner deployment.



### Phase 2 — Opinionated production starter

Objective: reduce the path from installation to a correct consequential tool.

Deliverables:

- `mycelium init --production`;
- strict classification defaults;
- SQLite for an explicit single-node profile and Redis/Postgres for cloud;
- death-signal protection and outcome emission enabled;
- one classified consequential tool with stable identity;
- a generated operator runbook;
- a generated crash/redispatch verification test;
- concise documentation that leads with AF-002 and moves optional guards into
an advanced section.

Exit criteria:

- a new user can configure, verify, crash-test, and inspect one consequential
tool without learning the complete internal envelope vocabulary;
- generated output passes `mycelium doctor --production` after required values
are supplied.



### Phase 3 — Public production proof

Objective: earn the trust milestone with a real user.

Select one consequential workflow and deliver:

- Redis or Postgres durability;
- stable server-authoritative identity;
- signed receipts and verdict export;
- crash and redispatch tests in the partner's CI;
- reconciler or documented operator resolution;
- an incident rehearsal performed by someone other than the author;
- DTTR tracking and a defined observation period;
- a public architecture write-up and, if possible, a named user.

Email is suitable when duplicate execution has material consequence and the
deployment is public. A payment-class workflow would provide stronger proof.

Exit criteria:

- one public production deployment;
- partner-owned verification running in CI;
- a documented period with no unauthorized duplicate execution;
- an operator has successfully rehearsed ambiguity resolution.



### Phase 4 — Reconciler conformance kit

Objective: prevent integrations from weakening the runtime guarantee.

Deliverables:

- a reusable reconciler conformance harness;
- checks that reconcile is read-only;
- conservative handling of missing, delayed, multiple, or ambiguous provider
matches;
- repeated-call stability checks;
- explicit provider indexing-window tests;
- one excellent Stripe-shaped reference adapter or equivalent consequential
provider example.

A potential test API:

```python
assert_reconciler_conformance(
    reconciler,
    eventual_consistency_window=30,
    supports={"COMPLETED", "NOT_EXECUTED", "UNKNOWN"},
)
```

Exit criteria:

- adapter authors can validate safety without reading ledger internals;
- zero matches during an indexing window cannot authorize blind re-execution;
- ambiguous evidence always fails closed.



### Phase 5 — Durable verdict export

Objective: make guard facts easy to operate and aggregate.

Extend the existing outcome model with:

- stable run, transition, parent-request, and handoff correlation;
- guard policy and configuration version;
- structured failure and resolution reason codes;
- run-terminal outcomes;
- operator-resolution metadata;
- Redis/Postgres, webhook, or OpenTelemetry-compatible export.

Do not build a dashboard in this phase. Existing observability systems should
consume the events.

Exit criteria:

- a warehouse query can explain every non-ALLOW verdict;
- a production alert can link directly to the transition, provider reference,
and operator action;
- DTTR and blocked-transition trends require no log parsing.



### Phase 6 — Thin MCP gateway

Objective: distribute the proven tool-call verdict to non-Python agent hosts.

Recommended v1 scope:

- stdio transport only;
- `tools/call` only;
- YAML-classified mutating tools;
- SQLite for local use and explicit Redis for multi-instance deployment;
- caller identity when present, otherwise clearly disclosed deterministic
derivation;
- structured MCP errors for HARD_BLOCK;
- existing CLI operator release;
- explicit reporting of unguarded pass-through tools;
- strict mode that refuses undeclared tools.

Defer from gateway v1:

- streamable HTTP;
- resources, prompts, and sampling;
- hosted multi-tenant IAM;
- a custom approval UI;
- AF-009 scanning;
- provider-adapter marketplace work.

Exit criteria:

- an MCP client can prove one classified mutating tool executes at most once
under duplicate dispatch;
- the gateway adds no new ambiguity to identity or operator resolution;
- the same production verifier can test SDK and gateway paths.



### Phase 7 — Earned expansion

Decision-validity work should begin only when a production user demonstrates a
concrete need.

The best candidates are:

- currency-at-use for consequential decisions based on mutable facts;
- policy validity rechecked between claim and execution;
- stronger server-authoritative identity policies.

Hosted receipts, reconcile services, operator consoles, and fleet metering
should begin only when a production user asks to pay for centralized operation.

AF-001, AF-005, AF-009, crypto identity, on-chain receipts, and general
observability dashboards remain deferred unless strong partner pull changes
the product boundary.

## Priority summary


| Priority | Work                            | Why now                                                          |
| -------- | ------------------------------- | ---------------------------------------------------------------- |
| P0       | Align roadmap and public claims | Trust requires an accurate current state                         |
| P0       | `mycelium doctor --production`  | Closes the gap between installed and protected                   |
| P0       | Public production proof         | The stated north-star milestone remains unmet                    |
| P1       | Production starter              | Reduces integration complexity and silent gaps                   |
| P1       | Reconciler conformance kit      | Protects the flagship guarantee at its weakest contract boundary |
| P1       | Durable verdict export          | Makes safe failure operable without becoming a dashboard         |
| P2       | Thin MCP tools gateway          | High-leverage distribution after the core path is proven         |
| P3       | Currency and policy validity    | Novel expansion, but only when partner-pulled                    |
| Later    | Hosted fleet product            | Commercial layer after demonstrated willingness to pay           |




## Measures of success

Engineering metrics:

- production validator false-pass rate: zero for known unsafe configurations;
- configured concurrency proofs executed rather than skipped;
- unauthorized duplicate tool transition rate: 0.0;
- all documented guarantees mapped to running tests;
- percentage of consequential deployments using durable storage, stable
identity, and verdict emission.

Adoption metrics:

- time from install to first verified consequential tool;
- number of external CI systems running the redispatch proof;
- number of production consequential transitions protected;
- blocked transitions resolved without author intervention;
- one public production reference before Ring 3 or Ring 4 expansion.

Operational metrics:

- time to detect a parked transition;
- time to resolve ambiguity;
- percentage of releases with reconciled roadmap and public claims;
- percentage of reconciler adapters passing the conformance suite.



## Decision rules

Use these rules when selecting work:

1. Prefer work that makes the AF-002 guarantee easier to adopt, verify, or
  operate.
2. Prefer one complete production path over several partial integrations.
3. Do not interpret warnings or optional wiring as a production guarantee.
4. Do not build UI when structured events and an existing operator path are
  sufficient.
5. Do not expand into judgment-based guards under the deterministic SDK claim.
6. Do not start hosted infrastructure before a user asks to pay for centralized
  receipts, reconciliation, or operation.
7. Batch releases around coherent user outcomes rather than individual
  features.



## Immediate next action

The first implementation proposal should be a small design for
`mycelium doctor --production`, including:

- the readiness checks and severity model;
- the capability-report schema;
- integration with existing config builders and storage types;
- a machine-readable JSON mode;
- generated remediation instructions;
- unit tests for every known silent-degradation path;
- one end-to-end verification against a durable backend.

That feature turns Mycelium's existing internal rigor into something every
user can verify before trusting it with a consequential action.