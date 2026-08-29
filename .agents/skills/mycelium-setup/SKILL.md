---
name: mycelium-setup
description: Set up or repair Mycelium in an existing Python agent project, including CRM agents, by inspecting the application, filling or merging mycelium.yaml, wiring real tool boundaries, and verifying the integration. Use when the user asks to install, configure, integrate, or fully wire Mycelium. Do not use for unrelated application work or ordinary Mycelium SDK development.
---

# Set Up Mycelium

Complete the integration in the target project rather than only explaining it.
Inspect first, make the smallest safe changes, and leave the project runnable.

## Sources of truth

- Follow the target repository's `AGENTS.md` and preserve unrelated changes.
- Use the installed Mycelium version's CLI, generated full template, and README
  as the configuration/API authority. Do not rely on remembered field names.
- If the Mycelium source checkout and `graphify-out/graph.json` are present, use
  its scoped Graphify query flow before broad source inspection.

Generate a reference configuration in a temporary directory with the installed
CLI (`mycelium init --full`), and inspect `mycelium --help`, `mycelium doctor
--help`, and `mycelium verify --help`. Never overwrite the application's current
configuration merely to obtain a template.

## Configuration completion contract

Treat `mycelium.yaml` as an evidence-backed application configuration, not a
generic scaffold. Fill every value that can be proven from source code,
deployment files, existing configuration, and declared environment-variable
names. Remove template examples and TODOs that do not describe a real callable.

For CRM and similar business agents, explicitly look for customer/contact reads,
record creation or updates, email/message sends, imports/exports, bulk actions,
deletes, payments, webhooks, scheduled jobs, and external API mutations. Bind
each reachable operation to its actual callable and classify its externally
observable effect. Do not assume a function is safe because its name sounds
read-only.

Do not stop after generating YAML. Completion requires a loadable configuration,
runtime wiring at the actual execution boundary, and behavioral verification.
If a host-owned value cannot be inferred safely, leave one precise documented
placeholder that fails closed and include it in the final questions list. Do not
leave vague TODOs such as "configure this later."

## Workflow

1. Inspect the dependency files, runtime entry points, framework, tool/task
   registration, existing retry behavior, provider clients, deployment files,
   environment-variable names, and current Mycelium configuration/wrappers.
2. Inventory every callable reachable as an agent tool or durable task. Trace
   aliases, registries, dynamic exports, and decorators so the same callable is
   not omitted or wrapped twice. Record the inventory while working: tool name,
   callable path, provider/effect, class, business identity source, and selected
   guard/storage path.
3. Classify each tool from code evidence. Read
   [references/tool-classification.md](references/tool-classification.md) before
   writing configuration.
4. Add the appropriate `mycelium-runtime` dependency/extras using the project's
   existing package manager. Preserve existing version policy. Do not bump the
   application's version.
5. Create or merge `mycelium.yaml`. Preserve deliberate existing settings.
   Prefer current template defaults, explicit callable paths, stable transition
   identity, strict policies for consequential tools, and durable storage that
   matches the discovered deployment topology. Load the completed file through
   the installed Mycelium version before treating it as valid.
6. Wire the real execution boundary. Prefer a supported framework integration
   or Mycelium's wrapper/config instrumentation path. For a custom loop, wrap the
   callable at the last boundary before execution. Ensure sync/async parity and
   idempotent installation. Merely creating YAML is not completion.
7. If stateful guards are enabled, configure one `state_backend`; use file only
   for a confirmed single-node deployment and Redis/Postgres for multiple
   workers. If legacy guard state exists, run the migration plan only. Applying
   a production migration requires explicit authorization and stopped workers.
8. When a consequential provider operation can become ambiguous, default to a
   hard block. Only add automatic reconciliation when the provider has a
   genuinely read-only lookup path and a stable external operation handle. Read
   [references/provider-reconciliation.md](references/provider-reconciliation.md)
   before creating an adapter.
9. Add behavioral tests proving the wrapper executes at the actual boundary,
   redispatch does not silently duplicate effects, missing identity fails as
   configured, and framework terminal/LLM hooks are active when selected.
10. Run focused application tests, configuration import/load, `mycelium doctor`,
    `mycelium doctor --strict`, and appropriate synthetic `mycelium verify`
    scenarios. Run the project's normal lint/test commands afterward. Fix
    integration failures before handoff. If strict Doctor cannot pass because a
    host-owned input is absent, preserve the fail-closed configuration and report
    the exact missing input instead of weakening the policy.

## Host-owned questions

Infer first; ask only for decisions or authority the repository cannot prove.
Group unresolved items into a short final list, such as:

- the stable business operation ID used across retries;
- production Redis/Postgres connection environment-variable name or topology;
- provider idempotency-key and read-only reconciliation guarantees;
- approved CRM destinations, destructive objects, or authority windows;
- secret/signing-key environment-variable names and production permissions.

Never ask the user to transcribe information already present in the repository.

## Safe autonomy

Do all work supported by repository evidence without asking the developer to
transcribe configuration or wire wrappers manually. Make these boundaries
non-negotiable:

- Never invent a secret, production DSN, business request identity, approved
  destination, destructive authority, signing authority, or provider fact.
- Reuse environment-variable names already declared by the project; otherwise
  add a documented placeholder name, never a value.
- Never weaken a policy merely to make Doctor green. If stable business identity
  cannot be inferred, configure the path to fail closed and report the precise
  unresolved field.
- Never claim `NOT_EXECUTED` from absence, timeout, indexing lag, duplicate
  matches, or malformed provider data.
- Never call a live business tool/provider, alter production data, apply a
  migration, release a blocked transition, push, publish, or deploy unless the
  user separately authorizes that action.
- A synthetic verification report does not prove live provider permissions.
  Production reconciliation credentials still need read-only scopes.

## Completion report

Return a short handoff containing:

- the completed `mycelium.yaml` path and any intentionally unresolved placeholders;
- files and execution boundaries wired;
- tool classifications and identity sources chosen;
- storage/topology choice;
- Doctor, Verify, and application-test results;
- only the fail-closed host-owned inputs the application owner must supply.

Do not describe the setup as complete if YAML exists but the runtime boundary is
unwired, tests fail, or a consequential tool lacks trustworthy identity.
