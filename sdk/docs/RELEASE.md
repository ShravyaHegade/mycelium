# Release policy & pre-release checklist

Mycelium is a **reliability layer**. Buyers (platform / infra engineers) trust
**calmness**, not velocity. A flood of PyPI versions reads as unstable — the
opposite of the product story.

**Batch. Prefer fewer, coherent releases over many small ones.**

---

## Why this policy exists (honest)

Through mid-2026 Mycelium cut a high volume of package versions while landing
the AF-00N catalog (on the order of **~26 MINOR bumps in ~3.5 months**). That
pace answered “are you building?” and failed the trust test for a reliability
layer: a thrashy changelog looks unstable.

**Fixing that is operational discipline, not a new SDK feature.** We keep
shipping code to `main`; we stop treating every merge as a PyPI event.
Semantic versioning applies only when a cut has **real user-facing change**
(behavior, API, packaging). Docs-only and positioning wait for the next real
batch. Quiet weeks with no PyPI cut are healthy.

Company milestone ≠ release count. One public production user on payment /
consequential tools beats the whole version arithmetic.

---

## Cadence rules (must follow)

1. **Batch by default.** Merge work to `main` without bumping
   `sdk/pyproject.toml`. Accumulate fixes, docs, and features. Cut **one**
   version when a coherent batch is ready.
2. **No same-day PyPI spam.** Do not publish multiple `mycelium-runtime`
   versions on the same calendar day unless a **critical** correctness /
   security fix requires a hotfix (document why in the CHANGELOG).
3. **Target rhythm:** about **one release per week** (or slower) when there is
   real user-facing change. Quiet weeks with no PyPI cut are healthy.
4. **Docs / positioning alone** usually wait for the next real batch — do not
   cut a patch solely to refresh README copy unless PyPI metadata must change
   for outreach *and* nothing else is pending (still never same-day thrash).
5. **Version bump = intentional release.** The only signal that tags/publishes
   is a new version in `sdk/pyproject.toml` (plus matching `CHANGELOG.md`).
   Merging without a bump publishes nothing — use that.
6. **Semver on real change only.**
   - **PATCH** — bugfixes, proofs, packaging, docs that ship *with* a real
     fix batch (no new schema/policy concepts).
   - **MINOR** — new backward-compatible durable fields or resolution
     behavior (batch several related landings into one MINOR when possible).
   - **MAJOR** — breaking defaults or removed paths.
   Do **not** mint a MINOR per merged feature PR. That is how ~26 minors
   happens.
7. **Hotfix exception:** production-breaking ledger/reconcile bug or security
   issue → ship a focused PATCH immediately; still one cut, clear notes, no
   pile-on features in the same tag.

Cadence is orthogonal to semver — batch *what* goes into each bump.

---

## Before you bump the version (checklist)

Complete **every** applicable item. If something does not apply, write N/A in
the PR description — do not skip silently.

### Intent

- [ ] This cut is a **batched** release (or a justified hotfix), not “ship
      because the PR is ready.”
- [ ] No other `mycelium-runtime` version was published **today** (unless hotfix).
- [ ] CHANGELOG tells a **coherent story** (why this batch, not a diary of
      micro-commits).
- [ ] Positioning matches the reliability-layer story (catalog / AF-002
      flagship; Gmail = demo adapter; no velocity-as-virtue language).

### Correctness

- [ ] `pytest tests/` passes locally from `sdk/` (full suite).
- [ ] `ruff check mycelium tests` clean from `sdk/`.
- [ ] CI green on the release PR (3.10–3.13).
- [ ] New guarantees mapped to tests (or explicitly “docs-only / no new
      guarantee” in CHANGELOG).
- [ ] Hotfix: repro + regression test included.

### Surface area

- [ ] Public symbols exported from `sdk/mycelium/__init__.py` if new API.
- [ ] YAML template / `mycelium init` updated if new config surface.
- [ ] SDK README (+ root README if user-facing) updated for shipped behavior.
- [ ] Failure-mode catalog / threat model updated if promise or residual risk
      changed.
- [ ] Handbook (`mycelium-labs.github.io`) version/lede not wildly stale for
      user-visible cuts (batch handbook updates with the release when practical).

### Versioning artifacts (same PR)

- [ ] `sdk/pyproject.toml` `version` bumped once for this cut.
- [ ] `CHANGELOG.md` has `## X.Y.Z (YYYY-MM-DD)` matching that version.
- [ ] Badge / “vX.Y.Z” lines in root + SDK README match (or intentionally
      omit and let badges track PyPI — do not leave contradictory numbers).
- [ ] No second bump planned the same day.

### After merge (automation)

On merge to `main`, [release.yml](../../.github/workflows/release.yml) tags
`v{version}` if the tag does not exist, then [publish.yml](../../.github/workflows/publish.yml)
uploads to PyPI. Confirm:

- [ ] GitHub Release notes look right (from CHANGELOG extract).
- [ ] PyPI shows the new version: https://pypi.org/project/mycelium-runtime/
- [ ] Hotfix: notify affected design partners; otherwise no “blast” needed.

---

## How to cut a release (mechanics)

1. Land work on `main` via PRs **without** version bumps (preferred).
2. Open a **release PR** that only (or primarily):
   - bumps `sdk/pyproject.toml`
   - adds the CHANGELOG section (summarize the batch)
   - syncs README version lines / critical docs
3. Pass the checklist above in the PR body (copy the checkboxes).
4. Merge. Do not manually tag unless automation fails (see root README escape hatch).

**Do not** open a version-bump PR for every merged feature. That is how
8-releases-a-day happens.

---

## What “batch” means in practice

| Good | Bad |
|------|-----|
| One MINOR after loop + completion + scope docs polish | Three PATCHes the same afternoon for each guard |
| Weekly PATCH with five fixes + one docs pass | PATCH per fix “so partners see progress” |
| Quiet week, no PyPI cut | Feeling obligated to ship daily |
| Hotfix PATCH alone, then resume batching | Hotfix + three drive-by features |

Trust compounds when the version line moves **rarely and for good reason**.

**Company milestone ≠ release count.** One public production user on payment
tools is worth more than the whole roadmap. Ship calmly so that user can trust
the layer — do not confuse PyPI version arithmetic with product validation.
