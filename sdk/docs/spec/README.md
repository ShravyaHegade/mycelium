# Effect State Spec Notes

`effect_state.tla` is a compact TLA+ model of the core `ActionLedger` state
machine for consequential tools.

## Mapping to runtime code

- `Claim` models `ActionLedger.claim_side_effecting()` +
  `LedgerStorage.try_claim_inflight()` CAS ownership/fence acquisition.
- `RecordDecision` models `ActionLedger.record_decision()` as the only allowed
  `INTENDED -> ATTEMPTING` mutation gate.
- `Complete` models `ActionLedger.complete()` on the same fence/owner.
- `Fail` models `ActionLedger.fail(..., failed_after_effect=False)` abort paths.
- `MarkUnknown` models `mark_maybe_crossed()` / fail-after-effect ambiguity.
- `StaleFenceWrite` models rejected stale-owner CAS writes after takeover.

## Mapping to verify scenarios

- `python -m mycelium verify --scenario simulation`
  - durable backend crash windows and stale-fence takeover.
  - asserts `check_at_most_one_committed_effect_state`,
    `check_effect_state_consistency`, and `check_unique_effect_id_index`.
- `python -m mycelium verify --scenario state-machine-exhaustive`
  - deterministic in-memory interleavings: stale-fence refusals, reconcile
    outcomes (`COMPLETED` / `NOT_EXECUTED` / `UNKNOWN`), and concurrent claims.
  - re-checks the same invariant set after each interleaving.

## Optional TLC run (not CI)

1. Install [TLA+ Toolbox](https://lamport.azurewebsites.net/tla/toolbox.html)
   or `tlc2`.
2. Create a model for module `effect_state`.
3. Provide:
   - `EffectStates = {"INTENDED","ATTEMPTING","COMMITTED","ABORTED","UNKNOWN"}`
   - `Workers = {"A","B"}`
4. Check invariant `AtMostOneCommitted`.

This model is documentation/proof aid only; CI correctness gates remain Python
tests + `mycelium verify` scenarios.
