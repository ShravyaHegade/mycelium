# Effect State Spec Notes

`effect_state.tla` is a TLA+ model of the core `ActionLedger` state machine for
consequential tools. The Python proof harness in `mycelium.verify.proof` is the
executable conformance layer; this file is the compact formal sketch.

## Mapping to runtime code

- `Claim` models `ActionLedger.claim_side_effecting()` +
  `LedgerStorage.try_claim_inflight()` CAS ownership/fence acquisition.
- `RecordDecision` models `ActionLedger.record_decision()` as the only allowed
  `INTENDED -> ATTEMPTING` mutation gate.
- `Complete` models `ActionLedger.complete()` on the same fence/owner.
- `Fail` models `ActionLedger.fail(..., failed_after_effect=False)` abort paths.
- `MarkUnknown` models `mark_maybe_crossed()` / fail-after-effect ambiguity.
- `ExpireLease` + `Reclaim` model EXPIRED/not_crossed reclaim after takeover.
- `StaleFenceWrite` models rejected stale-owner CAS writes after takeover.
- `RedispatchUnknown` models fail-closed redispatch on UNKNOWN rows.

## Mapping to verify scenarios

| Scenario | What it proves |
| -------- | -------------- |
| `simulation` | Durable-backend crash windows, fence takeover, effect_id alias dedupe |
| `state-machine-exhaustive` | Deterministic interleavings: matrix, stale-fence, reconcile, concurrent claim |
| `effect-protocol-proof` | **Crash/resume at every scripted step**, alias/fence/UNKNOWN sweeps, enumerated property cases |

Run the deep proof scenario:

```bash
python -m mycelium verify --scenario effect-protocol-proof
```

Pytest entry points:

```bash
pytest tests/test_effect_protocol_proof.py
```

## Optional TLC run (not CI)

1. Install [TLA+ Toolbox](https://lamport.azurewebsites.net/tla/toolbox.html)
   or `tlc2`.
2. Open module `effect_state.tla`.
3. Use model values from `effect_state.cfg` (or set manually):
   - `EffectStates = {"INTENDED","ATTEMPTING","COMMITTED","ABORTED","UNKNOWN"}`
   - `Workers = {"A","B"}`
   - `EffectIds = {"effect-0"}`
4. Check invariants:
   - `AtMostOneCommitted`
   - `UnknownNeverAutoCompletes`

This model is documentation/proof aid only; CI correctness gates remain Python
tests + `mycelium verify` scenarios.

## Proof depth roadmap (not yet exhaustive)

The `effect-protocol-proof` scenario is intentionally finite. Still open for
deeper moat work:

- Real process kill at every await in the ledger wrapper (not just storage
  resume between steps).
- Unbounded two-worker schedule exploration (currently finite scripts + Hypothesis
  sampling over legal prefixes).
- TLC model-checking wired into CI when `tlc2` is available.
- Cross-backend proof runs (file/sqlite/redis/postgres) for the same scripts.
