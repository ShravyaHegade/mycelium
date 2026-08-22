---- MODULE effect_state ----
EXTENDS Naturals, FiniteSets

(*
  Model of the canonical effect row used by ActionLedger for one dedupe
  identity (`effect_id`). Covers fenced CAS transitions, stale-fence refusal,
  lease expiry/reclaim, and fail-closed UNKNOWN redispatch.
*)

CONSTANTS EffectStates, Workers, EffectIds

ASSUME EffectStates = {"INTENDED", "ATTEMPTING", "COMMITTED", "ABORTED", "UNKNOWN"}
ASSUME EffectIds \subseteq {"effect-0", "effect-1"}
ASSUME Cardinality(EffectIds) >= 1

VARIABLES effect_state, fence, owner, decision_allowed, effect_id, lease_held

vars == <<effect_state, fence, owner, decision_allowed, effect_id, lease_held>>

Init ==
  /\ effect_state = "INTENDED"
  /\ fence = 0
  /\ owner = "none"
  /\ decision_allowed = FALSE
  /\ effect_id \in EffectIds
  /\ lease_held = FALSE

(* claim_side_effecting + storage.try_claim_inflight CAS *)
Claim(w) ==
  /\ w \in Workers
  /\ owner = "none"
  /\ effect_state = "INTENDED"
  /\ fence' = fence + 1
  /\ owner' = w
  /\ lease_held' = TRUE
  /\ UNCHANGED <<effect_state, decision_allowed, effect_id>>

(* record_decision CAS: the only mutation gate that can enter ATTEMPTING *)
RecordDecision(w, allow) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state = "INTENDED"
  /\ decision_allowed' = allow
  /\ effect_state' = IF allow THEN "ATTEMPTING" ELSE "ABORTED"
  /\ owner' = "none"
  /\ lease_held' = FALSE
  /\ UNCHANGED <<fence, effect_id>>

(* complete CAS *)
Complete(w) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state = "ATTEMPTING"
  /\ decision_allowed = TRUE
  /\ effect_state' = "COMMITTED"
  /\ owner' = "none"
  /\ lease_held' = FALSE
  /\ UNCHANGED <<fence, decision_allowed, effect_id>>

(* fail(..., failed_after_effect=FALSE) CAS *)
Fail(w) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state \in {"INTENDED", "ATTEMPTING"}
  /\ effect_state' = "ABORTED"
  /\ owner' = "none"
  /\ lease_held' = FALSE
  /\ UNCHANGED <<fence, decision_allowed, effect_id>>

(* mark_maybe_crossed / fail(..., failed_after_effect=TRUE) CAS *)
MarkUnknown(w) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state = "ATTEMPTING"
  /\ effect_state' = "UNKNOWN"
  /\ owner' = "none"
  /\ lease_held' = FALSE
  /\ UNCHANGED <<fence, decision_allowed, effect_id>>

(* lease expires while ATTEMPTING; a new worker may reclaim with a higher fence *)
ExpireLease ==
  /\ lease_held = TRUE
  /\ effect_state = "INTENDED"
  /\ lease_held' = FALSE
  /\ UNCHANGED <<effect_state, fence, owner, decision_allowed, effect_id>>

Reclaim(w) ==
  /\ w \in Workers
  /\ effect_state = "INTENDED"
  /\ lease_held = FALSE
  /\ owner = "none"
  /\ fence' = fence + 1
  /\ owner' = w
  /\ lease_held' = TRUE
  /\ UNCHANGED <<effect_state, decision_allowed, effect_id>>

(* stale-fence mutation attempt: CAS rejects and state is unchanged *)
StaleFenceWrite(w, stale_fence) ==
  /\ w \in Workers
  /\ stale_fence < fence
  /\ UNCHANGED vars

(* UNKNOWN rows refuse automatic redispatch (fail-closed) *)
RedispatchUnknown(w) ==
  /\ w \in Workers
  /\ effect_state = "UNKNOWN"
  /\ UNCHANGED vars

Next ==
  \/ \E w \in Workers: Claim(w)
  \/ \E w \in Workers: RecordDecision(w, TRUE)
  \/ \E w \in Workers: RecordDecision(w, FALSE)
  \/ \E w \in Workers: Complete(w)
  \/ \E w \in Workers: Fail(w)
  \/ \E w \in Workers: MarkUnknown(w)
  \/ ExpireLease
  \/ \E w \in Workers: Reclaim(w)
  \/ \E w \in Workers: \E stale_fence \in 0..fence: StaleFenceWrite(w, stale_fence)
  \/ \E w \in Workers: RedispatchUnknown(w)

Spec == Init /\ [][Next]_vars

CommittedEffectIds == IF effect_state = "COMMITTED" THEN {effect_id} ELSE {}
AtMostOneCommitted == Cardinality(CommittedEffectIds) <= 1
UnknownNeverAutoCompletes == effect_state = "UNKNOWN" => owner = "none"

THEOREM Spec => []AtMostOneCommitted
THEOREM Spec => []UnknownNeverAutoCompletes

====
