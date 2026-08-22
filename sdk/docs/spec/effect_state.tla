---- MODULE effect_state ----
EXTENDS Naturals, FiniteSets

(*
  Minimal model of the canonical effect row used by ActionLedger for one
  dedupe identity (`effect_id`). It focuses on fenced state transitions and the
  fail-closed UNKNOWN behavior.
*)

CONSTANTS EffectStates, Workers

ASSUME EffectStates = {"INTENDED", "ATTEMPTING", "COMMITTED", "ABORTED", "UNKNOWN"}

VARIABLES effect_state, fence, owner, decision_allowed, effect_id

vars == <<effect_state, fence, owner, decision_allowed, effect_id>>

Init ==
  /\ effect_state = "INTENDED"
  /\ fence = 0
  /\ owner = "none"
  /\ decision_allowed = FALSE
  /\ effect_id = "effect-0"

(* claim_side_effecting + storage.try_claim_inflight CAS *)
Claim(w) ==
  /\ w \in Workers
  /\ owner = "none"
  /\ effect_state = "INTENDED"
  /\ fence' = fence + 1
  /\ owner' = w
  /\ UNCHANGED <<effect_state, decision_allowed, effect_id>>

(* record_decision CAS: the only mutation gate that can enter ATTEMPTING *)
RecordDecision(w, allow) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state = "INTENDED"
  /\ decision_allowed' = allow
  /\ effect_state' = IF allow THEN "ATTEMPTING" ELSE "ABORTED"
  /\ UNCHANGED <<fence, owner, effect_id>>

(* complete CAS *)
Complete(w) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state = "ATTEMPTING"
  /\ decision_allowed = TRUE
  /\ effect_state' = "COMMITTED"
  /\ owner' = "none"
  /\ UNCHANGED <<fence, decision_allowed, effect_id>>

(* fail(..., failed_after_effect=FALSE) CAS *)
Fail(w) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state \in {"INTENDED", "ATTEMPTING"}
  /\ effect_state' = "ABORTED"
  /\ owner' = "none"
  /\ UNCHANGED <<fence, decision_allowed, effect_id>>

(* mark_maybe_crossed / fail(..., failed_after_effect=TRUE) CAS *)
MarkUnknown(w) ==
  /\ w \in Workers
  /\ owner = w
  /\ effect_state = "ATTEMPTING"
  /\ effect_state' = "UNKNOWN"
  /\ owner' = "none"
  /\ UNCHANGED <<fence, decision_allowed, effect_id>>

(* stale-fence mutation attempt: CAS rejects and state is unchanged *)
StaleFenceWrite(w, stale_fence) ==
  /\ w \in Workers
  /\ stale_fence < fence
  /\ UNCHANGED vars

Next ==
  \/ \E w \in Workers: Claim(w)
  \/ \E w \in Workers: RecordDecision(w, TRUE)
  \/ \E w \in Workers: RecordDecision(w, FALSE)
  \/ \E w \in Workers: Complete(w)
  \/ \E w \in Workers: Fail(w)
  \/ \E w \in Workers: MarkUnknown(w)
  \/ \E w \in Workers: \E stale_fence \in 0..fence: StaleFenceWrite(w, stale_fence)

Spec == Init /\ [][Next]_vars

CommittedEffectIds == IF effect_state = "COMMITTED" THEN {effect_id} ELSE {}
AtMostOneCommitted == Cardinality(CommittedEffectIds) <= 1

THEOREM Spec => []AtMostOneCommitted

====
