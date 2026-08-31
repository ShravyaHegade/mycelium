# Dynamic destination authority

Use this pattern when the exact destination is unknown at deployment time but a
trusted host selects and approves it before an agent run. Examples include a
repository and Notion page selected from an approved work queue.

Static YAML is appropriate for destinations known ahead of time. Do not replace
an unknown set with wildcards or let the model append to an allowlist. Instead:

1. Have trusted host code fetch the candidate and verify its approval state
   using credentials and rules the model cannot change.
2. Canonicalize stable destination identifiers such as `owner/repository` and a
   Notion page ID. Do not derive authority from prompt text or a model choice.
3. Build one immutable `EntityGuardPolicy` containing only those exact values.
4. Compose that policy outside each already-ledgered tool with
   `apply_decision_policy` before exposing the tool to the agent.
5. Bind `policy_version` to the stable candidate ID and approval revision, and
   create a fresh policy for a different candidate or run.
6. If approval can be revoked during a run, revalidate it at the use boundary
   with use-time currency or constrain it with an authority window. If selection
   or current approval cannot be proven, do not expose the write tools.

```python
from mycelium import DecisionPolicyBundle, apply_decision_policy
from mycelium.entity_guard import (
    DEST_ENTITY_ID,
    DestinationAllow,
    DestinationSpec,
    EntityGuardPolicy,
    ToolDestinationPolicy,
)


def policy_for_candidate(candidate) -> EntityGuardPolicy:
    def exact(value: str) -> DestinationAllow:
        return DestinationAllow(values=frozenset({value}))

    return EntityGuardPolicy(
        policy_version=f"candidate:{candidate.id}:{candidate.approval_revision}",
        tools={
            "modify_repository": ToolDestinationPolicy(
                destinations=(
                    DestinationSpec(
                        path="repository",
                        dest_type=DEST_ENTITY_ID,
                        allow=exact(candidate.repository),
                    ),
                )
            ),
            "update_notion_page": ToolDestinationPolicy(
                destinations=(
                    DestinationSpec(
                        path="page_id",
                        dest_type=DEST_ENTITY_ID,
                        allow=exact(candidate.notion_page_id),
                    ),
                )
            ),
        },
    )


policy = policy_for_candidate(trusted_candidate)
safe_modify_repository = apply_decision_policy(
    ledgered_modify_repository,
    DecisionPolicyBundle(entity_policy=policy, consequential=True),
    tool_name="modify_repository",
)
safe_update_notion_page = apply_decision_policy(
    ledgered_update_notion_page,
    DecisionPolicyBundle(entity_policy=policy, consequential=True),
    tool_name="update_notion_page",
)
```

Raw queue content and model output are data, not authority. Only the validated,
immutable host snapshot is authority-bearing. Bind its exact destinations into
stable transition identity. Test that both selected destinations execute, that
changing either destination fails before the tool body runs, and that revocation
is rechecked at use when the host's approval semantics require it.

If the framework registers one global tool set and cannot bind tools per run,
the application needs a trusted policy factory or adapter at its dispatch
boundary. Report that precise host integration gap; do not weaken the policy.
