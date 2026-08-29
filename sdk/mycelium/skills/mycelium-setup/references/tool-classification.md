# Tool classification and identity

Classify from the provider call and externally observable effect, not the Python
function name or HTTP verb.

| Class | Use when | Identity requirement |
|---|---|---|
| `read` | No external mutation occurs | Stable request identity is optional unless application semantics require it |
| `idempotent_mutate` | Repeating the same operation converges to the same external state | Bind the canonical target and operation inputs |
| `keyed_mutate` | The provider accepts and enforces an idempotency key | Use a host-owned business request ID and bind the provider key |
| `non_idempotent_mutate` | Repeating may create another effect | Require explicit stable business request identity; never derive it from a retry/tool-call ID |
| `irreversible` | The effect cannot be reliably undone or carries destructive authority | Require explicit identity and the current confirmation/authority mechanisms exposed by Mycelium |

Treat uncertain mutation semantics conservatively as `non_idempotent_mutate`.
Do not guess that a provider is idempotent.

Good identity candidates already exist before dispatch and describe the business
operation: `order_id`, `payment_request_id`, `job_id`, or a host-issued request
ID. `tool_call_id`, `run_id`, `thread_id`, random IDs created inside the tool,
timestamps, and argument hashes are not substitutes for business identity.

For keyed providers, confirm from code that the same provider idempotency key is
reused on retry. Recording a key without passing it to the provider is not
protection.

If no trustworthy identity exists, keep execution fail-closed and state what the
host must provide. Do not silently fall back to derived identity for a
consequential tool.
