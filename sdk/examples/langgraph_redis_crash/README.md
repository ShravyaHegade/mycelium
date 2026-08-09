# LangGraph + Redis + crash (copy-paste)

Adoption example — not a new capability. Stitches the pieces partners usually
have to discover separately:

| Piece | What you get |
|-------|----------------|
| LangGraph integration | `tool_call_id` / thread / run → transition identity |
| Redis ledger | durable claims across workers / restarts |
| Signed receipts | `receipt_ref` on completed transitions |
| Crash redispatch | mid-flight kill → `HARD_BLOCK`, body runs once |

## Run

```bash
docker run -d --name mycelium-redis -p 6379:6379 redis:7
cd sdk   # or: pip install 'mycelium-runtime[langgraph]' redis
export MYCELIUM_REDIS_URL=redis://127.0.0.1:6379/15
export MYCELIUM_SIGNING_KEY=demo-signing-key
python examples/langgraph_redis_crash/run.py
```

Expected:

```text
[ok] redispatch RETURN — executions=1 receipt_ref=...
[ok] crash HARD_BLOCK — worker_executions=1 redispatch_body_runs=0
```

## Drop into your app

1. Copy [`mycelium.yaml`](mycelium.yaml) next to your graph.
2. `pip install 'mycelium-runtime[langgraph]' redis`
3. Set `MYCELIUM_REDIS_URL` and `MYCELIUM_SIGNING_KEY`.
4. Wrap the payment (or other mutate) tool:

```python
from mycelium import load_config

config = load_config("mycelium.yaml")

@config.apply
def send_payment(amount: float) -> dict:
    # your provider call
    return {"charged": amount}

# Pass send_payment into LangGraph ToolNode as usual.
```

## Related

- In-process gate pack (no Redis): [`../failure_cases/`](../failure_cases/)
- Two-worker poll proof: `mycelium demo --redis`
- Gate table: [sdk/README.md § Resolution gates](../../README.md#resolution-gates)
