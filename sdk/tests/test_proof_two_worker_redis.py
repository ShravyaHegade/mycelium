"""Real Redis two-worker Cloud-style #7417 redispatch proof."""

from __future__ import annotations

from backend_gates import require_redis_or_skip

from mycelium.proofs.langgraph_7417_redis import prove_two_worker_redis_redispatch


def test_two_worker_redis_cloud_style_redispatch() -> None:
    url = require_redis_or_skip()
    result = prove_two_worker_redis_redispatch(url=url)
    assert result["workers"] == 2
    assert result["storage"] == "redis"
    assert result["executions"] == 1
    assert result["result"] == {"task": "analyze_market", "result": "done"}
