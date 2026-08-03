# Mycelium sandbox (local)

Interactive mini-agent loop — **no LLM**. Compose tools, preview YAML, compare
the same plan **without** vs **with** Mycelium — in-process or via the real
`mycelium run` CLI (temp `agent_app.py` + YAML).

## Run locally

```bash
cd sandbox
# Needs Python >=3.10 (use the SDK venv's interpreter if system python is older):
../sdk/.venv/bin/python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ../sdk
pip install 'fastapi>=0.115' 'uvicorn[standard]>=0.32'

uvicorn app.main:app --reload --port 8765
```

Open http://127.0.0.1:8765

## API

- `GET /api/health`
- `POST /api/yaml` — wizard → mycelium.yaml preview
- `POST /api/run` — `{ tools, plan, injector, mode }` → with/without results (in-process)
- `POST /api/run-cli` — same graph via subprocesses:
  `python run_agent.py` vs `mycelium run --config mycelium.yaml -- python run_agent.py`

Injectors: `redispatch` (RETURN), `peer_slow` (POLL), `crash_hard_block` (HARD_BLOCK), `none`.

UI: **Run with / without** for plain-English outcomes plus technical gates.
`POST /api/run-cli` remains available for the subprocess `mycelium run` path.

## Deploy later

Dockerfile + Azure Container Apps (credits). Keep memory ledger only for the free tier.
