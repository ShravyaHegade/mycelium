"""FastAPI entrypoint for the Mycelium sandbox."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.cli_demo import run_mycelium_cli_demo
from app.models import CliDemoRequest, RunRequest, RunResponse, YamlPreviewRequest
from app.runner import run_sandbox
from app.yaml_builder import build_yaml

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

app = FastAPI(title="Mycelium Sandbox", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/yaml")
def preview_yaml(body: YamlPreviewRequest) -> dict[str, str]:
    return {
        "yaml": build_yaml(
            body.tools,
            agent_id=body.agent_id,
            policy_version=body.policy_version,
            plan=body.plan,
        )
    }


@app.post("/api/run", response_model=RunResponse)
def run(body: RunRequest) -> RunResponse:
    try:
        results, yaml_preview = run_sandbox(
            body.tools,
            body.plan,
            injector=body.injector,
            mode=body.mode,
            agent_id=body.agent_id,
            policy_version=body.policy_version,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RunResponse(results=results, yaml_preview=yaml_preview)


@app.post("/api/run-cli")
def run_cli(body: CliDemoRequest) -> dict:
    """Same graph via real ``mycelium run`` vs plain ``python -m agent_app``."""
    try:
        return run_mycelium_cli_demo(
            body.plan,
            injector=body.injector,
            agent_id=body.agent_id,
            policy_version=body.policy_version,
            tools=body.tools,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
