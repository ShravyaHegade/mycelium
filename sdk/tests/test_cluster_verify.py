"""Tests for the optional cluster verifier primitives and fail-closed entrypoint."""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from mycelium.__main__ import main
from mycelium.audit_receipt import sign_payload
from mycelium.verify.cluster import (
    REQUIRED_CLUSTER_CHECKS,
    ClusterCheck,
    DeploymentAttestation,
    run_cluster_verify,
    verify_deployment_attestation_signature,
)
from mycelium.verify.cluster_provider import HttpSandboxProvider, load_sandbox_provider_config
from mycelium.verify.cluster_proxy import BackendFaultProxy


class _SandboxHandler(BaseHTTPRequestHandler):
    operations: dict[str, dict[str, object]] = {}
    puts = 0

    def do_PUT(self) -> None:  # noqa: N802
        operation_id = self.path.rsplit("/", 1)[-1]
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length) or b"{}")
        self.__class__.puts += 1
        result = {**payload, "operation_id": operation_id, "status": "completed"}
        self.__class__.operations[operation_id] = result
        self._send(200, result)

    def do_GET(self) -> None:  # noqa: N802
        operation_id = self.path.rsplit("/", 1)[-1]
        result = self.__class__.operations.get(operation_id)
        self._send(200, result) if result is not None else self._send(404, {})

    def _send(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_: object) -> None:
        return


def test_attestation_signature_binds_every_field() -> None:
    report = DeploymentAttestation(
        attestation_id="att-1",
        schema_version=1,
        generated_at=1.0,
        status="VERIFIED",
        config_sha256="abc",
        backend="redis",
        topology="multi_node",
        namespace="mycelium:verify:test:",
        provider_adapter="sandbox",
        worker_count=2,
        checks=[ClusterCheck("two_workers_started", "PASS", "ready")],
        signer_key_id="ci-key",
    )
    report.signature = sign_payload(report.payload(), "secret")
    assert verify_deployment_attestation_signature(report, "secret") is True
    report.status = "FAILED"
    assert verify_deployment_attestation_signature(report, "secret") is False


def test_cli_verifies_signed_attestation(tmp_path: Path, monkeypatch, capsys) -> None:
    report = DeploymentAttestation(
        attestation_id="att-cli",
        schema_version=1,
        generated_at=1.0,
        status="VERIFIED",
        config_sha256="abc",
        backend="postgres",
        topology="multi_node",
        namespace="mycelium:verify:test:",
        provider_adapter="sandbox",
        worker_count=2,
        checks=[ClusterCheck(name, "PASS", "ok") for name in sorted(REQUIRED_CLUSTER_CHECKS)],
        signer_key_id="ci-key",
    )
    report.signature = sign_payload(report.payload(), "secret")
    path = tmp_path / "attestation.json"
    path.write_text(json.dumps(report.to_dict()), encoding="utf-8")
    monkeypatch.setenv("TEST_ATTESTATION_KEY", "secret")
    assert (
        main(
            [
                "verify",
                "--verify-attestation",
                str(path),
                "--attestation-key-env",
                "TEST_ATTESTATION_KEY",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["signature_valid"] is True


def test_http_sandbox_adapter_executes_and_reads_without_hidden_defaults(
    monkeypatch,
) -> None:
    _SandboxHandler.operations = {}
    _SandboxHandler.puts = 0
    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _SandboxHandler)
    except PermissionError:
        pytest.skip("test sandbox does not allow local sockets")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv("MYCELIUM_TEST_SANDBOX_URL", f"http://127.0.0.1:{server.server_port}")
        config = load_sandbox_provider_config(
            {
                "adapter": "http_json",
                "sandbox": True,
                "base_url_env": "MYCELIUM_TEST_SANDBOX_URL",
                "name": "test-sandbox",
            }
        )
        provider = HttpSandboxProvider(config.worker_payload())
        assert provider.execute("op-1")["status"] == "completed"
        assert provider.lookup("op-1")["operation_id"] == "op-1"
        assert provider.lookup("missing") is None
        assert _SandboxHandler.puts == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_backend_fault_proxy_interrupts_and_restores_connections() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", 0))
    except PermissionError:
        listener.close()
        pytest.skip("test sandbox does not allow local sockets")
    listener.listen()
    listener.settimeout(0.1)
    stopped = threading.Event()

    def echo() -> None:
        while not stopped.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            threading.Thread(target=_echo_connection, args=(conn,), daemon=True).start()

    def _echo_connection(conn: socket.socket) -> None:
        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    return
                conn.sendall(data)

    thread = threading.Thread(target=echo, daemon=True)
    thread.start()
    proxy = BackendFaultProxy("127.0.0.1", listener.getsockname()[1])
    proxy.start()
    try:
        first = socket.create_connection(proxy.address, timeout=1)
        first.sendall(b"before")
        assert first.recv(64) == b"before"
        proxy.interrupt()
        time.sleep(0.05)
        try:
            first.sendall(b"during")
            assert first.recv(64) == b""
        except OSError:
            pass
        first.close()

        proxy.restore()
        second = socket.create_connection(proxy.address, timeout=1)
        second.sendall(b"after")
        assert second.recv(64) == b"after"
        second.close()
    finally:
        proxy.close()
        stopped.set()
        listener.close()
        thread.join(timeout=2)


def test_cluster_mode_is_optional_and_refuses_without_explicit_enablement(
    tmp_path: Path, capsys
) -> None:
    config = tmp_path / "mycelium.yaml"
    config.write_text(
        """
transition:
  agent_id: verify-agent
  policy_version: "1"
action_ledger:
  storage: memory
""",
        encoding="utf-8",
    )
    result = run_cluster_verify(config, connectivity=False)
    assert result.refused is True
    assert "disabled" in (result.error or "")

    assert main(["verify", "--config", str(config), "--cluster", "--json"]) == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "cluster"
    assert payload["attestation"] is None


def test_live_redis_cluster_flow_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_url = os.environ.get("MYCELIUM_TEST_CLUSTER_REDIS_URL")
    if not redis_url:
        pytest.skip("MYCELIUM_TEST_CLUSTER_REDIS_URL is not configured")
    _SandboxHandler.operations = {}
    _SandboxHandler.puts = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), _SandboxHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv("MYCELIUM_TEST_SANDBOX_URL", f"http://127.0.0.1:{server.server_port}")
        monkeypatch.setenv("MYCELIUM_TEST_ATTESTATION_KEY", "integration-secret")
        config = tmp_path / "mycelium.yaml"
        config.write_text(
            f"""
deployment:
  topology: multi_node
transition:
  agent_id: verify-agent
  policy_version: "1"
action_ledger:
  storage: redis
  url: {redis_url}
  tools: [charge]
tools:
  charge:
    side_effect_class: non_idempotent_mutate
    request_id_from: order_id
verify:
  cluster:
    enabled: true
    provider:
      adapter: http_json
      name: local-sandbox
      sandbox: true
      base_url_env: MYCELIUM_TEST_SANDBOX_URL
    attestation:
      signing_key_env: MYCELIUM_TEST_ATTESTATION_KEY
      key_id: integration-key
""",
            encoding="utf-8",
        )
        result = run_cluster_verify(config, connectivity=False, timeout_seconds=8)
        assert result.error is None, result.error
        assert result.attestation is not None
        assert result.attestation.verified is True, json.dumps(result.to_dict(), indent=2)
        assert verify_deployment_attestation_signature(result.attestation, "integration-secret")
        assert _SandboxHandler.puts == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
