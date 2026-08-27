"""Small verifier-owned TCP proxy used to interrupt backend connectivity."""

from __future__ import annotations

import selectors
import socket
import threading
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from mycelium.verify.types import IsolationRefused


@dataclass
class BackendFaultProxy:
    """Transparent TCP proxy whose active connections can be hard-interrupted."""

    host: str
    port: int
    _listener: socket.socket | None = field(init=False, default=None, repr=False)
    _thread: threading.Thread | None = field(init=False, default=None, repr=False)
    _stop: threading.Event = field(init=False, default_factory=threading.Event, repr=False)
    _available: threading.Event = field(init=False, default_factory=threading.Event, repr=False)
    _connections: set[socket.socket] = field(init=False, default_factory=set, repr=False)
    _lock: threading.Lock = field(init=False, default_factory=threading.Lock, repr=False)

    def start(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.1)
        self._listener = listener
        self._available.set()
        self._thread = threading.Thread(
            target=self._serve, name="mycelium-verify-proxy", daemon=True
        )
        self._thread.start()

    @property
    def address(self) -> tuple[str, int]:
        if self._listener is None:
            raise RuntimeError("backend fault proxy is not running")
        host, port = self._listener.getsockname()[:2]
        return str(host), int(port)

    def interrupt(self) -> None:
        """Reject new connections and sever every active backend connection."""
        self._available.clear()
        with self._lock:
            sockets = list(self._connections)
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def restore(self) -> None:
        self._available.set()

    def close(self) -> None:
        self._stop.set()
        self._available.set()
        self.interrupt()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=2)

    def __enter__(self) -> BackendFaultProxy:
        self.start()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _serve(self) -> None:
        assert self._listener is not None
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except (TimeoutError, OSError):
                continue
            if not self._available.is_set():
                client.close()
                continue
            threading.Thread(target=self._forward, args=(client,), daemon=True).start()

    def _forward(self, client: socket.socket) -> None:
        upstream: socket.socket | None = None
        try:
            upstream = socket.create_connection((self.host, self.port), timeout=3)
            with self._lock:
                self._connections.update((client, upstream))
            selector = selectors.DefaultSelector()
            selector.register(client, selectors.EVENT_READ, upstream)
            selector.register(upstream, selectors.EVENT_READ, client)
            while self._available.is_set() and not self._stop.is_set():
                events = selector.select(timeout=0.1)
                for key, _ in events:
                    destination = key.data
                    try:
                        data = key.fileobj.recv(65536)
                    except OSError:
                        return
                    if not data:
                        return
                    destination.sendall(data)
        except OSError:
            return
        finally:
            with self._lock:
                self._connections.discard(client)
                if upstream is not None:
                    self._connections.discard(upstream)
            for sock in (client, upstream):
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass


def proxy_worker_payload(payload: dict[str, object]) -> tuple[BackendFaultProxy, dict[str, object]]:
    """Build a fault proxy and rewrite a Redis/Postgres worker connection URL."""
    backend = str(payload.get("backend"))
    key = "url" if backend == "redis" else "dsn" if backend == "postgres" else None
    if key is None:
        raise IsolationRefused("cluster verification requires Redis or PostgreSQL")
    raw = str(payload.get(key) or "")
    parsed = urlsplit(raw)
    if not parsed.hostname or not parsed.port:
        default_port = 6379 if backend == "redis" else 5432
        port = parsed.port or default_port
    else:
        port = parsed.port
    if not parsed.hostname:
        raise IsolationRefused(f"cluster verification could not resolve {backend} backend host")
    if backend == "redis" and parsed.scheme == "rediss":
        raise IsolationRefused(
            "built-in cluster fault injection does not rewrite rediss:// TLS endpoints; "
            "use a private redis:// test endpoint"
        )
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if backend == "postgres" and query.get("sslmode") in {"verify-ca", "verify-full"}:
        raise IsolationRefused(
            "built-in cluster fault injection cannot preserve hostname-verified PostgreSQL TLS; "
            "use a private test endpoint with sslmode=require or disable"
        )
    proxy = BackendFaultProxy(parsed.hostname, int(port))
    proxy.start()
    local_host, local_port = proxy.address
    userinfo = ""
    if parsed.username is not None:
        userinfo = parsed.username
        if parsed.password is not None:
            userinfo += f":{parsed.password}"
        userinfo += "@"
    rewritten = urlunsplit(
        (
            parsed.scheme,
            f"{userinfo}{local_host}:{local_port}",
            parsed.path,
            urlencode(query),
            parsed.fragment,
        )
    )
    updated = dict(payload)
    updated[key] = rewritten
    return proxy, updated


__all__ = ["BackendFaultProxy", "proxy_worker_payload"]
