"""The server's web page: the console's read-outs and changes, for a browser (#450).

    python -m freestream … --web-port 8260          then open http://127.0.0.1:8260/

★ SERVED BY THE SERVER ITSELF, like HAProxy's `stats uri` (2026-09-24). An operator reaches for
this page when something is wrong, and a page that depended on the API would go dark exactly when the
API is what is wrong. The server already holds every number the page shows, so it serves them itself —
with the standard library only, because the server runs on free-threaded Python and imports nothing
from outside this repository.

★ ONE LIVE STREAM PER PAGE, AND IT AWAITS. `GET /events` is Server-Sent Events: the page is sent the
whole state at once and again whenever something changes, at most twice a second, and nothing at all
while the server is idle but a keep-alive comment. The same stance as a consumer's held NEXT, for the
same reason — a page that polled would ask the server "anything yet?" forever. The whole state in one
message keeps the view at a fixed cost however many topics and consumers there are.

★ LOOPBACK ONLY, AND THREE CHECKS BECAUSE LOOPBACK IS NOT ENOUGH. It binds 127.0.0.1, so it is reached
on the box or through an SSH tunnel (`ssh -L 8260:127.0.0.1:8260 <host>`) and never from the internet.
A page on some other site open in the same browser can still send requests to 127.0.0.1, though, so:
  * the Host header must name this machine (localhost, 127.0.0.1 or ::1), which defeats DNS rebinding;
  * a change must carry `X-Eventstream-Console: 1` — a custom header, which a cross-site form cannot
    send and a cross-site fetch cannot send without a CORS preflight this server never answers;
  * an Origin, when the browser sends one, must be this page's own.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, unquote, urlsplit

from .registry import TopicError
from .status import EventStreamStatus

if TYPE_CHECKING:
    from .server import EventStreamServer

log = logging.getLogger(__name__)

#: The page, read once when the server starts.
PAGE = Path(__file__).with_name("console.html")

#: The shortest gap between two pushes of the state to one page. A burst of produces — a 286-page scan
#: arriving — is one push every half second, not one per record.
PUSH_INTERVAL_SECONDS = 0.5

#: How long a quiet stream waits before sending a keep-alive, so a proxy or tunnel does not close it.
KEEP_ALIVE_SECONDS = 15.0

#: The largest request body accepted. The page sends each change as a few dozen bytes of JSON.
MAX_BODY_BYTES = 16 * 1024

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}

#: Three stacked strokes of a log, in the page's lag blue — so the tab is findable among others.
_ICON = (b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 16 16"><g stroke="#2f6fb3" '
         b'stroke-width="2" stroke-linecap="round"><path d="M3 4h10M3 8h7M3 12h9"/></g></svg>')
_CHANGE_HEADER = "x-eventstream-console"


class _RefusedError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class _Request:
    def __init__(self, method: str, target: str, headers: dict[str, str], body: bytes) -> None:
        self.method, self.headers, self.body = method, headers, body
        split = urlsplit(target)
        self.path = [unquote(part) for part in split.path.split("/") if part]
        self.query = {name: values[0] for name, values in parse_qs(split.query).items()}

    def json(self) -> dict[str, Any]:
        try:
            fields = json.loads(self.body or b"{}")
        except ValueError as unreadable:
            raise _RefusedError(400, f"the body is not JSON: {unreadable}") from unreadable
        if not isinstance(fields, dict):
            raise _RefusedError(400, "the body must be a JSON object")
        return fields


class WebConsole:
    """The listening server and every page currently streaming from it."""

    def __init__(self, event_stream_server: EventStreamServer) -> None:
        self._event_stream_server = event_stream_server
        self._status = EventStreamStatus(event_stream_server)
        self._page = PAGE.read_bytes()
        self._streams: set[asyncio.StreamWriter] = set()
        self.server: asyncio.base_events.Server | None = None

    def close(self) -> None:
        if self.server is not None:
            self.server.close()
        for writer in list(self._streams):
            writer.close()

    async def answer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """One HTTP request, and its response. The live stream keeps its connection; the rest close."""
        try:
            request = await self._read(reader)
            self._check_origin(request)
            if request.method == "GET" and request.path == ["events"]:
                await self._stream(writer)
                return
            status, content_type, body = await self._route(request)
        except _RefusedError as refused:
            status, content_type = refused.status, "application/json"
            body = json.dumps({"error": str(refused)}).encode()
        except (asyncio.IncompleteReadError, ConnectionResetError, TimeoutError):
            writer.close()
            return
        writer.write(_response(status, content_type, body))
        with contextlib.suppress(ConnectionResetError, BrokenPipeError):
            await writer.drain()
        writer.close()

    async def _read(self, reader: asyncio.StreamReader) -> _Request:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=10)
        request_line, *header_lines = head.decode("latin-1").split("\r\n")
        try:
            method, target, _version = request_line.split(" ", 2)
        except ValueError as malformed:
            raise _RefusedError(400, "malformed request line") from malformed
        headers = {}
        for line in header_lines:
            name, _colon, value = line.partition(":")
            if name:
                headers[name.strip().lower()] = value.strip()
        length = int(headers.get("content-length") or 0)
        if length > MAX_BODY_BYTES:
            raise _RefusedError(413, "the body is larger than any change needs")
        body = await asyncio.wait_for(reader.readexactly(length), timeout=10) if length else b""
        return _Request(method.upper(), target, headers, body)

    def _check_origin(self, request: _Request) -> None:
        host = request.headers.get("host", "")
        hostname = host.rsplit(":", 1)[0] if not host.startswith("[") else host.split("]")[0] + "]"
        if hostname not in _LOCAL_HOSTS:
            raise _RefusedError(421, "this page answers only to localhost")
        origin = request.headers.get("origin")
        if origin is not None and urlsplit(origin).netloc != host:
            raise _RefusedError(403, "a change must come from this page")
        if request.method != "GET" and request.headers.get(_CHANGE_HEADER) != "1":
            raise _RefusedError(403, "a change must come from this page")

    async def _route(self, request: _Request) -> tuple[int, str, bytes]:
        routes: dict[tuple[str, str], Callable[[_Request], Awaitable[dict[str, Any]]]] = {
            ("GET", "state"): self._get_state,
            ("GET", "dead"): self._get_dead,
            ("POST", "topics"): self._post_topic,
            ("DELETE", "topics"): self._delete_topic,
            ("POST", "consumers"): self._post_consumer_state,
            ("DELETE", "groups"): self._delete_group,
        }
        if request.method == "GET" and not request.path:
            return 200, "text/html; charset=utf-8", self._page
        if request.method == "GET" and request.path == ["favicon.svg"]:
            return 200, "image/svg+xml", _ICON
        route = routes.get((request.method, request.path[1] if request.path[:1] == ["api"]
                                                            and len(request.path) > 1 else ""))
        if route is None:
            raise _RefusedError(404, f"nothing at {request.method} /{'/'.join(request.path)}")
        try:
            return 200, "application/json", json.dumps(await route(request)).encode()
        except TopicError as refused:
            raise _RefusedError(409, str(refused)) from refused
        except (KeyError, ValueError, RuntimeError, PermissionError) as refused:
            raise _RefusedError(409, str(refused).strip("'")) from refused

    async def _stream(self, writer: asyncio.StreamWriter) -> None:
        """The whole state now, then again after every change, until the page goes away."""
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-store\r\n"
                     b"Connection: keep-alive\r\n\r\n")
        self._streams.add(writer)
        try:
            while not writer.is_closing():
                change = self._event_stream_server.next_change()
                writer.write(b"data: " + json.dumps(self._status.snapshot()).encode() + b"\n\n")
                await writer.drain()
                while True:
                    try:
                        await asyncio.wait_for(asyncio.shield(change), timeout=KEEP_ALIVE_SECONDS)
                        break
                    except TimeoutError:
                        writer.write(b": still here\n\n")
                        await writer.drain()
                await asyncio.sleep(PUSH_INTERVAL_SECONDS)
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            self._streams.discard(writer)
            writer.close()

    # ------------------------------------------------------------------ routes

    async def _get_state(self, _request: _Request) -> dict[str, Any]:
        return self._status.snapshot()

    async def _get_dead(self, request: _Request) -> dict[str, Any]:
        return self._status.dead(request.query.get("topic"))

    async def _post_topic(self, request: _Request) -> dict[str, Any]:
        fields = request.json()
        self._status.define_topic(str(fields.get("name", "")), int(fields.get("partitions", 0)),
                                  str(fields.get("delivery", "")))
        return {"defined": fields.get("name")}

    async def _delete_topic(self, request: _Request) -> dict[str, Any]:
        name = _one_after(request, "topics")
        self._status.remove_topic(name)
        return {"removed": name}

    async def _post_consumer_state(self, request: _Request) -> dict[str, Any]:
        if len(request.path) != 4 or request.path[3] != "state":
            raise _RefusedError(404, "POST /api/consumers/<id>/state")
        connection, state = int(request.path[2]), str(request.json().get("state", ""))
        self._status.set_state(connection, state)
        log.warning("web console: consumer %d set to %s", connection, state)
        return {"consumer": connection, "state": state}

    async def _delete_group(self, request: _Request) -> dict[str, Any]:
        if len(request.path) != 4:
            raise _RefusedError(404, "DELETE /api/groups/<topic>/<group>")
        self._status.delete_group(request.path[2], request.path[3])
        log.warning("web console: group %s/%s deleted", request.path[2], request.path[3])
        return {"deleted": f"{request.path[2]}/{request.path[3]}"}


def _one_after(request: _Request, collection: str) -> str:
    if len(request.path) != 3:
        raise _RefusedError(404, f"/api/{collection}/<name>")
    return request.path[2]


def _response(status: int, content_type: str, body: bytes) -> bytes:
    reasons = {200: "OK", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 409: "Conflict",
               413: "Content Too Large", 421: "Misdirected Request"}
    return (f"HTTP/1.1 {status} {reasons.get(status, 'Error')}\r\nContent-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\nCache-Control: no-store\r\nConnection: close\r\n"
            f"X-Content-Type-Options: nosniff\r\nX-Frame-Options: DENY\r\n"
            f"Content-Security-Policy: default-src 'self'; style-src 'self' 'unsafe-inline'; "
            f"script-src 'self' 'unsafe-inline'; frame-ancestors 'none'\r\n\r\n").encode() + body


async def serve_web(event_stream_server: EventStreamServer, port: int, host: str = "127.0.0.1") -> WebConsole:
    """Serve the page on loopback. Returns the console, whose `close` ends every live stream too."""
    console = WebConsole(event_stream_server)
    console.server = await asyncio.start_server(console.answer, host=host, port=port)
    log.info("event stream web console on http://%s:%d/", host, port)
    return console
