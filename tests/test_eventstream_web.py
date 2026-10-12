"""The server's web page: its routes, its live stream and the checks that keep it the operator's (#450).

Driven over a real loopback socket with hand-written HTTP, because what matters is what a browser —
or a page on some other site open in the same browser — actually gets back.
"""
import asyncio
import json

import pytest

from freestream import web as web_module
from freestream.client import FreeStreamClient
from freestream.interface.channel import Channel, Delivery
from freestream.server import EventStreamServer
from freestream.web import serve_web
from tests.eventually import eventually
from tests.topics import define

WORK = Channel("work", Delivery.COMPETING, partitions=2)


@pytest.fixture
async def page(tmp_path):
    running = define(EventStreamServer(tmp_path / "log"), WORK)
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    console = await serve_web(running, 0)
    port = console.server.sockets[0].getsockname()[1]
    yield running, tmp_path / "es.sock", port
    console.close()
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


async def _http(port, method, path, *, body=None, headers=None, host=None):
    """One request; returns (status, headers, body)."""
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    payload = json.dumps(body).encode() if body is not None else b""
    lines = [f"{method} {path} HTTP/1.1", f"Host: {host or f'127.0.0.1:{port}'}",
             f"Content-Length: {len(payload)}"]
    lines += [f"{name}: {value}" for name, value in (headers or {}).items()]
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode() + payload)
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), timeout=5)
    writer.close()
    head, _blank, content = raw.partition(b"\r\n\r\n")
    status_line, *header_lines = head.decode().split("\r\n")
    answer_headers = {line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip() for line in header_lines}
    return int(status_line.split()[1]), answer_headers, content


CHANGE = {"X-Eventstream-Console": "1", "Content-Type": "application/json"}


async def test_the_page_is_served_and_cannot_be_framed(page):
    _running, _socket, port = page
    status, headers, body = await _http(port, "GET", "/")
    assert status == 200 and b"<title>Event stream</title>" in body
    assert headers["x-frame-options"] == "DENY"
    assert "frame-ancestors 'none'" in headers["content-security-policy"]


async def test_the_page_has_an_icon_so_its_tab_can_be_found(page):
    _running, _socket, port = page
    status, headers, body = await _http(port, "GET", "/favicon.svg")
    assert status == 200 and headers["content-type"] == "image/svg+xml" and body.startswith(b"<svg")


async def test_the_whole_state_comes_in_one_answer(page):
    _running, _socket, port = page
    status, _headers, body = await _http(port, "GET", "/api/state")
    state = json.loads(body)
    assert status == 200
    assert {"info", "topics", "groups", "consumers", "backlog", "states"} <= set(state)
    assert [topic["name"] for topic in state["topics"]] == ["work"]


async def test_a_host_that_is_not_this_machine_is_refused(page):
    """DNS rebinding: a hostile name that resolves to 127.0.0.1 still says its own name in Host."""
    _running, _socket, port = page
    status, _headers, _body = await _http(port, "GET", "/api/state", host="attacker.example:8260")
    assert status == 421


async def test_a_change_without_the_page_header_is_refused(page):
    """A cross-site form can post to loopback, but it cannot add a custom header."""
    running, _socket, port = page
    status, _headers, _body = await _http(port, "POST", "/api/topics",
                                          body={"name": "sneaky", "partitions": 1,
                                                "delivery": "competing"},
                                          headers={"Content-Type": "application/json"})
    assert status == 403
    assert "sneaky" not in [definition.name for definition in running.registry.all()]


async def test_a_change_from_another_origin_is_refused(page):
    _running, _socket, port = page
    status, _headers, _body = await _http(port, "POST", "/api/topics", body={"name": "x"},
                                          headers={**CHANGE, "Origin": "https://attacker.example"})
    assert status == 403


async def test_a_topic_is_defined_refused_and_removed_from_the_page(page):
    running, _socket, port = page
    origin = {"Origin": f"http://127.0.0.1:{port}"}
    status, _headers, _body = await _http(port, "POST", "/api/topics", headers={**CHANGE, **origin},
                                          body={"name": "document.bakes", "partitions": 4,
                                                "delivery": "competing"})
    assert status == 200 and running.registry.get("document.bakes").partitions == 4

    status, _headers, body = await _http(port, "POST", "/api/topics", headers=CHANGE,
                                         body={"name": "document.bakes", "partitions": 8,
                                               "delivery": "competing"})
    assert status == 409 and "cannot change" in json.loads(body)["error"]

    status, _headers, _body = await _http(port, "DELETE", "/api/topics/document.bakes", headers=CHANGE)
    assert status == 200 and "document.bakes" not in [t.name for t in running.registry.all()]


async def test_a_consumer_is_drained_from_the_page(page):
    running, socket, port = page
    consumer = FreeStreamClient(socket, role="page-reader")
    try:
        await consumer.next("work", "readers", wait=False)
        status, _headers, _body = await _http(
            port, "POST", f"/api/consumers/{consumer.connection_id}/state", headers=CHANGE,
            body={"state": "drain"})
        assert status == 200
        assert [seen.state for seen in running.connections()
                if seen.connection_id == consumer.connection_id] == ["drain"]

        status, _headers, body = await _http(port, "POST", "/api/consumers/999/state", headers=CHANGE,
                                             body={"state": "drain"})
        assert status == 409 and json.loads(body)["error"] == "999"
    finally:
        await consumer.close()


async def test_a_retired_group_is_deleted_from_the_page_and_a_live_one_is_not(page):
    _running, socket, port = page
    consumer = FreeStreamClient(socket)
    await consumer.next("work", "retired", wait=False)
    status, _headers, _body = await _http(port, "DELETE", "/api/groups/work/retired", headers=CHANGE)
    assert status == 409
    await consumer.close()

    async def deleted() -> bool:
        status, _headers, _body = await _http(port, "DELETE", "/api/groups/work/retired", headers=CHANGE)
        return status == 200
    await eventually(deleted, "the group was never deletable after its last member left")


async def test_dead_letters_are_read_on_demand(page):
    _running, socket, port = page
    producer = FreeStreamClient(socket)
    try:
        await producer.produce("work.dead", value=b"page", headers={"dead.attempts": "3", "page": "4"})
    finally:
        await producer.close()
    status, _headers, body = await _http(port, "GET", "/api/dead?topic=work")
    dead = json.loads(body)
    assert status == 200 and dead["total"] == 1 and dead["letters"][0]["headers"]["page"] == "4"


@pytest.mark.parametrize(("method", "path", "status"), [
    ("GET", "/nowhere", 404), ("PATCH", "/api/topics", 404), ("POST", "/api/consumers/1", 404),
    ("DELETE", "/api/groups/only-one", 404), ("DELETE", "/api/topics", 404)])
async def test_a_request_for_nothing_says_so(page, method, path, status):
    _running, _socket, port = page
    assert (await _http(port, method, path, headers=CHANGE))[0] == status


async def test_a_body_that_is_not_a_json_object_is_refused(page):
    _running, _socket, port = page
    for payload, expected in ((b"not json", b"400"), (b"[1, 2]", b"400")):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(f"POST /api/topics HTTP/1.1\r\nHost: localhost\r\nX-Eventstream-Console: 1\r\n"
                     f"Content-Length: {len(payload)}\r\n\r\n".encode() + payload)
        await writer.drain()
        assert expected in await asyncio.wait_for(reader.read(), timeout=5)
        writer.close()


async def test_an_oversized_body_and_a_garbled_request_are_refused(page):
    _running, _socket, port = page
    for request, expected in (
            (b"POST /api/topics HTTP/1.1\r\nHost: localhost\r\nContent-Length: 999999\r\n\r\n", b"413"),
            (b"NONSENSE\r\nHost: localhost\r\n\r\n", b"400")):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(request)
        await writer.drain()
        assert expected in await asyncio.wait_for(reader.read(), timeout=5)
        writer.close()


# --------------------------------------------------------------------------- the live stream


async def _open_stream(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET /events HTTP/1.1\r\nHost: localhost:{port}\r\n\r\n".encode())
    await writer.drain()
    await reader.readuntil(b"\r\n\r\n")
    return reader, writer


async def _next_event(reader):
    while True:
        chunk = await asyncio.wait_for(reader.readuntil(b"\n\n"), timeout=5)
        if chunk.startswith(b"data: "):
            return json.loads(chunk[len(b"data: "):])
        assert chunk.startswith(b":"), chunk


async def test_the_stream_sends_the_state_then_again_when_it_changes(page, monkeypatch):
    """★ THE PAGE AWAITS, as a consumer does. Nothing is asked for; the state arrives when it moves."""
    monkeypatch.setattr(web_module, "PUSH_INTERVAL_SECONDS", 0.0)
    _running, socket, port = page
    reader, writer = await _open_stream(port)
    producer = FreeStreamClient(socket)
    try:
        first = await _next_event(reader)
        assert first["topics"][0]["records"] == 0
        await producer.produce("work", value=b"one")
        # The producer's HELLO is a change too, so the push that shows the record may be the second.
        for _ in range(5):
            if (await _next_event(reader))["topics"][0]["records"] == 1:
                break
        else:
            raise AssertionError("the produce never reached the page")
    finally:
        await producer.close()
        writer.close()


async def test_a_quiet_stream_is_kept_alive(page, monkeypatch):
    monkeypatch.setattr(web_module, "KEEP_ALIVE_SECONDS", 0.05)
    _running, _socket, port = page
    reader, writer = await _open_stream(port)
    try:
        await _next_event(reader)
        assert await asyncio.wait_for(reader.readuntil(b"\n\n"), timeout=5) == b": still here\n\n"
    finally:
        writer.close()


async def test_closing_the_page_server_ends_its_live_streams(tmp_path):
    running = EventStreamServer(tmp_path / "log")
    console = await serve_web(running, 0)
    port = console.server.sockets[0].getsockname()[1]
    reader, writer = await _open_stream(port)
    await _next_event(reader)

    console.close()

    assert await asyncio.wait_for(reader.read(), timeout=5) == b""
    writer.close()


async def test_the_broker_serves_its_page_when_asked_to(tmp_path):
    running = EventStreamServer(tmp_path / "log")
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", web_port=0, ready=ready))
    await ready.wait()
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)
