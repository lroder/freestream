"""The server's console and the states an operator sets through it (#450).

Every test here goes through the real sockets — the framed one the consumers speak and the text one an
operator types into with `nc -U` — because the console is only as good as what a person sees at the
other end of it.
"""
import asyncio

import pytest

from freestream import admin as admin_module
from freestream.admin import AdminConsole, serve_admin
from freestream.client import FreeStreamClient
from freestream.consumer import dead_letter_channel
from freestream.interface.channel import Channel, Delivery
from freestream.server import EventStreamServer
from tests.eventually import eventually
from tests.topics import define


@pytest.fixture
async def server(tmp_path):
    """A server with its console, both on real sockets."""
    # Two partitions each, so a handful of records lands in a known number of them.
    running = define(EventStreamServer(tmp_path / "log"),
                     Channel("pages.to_read", Delivery.COMPETING, partitions=2),
                     Channel("page.reads", Delivery.KEYED, partitions=2))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", admin_socket=tmp_path / "admin.sock",
                                                ready=ready))
    await ready.wait()
    yield running, tmp_path / "es.sock", tmp_path / "admin.sock"
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


async def _type(admin_socket, line):
    """What `echo "<line>" | nc -U <admin_socket>` prints."""
    reader, writer = await asyncio.open_unix_connection(str(admin_socket))
    writer.write(line.encode() + b"\n")
    await writer.drain()
    answer = await reader.read()
    writer.close()
    return answer.decode()


def _rows(table):
    """The data rows of one table, split the way `awk` would."""
    return [line.split() for line in table.splitlines() if line and not line.startswith("#")]


async def _member(socket, channel, group, *, role, keeps=True, partitions=2):
    client = FreeStreamClient(socket, role=role, name=f"{role}-under-test")
    await client.next(channel, group, wait=False)
    return client


# --------------------------------------------------------------------------- the console itself


async def test_help_lists_every_command_the_console_answers(server):
    _running, _socket, admin = server
    answer = await _type(admin, "help")
    for command in ("show info", "show topics", "show groups", "show consumers", "show backlog",
                    "show dead", "set consumer", "del group"):
        assert command in answer


async def test_several_commands_on_one_line_each_answer_and_end_in_a_blank_line(server):
    """HAProxy's `;` — and the blank line after each answer is how a script tells where one stops."""
    _running, _socket, admin = server
    answer = await _type(admin, "show info; nonsense here ;show backlog")
    parts = answer.split("\n\n")
    assert len(parts) == 4 and parts[-1] == ""
    assert "uptime" in parts[0]
    assert parts[1] == "unknown command: nonsense here — try 'help'"
    assert parts[2] == "no backlog"


async def test_show_consumers_says_who_each_connection_is(server):
    _running, socket, admin = server
    reader = await _member(socket, "pages.to_read", "page.readers", role="page-reader", keeps=False)
    try:
        rows = _rows(await _type(admin, "show consumers"))
        [row] = [row for row in rows if row[1] == "page-reader"]
        assert row[0] == str(reader.connection_id)
        assert row[3] == "ready" and row[5] == "pages.to_read" and row[6] == "page.readers"
        assert row[-1] == "page-reader-under-test"
    finally:
        await reader.close()


async def test_show_consumers_says_which_members_are_waiting_and_which_are_working(server):
    """An idle reader is one holding a NEXT the server has nothing for; a working one has a record."""
    _running, socket, admin = server
    producer = FreeStreamClient(socket, role="api")
    working = FreeStreamClient(socket, role="assembler")
    idle = FreeStreamClient(socket, role="page-reader")
    waiting = asyncio.create_task(idle.next("pages.to_read", "page.readers"))
    try:
        await producer.produce("page.reads", value=b"a read", key="doc-1")
        assert await working.next("page.reads", "page.assemblers", wait=False)
        expected = {"api": "-", "assembler": "working", "page-reader": "waiting"}

        async def shown() -> dict:
            return {row[1]: row[4] for row in _rows(await _type(admin, "show consumers"))}

        async def shown_as_expected() -> bool:
            return await shown() == expected

        await eventually(shown_as_expected, "the console never showed the reader waiting")
    finally:
        waiting.cancel()
        for client in (producer, working, idle):
            await client.close()


async def test_a_client_says_hello_again_after_a_reconnect(server):
    """To the server a reconnect is a stranger, so a client that had to reconnect introduces itself
    again — or `show consumers` would fill with `unknown` after every server restart."""
    running, socket, _admin = server
    client = FreeStreamClient(socket, role="assembler")
    try:
        await client.members("page.reads", "page.assemblers")
        first = client.connection_id
        await client._drop()
        await client.members("page.reads", "page.assemblers")
        assert client.connection_id != first
        assert [seen.role for seen in running.connections()
                if seen.connection_id == client.connection_id] == ["assembler"]
    finally:
        await client.close()


async def test_show_topics_and_backlog_read_the_log_as_it_is(server):
    _running, socket, admin = server
    producer = FreeStreamClient(socket, role="api")
    reader = await _member(socket, "pages.to_read", "page.readers", role="page-reader", keeps=False)
    try:
        placed = {(await producer.produce("pages.to_read", value=b"x" * 100,
                                          headers={"page": str(page)}))[0] for page in range(3)}
        topics = {row[0]: row for row in _rows(await _type(admin, "show topics"))}
        assert topics["pages.to_read"][1:4] == ["competing", "2", "3"]

        backlog = _rows(await _type(admin, "show backlog"))
        assert sum(int(row[0]) for row in backlog) == 3
        assert [int(row[0]) for row in backlog] == sorted((int(row[0]) for row in backlog),
                                                          reverse=True), "most behind is not first"

        groups = _rows(await _type(admin, "show groups pages.to_read"))
        assert groups == [["pages.to_read", "page.readers", "1", "1", "3", str(len(placed))]]
    finally:
        await producer.close()
        await reader.close()


async def test_show_dead_lists_what_was_given_up_on(server):
    _running, socket, admin = server
    producer = FreeStreamClient(socket)
    try:
        assert await _type(admin, "show dead") == "no dead letters\n\n"
        await producer.produce(dead_letter_channel("pages.to_read"), value=b"page",
                               headers={"dead.from_offset": "7", "dead.attempts": "3",
                                        "dead.group": "page.readers", "document": "abc", "page": "12"})
        answer = await _type(admin, "show dead pages.to_read")
        assert _rows(answer) == [["pages.to_read", "7", "3", "page.readers", "document=abc", "page=12"]]
        assert "1 dead letter(s) in all" in answer
        topics = {row[0]: row for row in _rows(await _type(admin, "show topics"))}
        assert dead_letter_channel("pages.to_read") not in topics
        assert topics["pages.to_read"][-1] == "1", "the dead letter is not counted against its topic"
    finally:
        await producer.close()


async def test_show_info_reports_the_broker(server):
    _running, _socket, admin = server
    fields = {row[0]: row[1:] for row in _rows(await _type(admin, "show info"))}
    assert {"pid", "uptime", "python", "topics", "connections", "bytes"} <= set(fields)


async def test_a_restarted_broker_shows_the_topics_already_defined(tmp_path):
    """A console on a server that has not been asked for a topic yet still shows it — an operator
    looking at a fresh restart needs to see the backlog it inherited."""
    first = EventStreamServer(tmp_path / "log")
    first.define_topic("page.reads", partitions=8, delivery="keyed")
    restarted = EventStreamServer(tmp_path / "log")
    assert [definition.name for definition in restarted.registry.all()
            if not definition.is_derived] == ["page.reads"]
    assert restarted.registry.get("page.reads").partitions == 8


# --------------------------------------------------------------------------- changing things


async def test_a_topic_is_created_and_removed_from_the_console(server):
    running, _socket, admin = server
    answer = await _type(admin, "create topic document.bakes 4 competing")
    assert answer.startswith("topic document.bakes is defined")
    assert running.registry.get("document.bakes").partitions == 4
    assert (await _type(admin, "create topic document.bakes 8 competing")).startswith("refused:")

    assert (await _type(admin, "del topic document.bakes")).startswith("topic document.bakes removed")
    assert "document.bakes" not in {row[0] for row in _rows(await _type(admin, "show topics"))}


async def test_show_topics_says_how_to_start_when_there_are_none(tmp_path):
    answer = AdminConsole(EventStreamServer(tmp_path / "log")).execute("show topics")
    assert "create topic" in answer


async def test_set_consumer_drain_is_seen_by_members_and_by_the_console(server):
    _running, socket, admin = server
    reader = await _member(socket, "pages.to_read", "page.readers", role="page-reader", keeps=False)
    asking = FreeStreamClient(socket)
    try:
        assert await asking.members("pages.to_read", "page.readers") == 1
        answer = await _type(admin, f"set consumer {reader.connection_id} state drain")
        assert answer.startswith(f"consumer {reader.connection_id} is drain")
        assert await asking.members("pages.to_read", "page.readers") == 0
        [row] = [row for row in _rows(await _type(admin, "show consumers")) if row[1] == "page-reader"]
        assert row[3] == "drain"
        [group] = _rows(await _type(admin, "show groups pages.to_read"))
        assert group[2:4] == ["1", "0"], "a drained member was counted as ready"

        await _type(admin, f"set consumer {reader.connection_id} state ready")
        assert await asking.members("pages.to_read", "page.readers") == 1
    finally:
        await reader.close()
        await asking.close()


async def test_a_membership_made_after_the_state_was_set_inherits_it(server):
    """An operator drains a process, and then it joins a second group — it is drained there too."""
    _running, socket, admin = server
    client = FreeStreamClient(socket, role="assembler")
    try:
        await client.members("page.reads", "page.assemblers")
        await _type(admin, f"set consumer {client.connection_id} state maint")
        await client.next("page.reads", "page.assemblers", wait=False)
        assert await client.members("page.reads", "page.assemblers") == 0
    finally:
        await client.close()


@pytest.mark.parametrize(("line", "refusal"), [
    ("set consumer 999 state drain", "refused: 999"),
    ("set consumer 1 state sideways", "refused: state must be one of ready, drain, maint"),
    ("set consumer 1 drain", "refused: usage: set consumer <id> state"),
    ("set consumer one state drain", "refused: invalid literal"),
    ("del group page.reads", "refused: usage: del group <topic> <group>"),
    ("del group page.reads nobody", "refused: 'page.reads/nobody'"),
    ("create topic ../escape 1 competing", "refused: '../escape' is not a topic name"),
    ("create topic ok 1", "refused: usage: create topic"),
    ("create topic ok many competing", "refused: invalid literal"),
    ("del topic", "refused: usage: del topic"),
    ("del topic nothing.here", "refused: no topic named"),
])
async def test_a_command_it_cannot_carry_out_is_refused_and_says_why(server, line, refusal):
    _running, _socket, admin = server
    assert (await _type(admin, line)).startswith(refusal)


async def test_a_group_with_live_members_is_not_deleted_and_a_retired_one_is(server):
    """A group is a promise somebody reads the channel, and retention holds segments for it. An
    operator deletes a retired group to let those segments go, and the server refuses while a member
    is connected, because the delete would reset that member's position and hand it everything again."""
    running, socket, admin = server
    member = await _member(socket, "page.reads", "retired", role="assembler")
    refused = await _type(admin, "del group page.reads retired")
    assert refused.startswith("refused:") and "live members" in refused
    await member.close()
    await asyncio.sleep(0.05)        # the server notices at the end of the connection

    assert (await _type(admin, "del group page.reads retired")).startswith("group page.reads/retired")
    assert not (running.root / "page.reads" / "groups" / "retired").exists()


async def test_a_console_path_the_kernel_cannot_hold_is_refused_at_the_bind(tmp_path):
    with pytest.raises(OSError, match="shorter"):
        await serve_admin(EventStreamServer(tmp_path / "log"), tmp_path / ("x" * 120))


async def test_a_console_connection_that_says_nothing_is_let_go(server, monkeypatch):
    monkeypatch.setattr(admin_module, "CONSOLE_IDLE_SECONDS", 0.05)
    _running, _socket, admin = server
    reader, writer = await asyncio.open_unix_connection(str(admin))
    try:
        assert await asyncio.wait_for(reader.read(), timeout=5) == b""
    finally:
        writer.close()


def test_sizes_read_the_way_a_person_reads_them():
    assert [admin_module._bytes(count) for count in (12, 2048, 5 * 1024 ** 2, 3 * 1024 ** 3)] == [
        "12B", "2.0KB", "5.0MB", "3.0GB"]


def test_the_table_lines_up_under_its_header():
    table = admin_module._table(("channel", "n"), [("page.reads", 8), ("x", 12)])
    header, *rows = table.splitlines()
    assert header.rindex("n") == rows[0].index("8") == rows[1].index("12")


def test_the_console_answers_without_a_socket_too(tmp_path):
    """The socket is transport; the console is a function of a line. A test, or a future HTTP status
    page, can ask it directly."""
    assert "no backlog" in AdminConsole(EventStreamServer(tmp_path / "log")).execute("show backlog")
