"""The server, and the frames it speaks (#450).

★ WHY A SERVER. The store began as a library — every producer opened the log and appended to it. That
holds while one process writes, and hop 2 is where it does not: twelve OCR consumers finish pages of
one document at once and the key routes every one of them to the SAME partition. Offsets assigned from
one process's memory would collide, and a 30 KB body can be split across write() calls and interleave.

Kafka, NATS and Redpanda do not have that problem because they are brokers. When we imitated their
semantics and kept the library shape, we kept the semantics and discarded what makes them hold
(2026-09-22). The test that matters here is the twelve-concurrent-publish one.
"""
import asyncio
import contextlib
import os
import time

import pytest

from freestream import consumer_group as consumer_group_module
from freestream import server as server_module
from freestream import strand as strand_module
from freestream.client import EventStreamUnavailableError, FreeStreamClient, Output
from freestream.consumer_group import CommitPlan, ConsumerGroup
from freestream.journal import JournalEntry, JournalOutput, pending_entries, write_entry
from freestream.protocol import (
    MAX_FRAME_BYTES,
    PREFIX_BYTES,
    Frame,
    ProtocolError,
    read_prefix,
)
from freestream.record import Record
from freestream.registry import PartitionNotHeldError, RequestRefusedError, UnknownTopicError
from freestream.sealing import Seal
from freestream.server import EventStreamServer
from freestream.topic import Topic
from tests.eventually import eventually

# --------------------------------------------------------------------------- frames


def test_a_frame_survives_the_wire_unchanged():
    original = Frame(header={"op": "produce", "channel": "ocr.pages"}, body=b"%PDF-body")
    raw = original.encoded()
    assert Frame.decoded(raw[:PREFIX_BYTES], raw[PREFIX_BYTES:]) == original


def test_a_body_may_hold_arbitrary_bytes():
    """A page is a PDF. Length-prefixed rather than delimited, so nothing has to be escaped —
    and never base64, which would cost a third of every message to put bytes in a text field."""
    awkward = bytes(range(256)) + b"\x00\x00" + b'}{"not": json'
    raw = Frame(header={"op": "produce"}, body=awkward).encoded()
    assert Frame.decoded(raw[:PREFIX_BYTES], raw[PREFIX_BYTES:]).body == awkward


def test_a_frame_with_no_body_is_ordinary():
    raw = Frame(header={"op": "commit", "partition": 0, "next_offset": 7}).encoded()
    assert Frame.decoded(raw[:PREFIX_BYTES], raw[PREFIX_BYTES:]).body == b""


def test_a_prefix_claiming_more_than_the_ceiling_is_refused_before_anything_is_allocated():
    """★ A malformed length must not make the server try to reserve gigabytes on one bad read."""
    import struct
    with pytest.raises(ProtocolError, match="over the ceiling"):
        read_prefix(struct.pack("<II", 16, MAX_FRAME_BYTES))


def test_a_frame_with_no_header_says_nothing_about_itself():
    import struct
    with pytest.raises(ProtocolError, match="says nothing"):
        read_prefix(struct.pack("<II", 0, 10))


def test_a_header_that_is_not_an_object_is_refused():
    import struct
    raw = struct.pack("<II", 2, 0) + b"[]"
    with pytest.raises(ProtocolError, match="must be an object"):
        Frame.decoded(raw[:PREFIX_BYTES], raw[PREFIX_BYTES:])


def test_a_truncated_frame_is_refused_rather_than_half_read():
    raw = Frame(header={"op": "produce"}, body=b"a body").encoded()
    with pytest.raises(ProtocolError, match="expected"):
        Frame.decoded(raw[:PREFIX_BYTES], raw[PREFIX_BYTES:-2])


# --------------------------------------------------------------------------- the server

#: The topics these tests use. A topic must be defined before anyone produces to it or reads it.
_TOPICS = (("page.reads", 4, "keyed"), ("narrow", 1, "competing"), ("pair", 2, "competing"),
           ("wide.work", 16, "competing"), ("cockpit", 1, "broadcast"), ("shrinking", 1, "competing"),
           ("held", 1, "competing"), ("to.read", 4, "competing"),
           ("work", 1, "competing"), ("one.keyed", 1, "keyed"))


def _defined(running):
    for name, partitions, delivery in _TOPICS:
        running.define_topic(name, partitions=partitions, delivery=delivery)
    return running


@pytest.fixture
async def server(tmp_path):
    """A server on its own socket, torn down with the test."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    yield tmp_path / "es.sock"
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


@pytest.fixture
async def served(tmp_path):
    """A server on its own socket, and the server itself, for a test that reaches into it."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    yield running, tmp_path / "es.sock"
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


async def _call(socket_path, frame):
    reader, writer = await asyncio.open_unix_connection(str(socket_path))
    try:
        writer.write(frame.encoded())
        await writer.drain()
        prefix = await reader.readexactly(PREFIX_BYTES)
        header_len, body_len = read_prefix(prefix)
        rest = await reader.readexactly(header_len + body_len)
        return Frame.decoded(prefix, rest)
    finally:
        writer.close()


def _produce(key=None, body=b"a page", **header):
    return Frame(header={"op": "produce", "channel": "page.reads", "key": key, **header}, body=body)


async def test_twelve_concurrent_publishes_to_one_key_get_twelve_distinct_offsets(server):
    """★ THE WHOLE REASON THE SERVER EXISTS, and the case hop 2 is made of.

    Twelve OCR consumers finish pages of one document at the same moment. The key sends every one of
    them to the same partition, and they all land — in one order, with no offset used twice and none
    skipped. No lock anywhere: there is one writer, so there is nothing to serialize.
    """
    answers = await asyncio.gather(*[
        _call(server, _produce(key="one-document", body=f"page {n}".encode())) for n in range(12)])

    assert {frame.header["partition"] for frame in answers} == {1} or len(
        {frame.header["partition"] for frame in answers}) == 1, "one key, one partition"
    offsets = sorted(frame.header["offset"] for frame in answers)
    assert offsets == list(range(12)), "distinct, gapless, and nobody's write was lost"


def _next(group, **header):
    return Frame(header={"op": "next", "channel": "page.reads", "group": group, "wait": False,
                         "id": 7, **header})


async def _take_all(client, channel, group, **declared):
    """Every record a consumer can be handed now, committing each as a handler would."""
    taken = []
    while handed := await client.next(channel, group, wait=False, **declared):
        partition, record = handed
        taken.append(record)
        await client.commit(channel, group, partition, record.offset + 1)
    return taken


async def test_what_was_published_is_what_comes_back(server):
    await _call(server, _produce(key="doc-1", body=b"%PDF-\x00 bytes", headers={"page": "6"}))

    answer = await _call(server, _next("aggregators"))

    assert answer.header["re"] == 7, "the answer does not name the request it answers"
    record, _end = Record.unframed(answer.body)
    assert record.value == b"%PDF-\x00 bytes"
    assert record.headers == {"page": "6"}
    assert record.key == b"doc-1"


async def test_records_come_one_at_a_time_in_order_as_each_is_committed(server):
    for page in range(3):
        await _call(server, _produce(key="doc-1", body=f"page {page}".encode()))
    client = FreeStreamClient(server)
    try:
        assert [record.value for record in await _take_all(client, "page.reads", "aggregators")] == [
            b"page 0", b"page 1", b"page 2"]
    finally:
        await client.close()


async def test_a_competing_channel_is_read_oldest_first_across_every_strand(tmp_path):
    """★ EVERY FOURTH PAGE WAS READ LATE. Each strand owns every Nth partition of a topic, so a return's
    pages are spread over all of them. A consumer went back to the strand it last read from and stayed
    there, so a strand with one reader fewer fell behind, and one page in four of a 286-page return
    was read long after its neighbors (2026-09-28). The strand with the oldest record is asked first."""
    running = _defined(EventStreamServer(tmp_path / "log", strands=4))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    producer, reader = FreeStreamClient(tmp_path / "es.sock"), FreeStreamClient(tmp_path / "es.sock")
    try:
        for page in range(16):
            await producer.produce("wide.work", value=f"page {page:02d}".encode())
        read = [record.value for record in await _take_all(reader, "wide.work", "readers")]
    finally:
        await producer.close()
        await reader.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)

    assert read == [f"page {page:02d}".encode() for page in range(16)]


async def test_a_record_not_committed_is_the_next_one_again(server):
    """How a failed handler's record comes back: nothing is remembered about the hand-over, the group's
    committed position simply has not moved."""
    await _call(server, _produce(key="doc-1", body=b"page 0"))
    client = FreeStreamClient(server)
    try:
        _p, first = await client.next("page.reads", "aggregators", wait=False)
        _p, again = await client.next("page.reads", "aggregators", wait=False)
        assert again.offset == first.offset
    finally:
        await client.close()


async def test_a_committed_group_does_not_see_what_it_has_handled(server):
    """One CLIENT throughout, because a membership belongs to a connection: the partitions a consumer
    holds are held by the connection that took them, and committing on a second connection is a
    different consumer committing somebody else's offset, which is refused."""
    for page in range(3):
        await _call(server, _produce(key="doc-1", body=str(page).encode()))

    consumer = FreeStreamClient(server)
    try:
        for _ in range(3):
            partition, record = await consumer.next("page.reads", "aggregators", wait=False)
            await consumer.commit("page.reads", "aggregators", partition, record.offset + 1)

        assert await consumer.next("page.reads", "aggregators", wait=False) is None
    finally:
        await consumer.close()


async def test_a_commit_past_what_was_handed_is_refused_and_says_why(server):
    """★ FROM THE ADVERSARIAL REVIEW (2026-09-25). A commit used to be written wherever it pointed, so a
    consumer handed offset 0 could commit 3 and records 1 and 2 were skipped for good — and retention
    could then delete them. It is answered with the reason, and the group has not moved."""
    for page in range(3):
        await _call(server, _produce(key="doc-1", body=str(page).encode()))
    consumer = FreeStreamClient(server)
    try:
        partition, record = await consumer.next("page.reads", "aggregators", wait=False)
        with pytest.raises(RequestRefusedError, match="passes what this consumer was handed"):
            await consumer.commit("page.reads", "aggregators", partition, 3)
        await consumer.commit("page.reads", "aggregators", partition, record.offset + 1)
        again, following = await consumer.next("page.reads", "aggregators", wait=False)
        assert (again, following.offset) == (partition, 1)
    finally:
        await consumer.close()


async def test_a_commit_that_would_move_the_group_back_is_refused(server):
    for page in range(2):
        await _call(server, _produce(key="doc-1", body=str(page).encode()))
    consumer = FreeStreamClient(server)
    try:
        for _ in range(2):
            partition, record = await consumer.next("page.reads", "aggregators", wait=False)
            await consumer.commit("page.reads", "aggregators", partition, record.offset + 1)
        with pytest.raises(RequestRefusedError, match="move the group back"):
            await consumer.commit("page.reads", "aggregators", partition, 1)
    finally:
        await consumer.close()


async def test_two_groups_each_read_everything(server):
    await _call(server, _produce(key="doc-1"))
    first = await _call(server, _next("aggregators"))
    second = await _call(server, _next("auditors"))
    assert "partition" in first.header and "partition" in second.header


async def test_an_unknown_operation_drops_the_connection_rather_than_guessing(server):
    """A client and a server that disagree cannot resynchronise — the next body would belong to the
    last request. Dropped, so the client finds out immediately."""
    reader, writer = await asyncio.open_unix_connection(str(server))
    writer.write(Frame(header={"op": "invent"}).encoded())
    await writer.drain()
    with pytest.raises(asyncio.IncompleteReadError):
        await reader.readexactly(PREFIX_BYTES)
    writer.close()


async def test_a_client_that_leaves_mid_frame_is_not_a_failure(server):
    """Ordinary: a worker is killed, or a deploy restarts it. The server keeps serving everyone else."""
    _reader, writer = await asyncio.open_unix_connection(str(server))
    writer.write(Frame(header={"op": "produce", "channel": "page.reads"},
                       body=b"a body").encoded()[:6])   # half a prefix, then go
    await writer.drain()
    writer.close()

    still_serving = await _call(server, _produce(key="doc-2"))
    assert "offset" in still_serving.header


# ------------------------------------------------- a group separates the CONSUMERS reading it


async def test_two_clients_of_one_group_never_both_get_the_same_record(server):
    """★ THE 8x BUG, AND THE ONE TEST THAT WOULD HAVE CAUGHT IT.

    The server held ONE membership per (channel, group) for its whole life, so every client reading
    that group shared it and every client was handed the same uncommitted records. Eight OCR readers
    therefore OCR'd all 286 pages EACH — 42 consumed pages came back as 336 page reads before the
    factor of eight told me what it was.

    A consumer group separates consumers, and a consumer is a connection. This drives two real
    connections because one client cannot show it: the whole defect lived in what the server shared
    between two of them.
    """
    for page in range(8):
        await _call(server, _produce(body=f"page {page}".encode(), channel="to.read"))

    first = FreeStreamClient(server)
    second = FreeStreamClient(server)
    by_first, by_second = [], []
    try:
        # Each takes one and holds it uncommitted, as a reader does while it reads the page, and then
        # each takes the rest in turn.
        held_by_first = await first.next("to.read", "readers", wait=False)
        held_by_second = await second.next("to.read", "readers",
                                           wait=False)
        for client, held, into in ((first, held_by_first, by_first),
                                   (second, held_by_second, by_second)):
            into.append(held[1])
            await client.commit("to.read", "readers", held[0], held[1].offset + 1)
        by_first += await _take_all(first, "to.read", "readers")
        by_second += await _take_all(second, "to.read", "readers")
    finally:
        await first.close()
        await second.close()

    together = [record.value for record in by_first + by_second]
    assert len(together) == len(set(together)), "a record reached two consumers of one group"
    assert by_first and by_second, "the work went to one of them and none to the other"


async def test_a_consumer_that_disconnects_hands_its_partitions_to_the_next(server):
    """A group must outlive its members. What one connection held is taken by the next one along —
    the same liveness story the flock gives us between processes, inside one server."""
    for page in range(4):
        await _call(server, _produce(body=f"page {page}".encode(), channel="to.read"))

    leaving = FreeStreamClient(server)
    held = await leaving.next("to.read", "readers", wait=False)
    assert held
    await leaving.close()
    await asyncio.sleep(0.05)      # the server notices at the end of the connection, not before

    arriving = FreeStreamClient(server)
    try:
        assert await arriving.next("to.read", "readers", wait=False), (
            "the departed consumer's partitions were never released")
    finally:
        await arriving.close()


async def test_a_topic_spreads_over_exactly_the_width_it_was_defined_with(server):
    """★ THE DEFINITION DECIDES. The width used to ride on every request, and the first request to name a
    topic fixed it; `cockpit` saying 1 and `ocr.pages` saying 8 once came out identical."""
    narrow = FreeStreamClient(server)
    wide = FreeStreamClient(server)
    try:
        await narrow.produce("cockpit", value=b"a nudge")
        await wide.produce("wide.work", value=b"work")

        landed = set()
        for number in range(40):
            partition, _offset = await wide.produce(
                "wide.work", value=str(number).encode())
            landed.add(partition)
        assert max(landed) > 7 and max(landed) < 16, "the server ignored the defined width"

        narrow_landed = set()
        for number in range(10):
            partition, _offset = await narrow.produce(
                "cockpit", value=str(number).encode())
            narrow_landed.add(partition)
        assert narrow_landed == {0}, "a one-partition channel spread over more than one"
    finally:
        await narrow.close()
        await wide.close()


async def test_a_partition_held_by_a_working_consumer_is_not_handed_to_a_second(server):
    holder = FreeStreamClient(server)
    second = FreeStreamClient(server)
    try:
        await holder.produce("narrow", value=b"work")
        assert await holder.next("narrow", "workers",
                                 wait=False)
        assert await second.next("narrow", "workers",
                                 wait=False) is None
    finally:
        await holder.close()
        await second.close()


# --------------------------------------------------------------------------- awaiting


async def test_a_consumer_awaits_its_next_record_and_is_answered_when_one_is_produced(server):
    """★ LEE'S MODEL: a consumer awaits, as a process blocked in `read()` does. The NEXT goes out once,
    and its answer comes back when there is something to answer with."""
    consumer, producer = FreeStreamClient(server), FreeStreamClient(server)
    try:
        waiting = asyncio.create_task(consumer.next("page.reads", "aggregators"))
        await asyncio.sleep(0.05)
        assert not waiting.done(), "a NEXT with nothing to hand over was answered anyway"

        await producer.produce("page.reads", value=b"at last", key="doc-1")

        _partition, record = await asyncio.wait_for(waiting, timeout=2)
        assert record.value == b"at last"
    finally:
        await consumer.close()
        await producer.close()


async def test_an_idle_consumer_asks_the_broker_nothing(tmp_path):
    """The number `show consumers` put in front of us: an idle reader made about 21 requests a second,
    every one answered empty. A consumer that waits now makes one request, however long it waits."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    consumer = FreeStreamClient(tmp_path / "es.sock")
    try:
        waiting = asyncio.create_task(consumer.next("page.reads", "aggregators"))
        await asyncio.sleep(0.05)
        [seen] = [seen for seen in running.connections()
                  if seen.connection_id == consumer.connection_id]
        before = seen.requests
        await asyncio.sleep(0.3)
        assert seen.requests == before
        waiting.cancel()
    finally:
        await consumer.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_one_connection_carries_other_requests_while_its_next_is_held(server):
    """★ WHY EVERY REQUEST CARRIES AN ID. The assembler closes a key from its assembly task while its
    consumer waits on the same connection — and a membership IS a connection, so there is no second
    socket to send it on. Here the waiting client produces the very record it is waiting for: both
    requests are answered, each with its own answer."""
    client = FreeStreamClient(server)
    try:
        waiting = asyncio.create_task(client.next("page.reads", "aggregators"))
        await asyncio.sleep(0.05)
        partition, offset = await client.produce("page.reads", value=b"mine", key="doc-1")
        handed_partition, record = await asyncio.wait_for(waiting, timeout=2)
        assert (handed_partition, record.offset, record.value) == (partition, offset, b"mine")
    finally:
        await client.close()


async def test_a_waiting_consumer_is_handed_what_a_departed_one_held(server):
    """A partition held by a consumer that leaves with a record uncommitted goes to whoever is waiting —
    without that consumer asking again, because it already asked."""
    holder, waiter = FreeStreamClient(server), FreeStreamClient(server)
    try:
        await holder.produce("narrow", value=b"left behind")
        assert await holder.next("narrow", "workers", wait=False)
        waiting = asyncio.create_task(waiter.next("narrow", "workers"))
        await asyncio.sleep(0.05)
        assert not waiting.done()

        await holder.close()

        _partition, record = await asyncio.wait_for(waiting, timeout=2)
        assert record.value == b"left behind"
    finally:
        await waiter.close()


async def test_a_drained_member_closing_its_last_key_hands_the_partition_to_one_already_waiting(tmp_path):
    """★ WHY THE SERVER OFFERS IN PASSES. B has been waiting since before A was drained, so B is asked
    first — while A still holds the partition. A lets go only when it is asked, later in the same pass,
    and B would sit there with a record it could now take unless the server goes round again."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    socket = tmp_path / "es.sock"
    draining, waiting, producer = (FreeStreamClient(socket) for _ in range(3))
    try:
        await producer.produce("one.keyed", value=b"doc-1 page 1", key="doc-1")
        partition, record = await draining.next("one.keyed", "assemblers", wait=False)
        await draining.commit("one.keyed", "assemblers", partition, record.offset + 1)

        b_waits = asyncio.create_task(waiting.next("one.keyed", "assemblers"))
        await asyncio.sleep(0.05)
        running.set_state(draining.connection_id, "drain")
        a_waits = asyncio.create_task(draining.next("one.keyed", "assemblers"))
        await asyncio.sleep(0.05)
        await producer.produce("one.keyed", value=b"doc-2 page 1", key="doc-2")
        await asyncio.sleep(0.05)
        assert not b_waits.done() and not a_waits.done(), "doc-2 went somewhere while doc-1 was open"

        await draining.close_key("one.keyed", "assemblers", "doc-1")

        _partition, handed = await asyncio.wait_for(b_waits, timeout=2)
        assert handed.value == b"doc-2 page 1"
        a_waits.cancel()
    finally:
        for client in (draining, waiting, producer):
            await client.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_a_member_set_ready_again_is_handed_what_arrived_while_it_was_out(tmp_path):
    """A member in maintenance keeps its NEXT held and is handed nothing. Set back to ready, it is
    answered from what arrived meanwhile — without asking again, because it already asked."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    consumer, producer = FreeStreamClient(tmp_path / "es.sock"), FreeStreamClient(tmp_path / "es.sock")
    try:
        await consumer.members("work", "workers")
        running.set_state(consumer.connection_id, "maint")
        waiting = asyncio.create_task(consumer.next("work", "workers"))
        await producer.produce("work", value=b"arrived meanwhile")
        await asyncio.sleep(0.05)
        assert not waiting.done(), "a member in maintenance was handed a record"

        running.set_state(consumer.connection_id, "ready")

        _partition, record = await asyncio.wait_for(waiting, timeout=2)
        assert record.value == b"arrived meanwhile"
    finally:
        await consumer.close()
        await producer.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


async def test_a_waiting_consumer_that_leaves_takes_its_wait_with_it(server):
    """Its held NEXT goes with the connection, so the next produce on that channel is not offered to a
    member that no longer exists."""
    leaving, producer = FreeStreamClient(server), FreeStreamClient(server)
    try:
        waiting = asyncio.create_task(leaving.next("one.keyed", "aggregators"))
        await asyncio.sleep(0.05)
        await leaving.close()
        with contextlib.suppress(EventStreamUnavailableError):
            await waiting
        await asyncio.sleep(0.05)

        assert await producer.produce("one.keyed", value=b"after", key="doc-1")
    finally:
        await producer.close()


async def test_a_second_next_while_the_first_is_held_drops_the_connection(server):
    """A consumer handles a record before it asks for another. Two held at once is a client that has
    lost track, and the server cannot tell which answer it wants."""
    reader, writer = await asyncio.open_unix_connection(str(server))
    for request_id in (1, 2):
        writer.write(Frame(header={"op": "next", "channel": "page.reads", "group": "g",
                                   "id": request_id}).encoded())
    await writer.drain()
    with pytest.raises(asyncio.IncompleteReadError):
        await reader.readexactly(PREFIX_BYTES)
    writer.close()


async def test_a_canceled_next_leaves_its_record_for_the_next_ask(served):
    """A consumer stopped while its NEXT is held may be answered after it stopped listening. The record
    was never committed, so asking again is handed the same one — nothing is lost."""
    running, server = served
    client = FreeStreamClient(server)

    def held() -> bool:
        return any(member[0] == client.connection_id and member[1] == "page.reads"
                   for strand in running.strands for member in strand._pending)

    try:
        waiting = asyncio.create_task(client.next("page.reads", "aggregators"))
        await eventually(held, "the NEXT was never held")
        waiting.cancel()
        await client.produce("page.reads", value=b"kept", key="doc-1")
        # The held NEXT is answered with the record before this client may ask again.
        await eventually(lambda: not held(), "the held NEXT was never answered")
        _partition, record = await client.next("page.reads", "aggregators", wait=False)
        assert record.value == b"kept"
    finally:
        await client.close()


async def test_a_closed_client_does_not_reconnect_for_a_next_still_held(server):
    client = FreeStreamClient(server)
    waiting = asyncio.create_task(client.next("page.reads", "aggregators"))
    await asyncio.sleep(0.05)
    await client.close()
    with pytest.raises(EventStreamUnavailableError):
        await waiting



# --------------------------------------------------------------------------- retention


async def test_a_segment_every_group_has_read_past_is_deleted_on_commit(server, monkeypatch):
    """★ THE LOG SHRINKS. It held every page of every return it ever carried — 19 MB for one 286-page return.
    A commit is the only moment that can make a segment deletable, so that is where it is checked."""
    from freestream import partition_log
    monkeypatch.setattr(partition_log, "SEGMENT_BYTES", 300)
    monkeypatch.setattr(strand_module, "RETENTION_CHECK_SECONDS", 0.0)

    client = FreeStreamClient(server)
    try:
        for n in range(20):
            await client.produce("shrinking", value=b"v" * 80 + str(n).encode())
        segments = server.parent / "log" / "shrinking" / "0000"
        before = len(list(segments.glob("*.log")))
        assert before > 2, "the test needs a log that rolled"

        await _take_all(client, "shrinking", "only-group")

        assert len(list(segments.glob("*.log"))) < before
    finally:
        await client.close()


async def test_a_group_that_has_not_caught_up_holds_the_segments_it_still_needs(server, monkeypatch):
    from freestream import partition_log
    monkeypatch.setattr(partition_log, "SEGMENT_BYTES", 300)
    monkeypatch.setattr(strand_module, "RETENTION_CHECK_SECONDS", 0.0)

    ahead, behind = FreeStreamClient(server), FreeStreamClient(server)
    try:
        for n in range(20):
            await ahead.produce("held", value=b"w" * 80 + str(n).encode())
        await behind.next("held", "slow-group", wait=False)   # joins, commits nothing
        segments = server.parent / "log" / "held" / "0000"
        before = len(list(segments.glob("*.log")))

        await _take_all(ahead, "held", "fast-group")

        assert len(list(segments.glob("*.log"))) == before, "a slower group's records were deleted"
    finally:
        await ahead.close()
        await behind.close()



async def test_a_group_with_more_members_than_partitions_is_reported_once(server, caplog):
    """★ STARVATION IS A SERVER FACT. Member N+1 against N partitions can never be handed anything, and
    only the server sees every member of a group — an idle consumer holding nothing is ordinary."""
    import logging

    clients = [FreeStreamClient(server) for _ in range(4)]
    try:
        with caplog.at_level(logging.WARNING):
            for client in clients:
                await client.next("pair", "workers", wait=False)
        warnings = [r for r in caplog.records if "will never be handed a record" in r.getMessage()]
        assert len(warnings) == 1, "said once, not once per extra member"
        assert "3 members" in warnings[0].getMessage()
    finally:
        for client in clients:
            await client.close()


async def test_a_broker_stops_promptly_with_clients_still_connected(tmp_path):
    """★ IT WAITED ON THEM FOR EVER. A connected client sits in a read waiting for its next answer, and
    `Server.wait_closed` waits for every connection to end — `serve_forever` does so before it even
    re-raises the cancel. So Ctrl-C on a dev server with readers attached hung, and so did any test that
    tore the server down before its client. The server closes their sockets itself now."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    client = FreeStreamClient(tmp_path / "es.sock")
    try:
        await client.produce("work", value=b"a page")
        serving.cancel()
        [outcome] = await asyncio.wait_for(asyncio.gather(serving, return_exceptions=True), timeout=2)
        assert isinstance(outcome, asyncio.CancelledError)
    finally:
        await client.close()


# --------------------------------------------------------------------------- defined topics


async def test_a_topic_nobody_defined_is_refused_and_the_connection_carries_on(server):
    """★ A TYPO NO LONGER WAITS FOREVER. A consumer of `pages.to_raed` used to create that topic and sit
    on it. It is told now — and told as an answer, so the same connection goes on working."""
    client = FreeStreamClient(server)
    try:
        with pytest.raises(UnknownTopicError, match=r"pages\.to_raed"):
            await client.next("pages.to_raed", "readers", wait=False)
        with pytest.raises(UnknownTopicError):
            await client.produce("pages.to_raed", value=b"x")
        assert await client.produce("work", value=b"still fine")
    finally:
        await client.close()


async def test_a_consumer_of_a_defined_topic_waits_with_no_producer_in_sight(server):
    """My case: the topic is known, so a consumer that arrives first comes online and waits."""
    client = FreeStreamClient(server)
    try:
        waiting = asyncio.create_task(client.next("work", "workers"))
        await asyncio.sleep(0.05)
        assert not waiting.done()
        waiting.cancel()
    finally:
        await client.close()


async def test_a_dead_letter_topic_exists_for_every_defined_topic(server):
    client = FreeStreamClient(server)
    try:
        assert await client.produce("work.dead", value=b"given up on")
        with pytest.raises(UnknownTopicError):
            await client.produce("nothing.dead", value=b"x")
    finally:
        await client.close()


# ------------------------------------------------------------- the topic's contract, held at the server


async def test_a_keyless_record_on_a_keyed_topic_is_refused_at_the_server(server):
    """★ FROM THE ADVERSARIAL REVIEW (2026-09-25). `Producer` refused it, but the client beneath it and
    anything speaking the protocol directly did not, and a document's pages scattered across partitions."""
    client = FreeStreamClient(server)
    try:
        with pytest.raises(RequestRefusedError, match="is keyed and this record has no key"):
            await client.produce("page.reads", value=b"a page")
        assert await client.produce("page.reads", value=b"a page", key="doc-1")
    finally:
        await client.close()


async def test_a_keyed_record_on_a_competing_topic_is_refused_at_the_server(server):
    client = FreeStreamClient(server)
    try:
        with pytest.raises(RequestRefusedError, match="is competing and this record has a key"):
            await client.produce("work", value=b"work", key="doc-1")
    finally:
        await client.close()


async def test_a_record_too_large_to_deliver_is_refused_before_it_is_written(server, monkeypatch):
    """A record accepted near the frame limit could not fit in the answer that delivers it, and would
    have blocked its partition for good. The limit here is shrunk so the test does not send 64MB."""
    monkeypatch.setattr(server_module, "MAX_FRAME_BYTES", 8192)
    client = FreeStreamClient(server)
    try:
        with pytest.raises(RequestRefusedError, match="too large to be delivered"):
            await client.produce("work", value=b"x" * 4096)
        assert await client.next("work", "readers", wait=False) is None
        assert await client.produce("work", value=b"x" * 1024)
    finally:
        await client.close()


async def test_a_group_name_that_is_not_a_name_is_refused(server):
    """★ A GROUP NAME BECOMES A DIRECTORY, so `../..` in one was a path out of the log's root."""
    client = FreeStreamClient(server)
    try:
        with pytest.raises(RequestRefusedError, match="is not a group name"):
            await client.next("work", "../../elsewhere", wait=False)
        assert await client.produce("work", value=b"still fine")
    finally:
        await client.close()


def test_deleting_a_group_by_a_name_that_is_not_a_name_is_refused(tmp_path):
    running = _defined(EventStreamServer(tmp_path / "log"))
    (tmp_path / "outside").mkdir()
    with pytest.raises(ValueError, match="is not a topic and group name"):
        running.delete_group("work", "../../../outside")
    assert (tmp_path / "outside").is_dir()


async def test_a_client_that_stalls_inside_a_frame_is_dropped(server, monkeypatch):
    """A client that sends a prefix and never the body used to hold its connection for good. One that
    sits BETWEEN frames is idle by design and is left alone."""
    monkeypatch.setattr(strand_module, "FRAME_BODY_SECONDS", 0.1)
    reader, writer = await asyncio.open_unix_connection(str(server))
    try:
        writer.write(_produce(key="doc-1").encoded()[:PREFIX_BYTES + 3])
        await writer.drain()
        async with asyncio.timeout(2):
            assert await reader.read() == b"", "the server kept a half-sent frame open"
    finally:
        writer.close()


# ------------------------------------------------------------- a resent produce is written once


async def test_a_produce_resent_with_the_same_sequence_is_written_once(server):
    """The client resends the same header after a drop; here the resend is sent by hand, twice."""
    frame = Frame(header={"op": "produce", "channel": "work", "key": None, "headers": {},
                          "producer": "producer-1", "sequence": 0}, body=b"work")
    first, again = await _call(server, frame), await _call(server, frame)
    assert (again.header["partition"], again.header["offset"]) == (
        first.header["partition"], first.header["offset"])
    reader = FreeStreamClient(server)
    try:
        partition, record = await reader.next("work", "readers", wait=False)
        await reader.commit("work", "readers", partition, record.offset + 1)
        assert await reader.next("work", "readers", wait=False) is None
    finally:
        await reader.close()


async def test_a_record_carries_no_stream_header_a_processor_copied_forward(server):
    """A page reader copies its input's headers into its output; the input's producer identity must not
    ride along, or the output would be taken for a resend of the input. Read back raw, past the client
    that hides the stream's headers, the record carries its own producer's identity and nobody else's."""
    client = FreeStreamClient(server)
    try:
        await client.produce("work", value=b"output",
                             headers={"page": "3", "stream.producer": "someone-else",
                                      "stream.sequence": "0"})
    finally:
        await client.close()
    answer = await _call(server, Frame(header={"op": "next", "channel": "work", "group": "raw",
                                               "wait": False}))
    stored, _end = Record.unframed(answer.body)
    assert stored.headers["page"] == "3"
    assert stored.headers["stream.producer"] not in ("someone-else", None)
    assert stored.headers["stream.sequence"] == "0"

    reader = FreeStreamClient(server)
    try:
        _partition, delivered = await reader.next("work", "readers", wait=False)
        assert delivered.headers == {"page": "3"}, "a handler saw the stream's own headers"
    finally:
        await reader.close()


# ------------------------------------------------------------- COMPLETE: outputs and commit, or nothing


def _complete_frame(partition, next_offset, outputs, *, channel="to.read", group="readers"):
    return Frame(header={"op": "complete", "channel": channel, "group": group,
                         "partition": partition, "next_offset": next_offset,
                         "outputs": [{"channel": out_channel, "key": key, "headers": {},
                                      "length": len(value)} for out_channel, key, value in outputs]},
                 body=b"".join(value for _channel, _key, value in outputs))


async def _everything_in(client, channel, group="inspector"):
    taken = []
    while handed := await client.next(channel, group, wait=False):
        taken.append(handed[1].value)
        await client.commit(channel, group, handed[0], handed[1].offset + 1)
    return taken


async def test_a_complete_writes_its_outputs_and_commits_its_input(server):
    client = FreeStreamClient(server)
    try:
        await client.produce("to.read", value=b"a page")
        partition, record = await client.next("to.read", "readers", wait=False)
        await client.complete("to.read", "readers", partition, record.offset + 1, [
            Output("page.reads", b"read", key="doc-1"), Output("work", b"work")])
        assert await client.next("to.read", "readers", wait=False) is None
        assert await _everything_in(client, "page.reads") == [b"read"]
        assert await _everything_in(client, "work") == [b"work"]
    finally:
        await client.close()


async def test_a_complete_from_a_member_that_lost_its_partition_writes_nothing(server):
    """★ THE FENCE. A consumer that reconnected holds nothing; its COMPLETE is refused before anything is
    written, so the outputs of a handler whose record went to someone else never reach the log."""
    client = FreeStreamClient(server)
    try:
        await client.produce("to.read", value=b"a page")
        partition, record = await client.next("to.read", "readers", wait=False)
        await client._drop()
        with pytest.raises(PartitionNotHeldError):
            await client.complete("to.read", "readers", partition, record.offset + 1,
                                  [Output("work", b"never")])
        assert await _everything_in(client, "work") == []
    finally:
        await client.close()


async def test_a_complete_with_one_refused_output_writes_none_of_them(server):
    client = FreeStreamClient(server)
    try:
        await client.produce("to.read", value=b"a page")
        partition, record = await client.next("to.read", "readers", wait=False)
        with pytest.raises(RequestRefusedError, match="is keyed and this record has no key"):
            await client.complete("to.read", "readers", partition, record.offset + 1,
                                  [Output("work", b"fine"), Output("page.reads", b"no key")])
        assert await _everything_in(client, "work") == []
        _partition, same = await client.next("to.read", "readers", wait=False)
        assert same.offset == record.offset, "the input was committed although the COMPLETE was refused"
    finally:
        await client.close()


def test_a_journal_entry_a_killed_server_left_is_finished_exactly_once(tmp_path):
    """★ KILLED PARTWAY THROUGH APPLYING. The first output was written and the second was not, and the
    commit was not. The server that starts next finishes the entry: the first output is recognized in
    the log and not written again, the second is written, and the group moves past the input."""
    key = os.urandom(32)
    running = _defined(EventStreamServer(tmp_path / "log", key=key))
    running.close()
    seal = Seal(key)
    Topic(tmp_path / "log" / "to.read", partitions=4, seal=seal).produce_to(0, Record(value=b"a page"))
    plan = CommitPlan(partition=0, position=1, acked=frozenset())
    entry = JournalEntry.new("to.read", "readers", plan, (
        JournalOutput("work", Record(value=b"first")), JournalOutput("work", Record(value=b"second"))))
    write_entry(tmp_path / "log", entry, seal)
    Topic(tmp_path / "log" / "work", partitions=1, seal=seal).produce_once(entry.outputs[0].record,
                                                                            entry.producer, 0)
    consumer_group_module.forget_positions(tmp_path / "log" / "to.read" / "groups" / "readers")

    restarted = EventStreamServer(tmp_path / "log", key=key)
    restarted.close()
    work = Topic(tmp_path / "log" / "work", partitions=1, seal=seal)
    assert [record.value for record in work.log(0).read_from(0)] == [b"first", b"second"]
    home = tmp_path / "log" / "to.read" / "groups" / "readers"
    assert (home / "0000.offset").read_text() == "1"
    assert pending_entries(tmp_path / "log", None) == []


def test_an_entry_the_server_was_killed_while_staging_never_happened(tmp_path):
    root = tmp_path / "log"
    (root / ".journal").mkdir(parents=True)
    (root / ".journal" / "half.writing").write_bytes(b"torn")
    _defined(EventStreamServer(root))
    assert list((root / ".journal").iterdir()) == []


def test_a_journal_entry_reads_back_as_it_was_written():
    entry = JournalEntry.new("page.reads", "document.assemblers",
                             CommitPlan(3, 17, frozenset({19, 21})),
                             (JournalOutput("documents.assembled", Record(value=b"\x00ids")),
                              JournalOutput("page.reads", Record(value=b"body", key=b"doc-1",
                                                                 headers={"page": "2"}))))
    assert JournalEntry.decoded(entry.encoded()) == entry


# ------------------------------------------------------------- dead letters are kept thirty days


def test_dead_letters_older_than_the_retention_are_swept_and_nothing_else_is(tmp_path):
    """★ TERRA #10. Nobody commits on a dead-letter topic, so group-based retention never shrank one.
    It goes by age: thirty days, my call. The ordinary topic beside it is not touched."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    for name, value in (("work.dead", b"a poison record"), ("work", b"ordinary work")):
        owner = running.strand_for(name, 0)
        owner.submit(owner.append, name, 0, Record(value=value), None, None).result(5)
    month_and_a_day = (strand_module.DEAD_LETTER_RETENTION_SECONDS + 86400) * 1_000_000_000

    assert running.sweep_dead_letters() == 0, "a fresh dead letter was swept"
    assert running.sweep_dead_letters(now_ns=time.time_ns() + month_and_a_day) == 1
    assert running.dead_letters("work.dead") == []
    assert [stats["next"] for stats in running.partition_stats()["work"].values()] == [1]
    running.close()


# ------------------------------------------------------------- a produce asks only who could take it


async def test_a_produce_asks_one_waiter_rather_than_every_waiter_twice(served, monkeypatch):
    """★ TERRA #11. Every produce re-polled every waiter on the channel, each walking every partition,
    and then did it again to confirm nothing had changed. One record answers one waiter; one is asked.

    The count starts only once every strand holds all four waiters. It used to start after a 50 ms sleep,
    and on the slower CI runner the waiters' own first polls landed after it: 4 waiters on 2 strands, plus
    the one that mattered, made the 9 it reported (2026-09-28)."""
    running, server = served
    waiters = [FreeStreamClient(server) for _ in range(4)]
    producer = FreeStreamClient(server)
    polls = []
    real_poll = ConsumerGroup.poll

    def counted_poll(self, **kwargs):
        polls.append(self)
        return real_poll(self, **kwargs)

    try:
        waiting = [asyncio.create_task(client.next("to.read", "readers")) for client in waiters]
        deadline = asyncio.get_running_loop().time() + 5
        while not all(sum(1 for member in strand._waiting if member[1] == "to.read") == len(waiters)
                      for strand in running.strands):
            assert asyncio.get_running_loop().time() < deadline, "the waiters never all waited"
            await asyncio.sleep(0.01)
        monkeypatch.setattr(ConsumerGroup, "poll", counted_poll)
        await producer.produce("to.read", value=b"one page")
        done, _pending = await asyncio.wait(waiting, timeout=2, return_when=asyncio.FIRST_COMPLETED)
        assert len(done) == 1
        assert len(polls) == 1, f"{len(polls)} waiters were asked about one record"
    finally:
        for task in waiting:
            task.cancel()
        for client in (*waiters, producer):
            await client.close()


async def test_a_record_in_a_partition_its_holder_is_busy_with_waits_for_that_holder(server, monkeypatch):
    """The holder reads it on its next NEXT; nobody else could take it, so nobody else is asked."""
    holder, other, producer = (FreeStreamClient(server) for _ in range(3))
    try:
        await producer.produce("one.keyed", value=b"page 1", key="doc-1")
        partition, first = await holder.next("one.keyed", "assemblers", wait=False)
        waiting = asyncio.create_task(other.next("one.keyed", "assemblers"))
        await asyncio.sleep(0.05)
        await producer.produce("one.keyed", value=b"page 2", key="doc-1")
        await asyncio.sleep(0.05)
        assert not waiting.done(), "a member that does not hold the partition was handed its record"
        await holder.commit("one.keyed", "assemblers", partition, first.offset + 1)
        _partition, second = await holder.next("one.keyed", "assemblers", wait=False)
        assert second.value == b"page 2"
        waiting.cancel()
    finally:
        for client in (holder, other, producer):
            await client.close()


# ------------------------------------------------------------- a client that stops reading is dropped


async def test_a_client_that_stops_reading_what_it_was_sent_is_dropped_and_its_record_moves_on(tmp_path,
                                                                                            monkeypatch):
    """★ TERRA #12. A held NEXT's answer is written without waiting, so a client that stopped reading
    kept a whole record in the server's memory for good. Past the deadline it is dropped: the memory is
    freed and the record, never committed, goes to another member."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    socket = tmp_path / "es.sock"
    _never_read, writer = await asyncio.open_unix_connection(str(socket))
    producer, successor = FreeStreamClient(socket), FreeStreamClient(socket)
    try:
        writer.write(Frame(header={"op": "next", "channel": "work", "group": "readers",
                                   "wait": True, "id": 1}).encoded())
        await writer.drain()
        await eventually(lambda: any(member[1] == "work" for strand in running.strands
                                     for member in strand._pending), "the NEXT was never held")
        await producer.produce("work", value=b"x" * (4 * 1024 * 1024))    # far more than a socket buffers
        monkeypatch.setattr(strand_module, "OUTBOUND_STALL_SECONDS", 0)
        # Nothing is dropped until the answer is written and backs up; then exactly this reader is.
        await eventually(lambda: running.drop_stalled_readers() == 1, "the stalled reader was never dropped")
        taken: list = []

        async def handed_on() -> bool:
            taken[:] = [await successor.next("work", "readers", wait=False)]
            return taken[0] is not None
        await eventually(handed_on, "the record never moved on")
        _partition, record = taken[0]
        assert len(record.value) == 4 * 1024 * 1024
    finally:
        writer.close()
        for client in (producer, successor):
            await client.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


# ------------------------------------------------------------- the judge's findings, 2026-09-26


async def test_a_complete_whose_outputs_fail_to_land_is_finished_without_a_restart(served, monkeypatch):
    """★ BRIEF R5. The step happened when its journal entry was written. An output append that fails is
    applied again in the background until it lands, rather than waiting for a restart to replay it, and
    the consumer is told its COMPLETE succeeded, so its committed record never comes back."""
    monkeypatch.setattr(strand_module, "FINISH_FIRST_WAIT_SECONDS", 0.05)
    running, server = served
    real_append = running.append_outputs
    failures = []

    async def append_failing_once(strand, entry):
        if not failures:
            failures.append(entry.entry_id)
            raise OSError("the disk said no")
        return await real_append(strand, entry)

    monkeypatch.setattr(running, "append_outputs", append_failing_once)
    client = FreeStreamClient(server)
    try:
        await client.produce("to.read", value=b"a page")
        partition, record = await client.next("to.read", "readers", wait=False)
        await client.complete("to.read", "readers", partition, record.offset + 1,
                              [Output("work", b"the page read")])
        assert await client.next("to.read", "readers", wait=False) is None, "the record came back"
        for _ in range(100):
            if await _everything_in(client, "work", group=f"check-{_}") == [b"the page read"]:
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("the output never landed")
        assert failures, "the append never failed, so nothing was tested"
        assert list((server.parent / "log" / ".journal").glob("*.complete")) == []
    finally:
        await client.close()


def test_a_group_that_gains_a_member_while_being_deleted_is_refused(tmp_path, monkeypatch):
    """★ BRIEF R2. Each strand deletes its partitions' files; a file still there afterwards is a member
    that joined in between, and the main thread refuses rather than delete what it does not own."""
    running = _defined(EventStreamServer(tmp_path / "log"))
    home = tmp_path / "log" / "work" / "groups" / "g"
    home.mkdir(parents=True)
    (home / "0000.offset").write_text("0")
    monkeypatch.setattr(strand_module.Strand, "remove_group_files", lambda self, *args: None)
    try:
        with pytest.raises(RuntimeError, match="gained a member"):
            running.delete_group("work", "g")
    finally:
        running.close()


async def test_a_topic_being_removed_refuses_new_work_before_its_files_go(served):
    """★ BRIEF R2. The definition goes first, so nothing reopens a partition while it is deleted."""
    running, server = served
    await asyncio.to_thread(running.remove_topic, "work")
    client = FreeStreamClient(server)
    try:
        with pytest.raises(UnknownTopicError):
            await client.produce("work", value=b"late")
        assert not (server.parent / "log" / "work").exists()
    finally:
        await client.close()


def test_a_journal_entry_replayed_long_after_its_first_output_landed_writes_it_once(tmp_path):
    """★ BRIEF R5. The server was down for more than an hour after a crash partway through an entry's
    outputs; the replay still recognizes the one that landed."""
    key = os.urandom(32)
    running = _defined(EventStreamServer(tmp_path / "log", key=key))
    running.close()
    seal = Seal(key)
    plan = CommitPlan(partition=0, position=0, acked=frozenset())
    entry = JournalEntry.new("to.read", "readers", plan, (
        JournalOutput("work", Record(value=b"first")), JournalOutput("work", Record(value=b"second"))))
    write_entry(tmp_path / "log", entry, seal)
    landed_two_hours_ago = Record(value=b"first", timestamp_ns=time.time_ns() - 2 * 3600 * 1_000_000_000)
    Topic(tmp_path / "log" / "work", partitions=1, seal=seal).produce_once(landed_two_hours_ago,
                                                                            entry.producer, 0)
    EventStreamServer(tmp_path / "log", key=key).close()
    work = Topic(tmp_path / "log" / "work", partitions=1, seal=seal)
    assert [record.value for record in work.log(0).read_from(0)] == [b"first", b"second"]


def test_the_replay_skips_an_output_its_entry_records_as_landed_even_after_retention_trimmed_it(tmp_path):
    """★ BRIEF R5. Retention does not know about journal entries, so a landed output can be consumed and its
    segment deleted while its entry waits on another output. The entry records each output as it lands, so
    the replay skips it by number rather than looking for it in a log that no longer has it."""
    key = os.urandom(32)
    _defined(EventStreamServer(tmp_path / "log", key=key)).close()
    seal = Seal(key)
    entry = JournalEntry.new("to.read", "readers", CommitPlan(partition=0, position=0, acked=frozenset()), (
        JournalOutput("work", Record(value=b"first")), JournalOutput("work", Record(value=b"second"))))
    entry.landed[0] = (0, 0)             # landed, consumed, and its segment since deleted
    write_entry(tmp_path / "log", entry, seal)
    EventStreamServer(tmp_path / "log", key=key).close()
    work = Topic(tmp_path / "log" / "work", partitions=1, seal=seal)
    assert [record.value for record in work.log(0).read_from(0)] == [b"second"]
