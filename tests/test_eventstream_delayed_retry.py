"""A handler asks to be tried again later, and nothing waits behind it meanwhile (#450, 2026-10-12).

clarifile's webhook deliveries go to a firm's own endpoint, which can be down for hours. The handler
raises `RetryLater(delay)`; the record waits on its channel's retry topic and is handed back no sooner
than the delay, the records behind it go on being handled, and the attempt limit spans every hop. These
tests drive a real server on a real socket, as the rest of FreeStream's tests do, with delays of a
fraction of a second.
"""
import asyncio
import os
import time
from datetime import timedelta

import pytest

from freestream.client import FreeStreamClient
from freestream.consumer import as_produced
from freestream.consumer_group import CommitPlan, ConsumerGroup, write_commit
from freestream.interface.channel import Channel, Delivery
from freestream.interface.transport import RetryLater
from freestream.record import RETRY_ATTEMPTS_HEADER, RETRY_KEY_HEADER, RETRY_NOT_BEFORE_HEADER, Record
from freestream.registry import RequestRefusedError, TopicError, TopicRegistry
from freestream.server import EventStreamServer
from freestream.topic import Topic
from freestream.transport import FreeStreamTransport
from tests.eventually import eventually

DELIVERIES = Channel("webhooks.deliveries", Delivery.KEYED, partitions=2)
JOBS = Channel("jobs", Delivery.COMPETING, partitions=2)
SHORT = timedelta(milliseconds=400)


async def _serve(root, socket, key=None):
    server = EventStreamServer(root, key=key)
    for channel in (DELIVERIES, JOBS):
        server.define_topic(channel.name, partitions=channel.partitions, delivery=channel.delivery)
    ready = asyncio.Event()
    serving = asyncio.create_task(server.serve(socket, ready=ready))
    await ready.wait()
    return server, serving


async def _stop(serving):
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


@pytest.fixture
async def stream(tmp_path):
    _server, serving = await _serve(tmp_path / "log", tmp_path / "es.sock")
    yield tmp_path / "es.sock"
    await _stop(serving)


async def _running(transport):
    stop = asyncio.Event()
    return stop, asyncio.create_task(transport.run(stop=stop))


async def _ended(stop, running, transport):
    stop.set()
    await asyncio.wait_for(running, timeout=5)
    await transport.close()


# --------------------------------------------------------------------------- the derived topic


def test_every_topic_has_a_retry_topic_as_wide_as_itself(tmp_path):
    registry = TopicRegistry(tmp_path)
    registry.define("webhooks.deliveries", partitions=4, delivery="keyed")
    retries = registry.get("webhooks.deliveries.retry")
    assert (retries.partitions, retries.delivery, retries.is_retries, retries.is_derived) == (
        4, Delivery.COMPETING, True, True)
    assert "webhooks.deliveries.retry" in [definition.name for definition in registry.all()]


def test_nobody_may_define_a_topic_under_the_retry_suffix(tmp_path):
    with pytest.raises(TopicError, match="derived topics"):
        TopicRegistry(tmp_path).define("sneaky.retry", partitions=1, delivery="competing")


def test_removing_a_topic_removes_its_retries(tmp_path):
    registry = TopicRegistry(tmp_path)
    registry.define("jobs", partitions=1, delivery="competing")
    (tmp_path / "jobs.retry").mkdir()
    registry.remove("jobs")
    assert not (tmp_path / "jobs.retry").exists()


# --------------------------------------------------------------------------- the group that waits


def _retry(value, not_before_ns):
    return Record(value=value, headers={RETRY_NOT_BEFORE_HEADER: str(not_before_ns)})


def test_a_retry_group_hands_out_only_what_is_due_and_says_when_the_rest_is(tmp_path):
    topic = Topic(tmp_path, partitions=1, seal=None)
    now = time.time_ns()
    topic.produce_to(0, _retry(b"later", now + 60_000_000_000))
    topic.produce_to(0, _retry(b"due", now - 1))
    group = ConsumerGroup(topic, "deliverers", tmp_path, keeps_its_partitions=False, due_by_header=True)

    [(partition, record)] = group.poll(limit=1)
    assert record.value == b"due", "the record waiting at the position held up the one behind it"
    assert group.next_due_ns == now + 60_000_000_000

    group.commit(partition, record.offset + 1)
    assert (group.position(0), group.acked(0)) == (0, {1}), "the waiting record must keep the position"
    assert group.poll(limit=1) == []


def test_the_due_record_whose_time_came_first_goes_first(tmp_path):
    topic = Topic(tmp_path, partitions=1, seal=None)
    now = time.time_ns()
    topic.produce_to(0, _retry(b"due second", now - 1))
    topic.produce_to(0, _retry(b"due first", now - 1_000))
    group = ConsumerGroup(topic, "deliverers", tmp_path, keeps_its_partitions=False, due_by_header=True)
    assert [record.value for _partition, record in group.poll(limit=1)] == [b"due first"]


def test_a_retry_group_commits_only_records_it_was_handed(tmp_path):
    topic = Topic(tmp_path, partitions=1, seal=None)
    topic.produce_to(0, _retry(b"due", 0))
    topic.produce_to(0, _retry(b"also due", 0))
    group = ConsumerGroup(topic, "deliverers", tmp_path, keeps_its_partitions=False, due_by_header=True)
    group.poll(limit=1)
    with pytest.raises(RequestRefusedError, match="not a record this consumer is handling"):
        group.commit(0, 2)
    with pytest.raises(RequestRefusedError, match="not a record this consumer is handling"):
        group.attempt(0, 1, None)


def test_a_commit_replayed_from_the_journal_never_takes_an_acknowledgment_back(tmp_path):
    """A plan journaled before later commits landed carries an older acked set; it must add, never remove."""
    home = tmp_path / "groups" / "g"
    write_commit(home, CommitPlan(0, 0, frozenset({3, 5})))
    write_commit(home, CommitPlan(0, 0, frozenset({3})))
    assert sorted((home / "0000.acked").read_text().strip("[]").split(", ")) == ["3", "5"]
    write_commit(home, CommitPlan(0, 3, frozenset()))
    assert ((home / "0000.offset").read_text(), (home / "0000.acked").read_text()) == ("4", "[5]")


def test_a_record_as_produced_has_its_key_back_and_no_retry_headers():
    waiting = Record(value=b"x", headers={RETRY_KEY_HEADER: "endpoint-7", RETRY_ATTEMPTS_HEADER: "2",
                                          RETRY_NOT_BEFORE_HEADER: "1", "event": "return.updated"})
    produced = as_produced(waiting)
    assert (produced.key, produced.headers) == (b"endpoint-7", {"event": "return.updated"})
    plain = Record(value=b"y", key=b"k")
    assert as_produced(plain) is plain


# --------------------------------------------------------------------------- end to end


async def test_a_record_that_asks_to_wait_is_not_run_again_before_its_delay(stream):
    runs: list[tuple[float, bytes | None, dict[str, str]]] = []

    async def deliver(record):
        runs.append((time.monotonic(), record.key, dict(record.headers)))
        if len(runs) == 1:
            raise RetryLater(SHORT)

    transport = FreeStreamTransport(FreeStreamClient(stream))
    transport.register(DELIVERIES, group="deliverers", handler=deliver, max_attempts=5, delayed_retry=True)
    stop, running = await _running(transport)
    try:
        await transport.produce(DELIVERIES, value=b"payload", key="endpoint-7", headers={"event": "updated"})
        await eventually(lambda: len(runs) == 2, "the record never ran a second time")
    finally:
        await _ended(stop, running, transport)
    first, second = runs
    assert second[0] - first[0] >= SHORT.total_seconds() * 0.9, "it ran again before its delay"
    assert second[1:] == first[1:] == (b"endpoint-7", {"event": "updated"}), \
        "the handler must see the record as it was produced, key and headers"


async def test_a_waiting_record_holds_up_nothing_behind_it(stream):
    """★ ONE FIRM'S DEAD ENDPOINT MUST NEVER DELAY ANOTHER'S. Keyed alike, so both share a partition."""
    handled: list[bytes] = []

    async def deliver(record):
        if record.value == b"dead endpoint":
            raise RetryLater(timedelta(seconds=60))
        handled.append(record.value)

    transport = FreeStreamTransport(FreeStreamClient(stream))
    transport.register(DELIVERIES, group="deliverers", handler=deliver, max_attempts=5, delayed_retry=True)
    stop, running = await _running(transport)
    try:
        await transport.produce(DELIVERIES, value=b"dead endpoint", key="same")
        for number in range(3):
            await transport.produce(DELIVERIES, value=f"live {number}".encode(), key="same")
        await eventually(lambda: len(handled) == 3, "the records behind the waiting one were held up")
    finally:
        await _ended(stop, running, transport)
    assert handled == [b"live 0", b"live 1", b"live 2"]


async def test_past_its_attempts_a_record_is_dead_lettered_and_closed_out(stream):
    starts, closed = [], []

    async def deliver(record):
        starts.append(record.value)
        raise RetryLater(timedelta(milliseconds=50))

    async def give_up(record, attempts):
        closed.append((record.value, record.key, attempts))

    transport = FreeStreamTransport(FreeStreamClient(stream))
    transport.register(DELIVERIES, group="deliverers", handler=deliver, on_exhausted=give_up,
                       max_attempts=3, delayed_retry=True)
    inspector, dead_letters = FreeStreamClient(stream), []

    async def dead_lettered():
        # The hop closes the record out before the COMPLETE that writes the dead letter, so the test
        # waits for the dead letter itself rather than reading it the moment `closed` fills.
        handed = await inspector.next("webhooks.deliveries.dead", "inspector", wait=False)
        if handed is not None:
            dead_letters.append(handed[1])
        return dead_letters

    stop, running = await _running(transport)
    try:
        await transport.produce(DELIVERIES, value=b"never lands", key="endpoint-9")
        await eventually(lambda: closed, "the record was never given up on")
        await eventually(dead_lettered, "the record was never dead-lettered")
    finally:
        await _ended(stop, running, transport)
        await inspector.close()
    assert starts == [b"never lands"] * 3
    assert closed == [(b"never lands", b"endpoint-9", 3)]
    [dead] = dead_letters
    assert (dead.value, dead.headers["dead.key"], dead.headers["dead.attempts"]) == (
        b"never lands", "endpoint-9", "3")
    assert not any(name.startswith("retry.") for name in dead.headers)


async def test_a_record_waiting_out_its_delay_survives_the_server_restarting(tmp_path):
    # The same key both times, as a restarted server reads it from its key file: a server built without
    # one seals under a key of its own, and could not open what the first wrote.
    root, socket, key = tmp_path / "log", tmp_path / "es.sock", os.urandom(32)
    _server, serving = await _serve(root, socket, key)
    asked = []

    async def asks_to_wait(record):
        asked.append(record.value)
        raise RetryLater(SHORT)

    first = FreeStreamTransport(FreeStreamClient(socket))
    first.register(JOBS, group="workers", handler=asks_to_wait, max_attempts=5, delayed_retry=True)
    stop, running = await _running(first)
    await first.produce(JOBS, value=b"a job")
    await eventually(lambda: asked, "the job never ran")
    await _ended(stop, running, first)
    await _stop(serving)

    _server, serving = await _serve(root, socket, key)
    done = []

    async def does_it(record):
        done.append((time.monotonic(), record.value))

    second = FreeStreamTransport(FreeStreamClient(socket))
    second.register(JOBS, group="workers", handler=does_it, max_attempts=5, delayed_retry=True)
    stop, running = await _running(second)
    try:
        await eventually(lambda: done, "the waiting job never ran after the restart")
    finally:
        await _ended(stop, running, second)
        await _stop(serving)
    assert [value for _when, value in done] == [b"a job"]


async def test_a_handler_registered_without_delayed_retry_that_asks_to_wait_has_simply_failed(stream):
    """It is retried at once, as any failure is, and never reaches a retry topic nobody reads."""
    starts = []

    async def deliver(record):
        starts.append(record.value)
        if len(starts) == 1:
            raise RetryLater(timedelta(hours=1))

    transport = FreeStreamTransport(FreeStreamClient(stream))
    transport.register(JOBS, group="workers", handler=deliver)
    stop, running = await _running(transport)
    try:
        await transport.produce(JOBS, value=b"a job")
        await eventually(lambda: len(starts) == 2, "the failure was never retried")
    finally:
        await _ended(stop, running, transport)
    assert [registration.channel.name for registration in transport.registrations] == ["jobs"]
