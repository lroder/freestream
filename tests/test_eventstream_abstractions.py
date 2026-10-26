"""FreeStream's transport and its consumer loop — what keeps the promises the pipeline relies on (#450).

These tests drive a REAL event stream server on a REAL socket, because that is what a caller talks to.
A test that drove the store directly would check a shape nobody uses, and would miss the whole reason
the server exists, which is that producers do not touch the log.
"""
import asyncio

import pytest
import pytest_asyncio

from freestream import consumer as consumer_module
from freestream.client import FreeStreamClient, HandlingEndedError
from freestream.consumer import Consumer, Registration
from freestream.interface.channel import Channel, ChannelContractError, Delivery
from freestream.interface.transport import EventStreamUnavailableError
from freestream.record import Record
from freestream.registry import RequestRefusedError
from freestream.server import EventStreamServer
from freestream.transport import FreeStreamTransport
from tests.eventually import eventually
from tests.topics import define

PAGES = Channel("ocr.pages", Delivery.KEYED, partitions=4)
WORK = Channel("scan.work", Delivery.COMPETING, partitions=4)
COCKPIT = Channel("cockpit", Delivery.BROADCAST, partitions=1)
#: Every other topic these tests read or write, one partition each so a record's place is known.
_ONE_PARTITION = [Channel(name, Delivery.COMPETING, partitions=1)
                  for name in ("poison.work", "crashing.work", "live.work", "doubled.work", "counted",
                               "outputs")]
POISON_KEYED = Channel("poison.keyed", Delivery.KEYED, partitions=1)


@pytest_asyncio.fixture
async def client(tmp_path):
    server = define(EventStreamServer(tmp_path / "log"), PAGES, WORK, COCKPIT, POISON_KEYED,
                    *_ONE_PARTITION)
    ready = asyncio.Event()
    serving = asyncio.create_task(server.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    talking = FreeStreamClient(tmp_path / "es.sock")
    yield talking
    await talking.close()
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


def _reader(channel, client, handler, group="readers"):
    return Consumer(Registration(channel=channel, group=group, handler=handler), client)


# --------------------------------------------------------------------------- the declaration


async def test_a_keyed_channel_refuses_a_record_with_no_key(client):
    """★ REFUSED AT THE PRODUCER, because this is the last place the caller's INTENT is visible.

    By the time a record reaches the server there is only a key or no key, and it does the only thing
    it can with either. When the producer refuses the record here, the caller gets an error at the call
    site instead of a document quietly assembled by two consumers.
    """
    with pytest.raises(ChannelContractError, match="assembled twice"):
        await FreeStreamTransport(client).produce(PAGES, value=b"a page")


async def test_a_competing_channel_is_happy_without_one(client):
    await FreeStreamTransport(client).produce(WORK, value=b"a job")
    assert len(await _everything_on(client, WORK.name)) == 1


async def test_a_competing_channel_refuses_a_record_with_one(client):
    """A key on a channel that exists to share its work out would pin every record sharing it to one
    consumer, which is how 286 pages of one return once reached a single reader while seven sat idle."""
    with pytest.raises(ChannelContractError, match="share its work out"):
        await FreeStreamTransport(client).produce(WORK, value=b"a job", key="doc-1")


async def test_a_produce_hands_back_nothing_to_wait_on(client):
    """★ THE CAVEAT, MADE STRUCTURAL. A nudge we are happy to lose must not be able to block anything.

    One set of utilities across three hops is good; one set of GUARANTEES across them is not. No produce
    returns a position, so nothing can await the delivery of a broadcast.
    """
    assert await FreeStreamTransport(client).produce(COCKPIT, value=b"a nudge") is None


def test_a_channel_needs_at_least_one_partition():
    with pytest.raises(ValueError, match="at least one partition"):
        Channel("bad", Delivery.COMPETING, partitions=0)


# --------------------------------------------------------------------------- the handler


async def test_a_handler_sees_the_record_and_nothing_else(client):
    seen: list[Record] = []
    await FreeStreamTransport(client).produce(PAGES, value=b"page bytes", key="doc-1", headers={"page": "6"})

    async def handler(record):
        seen.append(record)

    assert await _reader(PAGES, client, handler).drain() == 1
    assert seen[0].value == b"page bytes"
    assert seen[0].headers == {"page": "6"}


async def test_a_handled_record_is_not_handed_out_again(client):
    transport = FreeStreamTransport(client)
    for page in range(3):
        await transport.produce(PAGES, value=str(page).encode(), key="doc-1")

    async def handler(record):
        return None

    reader = _reader(PAGES, client, handler)
    assert await reader.drain() == 3
    assert await reader.drain() == 0


async def test_a_handler_that_raises_keeps_the_record(client):
    """★ AT-LEAST-ONCE, AND THE OFFSET MOVES BEHIND THE HANDLER, NEVER AHEAD OF IT.

    A stage that silently drops what it cannot process is a document nobody is ever told about. So a
    failure stops the partition where it is and the record comes back — every handler downstream is
    already built for that: the page write is idempotent on (run, page), the carve on (run, first_page).
    """
    attempts: list[bytes] = []
    await FreeStreamTransport(client).produce(PAGES, value=b"poison", key="doc-1")

    async def fails_once(record):
        attempts.append(record.value)
        if len(attempts) == 1:
            raise RuntimeError("not this time")

    reader = _reader(PAGES, client, fails_once)
    assert await reader.drain() == 0, "nothing was handled, so nothing is committed"
    assert await reader.drain() == 1, "and the record comes back"
    assert attempts == [b"poison", b"poison"]


async def test_a_failure_does_not_skip_past_the_record_that_caused_it(client):
    """The records behind a poison one wait for it rather than being consumed out from under it."""
    handled: list[bytes] = []
    transport = FreeStreamTransport(client)
    for value in (b"first", b"poison", b"third"):
        await transport.produce(PAGES, value=value, key="doc-1")

    async def stops_at_poison(record):
        if record.value == b"poison":
            raise RuntimeError("stop here")
        handled.append(record.value)

    await _reader(PAGES, client, stops_at_poison).drain()
    assert handled == [b"first"], "it must not run ahead to 'third'"


# --------------------------------------------------------------------------- registering


async def test_a_registered_handler_stays_callable_without_a_stream(client):
    """A handler you cannot call without a running stream is one that only gets tested through one."""
    async def handle(record):
        return record.value

    transport = FreeStreamTransport(client)
    transport.register(WORK, group="workers", handler=handle)
    registered = transport.registrations[-1]
    assert (registered.channel, registered.group, registered.handler) == (WORK, "workers", handle)
    assert await handle(Record(value=b"direct")) == b"direct"


async def test_two_groups_on_one_channel_each_see_every_record(client):
    """Broadcast: a second group reads from its own beginning rather than sharing a position."""
    await FreeStreamTransport(client).produce(COCKPIT, value=b"a nudge")
    first, second = [], []

    async def into_first(record):
        first.append(record.value)

    async def into_second(record):
        second.append(record.value)

    assert await _reader(COCKPIT, client, into_first, "rail").drain() == 1
    assert await _reader(COCKPIT, client, into_second, "audit").drain() == 1
    assert first == second == [b"a nudge"]


# --------------------------------------------------------------------------- the connection


async def test_a_producer_that_cannot_reach_the_broker_raises_rather_than_losing_the_work(tmp_path):
    """★ Losing work is not a decision a transport takes on a caller's behalf. For an upload that
    means telling the person now, rather than a document that never arrives and nobody misses."""
    nowhere = FreeStreamClient(tmp_path / "nothing-is-listening.sock")
    with pytest.raises(EventStreamUnavailableError):
        await FreeStreamTransport(nowhere).produce(WORK, value=b"a job")


# ------------------------------------------------------------- a record that fails every time


async def _everything_on(client, channel):
    """What a channel holds, read by a group of its own so nobody else's position moves."""
    taken = []
    while handed := await client.next(channel, "inspector", wait=False):
        partition, record = handed
        taken.append(record)
        await client.commit(channel, "inspector", partition, record.offset + 1)
    return taken


def _one_partition_reader(client, handler, on_exhausted=None):
    one = Channel("poison.work", Delivery.COMPETING, partitions=1)
    return Consumer(Registration(one, "workers", handler, on_exhausted), client), one


async def test_a_poison_record_stops_blocking_its_partition(client):
    """★ THE DEFECT. A handler that always raised left its record uncommitted for good, so the partition
    stopped there — and every later record routed to it waited behind a record that would never pass."""
    handled = []

    async def handler(record):
        if record.value == b"poison":
            raise ValueError("this one never works")
        handled.append(record.value)

    reader, one = _one_partition_reader(client, handler)
    for value in (b"poison", b"after"):
        await client.produce(one.name, value=value)

    for _ in range(consumer_module.MAX_ATTEMPTS + 1):
        await reader.drain()

    assert handled == [b"after"], "the record behind the poison never ran"
    dead = await _everything_on(client, consumer_module.dead_letter_channel(one.name))
    assert [record.value for record in dead] == [b"poison"]
    assert dead[0].headers["dead.attempts"] == str(consumer_module.MAX_ATTEMPTS)


async def test_the_records_behind_a_failing_one_are_not_charged_for_its_failures(client):
    """The consumer stops at a failing record, and the one behind it is not reached until it passes.
    A consumer that counted anything but STARTS could charge the record behind with failures it never
    had."""
    attempts_of_second = []

    async def handler(record):
        if record.value == b"flaky" and len(attempts_of_second) == 0:
            raise ValueError("fails until the second record has been reached")
        attempts_of_second.append(record.value)

    reader, one = _one_partition_reader(client, handler)
    for value in (b"flaky", b"innocent"):
        await client.produce(one.name, value=value)

    for _ in range(consumer_module.MAX_ATTEMPTS + 2):
        await reader.drain()

    dead = await _everything_on(client, consumer_module.dead_letter_channel(one.name))
    assert b"innocent" not in [record.value for record in dead]


async def test_the_count_survives_the_consumer_that_was_counting(client, tmp_path):
    """★ WHY THE SERVER COUNTS. A record that kills its process resets any count the process kept, and
    the replacement reads it again, for ever. Here each attempt is made by a FRESH connection — a new
    process, as far as the server can tell — and the record is still retired after MAX_ATTEMPTS."""
    one = Channel("crashing.work", Delivery.COMPETING, partitions=1)
    await client.produce(one.name, value=b"kills its reader")
    runs = []

    async def handler(record):
        runs.append(record.value)
        raise RuntimeError("the process would have died here")

    async def attempt_from_a_fresh_connection() -> None:
        fresh = FreeStreamClient(tmp_path / "es.sock")
        await Consumer(Registration(one, "workers", handler), fresh).drain()
        await fresh.close()

    # Each attempt waits for the departed connection's partition to come free: a fresh connection asks
    # again until the record has run once more.
    for attempt in range(1, consumer_module.MAX_ATTEMPTS + 1):
        async def ran_again(attempt: int = attempt) -> bool:
            await attempt_from_a_fresh_connection()
            return len(runs) == attempt
        await eventually(ran_again, f"attempt {attempt} never ran")

    # The next fresh connection retires it rather than running it: it reaches the dead letters.
    dead_letters = consumer_module.dead_letter_channel(one.name)

    async def retired() -> bool:
        await attempt_from_a_fresh_connection()
        return b"kills its reader" in [record.value for record in await _everything_on(client, dead_letters)]

    await eventually(retired, "the record was never retired")
    assert len(runs) == consumer_module.MAX_ATTEMPTS, "it ran again after its attempts were spent"


async def test_a_hop_closes_out_what_it_gave_up_on(client):
    """The dead letter is somewhere a person looks; the hop's own hook is how the pipeline moves on."""
    closed = []

    async def handler(record):
        raise ValueError("never")

    async def on_exhausted(record, attempts):
        closed.append((record.value, attempts))

    reader, one = _one_partition_reader(client, handler, on_exhausted)
    await client.produce(one.name, value=b"hopeless")
    for _ in range(consumer_module.MAX_ATTEMPTS + 1):
        await reader.drain()

    assert closed == [(b"hopeless", consumer_module.MAX_ATTEMPTS)]


async def test_a_keyed_record_is_dead_lettered_with_its_key_in_a_header(client):
    """A dead-letter topic is COMPETING and the server refuses a key on one, so the key the record
    carried rides in `dead.key` — whoever replays it still knows which document it belonged to."""
    async def handler(record):
        raise ValueError("never")

    reader = Consumer(Registration(POISON_KEYED, "workers", handler), client)
    await client.produce(POISON_KEYED.name, value=b"a page", key="doc-1")
    for _ in range(consumer_module.MAX_ATTEMPTS + 1):
        await reader.drain()

    dead = await _everything_on(client, consumer_module.dead_letter_channel(POISON_KEYED.name))
    assert [(record.value, record.key, record.headers["dead.key"]) for record in dead] == [
        (b"a page", None, "doc-1")]


async def test_a_close_out_that_fails_leaves_one_dead_letter_and_the_partition_moving(client):
    """★ FROM THE ADVERSARIAL REVIEW (2026-09-25). The commit came after `on_exhausted`, so a close-out
    that raised left the record uncommitted, and every retry dead-lettered it again."""
    handled = []

    async def handler(record):
        if record.value == b"hopeless":
            raise ValueError("never")
        handled.append(record.value)

    async def on_exhausted(record, attempts):
        raise RuntimeError("the database is away")

    reader, one = _one_partition_reader(client, handler, on_exhausted)
    for value in (b"hopeless", b"after"):
        await client.produce(one.name, value=value)
    for _ in range(consumer_module.MAX_ATTEMPTS + 3):
        await reader.drain()

    dead = await _everything_on(client, consumer_module.dead_letter_channel(one.name))
    assert [record.value for record in dead] == [b"hopeless"]
    assert handled == [b"after"]


async def test_a_record_whose_partition_changed_hands_mid_handler_is_let_go(client):
    """★ FOUND BY THE SOAK (2026-09-25). A server kill dropped a reader's connection while it read a page;
    the client reconnected as a new member holding nothing, and the commit that followed was refused. The
    reader died on the refusal and its supervisor restarted it. It lets the record go now, and the
    group's position leaves the record for whoever holds the partition to be handed again."""
    handled = []

    async def handler(record):
        handled.append(record.value)
        if len(handled) == 1:
            await client._drop()          # the connection goes, as it does when the server is killed

    reader, one = _one_partition_reader(client, handler)
    await client.produce(one.name, value=b"in hand")

    assert await reader.handle(*await client.next(one.name, "workers", wait=False)) is False
    await reader.drain()
    assert handled == [b"in hand", b"in hand"], "the record was not handed again after it was let go"


async def test_a_committed_record_can_no_longer_be_charged_with_a_start(client):
    """Each start is its own; a record committed is finished with, and a start on it is refused."""
    await client.produce("counted", value=b"x")
    _partition, record = await client.next("counted", "g", wait=False)
    assert await client.attempt("counted", "g", 0, record.offset) == 1
    assert await client.attempt("counted", "g", 0, record.offset) == 2
    await client.commit("counted", "g", 0, record.offset + 1)
    with pytest.raises(RequestRefusedError, match="not a record this consumer is handling"):
        await client.attempt("counted", "g", 0, record.offset)


# --------------------------------------------------------------------------- running for real


async def test_run_consumers_hands_each_record_to_its_handler_as_it_arrives(client):
    """The production path: every handler a task of its own, each awaiting its next record."""
    seen = asyncio.Queue()

    async def handler(record):
        await seen.put(record.value)

    one = Channel("live.work", Delivery.COMPETING, partitions=1)
    transport = FreeStreamTransport(client)
    transport.register(one, group="workers", handler=handler)
    stop = asyncio.Event()
    running = asyncio.create_task(transport.run(stop=stop))
    try:
        for value in (b"first", b"second"):
            await client.produce(one.name, value=value)
        assert [await asyncio.wait_for(seen.get(), timeout=2) for _ in range(2)] == [b"first", b"second"]
    finally:
        stop.set()
        await asyncio.wait_for(running, timeout=2)


async def test_a_consumer_that_cannot_reach_the_broker_ends_the_run(client, tmp_path):
    """Half the handlers carrying on while the other half has lost the server is worse than a restart."""
    async def handler(record):
        return None

    nowhere = FreeStreamTransport(FreeStreamClient(tmp_path / "nothing-here.sock"))
    nowhere.register(Channel("unreachable.work", Delivery.COMPETING, partitions=1), group="workers",
                     handler=handler)
    with pytest.raises(EventStreamUnavailableError):
        await asyncio.wait_for(nowhere.run(), timeout=5)


async def test_two_handlers_for_one_group_on_one_connection_are_refused(client):
    async def handler(record):
        return None

    one = Channel("doubled.work", Delivery.COMPETING, partitions=1)
    transport = FreeStreamTransport(client)
    transport.register(one, group="workers", handler=handler)
    transport.register(one, group="workers", handler=handler)
    with pytest.raises(ValueError, match="same channel and group"):
        await transport.run()


# ------------------------------------------------------------- what a handler produces, and when it lands


async def test_what_a_handler_produces_lands_only_when_its_record_is_completed(client):
    """★ THE HANDLER DOES NOT KNOW. It calls produce as it always did; the record is held until the
    handler returns and goes out in the same step as the commit of the record it handled."""
    seen_during = []

    async def handler(record):
        assert await client.produce("outputs", value=b"out of " + record.value) is None
        seen_during.extend(await _everything_on(client, "outputs"))

    reader, one = _one_partition_reader(client, handler)
    await client.produce(one.name, value=b"in")
    assert await reader.drain() == 1
    assert seen_during == [], "an output reached the log before its record was completed"
    assert [record.value for record in await _everything_on(client, "outputs")] == [b"out of in"]


async def test_a_handler_that_raises_leaves_nothing_it_produced(client):
    async def handler(record):
        await client.produce("outputs", value=b"half done")
        raise ValueError("died partway")

    reader, one = _one_partition_reader(client, handler)
    await client.produce(one.name, value=b"in")
    await reader.drain()
    assert await _everything_on(client, "outputs") == []


async def test_a_handler_whose_partition_changed_hands_publishes_nothing_and_the_retry_publishes_once(client):
    """★ THE DUPLICATE THE SOAK FOUND, NOW PREVENTED. The connection drops after the handler produced;
    its COMPLETE is refused, so the output was never written, and the second run's is the only one."""
    runs = []

    async def handler(record):
        runs.append(record.value)
        await client.produce("outputs", value=b"the page read")
        if len(runs) == 1:
            await client._drop()

    reader, one = _one_partition_reader(client, handler)
    await client.produce(one.name, value=b"a page")
    assert await reader.handle(*await client.next(one.name, "workers", wait=False)) is False
    await reader.drain()
    assert len(runs) == 2
    assert [record.value for record in await _everything_on(client, "outputs")] == [b"the page read"]


async def test_a_produce_from_a_task_that_outlived_its_handler_raises(client):
    """A task started inside a handler carries the handler's context. Once the record is completed, a
    produce from it has lost the step that would have made it land once, and it says so."""
    started = []

    async def handler(record):
        started.append(asyncio.create_task(asyncio.sleep(0)))

    reader, one = _one_partition_reader(client, handler)
    await client.produce(one.name, value=b"in")
    await reader.drain()

    async def later():
        await client.produce("outputs", value=b"too late")

    context = started[0].get_context()
    with pytest.raises(HandlingEndedError):
        await context.run(asyncio.create_task, later())


async def test_a_record_given_up_on_goes_out_with_its_dead_letter_and_its_close_out(client):
    async def handler(record):
        raise ValueError("never")

    async def on_exhausted(record, attempts):
        await client.produce("outputs", value=b"marked unreadable")

    reader, one = _one_partition_reader(client, handler, on_exhausted)
    await client.produce(one.name, value=b"hopeless")
    for _ in range(consumer_module.MAX_ATTEMPTS + 1):
        await reader.drain()
    assert [record.value for record in await _everything_on(client, "outputs")] == [b"marked unreadable"]
    dead = await _everything_on(client, consumer_module.dead_letter_channel(one.name))
    assert [record.value for record in dead] == [b"hopeless"]


async def test_a_close_out_that_raises_drops_what_it_produced_and_still_dead_letters(client):
    async def handler(record):
        raise ValueError("never")

    async def on_exhausted(record, attempts):
        await client.produce("outputs", value=b"half closed")
        raise RuntimeError("the database is away")

    reader, one = _one_partition_reader(client, handler, on_exhausted)
    await client.produce(one.name, value=b"hopeless")
    for _ in range(consumer_module.MAX_ATTEMPTS + 1):
        await reader.drain()
    assert await _everything_on(client, "outputs") == []
    dead = await _everything_on(client, consumer_module.dead_letter_channel(one.name))
    assert [record.value for record in dead] == [b"hopeless"]


async def test_an_output_the_server_refuses_is_a_failed_attempt(client):
    async def handler(record):
        await client.produce("outputs", value=b"x", key="a key a competing topic refuses")

    reader, one = _one_partition_reader(client, handler)
    await client.produce(one.name, value=b"in")
    assert await reader.drain() == 0
    assert await _everything_on(client, "outputs") == []
