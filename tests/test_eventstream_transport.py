"""The event stream's interface, and FreeStream behind it as the address names it (#450).

The interface is what keeps the pipeline free of any one queue, so most of what is checked here is that
the choice of transport is refused when it would be wrong rather than degraded: an address with no
transport behind it, a transport missing a guarantee the pipeline relies on, a record that breaks its
channel before any transport sees it.
"""
import asyncio
import socket as socket_module
from typing import ClassVar

import pytest

from freestream import transport as freestream_transport
from freestream.interface import transport as stream_interface
from freestream.interface.channel import Channel, ChannelContractError, Delivery
from freestream.interface.transport import (
    EventStreamTransport,
    EventStreamUnavailableError,
    Guarantee,
    TransportLacksGuaranteeError,
    UnknownTransportError,
    open_transport,
)
from freestream.server import EventStreamServer
from freestream.transport import FreeStreamAddressError, FreeStreamTransport, socket_paths
from tests.topics import define

#: A keyed channel, and the guarantees bosstax-web's pipeline requires, which FreeStream was built to keep.
PAGE_READS = Channel("page.reads", Delivery.KEYED, partitions=8)
REQUIRED_GUARANTEES = frozenset({Guarantee.KEY_AFFINITY, Guarantee.OUTPUTS_WITH_COMMIT,
                                 Guarantee.PRODUCED_ONCE, Guarantee.DEAD_LETTER})


class _Forgetful(EventStreamTransport):
    """A transport that keeps no promise at all, and records what reached it."""

    guarantees: ClassVar[frozenset[Guarantee]] = frozenset()
    sent: ClassVar[list[tuple[str, bytes, str | None]]] = []

    @classmethod
    def open(cls, url, *, role):
        return cls()

    async def ensure_reachable(self):
        return None

    async def _produce(self, channel, *, value, key, headers):
        self.sent.append((channel.name, value, key))

    def register(self, channel, *, group, handler, on_exhausted=None, max_attempts=3, delayed_retry=False):
        return None

    async def run(self, *, stop=None):
        return None

    async def close_key(self, channel, *, group, key):
        return None

    async def define(self, channels):
        return []

    async def close(self):
        return None


@pytest.fixture
def _forgetful_is_a_transport(monkeypatch):
    monkeypatch.setitem(stream_interface.TRANSPORTS, "forgetful", f"{__name__}:_Forgetful")
    _Forgetful.sent = []


# --------------------------------------------------------------------------- choosing a transport


def test_the_scheme_picks_the_transport():
    transport = open_transport("freestream:///var/lib/freestream/eventstream.sock", role="api",
                               requires=REQUIRED_GUARANTEES)
    assert isinstance(transport, FreeStreamTransport)


def test_an_address_with_no_transport_behind_it_is_refused_by_name():
    with pytest.raises(UnknownTransportError, match="schemes are freestream"):
        open_transport("nats://localhost:4222", role="api", requires=())


@pytest.mark.usefixtures("_forgetful_is_a_transport")
def test_a_transport_missing_a_guarantee_the_pipeline_needs_is_refused():
    """★ INVARIANT 3, APPLIED TO THE CHOICE OF QUEUE. A transport that spread one document over three
    assemblers would not make the pipeline slower, it would make it wrong — so it never starts."""
    with pytest.raises(TransportLacksGuaranteeError, match="key_affinity"):
        open_transport("forgetful:", role="page-reader", requires=REQUIRED_GUARANTEES)


@pytest.mark.usefixtures("_forgetful_is_a_transport")
def test_a_transport_is_enough_for_a_caller_that_needs_nothing_of_it():
    assert isinstance(open_transport("forgetful:", role="tool", requires=()), _Forgetful)


def test_freestream_keeps_every_guarantee_the_pipeline_requires():
    assert REQUIRED_GUARANTEES <= FreeStreamTransport.guarantees == frozenset(Guarantee)


# --------------------------------------------------------------------------- the channel's contract


@pytest.mark.usefixtures("_forgetful_is_a_transport")
async def test_a_record_that_breaks_its_channel_never_reaches_any_transport():
    """The check is the interface's, so a transport written tomorrow cannot forget to make it."""
    transport = open_transport("forgetful:", role="tool", requires=())
    with pytest.raises(ChannelContractError, match="no key"):
        await transport.produce(PAGE_READS, value=b"a page read")
    with pytest.raises(ChannelContractError, match="has a key"):
        await transport.produce(Channel("work", Delivery.COMPETING), value=b"a job", key="doc-1")
    await transport.produce(PAGE_READS, value=b"a page read", key="doc-1")
    assert _Forgetful.sent == [("page.reads", b"a page read", "doc-1")]


def test_a_broadcast_channel_takes_a_record_with_or_without_a_key():
    for key in (None, "doc-1"):
        Channel("cockpit", Delivery.BROADCAST).check_key(key)


# --------------------------------------------------------------------------- FreeStream's address


@pytest.mark.parametrize(("address", "socket_path", "admin"), [
    ("freestream:///var/lib/freestream/eventstream.sock", "/var/lib/freestream/eventstream.sock", None),
    ("freestream:var/eventstream.sock", "var/eventstream.sock", None),
    ("freestream:///run/es.sock?admin=/run/es-admin.sock", "/run/es.sock", "/run/es-admin.sock"),
])
def test_a_freestream_address_is_a_socket_path_and_maybe_the_console(address, socket_path, admin):
    assert socket_paths(address) == (socket_path, admin)


@pytest.mark.parametrize(("address", "why"), [
    ("freestream://somehost/es.sock", "names a host"),
    ("freestream:", "names no socket path"),
])
def test_a_freestream_address_that_is_not_a_socket_path_is_refused(address, why):
    with pytest.raises(FreeStreamAddressError, match=why):
        socket_paths(address)


async def test_a_server_nobody_is_running_is_said_at_once(tmp_path):
    transport = FreeStreamTransport.open(f"freestream:{tmp_path / 'nothing.sock'}", role="api")
    with pytest.raises(EventStreamUnavailableError, match="did not answer"):
        await transport.ensure_reachable()
    await transport.close()


async def test_defining_topics_needs_the_console(tmp_path):
    transport = FreeStreamTransport.open(f"freestream:{tmp_path / 'es.sock'}", role="topics")
    with pytest.raises(FreeStreamAddressError, match=r"\?admin="):
        await transport.define([PAGE_READS])


async def test_a_stale_console_file_is_not_mistaken_for_a_server(tmp_path):
    """A restarted server's old socket file is there before the new one listens. Found on the first
    real run of the topics step: it saw the file, connected, was refused, and defined nothing."""
    stale = tmp_path / "a.sock"
    left_behind = socket_module.socket(socket_module.AF_UNIX)
    left_behind.bind(str(stale))
    left_behind.close()                      # the file stays; nothing listens on it
    with pytest.raises(EventStreamUnavailableError, match="no event stream server console"):
        await freestream_transport._await_the_console(str(stale), 0.3)


async def test_a_reachable_server_answers_and_a_key_is_closed_through_it(tmp_path):
    running = define(EventStreamServer(tmp_path / "log"))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    transport = FreeStreamTransport.open(f"freestream:{tmp_path / 'es.sock'}", role="api")
    try:
        await transport.ensure_reachable()
        assert transport.client.connection_id is not None
        await transport.close_key(PAGE_READS, group="document.assemblers", key="doc-1")
    finally:
        await transport.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)


# --------------------------------------------------------------------------- defining topics, the deploy step


@pytest.fixture
async def console(tmp_path):
    running = EventStreamServer(tmp_path / "log")
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", admin_socket=tmp_path / "a.sock",
                                                ready=ready))
    await ready.wait()
    yield running, f"freestream:{tmp_path / 'es.sock'}?admin={tmp_path / 'a.sock'}"
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


async def test_each_channel_is_defined_at_its_declared_shape_and_again_changes_nothing(console):
    running, address = console
    channels = [PAGE_READS, Channel("jobs", Delivery.COMPETING, partitions=4)]
    transport = FreeStreamTransport.open(address, role="deploy")
    answers = await transport.define(channels)
    assert len(answers) == 2
    for channel in channels:
        definition = running.registry.get(channel.name)
        assert (definition.partitions, definition.delivery) == (channel.partitions, channel.delivery)
    before = running.registry.all()
    await transport.define(channels)
    assert running.registry.all() == before
    await transport.close()


async def test_a_topic_held_at_another_width_stops_the_deploy(console):
    running, address = console
    running.define_topic(PAGE_READS.name, partitions=PAGE_READS.partitions * 2, delivery="keyed")
    with pytest.raises(RuntimeError, match="cannot change"):
        await FreeStreamTransport.open(address, role="deploy").define([PAGE_READS])


async def test_produce_register_and_run_go_through_the_server(tmp_path):
    running = define(EventStreamServer(tmp_path / "log"), Channel("jobs", Delivery.COMPETING, partitions=2))
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    seen, stop = asyncio.Queue(), asyncio.Event()

    async def handle(record):
        await seen.put(record.value)

    transport = FreeStreamTransport.open(f"freestream:{tmp_path / 'es.sock'}", role="worker")
    transport.register(Channel("jobs", Delivery.COMPETING, partitions=2), group="workers", handler=handle)
    running_consumers = asyncio.create_task(transport.run(stop=stop))
    try:
        await transport.produce(Channel("jobs", Delivery.COMPETING, partitions=2), value=b"a job")
        assert await asyncio.wait_for(seen.get(), timeout=5) == b"a job"
    finally:
        stop.set()
        await asyncio.wait_for(running_consumers, timeout=5)
        await transport.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
