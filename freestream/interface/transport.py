"""The event stream as a product's code sees it: one interface, and whichever transport the address names.

    transport = open_transport(settings.event_stream_url, role="page-reader", requires=REQUIRED_GUARANTEES)
    transport.register(PAGES_TO_READ, group="page.readers", handler=read, on_exhausted=give_up)
    await transport.run(stop=stop)                      # every handler, each awaiting its next record
    await transport.produce(PAGE_READS, value=..., key=document_id.hex, headers={"page": "6"})
    await transport.close()

★ A PRODUCT KNOWS NO QUEUE. Its code imports this module, `channel` and `record`, and nothing else about
the stream, so moving to another queue is invisible to it. Which transport runs is the scheme of one
address, `freestream:///var/lib/freestream/eventstream.sock` today and `nats://host:4222` the day a NATS
transport is written, the way a database URL names its driver. A new transport is one class and one line
in `TRANSPORTS`. bosstax-web, where FreeStream was built, keeps the rule with an import contract that
forbids its pipeline from importing any transport; until 2026-10-12 its stages imported FreeStream's own
client, consumer and record types, and moving to another queue would have meant rewriting five modules
rather than adding a class.

★ THE GUARANTEES ARE THE INTERFACE, AND THE SIGNATURES ARE THE SMALL PART OF IT. Every queue offers
`produce` and something that hands out records. What differs, and what a product is written against, is
what the queue promises about them: whether one consumer holds a key, whether a handler's outputs land
with the commit of its input, whether a resend is written twice, whether a record that always fails stops
being tried. A transport states which of those it keeps (`Guarantee`), the product states which it needs,
and `open_transport` refuses a transport that lacks one: a transport that spread one document over three
consumers assembling it would not make bosstax-web's pipeline slower, it would make it wrong.

★ THE RETRY LOOP BELONGS TO THE TRANSPORT. NATS counts deliveries and dead-letters on its own
(MaxDeliver, then a term), Kafka's share groups count acquisitions, and FreeStream counts starts on its
server. So a handler only returns or raises, and each transport keeps the promise in `DEAD_LETTER` its
own way, rather than one loop above every transport driving the calls of one of them.

★ A HANDLER MAY ASK TO BE TRIED AGAIN LATER (2026-10-12). A handler that raises `RetryLater(delay)` has
its record handed to its group again no sooner than `delay` from now, and the records behind it are not
held up while it waits. clarifile asked for it for webhook deliveries to a firm's own endpoint, which can
be down for hours: it backs off from thirty seconds to six hours over about twelve attempts, and one
firm's dead endpoint must never delay another firm's deliveries. A registration opts in
(`delayed_retry=True`) and sets its own `max_attempts`; a plain raise is retried at once, as before. The
bosstax-web's pipeline neither raises it nor requires `DELAYED_RETRY`. NATS keeps the promise with
`NakWithDelay`, and FreeStream with a retry topic whose records it hands out only once they are due
(`freestream/consumer_group.py`).
"""
from __future__ import annotations

import asyncio
import importlib
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Iterable
from datetime import timedelta
from enum import StrEnum
from typing import ClassVar, Self
from urllib.parse import urlsplit

from .channel import Channel
from .record import StreamRecord

#: A handler sees one record. A handler that returns has acknowledged it, and one that raises has refused it.
Handler = Callable[[StreamRecord], Awaitable[None]]

#: What a hop does with a record it has given up on, once it is dead-lettered: the record and how many
#: times it was started.
Exhausted = Callable[[StreamRecord, int], Awaitable[None]]

#: How many times a record may be started before it is given up on, unless its registration says
#: otherwise. Three tells a transient failure (a server restart, a pool timeout) from a record that will
#: fail every time, which is what the pipeline's stages need; a webhook retried over hours asks for more.
DEFAULT_MAX_ATTEMPTS = 3


class Guarantee(StrEnum):
    """One promise a transport keeps about the records it carries."""

    #: On a KEYED channel, every record carrying one key reaches one consumer of a group, until that
    #: consumer closes the key (`close_key`).
    KEY_AFFINITY = "key_affinity"
    #: What a handler produced, and the commit of the record it handled, are written together or not at
    #: all. A handler that dies partway leaves nothing in the next channel, and its retry writes the only
    #: copy.
    OUTPUTS_WITH_COMMIT = "outputs_with_commit"
    #: A produce the transport sends again, after its answer was lost, is written once.
    PRODUCED_ONCE = "produced_once"
    #: A record that fails every allowed attempt is written to `<channel>.dead`, the hop's `on_exhausted`
    #: runs, and the record is never started again.
    DEAD_LETTER = "dead_letter"
    #: A handler that raises `RetryLater(delay)` is handed its record again no sooner than `delay` from
    #: now, counted toward its registration's `max_attempts`, and no record behind it waits meanwhile.
    DELAYED_RETRY = "delayed_retry"


class RetryLater(Exception):  # noqa: N818 — it is a request, and its name says what it asks for
    """A handler raises this to have its record handed to its group again, no sooner than `delay`.

    The attempt counts toward the registration's `max_attempts`; past it the record is dead-lettered and
    `on_exhausted` runs, as for any record that keeps failing. Only a registration made with
    `delayed_retry=True` may raise it; from any other it counts as an ordinary failure.
    """

    def __init__(self, delay: timedelta) -> None:
        super().__init__(f"try again in {delay}")
        self.delay = delay


class EventStreamUnavailableError(Exception):
    """The transport could not reach its queue.

    Raised rather than swallowed, because a transport has no business deciding on a caller's behalf that
    work may be lost. The caller decides, and for an upload that means telling the person now.
    """


class TransportLacksGuaranteeError(Exception):
    """The address named a transport that does not keep a guarantee the pipeline requires."""


class UnknownTransportError(ValueError):
    """The address's scheme names no transport we have."""


class EventStreamTransport(ABC):
    """One process's connection to the event stream: what it produces, and the handlers it runs."""

    #: The promises this transport keeps. See `Guarantee`.
    guarantees: ClassVar[frozenset[Guarantee]]

    @classmethod
    @abstractmethod
    def open(cls, url: str, *, role: str) -> Self:
        """Make the transport for this address, without reaching the queue yet.

        `role` says which part of the pipeline this process is, for whatever the queue shows its operator.
        """

    @abstractmethod
    async def ensure_reachable(self) -> None:
        """Reach the queue now, rather than on the first produce. Raises `EventStreamUnavailableError`.

        The API calls it at startup, and a maintenance tool before it touches anything, so that a queue
        nobody can reach is said once and loudly instead of document by document.
        """

    async def produce(self, channel: Channel, *, value: bytes, key: str | None = None,
                      headers: dict[str, str] | None = None) -> None:
        """Put one record on a channel.

        Inside a handler the record is held and written with the commit of the record being handled
        (`Guarantee.OUTPUTS_WITH_COMMIT`); the caller writes the same call either way.

        The channel's declaration is checked here, for every transport, before the record goes anywhere
        (`Channel.check_key`).
        """
        channel.check_key(key)
        await self._produce(channel, value=value, key=key, headers=dict(headers or {}))

    @abstractmethod
    async def _produce(self, channel: Channel, *, value: bytes, key: str | None,
                       headers: dict[str, str]) -> None:
        """Send a record that has already passed its channel's check."""

    @abstractmethod
    def register(self, channel: Channel, *, group: str, handler: Handler,
                 on_exhausted: Exhausted | None = None, max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 delayed_retry: bool = False) -> None:
        """Declare that `handler` reads `channel` as a member of `group`. Runs once `run` is awaited.

        `max_attempts` is how many times a record may be started before it is dead-lettered. With
        `delayed_retry`, the handler may raise `RetryLater`, and the transport must keep `DELAYED_RETRY`.
        """

    @abstractmethod
    async def run(self, *, stop: asyncio.Event | None = None) -> None:
        """Run every registered handler until `stop` is set.

        A handler that fails is the transport's to retry and, in the end, to dead-letter. The transport
        failing — its queue gone for good — ends the lot and raises, so the process exits and systemd
        starts a fresh one, rather than half the handlers carrying on silently.
        """

    @abstractmethod
    async def close_key(self, channel: Channel, *, group: str, key: str) -> None:
        """Say this consumer has finished with a key: on a keyed channel, a document done."""

    @abstractmethod
    async def define(self, channels: Iterable[Channel]) -> list[str]:
        """Make the queue hold these channels at the shape they declare — the deploy step.

        Applied like a migration: a channel already held at the same shape changes nothing, so this runs
        on every deploy, and a channel held at a different shape raises with the queue's reason, because
        a keyed channel cannot change width under the data it holds. Returns the queue's answers.
        """

    @abstractmethod
    async def close(self) -> None:
        """Let go of the queue. Safe to call twice."""


#: Each transport, by the scheme of the address that names it. We name each by its import path, so this
#: module loads no implementation and a process loads only the transport its address names.
TRANSPORTS: dict[str, str] = {
    "freestream": "freestream.transport:FreeStreamTransport",
}


def open_transport(url: str, *, role: str, requires: Iterable[Guarantee]) -> EventStreamTransport:
    """The transport the address names, refused unless it keeps every guarantee in `requires`."""
    scheme = urlsplit(url).scheme
    if scheme not in TRANSPORTS:
        raise UnknownTransportError(
            f"the event stream address {url!r} names no transport we have; the schemes are "
            f"{', '.join(sorted(TRANSPORTS))}")
    module_name, class_name = TRANSPORTS[scheme].split(":")
    transport_class: type[EventStreamTransport] = getattr(importlib.import_module(module_name), class_name)
    missing = set(requires) - transport_class.guarantees
    if missing:
        raise TransportLacksGuaranteeError(
            f"the {scheme} transport does not keep {', '.join(sorted(missing))}, which the pipeline "
            f"requires")
    return transport_class.open(url, role=role)
