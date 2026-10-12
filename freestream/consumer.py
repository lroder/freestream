"""The consumer loop that keeps FreeStream's promises to a handler (#450).

    transport.register(PAGES_TO_READ, group="readers", handler=read_one_page)
    await transport.run(stop=stop)      # run_consumers: one task per handler, each awaiting its next record

`FreeStreamTransport` holds the registrations and hands them here. The loop is FreeStream's own, not
the pipeline's: it counts starts with the server, completes with the server, and dead-letters through
the server, so another queue keeps the same promises its own way (`freestream.interface.transport`).

★ A HANDLER SEES ONE RECORD AND KNOWS NOTHING ELSE. No offsets, no partitions, no acknowledgement —
returning is the acknowledgement, and raising is the refusal. That is not sugar: a handler that could
commit its own offset could commit one it had not finished, and a handler that could see its partition
would eventually make a decision from it. The stage that reads a page must not be able to care which
partition it came from.

★ A RECORD THAT FAILS EVERY TIME STOPS BEING TRIED. After MAX_ATTEMPTS starts — counted by the server,
so a record that crashes its process is counted too — it goes to `<channel>.dead`, the hop's
`on_exhausted` closes it out, and the offset moves past it. Before this, one poison record held its
partition for good, and with it every later record the partition would ever carry.

★ A HANDLER THAT RAISES `RetryLater` IS TRIED AGAIN AFTER ITS DELAY, AND HOLDS NOTHING UP MEANWHILE
(2026-10-12). Its record is written to `<channel>.retry` with the time it may run (`retry.not_before`), how
many starts it has had (`retry.attempts`) and its key (`retry.key`), in the same COMPLETE that commits the
original, and a second registration on the retry topic hands it back to the same handler once the
server judges it due. The handler sees the record as it was produced, key and headers as they were, and
the limit counts the starts on every hop, so a record retried eleven times and failing a twelfth is
dead-lettered to `<channel>.dead` like any other.

★ THE OFFSET MOVES ONLY AFTER THE HANDLER RETURNS, one record at a time, and together with whatever the
handler produced (`Consumer.handle`). So a record the group completed is never handed to it again, and a
consumer that dies mid-record sees that record again with nothing of its first attempt left in the next
topic. What the handler wrote anywhere else, the database above all, is its own to make idempotent: the
page write is on (run, page) and the carve on (run, first_page). A consumer that committed a batch up
front would be faster, and it would silently turn one crash into a batch of work nobody ever does.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, replace
from datetime import timedelta

from .client import FreeStreamClient, Output
from .interface.channel import Channel
from .interface.transport import DEFAULT_MAX_ATTEMPTS, Exhausted, Handler, RetryLater
from .record import (
    RETRY_ATTEMPTS_HEADER,
    RETRY_HEADER_PREFIX,
    RETRY_KEY_HEADER,
    RETRY_NOT_BEFORE_HEADER,
    Record,
)
from .registry import DEAD_SUFFIX, RETRY_SUFFIX, PartitionNotHeldError, RequestRefusedError

log = logging.getLogger(__name__)

#: How many times a record may be STARTED before its consumer stops trying, unless its registration says
#: otherwise. Three distinguishes a transient failure — a server restart, a pool timeout — from a record
#: that will fail every time.
MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS


def dead_letter_channel(channel: str) -> str:
    """Where a channel's abandoned records go. Readable, and listed by the admin socket."""
    return channel + DEAD_SUFFIX


def retry_channel(channel: str) -> str:
    """Where a channel's records wait out a `RetryLater`."""
    return channel + RETRY_SUFFIX


@dataclass(frozen=True, slots=True)
class Registration:
    """One handler, the channel and group it reads, and what it does when a record cannot be done.

    `on_exhausted` is the hop's own terminal state for a record it gave up on. The generic answer is
    the dead-letter channel, which every exhausted record goes to regardless — but a dead letter is
    somewhere a person looks, and for some hops that is not enough: a page that can never be read
    still has to REACH the aggregator, or the document it belongs to waits for a page that is never
    coming and fails as a whole three minutes later. The blueprint's own rule — every promised message
    reaches a terminal state — is kept by the hop that knows what "terminal" means for it.
    """

    channel: Channel
    group: str
    handler: Handler
    on_exhausted: Exhausted | None = None
    #: How many starts a record may have, across every hop, before it is dead-lettered.
    max_attempts: int = MAX_ATTEMPTS
    #: Whether the handler may raise `RetryLater`. From any other handler it counts as a failure.
    delayed_retry: bool = False
    #: Set on the registration that reads a channel's retry topic: the channel whose records it retries,
    #: which is where they are dead-lettered and written again if they ask to wait once more.
    retries_of: str | None = None

    @property
    def source(self) -> str:
        """The channel this registration's records were produced to, whether it reads it or its retries."""
        return self.retries_of or self.channel.name


class Consumer:
    """One handler's member of its group: awaits a record, runs the handler, moves the offset behind it."""

    def __init__(self, registration: Registration, client: FreeStreamClient) -> None:
        self._registration = registration
        self._client = client

    async def run(self) -> None:
        """Handle records for as long as the process runs. Idle, it asks the server for nothing."""
        channel = self._registration.channel
        async for partition, record in self._client.subscribe(channel.name, self._registration.group):
            await self.handle(partition, record)

    async def drain(self) -> int:
        """Handle what is waiting NOW, without waiting for more, and say how many were handled.

        For a test, or a one-shot tool, that wants the channel emptied and control back — the same
        handling `run` does, one record at a time and committing after each. It stops at a record whose
        handler fails, because that record is not committed and so would be the next one again.
        """
        channel = self._registration.channel
        handled = 0
        while taken := await self._client.next(channel.name, self._registration.group, wait=False):
            if not await self.handle(*taken):
                break
            handled += 1
        return handled

    async def handle(self, partition: int, record: Record) -> bool:
        """Run one record, and complete it unless its handler failed. Says whether it was completed.

        A handler that raises leaves the record uncommitted, so it is the next one this consumer is
        handed — rather than skipped, because a stage that silently drops what it cannot process is a
        document nobody is told about. After MAX_ATTEMPTS starts it is dead-lettered and committed.

        ★ WHAT THE HANDLER PRODUCED AND THE COMMIT OF ITS RECORD GO TOGETHER, as one COMPLETE. Every
        produce the handler makes on this client is held (`FreeStreamClient.handling`) and sent with the
        commit; the server writes all of it or none of it. So a handler that dies partway, or a consumer
        that loses its partition before it finishes, leaves nothing in the next topic, and the retry
        produces the only copy. Until 2026-09-25 the outputs went out as the handler made them, and a
        page reader killed between publishing a page and committing it published the page twice.
        """
        registration = self._registration
        channel, group = registration.channel.name, registration.group
        try:
            attempts = _earlier_starts(record) + await self._client.attempt(
                channel, group, partition, record.offset)
            if attempts > registration.max_attempts:
                await self._give_up_on(partition, record, attempts - 1)
                return True
            later: RetryLater | None = None
            with self._client.handling() as handling:
                try:
                    await registration.handler(as_produced(record))
                except RetryLater as asked:
                    if not registration.delayed_retry:
                        log.exception("handler for %s raised RetryLater but was registered without "
                                      "delayed_retry, so it counts as a failure", channel)
                        return False
                    later = asked
                except Exception:
                    log.exception("handler for %s failed at partition %d offset %d (attempt %d of %d)",
                                  channel, partition, record.offset, attempts, registration.max_attempts)
                    return False
            if later is None:
                outputs = handling.outputs
            elif attempts >= registration.max_attempts:
                await self._give_up_on(partition, record, attempts)
                return True
            else:
                # What the handler produced before it asked to wait belongs to an attempt that did not
                # finish, so it goes nowhere; the retry makes its own.
                outputs = [self._to_retry(record, attempts, later.delay)]
            await self._client.complete(channel, group, partition, record.offset + 1, outputs)
        except PartitionNotHeldError:
            # The server restarted, or this connection dropped, while the record was in hand, and the
            # client came back as a new member that holds nothing. Whoever holds the partition now is
            # handed the record from the group's position, so this consumer lets it go and asks for
            # its next one. What the handler produced goes with it, never having been written.
            log.info("%s partition %d changed hands before offset %d was completed; its holder now "
                     "will be handed it", channel, partition, record.offset)
            return False
        except RequestRefusedError:
            # The server refused an output — a topic nobody defined, a key the topic forbids. That is a
            # defect in the handler, and it counts as a failed attempt like any other.
            log.exception("%s offset %d: the server refused what its handler produced", channel,
                          record.offset)
            return False
        return True

    async def _give_up_on(self, partition: int, record: Record, attempts: int) -> None:
        """Dead-letter a record that has failed every attempt, let its hop close it out, and complete it.

        Checked BEFORE the handler runs, so a record that kills its process is never run a fourth time:
        the count is the server's, and it survived every one of those deaths.

        ★ ONE COMPLETE CARRIES THE DEAD LETTER, WHATEVER THE HOP PRODUCED CLOSING IT OUT, AND THE COMMIT.
        A hop closes a record out by writing to the database, and a page reader by publishing the page as
        unreadable. A close-out that raises is logged, and what it had produced is dropped; the dead
        letter and the commit still go, so the record is dead-lettered once and never retried again.
        Before COMPLETE, the commit had to come first for that reason, and a close-out's own output went
        out on its own, able to be published twice.
        """
        channel, group = self._registration.channel.name, self._registration.group
        dead_letters = dead_letter_channel(self._registration.source)
        log.error("%s offset %d failed %d attempt(s) — dead-lettered to %s", channel, record.offset,
                  attempts, dead_letters)
        produced = as_produced(record)
        # A dead-letter topic is COMPETING and refuses a key, so the key the record carried rides in a
        # header — kept, so whoever replays the record knows which document it belonged to.
        dead_headers = {**produced.headers, "dead.from_offset": str(record.offset),
                        "dead.attempts": str(attempts), "dead.group": group}
        if produced.key is not None:
            dead_headers["dead.key"] = produced.key.decode()
        outputs = [Output(dead_letters, record.value, None, dead_headers)]
        if self._registration.on_exhausted is not None:
            with self._client.handling() as closing_out:
                try:
                    await self._registration.on_exhausted(produced, attempts)
                except Exception:
                    log.exception("%s offset %d is dead-lettered, but its hop could not close it out",
                                  channel, record.offset)
                    closing_out.outputs.clear()
            outputs += closing_out.outputs
        await self._client.complete(channel, group, partition, record.offset + 1, outputs)


    def _to_retry(self, record: Record, attempts: int, delay: timedelta) -> Output:
        """The record written to its channel's retry topic, to run again no sooner than `delay` from now."""
        produced = as_produced(record)
        headers = {**produced.headers, RETRY_ATTEMPTS_HEADER: str(attempts),
                   RETRY_NOT_BEFORE_HEADER: str(time.time_ns() + int(delay.total_seconds() * 1e9))}
        if produced.key is not None:
            headers[RETRY_KEY_HEADER] = produced.key.decode()
        log.info("%s offset %d asked to wait %s (attempt %d of %d)", self._registration.channel.name,
                 record.offset, delay, attempts, self._registration.max_attempts)
        return Output(retry_channel(self._registration.source), record.value, None, headers)


def _earlier_starts(record: Record) -> int:
    """How many starts a record had on the hops before this one: none, unless it is a retry."""
    try:
        return int(record.headers.get(RETRY_ATTEMPTS_HEADER, 0))
    except ValueError:
        return 0


def as_produced(record: Record) -> Record:
    """The record as its producer wrote it: a retry's key restored, and the retry's own headers gone."""
    if not any(name.startswith(RETRY_HEADER_PREFIX) for name in record.headers):
        return record
    key = record.headers.get(RETRY_KEY_HEADER)
    return replace(record, key=key.encode() if key is not None else record.key,
                   headers={name: value for name, value in record.headers.items()
                            if not name.startswith(RETRY_HEADER_PREFIX)})


async def run_consumers(client: FreeStreamClient, declared: list[Registration], *,
                        stop: asyncio.Event | None = None) -> None:
    """Run every registered handler until `stop` is set — a task each, all on the one connection.

    A consumer that fails — the server gone past its one reconnect — ends the lot and raises, so the
    process exits and systemd starts a fresh one, rather than half the handlers carrying on silently.
    """
    members = [(registration.channel.name, registration.group) for registration in declared]
    if len(set(members)) != len(members):
        # A member of a group IS a connection, so two handlers for one group on one client would be
        # one member asking for two records at once — which the server refuses by dropping the
        # connection, and the reconnect would meet the same refusal.
        raise ValueError(f"two handlers for the same channel and group on one connection: {members}")
    stop = stop if stop is not None else asyncio.Event()
    running = [asyncio.create_task(Consumer(registration, client).run()) for registration in declared]
    stopping = asyncio.create_task(stop.wait())
    try:
        done, _pending = await asyncio.wait([stopping, *running], return_when=asyncio.FIRST_COMPLETED)
        for finished in done:
            if finished is not stopping:
                finished.result()
    finally:
        for task in [stopping, *running]:
            task.cancel()
        await asyncio.gather(stopping, *running, return_exceptions=True)
