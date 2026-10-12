"""A strand: one thread, its own event loop, and the partitions it owns outright (#450).

The event processing system must be fully multithreaded (2026-09-25). The server was one asyncio
loop, so it used one core under any load, and the free-threaded interpreter it runs on went unused. It is
now a set of strands, each a thread with its own loop, the design Redpanda runs on Seastar:

  · every partition of every topic belongs to exactly one strand (`EventStreamServer.strand_for`), and
    that strand alone touches its log and each group's state for it: position, acked set, attempt
    counts, the file lock that says which consumer holds it, and the consumers waiting on it;
  · every client connection is homed on one strand, which reads its requests and writes its answers;
  · a strand reaches another only by message — it runs a function on the other strand's loop and awaits
    the result (`ask`), or hands it one to run (`tell`). No lock is shared between strands.

So inside a strand nothing interleaves, exactly as inside the one loop it replaced, and the consumer-group
logic that was correct because of that runs unchanged over each strand's partitions: one ConsumerGroup
per consumer per strand, a sub-member, holding only that strand's partitions.

★ ONE RECORD PER NEXT, BY MESSAGE AND NOT BY LOCK (2026-09-26). A consumer waiting for a record
waits on every strand at once, and two strands can find a record for it at the same moment. Each offers
its record to the consumer's home strand; the home accepts the first, while the NEXT is still held, and
declines the rest. A declined record stays where it was: handed to that strand's sub-member, uncommitted,
in a partition the consumer now holds, so the consumer's next NEXT asks that strand first and receives it.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import inspect
import logging
import shutil
import sys
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field, replace
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .consumer_group import ConsumerGroup, GroupPositions, MemberState, committed_by_every_group
from .durability import Durability
from .journal import JournalEntry, JournalOutput, write_entry
from .protocol import PREFIX_BYTES, Frame, ProtocolError, read_prefix
from .record import Record
from .registry import PartitionNotHeldError, RequestRefusedError, TopicError, is_valid_name
from .topic import Topic, partition_for

if TYPE_CHECKING:
    from .server import EventStreamServer

log = logging.getLogger(__name__)

#: (connection, channel, group): one consumer's membership of one group.
Member = tuple[int, str, str]

#: The least time between two retention checks of one partition. See `Strand._retain`.
RETENTION_CHECK_SECONDS = 1.0

#: How long a dead letter is kept. Nobody consumes a dead-letter topic as a pipeline stage — a person
#: reads it, and replays what they choose — so no group's commits ever let it shrink, and retention for it
#: goes by age instead (2026-09-25: thirty days).
DEAD_LETTER_RETENTION_SECONDS = 30 * 24 * 3600

#: How often each strand looks for dead letters past that age.
DEAD_LETTER_SWEEP_SECONDS = 3600

#: How long a client may leave unread what it was sent before the server drops it. A held NEXT's answer is
#: written without waiting on the socket (`_send`), so a client that stops reading keeps a whole record in
#: this process's memory; dropped, it frees that and its uncommitted record goes to another member.
OUTBOUND_STALL_SECONDS = 30

#: How often each strand looks for those clients.
OUTBOUND_CHECK_SECONDS = 5

#: How long a client may take to send the rest of a frame once its prefix has arrived. The wait for a
#: prefix has no limit, because a consumer between records is idle by design; the rest does, because a
#: client that announces a body and stalls holds a connection and a partly read buffer for good.
FRAME_BODY_SECONDS = 30

#: How long a strand waits before applying a journal entry again whose outputs did not land, and the
#: longest it waits between attempts after that.
FINISH_FIRST_WAIT_SECONDS = 1.0
FINISH_LONGEST_WAIT_SECONDS = 30.0

#: How long `Strand.stop` waits for the thread to finish before giving up on it.
STOP_SECONDS = 10


class _Offered(Enum):
    """What asking one waiting sub-member came to (`Strand._answer_if_it_can_be`)."""

    OFFERED = "offered"
    RELEASED = "released"
    NOTHING = "nothing"


@dataclass
class Connection:
    """Who is on the other end of one connection, as they said and as the server saw them.

    `role`, `name`, `pid` and `host` come from the client's HELLO; the rest the server keeps. A client
    that never says hello is still a connection — shown as `unknown` — because identity is for the
    people reading the admin socket, not a gate on what a connection may do.
    """

    connection_id: int
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    requests: int = 0
    role: str = "unknown"
    name: str = ""
    pid: int | None = None
    host: str = ""
    state: str = MemberState.READY


class Strand:
    """One thread and its event loop: a share of every topic's partitions, and the connections homed here."""

    def __init__(self, number: int, server: EventStreamServer) -> None:
        self.number = number
        self._server = server
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name=f"eventstream-strand-{number}",
                                        daemon=True)
        self._periodic: list[asyncio.Task[None]] = []
        #: This strand's flush barrier: what it writes is on the disk before it answers (`durability.py`).
        self.durability = Durability(self.loop)
        # ---- what this strand owns: its partitions, and everything about them
        #: The topics opened over this strand's partitions, by name.
        self._topics: dict[str, Topic] = {}
        #: Each consumer's sub-member here: its ConsumerGroup over this strand's partitions only.
        self._memberships: dict[Member, ConsumerGroup] = {}
        #: Sub-members waiting for a record from this strand, on behalf of a held NEXT at their home.
        self._waiting: set[Member] = set()
        #: Each group's place in each of this strand's partitions.
        self._positions = GroupPositions()
        #: (channel, partition) -> when retention last looked at it.
        self._retained_at: dict[tuple[str, int], float] = {}
        #: retry channel -> when its waiters are next woken, and the timer that will wake them: the earliest
        #: time a record waiting out its delay falls due (`_wake_when_due`).
        self._wakes: dict[str, tuple[int, asyncio.TimerHandle]] = {}
        # ---- the connections homed here
        self._connections: dict[int, Connection] = {}
        self._writers: dict[int, asyncio.StreamWriter] = {}
        #: Members whose consumer has made a NEXT, homed here.
        self._members: set[Member] = set()
        #: member -> the id of its NEXT, while it is held.
        self._pending: dict[Member, Any] = {}
        #: member -> the strands where it holds a partition, asked first by its next NEXT.
        self._holding: dict[Member, set[int]] = {}
        #: connection -> when an answer written to it without waiting first left bytes unsent.
        self._unsent_since: dict[int, float] = {}
        #: Spreads a keyless record with no producer sequence; only a direct write has neither.
        self._spread = 0
        #: Answers waiting for a flush before they are sent (`_keep_answering`).
        self._answering: set[asyncio.Task[None]] = set()
        #: Journal entries still being finished: removed after their commit is flushed, or applied again
        #: after their outputs failed to land (`_finish_entry`).
        self._finishing: set[asyncio.Task[None]] = set()

    # ================================================================== running

    def start(self) -> None:
        self._thread.start()
        self.submit(self._start_periodic).result(timeout=STOP_SECONDS)

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.durability.install()
        self.loop.run_forever()
        self.loop.close()

    def _start_periodic(self) -> None:
        self._periodic = [self.loop.create_task(self._sweep_dead_letters_periodically()),
                          self.loop.create_task(self._drop_stalled_readers_periodically())]

    def stop(self) -> None:
        """Close this strand's connections, stop its loop, and wait for the thread to end."""
        if not self._thread.is_alive():
            return
        with contextlib.suppress(concurrent.futures.TimeoutError, RuntimeError):
            asyncio.run_coroutine_threadsafe(self._shut_down(), self.loop).result(STOP_SECONDS)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(STOP_SECONDS)

    async def _shut_down(self) -> None:
        # ★ THE SERVER CLOSES ITS CLIENTS' SOCKETS ITSELF. A connected reader never ends its own
        # connection: it sits in a read waiting for the next answer. A client whose socket closes
        # reconnects, which is what it does after any server restart.
        for task in self._periodic:
            task.cancel()
        for _due_ns, wake in self._wakes.values():
            wake.cancel()
        # A clean shutdown gives every partition back, as a crash does through the kernel, so a server
        # started again in the same process (a test, or a tool) can take them.
        for membership in self._memberships.values():
            membership.release()
        for writer in list(self._writers.values()):
            writer.close()
        pending = [task for task in asyncio.all_tasks(self.loop) if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    # ================================================================== between strands

    def submit(self, function: Callable[..., Any], *args: Any) -> concurrent.futures.Future[Any]:
        """Run `function` on this strand's loop, from any thread; a coroutine function is awaited there."""
        if inspect.iscoroutinefunction(function):
            return asyncio.run_coroutine_threadsafe(function(*args), self.loop)
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()

        def run() -> None:
            try:
                future.set_result(function(*args))
            except BaseException as failure:  # noqa: BLE001 — handed to whoever awaits the future
                future.set_exception(failure)

        self.loop.call_soon_threadsafe(run)
        return future

    async def ask(self, other: Strand, function: Callable[..., Any], *args: Any) -> Any:
        """Run `function` on `other` and await its answer; on this strand itself, call it directly."""
        if other is self:
            answer = function(*args)
            return await answer if inspect.isawaitable(answer) else answer
        return await asyncio.wrap_future(other.submit(function, *args))

    def tell(self, other: Strand, function: Callable[..., Any], *args: Any) -> None:
        """Hand `other` a function to run on its loop, and carry on without waiting for it."""
        other.loop.call_soon_threadsafe(function, *args)

    # ================================================================== the partitions this strand owns

    def _topic(self, name: str) -> Topic:
        """This strand's opening of a defined topic: the partitions of it that this strand owns."""
        if name not in self._topics:
            definition = self._server.registry.get(name)
            self._topics[name] = Topic(
                self._server.root / name, partitions=definition.partitions,
                owned=self._server.partitions_of(self.number, name, definition.partitions),
                seal=self._server.seal)
        return self._topics[name]

    def _membership(self, member: Member, state: str) -> ConsumerGroup:
        """This consumer's sub-member here, made the first time it asks this strand for anything."""
        if member not in self._memberships:
            _connection, channel, group = member
            definition = self._server.registry.get(channel)
            membership = ConsumerGroup(
                self._topic(channel), group, self._server.root / channel,
                keeps_its_partitions=definition.keeps_its_partitions,
                due_by_header=definition.is_retries, positions=self._positions)
            membership.state = state
            self._memberships[member] = membership
        return self._memberships[member]

    def _held_membership(self, member: Member, partition: int) -> ConsumerGroup:
        membership = self._memberships.get(member)
        if membership is None:
            raise PartitionNotHeldError(f"partition {partition} is not held by this consumer")
        return membership

    def append(self, channel: str, partition: int, record: Record, producer: str | None,
               sequence: int | None) -> int:
        """Append one record to a partition this strand owns; it is offered to a consumer once flushed.

        The offset comes back at once, so a connection's produces are written in the order it sent them;
        whoever answers the producer awaits `flushed` first (`_produce`).
        """
        topic = self._topic(channel)
        if producer is not None and sequence is not None:
            offset = topic.produce_once_to(partition, record, producer, sequence)
        else:
            offset = topic.produce_to(partition, record)
        self.durability.when_flushed(lambda: self._offer_flushed_record(channel, partition))
        return offset

    def _offer_flushed_record(self, channel: str, partition: int) -> None:
        self._offer_new_record(channel, partition)
        self._server.notice_a_change()

    async def flushed(self) -> None:
        """Return once everything this strand has written so far is on the disk."""
        await self.durability.barrier()

    def poll_member(self, member: Member, state: str,
                    wait: bool) -> tuple[tuple[int, Record] | None, bool]:
        """One record for this consumer from this strand's partitions, and whether it holds any here.

        With nothing to hand it and `wait`, the sub-member waits here, in the same step, so a record
        produced a moment later is offered to it rather than missed between the look and the wait.
        """
        membership = self._membership(member, state)
        if member in self._waiting:
            self._waiting.discard(member)
        taken = membership.poll(limit=1)
        if taken:
            return taken[0], True
        if wait:
            self._waiting.add(member)
            self._wake_when_due(member[1], membership)
        return None, bool(membership.owned)

    def oldest_waiting(self, channel: str, group: str) -> int | None:
        """When the oldest record a group has not read in this strand's partitions was written."""
        return ConsumerGroup(self._topic(channel), group, self._server.root / channel,
                             positions=self._positions).oldest_waiting_ns()

    def let_go(self, member: Member) -> None:
        """A competing consumer took its record elsewhere: hand back whatever it holds here.

        It commits what it was handed before it asks again, so nothing is in hand; a partition it went on
        holding here would only keep another consumer from the next record in it.
        """
        membership = self._memberships.get(member)
        if membership is not None and not membership.keeps_its_partitions:
            membership.release()

    def forget_producer(self, channel: str, partition: int, producer: str) -> None:
        self._topic(channel).forget_producer(partition, producer)

    def drop_waiter(self, member: Member) -> None:
        self._waiting.discard(member)

    async def commit(self, member: Member, partition: int, next_offset: int) -> None:
        """Record where a group resumes, on the disk before the consumer is told."""
        self._held_membership(member, partition).commit(partition, next_offset)
        _connection, channel, _group = member
        self._retain(channel, partition)
        await self.durability.barrier()

    async def attempt(self, member: Member, partition: int, offset: int, token: str | None) -> int:
        count = self._held_membership(member, partition).attempt(partition, offset, token)
        await self.durability.barrier()
        return count

    async def complete(self, member: Member, partition: int, next_offset: int,
                       outputs: tuple[JournalOutput, ...]) -> list[list[int]]:
        """Commit a handled record and see every one of its handler's outputs land, exactly once.

        The member must still hold the partition before anything is written. The whole step is then
        journaled, and the journal entry is applied (see `journal.py`). The commit is applied first,
        here, before any output: its partition is this strand's, so nothing can come between the plan
        and the write, and a consumer that leaves while the outputs are carried to their own strands
        cannot have its record handed to another member first. An output lands on the strand that owns
        its partition, by message, under the entry's producer id, so a replay never writes one twice.
        """
        _connection, channel, group = member
        membership = self._held_membership(member, partition)
        plan = membership.plan_commit(partition, next_offset)
        entry = JournalEntry.new(channel, group, plan, outputs)
        path = write_entry(self._server.root, entry, self._server.seal)
        await self.durability.barrier()                   # the entry is on the disk before anything it does
        membership.apply(plan)
        self._retain(channel, partition)
        try:
            placements = await self._server.append_outputs(self, entry)
        except Exception:
            # ★ THE STEP HAPPENED WHEN THE JOURNAL ENTRY WAS WRITTEN, so an output that fails to land now
            # is finished, not abandoned. The consumer is told the COMPLETE succeeded — its record is
            # committed and must not come back — and the strand applies the entry again until its outputs
            # land, which is what a restart's replay would do, without waiting for a restart. An output
            # the failed attempt did write is recognized in its partition and not written twice.
            log.exception("%s/%s: a COMPLETE's outputs did not all land; finishing its journal entry",
                          channel, group)
            self._keep_finishing(self._finish_entry(entry, path))
            return []
        # The consumer has its answer now: the entry on the disk already makes the step happen. The entry goes
        # once the commit is on the disk too, and the COMPLETE does not wait for that second flush.
        self._keep_finishing(self._remove_entry(entry, path))
        return placements

    def _keep_finishing(self, work: Coroutine[Any, Any, None]) -> None:
        """Run a journal entry's remaining work as a task the strand holds until it ends."""
        task = self.loop.create_task(work)
        self._finishing.add(task)
        task.add_done_callback(self._finishing.discard)

    async def _remove_entry(self, entry: JournalEntry, path: Path) -> None:
        """Remove a journal entry whose outputs have landed, once its commit is on the disk."""
        await self.durability.barrier()
        path.unlink(missing_ok=True)
        self._server.forget_journal_producer(self, entry)

    async def _finish_entry(self, entry: JournalEntry, path: Path) -> None:
        """Apply a journal entry's outputs again until they land, then remove the entry."""
        wait = FINISH_FIRST_WAIT_SECONDS
        while True:
            await asyncio.sleep(wait)
            try:
                await self._server.append_outputs(self, entry)
            except Exception:
                log.exception("a journal entry's outputs still did not land; trying again in %.0fs", wait)
                wait = min(wait * 2, FINISH_LONGEST_WAIT_SECONDS)
                continue
            await self._remove_entry(entry, path)
            log.info("a journal entry's outputs have landed")
            return

    def close_key(self, member: Member, key: bytes) -> None:
        """A consumer has finished with a key: what lets a DRAINING sub-member give its partitions up."""
        membership = self._memberships.get(member)
        if membership is not None:
            membership.close_key(key)
            self._offer(member[1])

    def forget(self, connection: int) -> None:
        """Release every sub-member of a departed connection, so the next consumer can take its partitions."""
        channels = set()
        for member in [held for held in self._memberships if held[0] == connection]:
            self._memberships.pop(member).release()
            self._waiting.discard(member)
            channels.add(member[1])
        for channel in channels:
            self._offer(channel)

    def set_state(self, connection: int, state: str) -> None:
        channels = set()
        for member, membership in self._memberships.items():
            if member[0] == connection:
                membership.state = state
                channels.add(member[1])
        for channel in channels:
            self._offer(channel)

    def has_members(self, channel: str, group: str | None = None) -> bool:
        return any(held_channel == channel and group in (None, held_group)
                   for (_connection, held_channel, held_group) in self._memberships)

    def remove_topic_files(self, name: str, partitions: int) -> None:
        """Delete this strand's partitions of a topic being removed, and every group's files for them.

        The topic is no longer defined by now, so its width comes from the caller.
        """
        self._topics.pop(name, None)
        for partition in self._server.partitions_of(self.number, name, partitions):
            shutil.rmtree(self._server.root / name / f"{partition:04d}", ignore_errors=True)
        for group_home in (self._server.root / name / "groups").glob("*"):
            self.remove_group_files(name, group_home.name, partitions)

    def remove_group_files(self, channel: str, group: str, partitions: int) -> None:
        """Delete a retired group's files for this strand's partitions, and forget its positions here."""
        home = self._server.root / channel / "groups" / group
        self._positions.forget(home)
        for partition in self._server.partitions_of(self.number, channel, partitions):
            for path in home.glob(f"{partition:04d}.*"):
                path.unlink(missing_ok=True)

    # ---- waking the consumers that wait on this strand

    def _offer(self, channel: str) -> None:
        """Offer every waiting sub-member on a channel whatever it can now be handed.

        For a change in who holds what — a consumer gone, a key closed, a state set. A produce adds one
        record to one partition and uses `_offer_new_record`, which asks only who could take it.

        ★ IN PASSES, UNTIL A PASS CHANGES NOTHING. A member that is draining or in maintenance lets its
        partitions go when it is asked, and a waiter asked earlier in the same pass may be the one that
        could take them.
        """
        changed = True
        while changed:
            changed = False
            for member in [held for held in self._waiting if held[1] == channel]:
                if self._answer_if_it_can_be(member) is not _Offered.NOTHING:
                    changed = True

    def _offer_new_record(self, channel: str, partition: int) -> None:
        """Offer one new record in `partition` to the one waiter per group that can take it.

        ★ ONE RECORD ANSWERS AT MOST ONE WAITER PER GROUP, so only the sub-members that could take it
        are asked: the one that holds the partition, if it is waiting — and if it holds it and is busy,
        it reads the record on its next NEXT and nobody else can have it — or, when nobody holds it, the
        waiters in turn until one takes it (the adversarial review, 2026-09-25, #11). A waiter that lets
        partitions go when asked is a change in who holds what, and the whole-channel pass runs.
        """
        groups = {group for (_connection, held_channel, group) in self._waiting if held_channel == channel}
        for group in groups:
            members = [member for member in self._memberships
                       if member[1] == channel and member[2] == group]
            holder = next((member for member in members
                           if self._memberships[member].holds(partition)), None)
            if holder is not None:
                if holder in self._waiting:
                    self._answer_if_it_can_be(holder)
                continue
            for member in [member for member in members if member in self._waiting]:
                offered = self._answer_if_it_can_be(member)
                if offered is _Offered.RELEASED:
                    self._offer(channel)
                    return
                if offered is _Offered.OFFERED:
                    break

    def _answer_if_it_can_be(self, member: Member) -> _Offered:
        """Poll one waiting sub-member, and offer its home any record the poll handed it."""
        membership = self._memberships[member]
        holding = len(membership.owned)
        taken = membership.poll(limit=1)
        if taken:
            self._waiting.discard(member)
            home = self._server.home_of(member[0])
            self.tell(home, home.accept_offer, member, self.number, *taken[0])
            return _Offered.OFFERED
        self._wake_when_due(member[1], membership)
        return _Offered.RELEASED if len(membership.owned) < holding else _Offered.NOTHING

    def _wake_when_due(self, channel: str, membership: ConsumerGroup) -> None:
        """On a retry topic, wake this channel's waiters when the next waiting record falls due.

        Nothing is produced when a delay runs out, so nothing else would offer the record to anybody: a
        timer stands in for the produce. One per channel, at the earliest time any poll here found, and a
        waiter that finds nothing when it fires sets the next one.
        """
        due_ns = membership.next_due_ns
        if due_ns is None:
            return
        scheduled = self._wakes.get(channel)
        if scheduled is not None:
            if scheduled[0] <= due_ns:
                return
            scheduled[1].cancel()
        delay_seconds = max(0.0, (due_ns - time.time_ns()) / 1_000_000_000)
        self._wakes[channel] = (due_ns, self.loop.call_later(delay_seconds, self._wake, channel))

    def _wake(self, channel: str) -> None:
        self._wakes.pop(channel, None)
        self._offer(channel)

    # ---- keeping the partitions small

    def _retain(self, channel: str, partition: int) -> None:
        """Drop whole segments of this partition that every group has read past.

        A commit is the only moment that can make a segment deletable, so it is checked here rather than
        on a timer — and at most once a second per partition, because a reader commits after every page.
        """
        now = time.monotonic()
        if now - self._retained_at.get((channel, partition), 0.0) < RETENTION_CHECK_SECONDS:
            return
        self._retained_at[(channel, partition)] = now
        dropped = self._topic(channel).log(partition).drop_segments_before(
            committed_by_every_group(self._server.root / channel, partition))
        if dropped:
            log.info("%s/%04d: %d segment(s) every group had passed, deleted", channel, partition,
                     dropped)

    def sweep_dead_letters(self, now_ns: int | None = None) -> int:
        """Delete this strand's dead-letter segments whose newest record is past the retention."""
        cutoff_ns = (now_ns or time.time_ns()) - DEAD_LETTER_RETENTION_SECONDS * 1_000_000_000
        dropped = 0
        for definition in self._server.registry.all():
            if definition.is_dead_letters:
                topic = self._topic(definition.name)
                for partition in topic.owned_partitions:
                    dropped += topic.log(partition).drop_segments_older_than(cutoff_ns)
        if dropped:
            log.info("%d dead-letter segment(s) past %d days, deleted", dropped,
                     DEAD_LETTER_RETENTION_SECONDS // 86400)
            self._server.notice_a_change()
        return dropped

    async def _sweep_dead_letters_periodically(self) -> None:
        while True:
            self.sweep_dead_letters()
            await asyncio.sleep(DEAD_LETTER_SWEEP_SECONDS)

    # ---- what the consoles read, as plain data

    def snapshot(self) -> dict[str, Any]:
        """Everything the consoles read from this strand, copied out on its own loop."""
        topics: dict[str, dict[int, dict[str, int]]] = {}
        for definition in self._server.registry.all():
            topic = self._topic(definition.name)
            topics[definition.name] = {
                partition: {"next": topic.log(partition).next_offset,
                            "first": topic.log(partition).first_offset,
                            "bytes": topic.log(partition).bytes_held,
                            "segments": topic.log(partition).segment_count}
                for partition in topic.owned_partitions}
        members = [{"connection": member[0], "channel": member[1], "group": member[2],
                    "state": membership.state, "partitions": membership.owned,
                    "open_keys": len(membership.open_keys),
                    "keys": sorted(key.decode(errors="replace") for key in membership.open_keys),
                    "waiting": member in self._waiting}
                   for member, membership in self._memberships.items()]
        return {"topics": topics, "members": members,
                "connections": [replace(seen) for seen in self._connections.values()],
                "homed": sorted(self._members), "pending": sorted(self._pending)}

    def lag(self, channel: str, group: str) -> dict[int, int]:
        """A group's uncommitted records in each of this strand's partitions of `channel`."""
        return ConsumerGroup(self._topic(channel), group, self._server.root / channel,
                             positions=self._positions).lag()

    def dead_letters(self, name: str) -> list[Record]:
        topic = self._topic(name)
        return [record for partition in topic.owned_partitions
                for record in topic.log(partition).read_from(0)]

    # ================================================================== the connections homed here

    def adopt(self, sock: Any, connection: int) -> None:
        """Take a connection the server accepted, and serve it on this strand's loop."""
        self.loop.create_task(self._serve_connection(sock, connection))

    async def _serve_connection(self, sock: Any, connection: int) -> None:
        """One client, until it goes away. A frame it cannot read ends the connection.

        Dropped rather than skipped: a client and a server that disagree about where a frame ends cannot
        resynchronize, and pretending otherwise would hand the next request a body that belongs to the
        last one.
        """
        reader, writer = await asyncio.open_unix_connection(sock=sock)
        self._writers[connection] = writer
        self._connections[connection] = Connection(connection)
        try:
            while True:
                prefix = await reader.readexactly(PREFIX_BYTES)
                header_len, body_len = read_prefix(prefix)
                async with asyncio.timeout(FRAME_BODY_SECONDS):
                    rest = await reader.readexactly(header_len + body_len)
                response = await self._handle(Frame.decoded(prefix, rest), connection)
                if isinstance(response, Coroutine):
                    self._keep_answering(connection, response)
                elif response is not None:
                    writer.write(response.encoded())
                async with asyncio.timeout(OUTBOUND_STALL_SECONDS):
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass                                        # the client left; ordinary
        except ProtocolError:
            log.exception("dropping a connection that sent a frame we cannot read")
        except TimeoutError:
            log.warning("dropping connection %d: it stalled mid-frame, or stopped reading its answers",
                        connection)
        finally:
            self._forget_connection(connection)
            writer.close()
            with contextlib.suppress(ConnectionResetError, BrokenPipeError):
                await writer.wait_closed()

    def _forget_connection(self, connection: int) -> None:
        for member in [held for held in self._members if held[0] == connection]:
            self._members.discard(member)
            self._pending.pop(member, None)
            self._holding.pop(member, None)
            self._server.count_member(member, -1)
        self._connections.pop(connection, None)
        self._writers.pop(connection, None)
        self._unsent_since.pop(connection, None)
        for strand in self._server.strands:
            self.tell(strand, strand.forget, connection)
        self._server.notice_a_change()

    async def _handle(self, request: Frame,
                      connection: int) -> Frame | Coroutine[Any, Any, Frame] | None:
        """One request in; its answer, the answer still to come, or None for a NEXT the server holds."""
        seen = self._connections[connection]
        seen.last_seen, seen.requests = time.time(), seen.requests + 1
        try:
            answer = await self._answer(request, connection, seen)
        except TopicError as refused:
            # Answered, not dropped: a topic nobody defined is the caller's mistake to be told about,
            # where a frame the server cannot read leaves nothing to answer.
            answer = Frame(header={"error": str(refused), "kind": type(refused).__name__})
        self._server.notice_a_change()
        if isinstance(answer, Coroutine):
            return self._answered_later(answer, request.header.get("id"))
        if answer is not None:
            answer.header["re"] = request.header.get("id")
        return answer

    @staticmethod
    async def _answered_later(pending: Coroutine[Any, Any, Frame], request_id: Any) -> Frame:
        answer = await pending
        answer.header["re"] = request_id
        return answer

    def _keep_answering(self, connection: int, pending: Coroutine[Any, Any, Frame]) -> None:
        """Send an answer once it is ready, while the connection goes on reading its next requests."""
        async def answer() -> None:
            self._send(connection, await pending)
        task = self.loop.create_task(answer())
        self._answering.add(task)
        task.add_done_callback(self._answering.discard)

    async def _answer(self, request: Frame, connection: int,
                      seen: Connection) -> Frame | Coroutine[Any, Any, Frame] | None:
        operation = request.header.get("op")
        if operation == "produce":
            return await self._produce(request)
        if operation == "next":
            return await self._next(request, connection, seen)
        if operation in ("commit", "attempt", "complete"):
            return await self._to_the_partition(operation, request, connection)
        if operation == "hello":
            seen.role = str(request.header.get("role") or "unknown")
            seen.name = str(request.header.get("name") or "")
            seen.pid = request.header.get("pid")
            seen.host = str(request.header.get("host") or "")
            return Frame(header={"connection": connection})
        if operation == "close":
            return await self._close(request, connection)
        if operation == "members":
            return await self._count_ready(request)
        raise ProtocolError(f"unknown operation {operation!r}")

    async def _produce(self, request: Frame) -> Coroutine[Any, Any, Frame]:
        """Append one record: checked here, chosen a partition here, written by the strand that owns it.

        What comes back is the answer still to be given, once the record is on the disk: the connection
        reads its next request meanwhile, and the produces it sends in that time share the owner's flush.
        """
        channel = request.header["channel"]
        record = self._server.record_for(channel, request.header.get("key"),
                                         request.header.get("headers") or {}, request.body)
        producer, sequence = request.header.get("producer"), request.header.get("sequence")
        numbered = isinstance(producer, str) and isinstance(sequence, int)
        partitions = self._server.registry.get(channel).partitions
        self._spread += 1
        partition = partition_for(record.key, partitions, spread=self._spread,
                                  producer=producer if numbered else None,
                                  sequence=sequence if numbered else None)
        owner = self._server.strand_for(channel, partition)
        offset = await self.ask(owner, owner.append, channel, partition, record,
                                producer if numbered else None, sequence if numbered else None)

        async def once_flushed() -> Frame:
            await self.ask(owner, owner.flushed)
            return Frame(header={"partition": partition, "offset": offset})
        return once_flushed()

    async def _next(self, request: Frame, connection: int, seen: Connection) -> Frame | None:
        """This consumer's next record — now if any strand has one, and otherwise when one does.

        The NEXT is held here before any strand is asked, so an offer that arrives while the others are
        still being asked is accepted rather than declined and left behind.
        """
        channel, group = request.header["channel"], request.header["group"]
        # ★ A GROUP NAME BECOMES A DIRECTORY, so it is held to the rule a topic's name is.
        if not is_valid_name(group):
            raise RequestRefusedError(f"{group!r} is not a group name")
        self._server.registry.get(channel)            # refuses a topic nobody defined
        member = (connection, channel, group)
        if member in self._pending:
            raise ProtocolError(f"{channel}/{group}: a second NEXT while the first is still held")
        if member not in self._members:
            self._members.add(member)
            self._server.count_member(member, +1)
        wait = bool(request.header.get("wait", True))
        self._pending[member] = request.header.get("id")
        holding = self._holding.setdefault(member, set())
        strands = self._server.strands
        competing = not self._server.registry.get(channel).keeps_its_partitions
        if competing:
            # ★ OLDEST FIRST ACROSS THE SERVER, NOT ONLY WITHIN A STRAND. Each strand owns every Nth
            # partition, so a return's pages are spread over all of them; a consumer that went back to the
            # strand it last read from stayed there, a strand with one reader fewer fell behind, and every
            # fourth page of a 286-page return was read late, holding back each form it belonged to
            # (2026-09-28). The strand holding the oldest waiting record is asked first.
            waiting = await asyncio.gather(
                *(self.ask(strand, strand.oldest_waiting, channel, group) for strand in strands))
            order = [strand for _stamp, _number, strand in sorted(
                (stamp if stamp is not None else sys.maxsize, strand.number, strand)
                for stamp, strand in zip(waiting, strands, strict=True))]
        else:
            others = [strand for strand in strands if strand.number not in holding]
            start = connection % len(others) if others else 0
            order = ([strand for strand in strands if strand.number in holding]
                     + others[start:] + others[:start])
        for strand in order:
            if member not in self._pending:
                break                                   # an offer answered it meanwhile
            taken, holds = await self.ask(strand, strand.poll_member, member, seen.state, wait)
            (holding.add if holds else holding.discard)(strand.number)
            if taken is not None and member in self._pending:
                del self._pending[member]
                self._drop_waiters(member, except_strand=strand.number)
                if competing:
                    for other in strands:
                        if other is not strand:
                            self.tell(other, other.let_go, member)
                    holding.intersection_update({strand.number})
                return self._delivery(*taken)
        if member not in self._pending:
            return None
        if not wait:
            del self._pending[member]
            return Frame(header={"empty": True})
        return None

    def accept_offer(self, member: Member, strand_number: int, partition: int, record: Record) -> None:
        """A strand found a record for a waiting consumer: answer its held NEXT, or decline.

        Declined — the NEXT was answered already, by another strand or directly — the record stays with
        the strand that offered it, handed to its sub-member and uncommitted, and the consumer's next NEXT
        asks that strand first.
        """
        self._holding.setdefault(member, set()).add(strand_number)
        if member not in self._pending:
            return
        answer = self._delivery(partition, record)
        answer.header["re"] = self._pending.pop(member)
        self._drop_waiters(member, except_strand=strand_number)
        self._send(member[0], answer)

    def _drop_waiters(self, member: Member, *, except_strand: int) -> None:
        for strand in self._server.strands:
            if strand.number != except_strand:
                self.tell(strand, strand.drop_waiter, member)

    @staticmethod
    def _delivery(partition: int, record: Record) -> Frame:
        """One record as a NEXT's answer. Framed, not JSON: a page body is bytes."""
        return Frame(header={"partition": partition}, body=record.framed(record.offset))

    async def _to_the_partition(self, operation: str, request: Frame, connection: int) -> Frame:
        channel, group, partition = (request.header["channel"], request.header["group"],
                                     request.header["partition"])
        member = (connection, channel, group)
        owner = self._server.strand_for(channel, partition)
        if operation == "commit":
            await self.ask(owner, owner.commit, member, partition, request.header["next_offset"])
            return Frame(header={"committed": True})
        if operation == "attempt":
            count = await self.ask(owner, owner.attempt, member, partition, request.header["offset"],
                                   request.header.get("token"))
            return Frame(header={"attempt": count})
        outputs, at = [], 0
        for described in request.header.get("outputs") or []:
            value = request.body[at:at + described["length"]]
            at += described["length"]
            outputs.append(JournalOutput(described["channel"], self._server.record_for(
                described["channel"], described.get("key"), described.get("headers") or {}, value)))
        placements = await self.ask(owner, owner.complete, member, partition,
                                    request.header["next_offset"], tuple(outputs))
        return Frame(header={"completed": True, "placements": placements})

    async def _close(self, request: Frame, connection: int) -> Frame:
        channel, group = request.header["channel"], request.header["group"]
        key = str(request.header["key"]).encode()
        partition = partition_for(key, self._server.registry.get(channel).partitions)
        owner = self._server.strand_for(channel, partition)
        await self.ask(owner, owner.close_key, (connection, channel, group), key)
        return Frame(header={"closed": True})

    async def _count_ready(self, request: Frame) -> Frame:
        """How many READY members a group has right now, counted at every strand's homed consumers."""
        channel, group = request.header["channel"], request.header["group"]
        counts = [await self.ask(strand, strand.ready_members_homed, channel, group)
                  for strand in self._server.strands]
        return Frame(header={"ready": sum(counts)})

    def ready_members_homed(self, channel: str, group: str) -> int:
        return sum(1 for (connection, held_channel, held_group) in self._members
                   if (held_channel, held_group) == (channel, group)
                   and self._connections.get(connection, Connection(connection)).state
                   == MemberState.READY)

    def set_connection_state(self, connection: int, state: str) -> bool:
        """READY, DRAIN or MAINT for a connection homed here, and for its sub-members on every strand."""
        seen = self._connections.get(connection)
        if seen is None:
            return False
        seen.state = state
        for strand in self._server.strands:
            self.tell(strand, strand.set_state, connection, state)
        return True

    # ---- answers written without waiting

    def _send(self, connection: int, answer: Frame) -> None:
        """Write an answer the server held back. Its connection may have gone in the meantime.

        Written without awaiting the socket, since it runs inside another step's handling. So a client
        that has stopped reading keeps the answer in this process's memory, and one that is still
        holding bytes of it after `OUTBOUND_STALL_SECONDS` is dropped (`_drop_stalled_readers`), which
        frees them and hands the record, never committed, to another member.
        """
        writer = self._writers.get(connection)
        if writer is not None and not writer.is_closing():
            writer.write(answer.encoded())
            if writer.transport.get_write_buffer_size():
                self._unsent_since.setdefault(connection, time.monotonic())

    def drop_stalled_readers(self) -> int:
        """Drop every connection still holding bytes it was sent `OUTBOUND_STALL_SECONDS` ago."""
        now, dropped = time.monotonic(), 0
        for connection, since in list(self._unsent_since.items()):
            writer = self._writers.get(connection)
            if writer is None or writer.is_closing() or not writer.transport.get_write_buffer_size():
                del self._unsent_since[connection]
            elif now - since > OUTBOUND_STALL_SECONDS:
                log.warning("dropping connection %d: it has not read what it was sent %ds ago",
                            connection, OUTBOUND_STALL_SECONDS)
                writer.transport.abort()
                del self._unsent_since[connection]
                dropped += 1
        return dropped

    async def _drop_stalled_readers_periodically(self) -> None:
        while True:
            await asyncio.sleep(OUTBOUND_CHECK_SECONDS)
            self.drop_stalled_readers()


