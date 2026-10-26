"""FreeStream's server: the one process that owns the log, running as strands (#450).

    PRODUCE  channel, key, headers, producer, sequence + body  ->  partition, offset (a resend: the same)
    NEXT     channel, group, wait               ->  one record and its partition, WHEN there is one
    COMMIT   channel, group, partition, next_offset   (refused past what was handed, or backward)
    COMPLETE channel, group, partition, next_offset, outputs + bodies  ->  their placements (all or none)
    ATTEMPT  channel, group, partition, offset, token ->  how many distinct starts it has had
    HELLO    role, name, pid, host              ->  this connection's number
    CLOSE    channel, group, key                    a consumer has finished with a key
    MEMBERS  channel, group                     ->  how many READY members the group has

and, on a socket of its own, a plain-text console for an operator — `freestream.admin`.

★ FULLY MULTITHREADED (2026-09-25). The work runs on strands (`strand.py`): threads, each with its
own event loop, each owning a share of every topic's partitions outright and a share of the connections.
This module holds what belongs to no one strand — which strand owns which partition, accepting
connections and handing each to a strand, the topic registry, finishing a journal a killed server left,
and gathering what the consoles read from every strand. The console and the web page run on the thread
that called `serve`, and reach the strands by message like everything else.

★ A CONSUMER AWAITS; IT DOES NOT POLL (2026-09-24). A NEXT that finds nothing is held, and answered
the moment a record it may have arrives on any strand.
"""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import socket
import time
import zlib
from pathlib import Path
from typing import Any

from .consumer_group import GroupPositions, MemberState, write_commit
from .interface.channel import Delivery
from .journal import JournalEntry, pending_entries, write_entry
from .protocol import ANSWER_HEADER_ALLOWANCE, MAX_FRAME_BYTES
from .record import STREAM_HEADER_PREFIX, Record
from .registry import (
    DEAD_SUFFIX,
    RETRY_SUFFIX,
    RequestRefusedError,
    TopicDefinition,
    TopicError,
    TopicRegistry,
    is_valid_name,
)
from .sealing import Seal
from .strand import Connection, Member, Strand
from .topic import Topic, partition_for

log = logging.getLogger(__name__)

#: How many strands a server runs when it is not told. Each is a thread, and the work spreads over them
#: by partition, so more than the cores the server may use gains nothing; four keeps a small footprint
#: beside a database and other workers on a shared box. Never fewer than two, so the server is
#: multithreaded wherever it runs. `EVENTSTREAM_STRANDS` or `--strands` sets it (`__main__.py`).
DEFAULT_STRANDS = max(2, min(4, len(os.sched_getaffinity(0))))

#: How long a console read waits on the strands before giving up.
GATHER_SECONDS = 10

#: How long a unix socket's path may be, from `sockaddr_un.sun_path` — 108 bytes on Linux including
#: the terminator, so 107 usable. It is a kernel limit and not ours to raise.
MAX_SOCKET_PATH_BYTES = 107


def _refuse_a_path_the_kernel_cannot_hold(path: Path) -> None:
    """Fail at the bind, naming the cause, rather than at every client that tries to connect."""
    encoded = len(str(path).encode())
    if encoded > MAX_SOCKET_PATH_BYTES:
        raise OSError(
            f"the server's socket path is {encoded} bytes and a unix socket allows "
            f"{MAX_SOCKET_PATH_BYTES}: {path}. Put it somewhere shorter — the deployed one is "
            f"/var/lib/freestream/eventstream.sock — and point --socket and the clients at that.")


class EventStreamServer:
    """Owns the log directory, and runs the stream's operations on its strands."""

    def __init__(self, root: str | Path, *, strands: int | None = None, key: bytes | None = None) -> None:
        self._root = Path(root)
        #: What seals every record and journal entry the server writes (`sealing.py`). There is no
        #: unsealed mode: a server built without a key — a test's — seals under a key of its own.
        self.seal = Seal(key) if key is not None else Seal.with_a_new_key()
        #: What each topic IS — see `registry.py`. The server refuses a name nobody has defined.
        self.registry = TopicRegistry(self._root)
        self.started_at = time.time()
        #: The loop `serve` runs on — the consoles' — which a strand tells when something changed.
        self._main_loop: asyncio.AbstractEventLoop | None = None
        #: Resolved and replaced whenever anything an operator could see has changed.
        self._change: asyncio.Future[None] | None = None
        self._next_connection = 1
        #: (channel, group) -> members, kept on the main loop for the starvation warning.
        self._member_counts: dict[tuple[str, str], int] = {}
        self._said_too_many: set[tuple[str, str]] = set()
        self._replay_the_journal()
        self.strands = [Strand(number, self) for number in range(strands or DEFAULT_STRANDS)]
        for strand in self.strands:
            strand.start()

    @property
    def root(self) -> Path:
        return self._root

    # ------------------------------------------------------------------ who owns what

    def strand_for(self, channel: str, partition: int) -> Strand:
        """The strand that owns a partition. Topics start at different strands, so small ones spread."""
        return self.strands[(zlib.crc32(channel.encode()) + partition) % len(self.strands)]

    def partitions_of(self, number: int, channel: str, partitions: int) -> list[int]:
        return [partition for partition in range(partitions)
                if self.strand_for(channel, partition).number == number]

    def home_of(self, connection: int) -> Strand:
        """The strand a connection is homed on — worked out from its number, so nothing keeps a table."""
        return self.strands[connection % len(self.strands)]

    def record_for(self, channel: str, key: Any, headers: dict[str, str], value: bytes) -> Record:
        """The record a produce asks for, checked against its topic's key rule and what a delivery holds.

        ★ THE SERVER ENFORCES THE TOPIC'S KEY RULE ITSELF. A keyless record on a KEYED topic is
        round-robined, and a document's pages scatter across assemblers; a keyed record on a COMPETING
        topic piles one key's work into one partition.

        ★ A RECORD MUST ALSO FIT IN THE ANSWER THAT DELIVERS IT, or it would be written and then never
        delivered, blocking its partition. The stream's own headers are dropped from what the caller
        sent: a processor that copies its input's headers forward would otherwise carry another
        producer's identity into its output.
        """
        delivery = self.registry.get(channel).delivery
        if delivery is Delivery.KEYED and key is None:
            raise RequestRefusedError(f"{channel} is keyed and this record has no key")
        if delivery is Delivery.COMPETING and key is not None:
            raise RequestRefusedError(f"{channel} is competing and this record has a key")
        record = Record(value=value, key=key.encode() if isinstance(key, str) else None,
                        headers={name: header for name, header in headers.items()
                                 if not name.startswith(STREAM_HEADER_PREFIX)})
        if record.framed_size() + ANSWER_HEADER_ALLOWANCE > MAX_FRAME_BYTES:
            raise RequestRefusedError(
                f"a record of {record.framed_size()} bytes is too large to be delivered")
        return record

    async def append_outputs(self, strand: Strand, entry: JournalEntry) -> list[list[int]]:
        """Append a journal entry's outputs, each on the strand that owns its partition, and see them flushed.

        The outputs are appended in order, and then every strand that took one flushes; only then does the
        entry record them as landed, since a replay skips a landed output and must never skip one a power cut
        erased. When an append fails, the outputs before it are flushed and recorded in the entry's file,
        so finishing the entry later skips them by number. An output the entry already records is not sent
        again.
        """
        appended: list[tuple[int, int, int, Strand]] = []
        try:
            for sequence, output in enumerate(entry.outputs):
                if sequence in entry.landed:
                    continue
                partition = partition_for(output.record.key, self.registry.get(output.channel).partitions,
                                          producer=entry.producer, sequence=sequence)
                owner = self.strand_for(output.channel, partition)
                offset = await strand.ask(owner, owner.append, output.channel, partition, output.record,
                                          entry.producer, sequence)
                appended.append((sequence, partition, offset, owner))
        except Exception:
            await self._record_landed(strand, entry, appended)
            write_entry(self._root, entry, self.seal)
            raise
        await self._record_landed(strand, entry, appended)
        return [list(entry.landed[sequence]) for sequence in range(len(entry.outputs))]

    @staticmethod
    async def _record_landed(strand: Strand, entry: JournalEntry,
                             appended: list[tuple[int, int, int, Strand]]) -> None:
        owners = {owner.number: owner for _sequence, _partition, _offset, owner in appended}
        await asyncio.gather(*(strand.ask(owner, owner.flushed) for owner in owners.values()))
        for sequence, partition, offset, _owner in appended:
            entry.landed[sequence] = (partition, offset)

    def forget_journal_producer(self, strand: Strand, entry: JournalEntry) -> None:
        """A finished entry's outputs are never sent again, so their strands stop remembering them."""
        for sequence, output in enumerate(entry.outputs):
            partition = partition_for(output.record.key, self.registry.get(output.channel).partitions,
                                      producer=entry.producer, sequence=sequence)
            owner = self.strand_for(output.channel, partition)
            strand.tell(owner, owner.forget_producer, output.channel, partition, entry.producer)

    def _replay_the_journal(self) -> None:
        """Finish every COMPLETE a killed server journaled and did not get to the end of.

        Run before any strand starts, on topics opened whole, so nothing else is writing. Each output goes
        to the partition the live path would have chosen, under the entry's producer id, so an output the
        first application wrote is recognized in its partition's log and not written again.
        """
        opened: dict[str, Topic] = {}
        for path, entry in pending_entries(self._root, self.seal):
            log.warning("finishing a COMPLETE on %s/%s partition %d that a previous server journaled",
                        entry.channel, entry.group, entry.plan.partition)
            write_commit(self._root / entry.channel / "groups" / entry.group, entry.plan,
                         GroupPositions())
            for sequence, output in enumerate(entry.outputs):
                if sequence in entry.landed:
                    continue
                if output.channel not in opened:
                    opened[output.channel] = Topic(
                        self._root / output.channel,
                        partitions=self.registry.get(output.channel).partitions, seal=self.seal,
                        recall_every_journal_output=True)
                opened[output.channel].produce_once(output.record, entry.producer, sequence)
            os.sync()     # the outputs and the commit reach the disk before the entry that promised them goes
            path.unlink()

    # ------------------------------------------------------------------ members, for the warning

    def count_member(self, member: Member, change: int) -> None:
        """A strand's consumer joined or left a group; counted on the main loop, by message."""
        if self._main_loop is not None and not self._main_loop.is_closed():
            self._main_loop.call_soon_threadsafe(self._count_member, member[1], member[2], change)

    def _count_member(self, channel: str, group: str, change: int) -> None:
        """Say so, once, when a group has more members than its channel has partitions.

        ★ STARVATION IS A FACT ABOUT THE GROUP, SO THE SERVER REPORTS IT. A partition reaches one member
        at a time, so member N+1 against N partitions can never be handed anything.
        """
        members = self._member_counts.get((channel, group), 0) + change
        self._member_counts[(channel, group)] = members
        with_partitions = self.registry.get(channel).partitions
        if members > with_partitions and (channel, group) not in self._said_too_many:
            self._said_too_many.add((channel, group))
            log.warning("%s: group %r has %d members but the channel has %d partition(s) — the "
                        "members past %d will never be handed a record", channel, group, members,
                        with_partitions, with_partitions)

    # ------------------------------------------------------------------ what the consoles see and do

    def gather(self, method: str, *args: Any) -> list[Any]:
        """Run one strand method on every strand, by message, and return their answers.

        For the consoles and tools on the main thread; a strand never calls it, since waiting on itself
        would never return.
        """
        futures = [strand.submit(getattr(strand, method), *args) for strand in self.strands]
        return [future.result(GATHER_SECONDS) for future in futures]

    def next_change(self) -> asyncio.Future[None]:
        """Resolved the next time anything visible changes. Await it through `asyncio.shield`."""
        if self._change is None or self._change.done():
            self._change = asyncio.get_running_loop().create_future()
        return self._change

    def notice_a_change(self) -> None:
        """Safe from any strand: the change is resolved on the main loop."""
        if self._main_loop is not None and not self._main_loop.is_closed():
            self._main_loop.call_soon_threadsafe(self._resolve_change)

    def _resolve_change(self) -> None:
        if self._change is not None and not self._change.done():
            self._change.set_result(None)

    def snapshots(self) -> list[dict[str, Any]]:
        return self.gather("snapshot")

    def connections(self) -> list[Connection]:
        return sorted((seen for snapshot in self.snapshots() for seen in snapshot["connections"]),
                      key=lambda seen: seen.connection_id)

    def memberships(self) -> list[dict[str, Any]]:
        """Every consumer's membership of every group, merged from its sub-members on every strand."""
        merged: dict[tuple[int, str, str], dict[str, Any]] = {}
        pending = {tuple(member) for snapshot in self.snapshots() for member in snapshot["pending"]}
        for snapshot in self.snapshots():
            for sub in snapshot["members"]:
                key = (sub["connection"], sub["channel"], sub["group"])
                row = merged.setdefault(key, {**sub, "partitions": [], "open_keys": 0, "keys": [],
                                              "waiting": key in pending})
                row["partitions"] = sorted(row["partitions"] + sub["partitions"])
                row["open_keys"] += sub["open_keys"]
                row["keys"] = sorted(row["keys"] + sub["keys"])
        return [merged[key] for key in sorted(merged)]

    def partition_stats(self) -> dict[str, dict[int, dict[str, int]]]:
        """Every defined topic's partitions and what each holds, merged from every strand."""
        merged: dict[str, dict[int, dict[str, int]]] = {}
        for snapshot in self.snapshots():
            for name, partitions in snapshot["topics"].items():
                merged.setdefault(name, {}).update(partitions)
        return dict(sorted(merged.items()))

    def lag(self, channel: str, group: str) -> dict[int, int]:
        merged: dict[int, int] = {}
        for part in self.gather("lag", channel, group):
            merged.update(part)
        return dict(sorted(merged.items()))

    def dead_letters(self, name: str) -> list[Record]:
        return sorted((record for part in self.gather("dead_letters", name) for record in part),
                      key=lambda record: record.timestamp_ns)

    def define_topic(self, name: str, *, partitions: int, delivery: str) -> TopicDefinition:
        """Define a topic, or confirm it is defined with this shape — the web page, the console, deploy."""
        definition = self.registry.define(name, partitions=partitions, delivery=delivery)
        log.info("topic %s defined: %s, %d partition(s)", name, definition.delivery,
                 definition.partitions)
        self.notice_a_change()
        return definition

    def remove_topic(self, name: str) -> None:
        """Forget a topic and everything it holds. Refused while anybody is a member of it."""
        held = (name, name + DEAD_SUFFIX, name + RETRY_SUFFIX)
        if any(any(self.gather("has_members", topic_name)) for topic_name in held):
            raise TopicError(f"{name!r} has live members — drain and stop them first")
        # The definition goes first, so a produce or a NEXT arriving from now on is refused, and nothing
        # reopens a partition while its owning strand deletes it. Each strand deletes its own partitions;
        # what is left — the definition file, and group directories emptied of partition files — belongs
        # to no strand, and nothing can recreate it now.
        definition = self.registry.forget(name)
        for doomed, partitions in ((name, definition.partitions), (name + DEAD_SUFFIX, 1),
                                   (name + RETRY_SUFFIX, definition.partitions)):
            self.gather("remove_topic_files", doomed, partitions)
            shutil.rmtree(self._root / doomed, ignore_errors=True)
        log.warning("topic %s removed", name)
        self.notice_a_change()

    def set_state(self, connection: int, state: str) -> None:
        """READY, DRAIN or MAINT for a connection and every membership it has — `set consumer`."""
        if state not in MemberState.ALL:
            raise ValueError(f"state must be one of {', '.join(MemberState.ALL)}")
        home = self.home_of(connection)
        if not home.submit(home.set_connection_state, connection, state).result(GATHER_SECONDS):
            raise KeyError(connection)
        self.notice_a_change()

    def delete_group(self, channel: str, group: str) -> None:
        """Remove a retired group, so it stops holding segments that nobody will read.

        Refused while any member of it is connected: deleting a group out from under a live consumer
        would reset its position to zero and hand it everything again.
        """
        if not (is_valid_name(channel) and is_valid_name(group)):
            raise ValueError(f"{channel}/{group} is not a topic and group name")
        if any(self.gather("has_members", channel, group)):
            raise RuntimeError(f"{channel}/{group} has live members — drain and stop them first")
        home = self._root / channel / "groups" / group
        if not home.is_dir():
            raise KeyError(f"{channel}/{group}")
        self.gather("remove_group_files", channel, group, self.registry.get(channel).partitions)
        # Each strand has deleted its partitions' files. A consumer that joined the group since would
        # have made new ones, and then the directory is not empty: refused, rather than deleting a live
        # member's files from a thread that does not own them.
        try:
            home.rmdir()
        except OSError as occupied:
            raise RuntimeError(f"{channel}/{group} gained a member while it was being deleted") from occupied
        self.notice_a_change()

    def sweep_dead_letters(self, *, now_ns: int | None = None) -> int:
        """Delete dead-letter segments past the retention, on every strand. Returns how many went."""
        return sum(self.gather("sweep_dead_letters", now_ns))

    def drop_stalled_readers(self) -> int:
        return sum(self.gather("drop_stalled_readers"))

    def close(self) -> None:
        """Stop every strand. The server does not serve again after this."""
        for strand in self.strands:
            strand.stop()

    # ------------------------------------------------------------------ serving

    async def serve(self, socket_path: str | Path, *, admin_socket: str | Path | None = None,
                    web_port: int | None = None, ready: asyncio.Event | None = None) -> None:
        """Listen until canceled, handing each connection to a strand. Stops the strands on the way out.

        `admin_socket` is the operator's console (see `freestream.admin`), on a socket of its own
        so that nothing speaking the framed protocol can reach it by accident.
        """
        from .admin import serve_admin
        from .web import serve_web

        self._main_loop = asyncio.get_running_loop()
        path = Path(socket_path)
        _refuse_a_path_the_kernel_cannot_hold(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.unlink(missing_ok=True)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(256)
        listener.setblocking(False)
        accepting = asyncio.create_task(self._accept(listener))
        log.info("event stream server listening on %s with %d strand(s)", path, len(self.strands))
        console = await serve_admin(self, admin_socket) if admin_socket is not None else None
        page = await serve_web(self, web_port) if web_port is not None else None
        if ready is not None:
            ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            accepting.cancel()
            listener.close()
            for extra in (console, page):
                if extra is not None:
                    extra.close()
            await asyncio.to_thread(self.close)

    async def _accept(self, listener: socket.socket) -> None:
        """Accept connections, and home each on the next strand in turn."""
        loop = asyncio.get_running_loop()
        while True:
            client, _address = await loop.sock_accept(listener)
            connection = self._next_connection
            self._next_connection += 1
            home = self.home_of(connection)
            home.loop.call_soon_threadsafe(home.adopt, client, connection)
