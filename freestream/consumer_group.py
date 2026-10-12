"""A consumer group: who owns which partition, and how far each has read (#450).

Kafka's model. A group is a set of consumers sharing a topic; each partition is assigned to EXACTLY
ONE of them, and each records how far it has read as an offset. Those two facts together are the whole
of what we spent 2026-09-22 failing to have:

  · one owner per partition  ->  one consumer assembles a document, because its key picks its partition
  · an offset per partition  ->  a replacement resumes where the dead one left off, with no retention
                                 policy for anyone to maintain

    <topic>/groups/<group>/NNNN.owner    the file whose flock IS ownership of partition NNNN
    <topic>/groups/<group>/NNNN.offset   the next offset this group should read from that partition
    <topic>/groups/<group>/NNNN.acked    offsets past that one the group has already committed (see below)

★ OWNERSHIP IS AN FLOCK, WHICH IS BOTH HALVES. It is the exclusivity, decided by the kernel rather than
by a protocol of ours; and it is the liveness, because the kernel releases it when the holder dies,
however it dies — crash, kill -9, a machine going away. There is no heartbeat, nothing to time out, and
nothing to notice a death. The next consumer to look simply succeeds.

That is the durable, cross-process version of what `main` had as a synchronous `if id in _inflight`,
and what the branch lost by making that guard an async database read.

★ ONE PARTITION AT A TIME, TAKEN ONLY WHILE IDLE — see `poll`. That is what spreads a channel
over the consumers reading it, and taking everything unowned instead is the bug that had one OCR
reader doing the work of eight.

★ ONE PARTITION AT A TIME, TAKEN ONLY UNTIL THERE IS WORK, AND ONLY ONE KEPT WHILE IDLE — see `poll`.
That is what spreads a channel over the consumers reading it, and taking everything unowned instead is
the bug that had one OCR reader doing the work of eight.

★ ASSIGN ON FIRST SIGHT, NEVER REBALANCE (2026-09-22). Kafka must move partitions between
consumers because partitions are eternal — a new consumer can only get work by taking some away. Ours
are eternal too, but the KEYS in them are not: a document finishes and its key retires. So a consumer
arriving takes whatever is unowned and the rest keep what they have. No stop-the-world, no partition
changing hands mid-document, and none of the hardest machinery in a consumer group.

★ THE COMMITTED OFFSET IS THE NEXT ONE TO READ, which is Kafka's convention and worth stating because
the off-by-one is silent: committing "the last one I read" re-delivers it forever to a replacement.

★ ON A RETRY TOPIC A RECORD IS HANDED OUT ONLY ONCE IT IS DUE, AND NOTHING WAITS BEHIND IT (2026-10-12).
A record a handler asked to run later carries the time it may run (`retry.not_before`), and records with
different delays share a partition, so the record at the position is often not the one that is due. A
member of a retry topic's group (`due_by_header`) is handed the due record whose time came first, from
anywhere past the position, and its commit adds that one offset to the acked-ahead set below rather than
moving the position over records still waiting. Kafka cannot skip a record, which is why Kafka shops keep
one retry topic per delay; this keeps one per topic, and a delay of any length. `next_due_ns` says when
the next waiting record falls due, so the strand can wake a held NEXT then.

★ A GROUP'S PLACE IS AN OFFSET AND THE RECORDS COMMITTED PAST IT. One number cannot say "offset 1 is
done and offset 0 is not", and a DRAINING member makes exactly that true: it is handed the records of
the keys it has open and skips the rest, so its position stops at the first record it skipped. With the
position alone, the member that inherited the partition started there and was handed again every record
the draining one had committed past it (the adversarial review, 2026-09-25). The `.acked` file holds
those offsets; nothing in it is delivered again, and the position absorbs them as it reaches them. Kafka
share groups keep the same per-record state for the same reason.
"""
from __future__ import annotations

import fcntl
import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from .durability import mark_written
from .record import RETRY_NOT_BEFORE_HEADER, Record
from .registry import PartitionNotHeldError, RequestRefusedError
from .topic import Topic

#: The most a single poll reads, across every partition the consumer holds. A NEXT takes one record and
#: reads one; the budget binds on a DRAINING member, which reads past the keys it may not take to find
#: the ones it may, and it keeps that walk from pulling a whole return's pages through memory at once.
POLL_BUDGET_BYTES = 4 * 1024 * 1024


def committed_by_every_group(topic_path: Path, partition: int) -> int:
    """The lowest offset any group on disk has committed for a partition — what retention may reach.

    A group that has a directory but never committed this partition answers 0, which holds the whole
    partition. That is deliberate: a group is a promise that somebody reads this channel, and a
    promise nobody has started keeping does not license us to delete what it will want. A group that has
    been retired for good is removed through the admin socket, and then stops holding anything.
    """
    groups = topic_path / "groups"
    if not groups.is_dir():
        return 0
    lowest: int | None = None
    for group in groups.iterdir():
        marker = group / f"{partition:04d}.offset"
        committed = int(marker.read_text()) if marker.exists() else 0
        lowest = committed if lowest is None else min(lowest, committed)
    return lowest or 0


class GroupPositions:
    """Each group's place in each partition, kept in memory, for the partitions of one strand.

    ★ IN MEMORY, BECAUSE A POLL ASKS ABOUT EVERY PARTITION. A consumer takes a partition only when it holds
    something unread, which means comparing each partition's end against the group's position in it — 64
    of them on `pages.to_read`, on every poll of every reader. From files, that is thousands of small reads
    a second on an idle channel. The files are what survives the server restarting; this is what a poll
    reads.

    ★ ONE PER STRAND. A strand owns its partitions outright, so each strand keeps the positions of its own
    partitions in its own table, and no two threads ever touch one (`strand.py`). A tool or a test that
    runs everything on one thread uses `SHARED`.
    """

    def __init__(self) -> None:
        #: (group home, partition) -> the next offset that group should read.
        self.positions: dict[tuple[Path, int], int] = {}
        #: (group home, partition) -> offsets past the position the group has committed; absent for every
        #: partition no DRAINING member has skipped records in.
        self.acked: dict[tuple[Path, int], set[int]] = {}

    def forget(self, home: Path) -> None:
        """Drop a deleted group's positions, so a group made again under the same name starts at zero."""
        for key in [key for key in self.positions if key[0] == home]:
            del self.positions[key]
        for key in [key for key in self.acked if key[0] == home]:
            del self.acked[key]


#: The table for code that runs on one thread: tools, tests, and the journal replay before strands start.
SHARED = GroupPositions()


def forget_positions(home: Path, positions: GroupPositions = SHARED) -> None:
    """Drop a deleted group's positions from one table."""
    positions.forget(home)


@dataclass(frozen=True, slots=True)
class CommitPlan:
    """What a commit does to a group's place in one partition, worked out before it is done.

    `committing` is the offsets a DRAINING member is adding to the acked-ahead set, which it stops
    tracking once the plan is applied.
    """

    partition: int
    position: int
    acked: frozenset[int]
    committing: frozenset[int] = frozenset()


def _position(home: Path, partition: int, positions: GroupPositions) -> int:
    if (home, partition) not in positions.positions:
        marker = home / f"{partition:04d}.offset"
        positions.positions[(home, partition)] = int(marker.read_text()) if marker.exists() else 0
    return positions.positions[(home, partition)]


def _acked(home: Path, partition: int, positions: GroupPositions) -> set[int]:
    """The group's acked-ahead offsets for a partition.

    An offset below the position is dropped as it is read: it is the trace of a crash between the
    position's write and the set's, and the position already covers it.
    """
    if (home, partition) not in positions.acked:
        path = home / f"{partition:04d}.acked"
        stored: list[int] = json.loads(path.read_text()) if path.exists() else []
        positions.acked[(home, partition)] = {
            offset for offset in stored if offset >= _position(home, partition, positions)}
    return positions.acked[(home, partition)]


def write_commit(home: Path, plan: CommitPlan, positions: GroupPositions = SHARED) -> None:
    """Write a planned commit into a group's directory. Needs no member, so a journal can replay it.

    A plan behind where the group already is changes nothing: replayed after its first application had
    already landed, it is a commit the group has made.

    ★ THE POSITION IS WRITTEN BEFORE THE SET. Two files cannot change in one rename, so the order decides
    what a crash between them leaves: this way, offsets below the new position still in the old set,
    which `_acked` drops on reading. The other order could lose an acknowledgment.
    """
    home.mkdir(parents=True, exist_ok=True)
    partition = plan.partition
    if plan.position < _position(home, partition, positions):
        return
    # ★ ACKNOWLEDGMENTS ARE ONLY EVER ADDED. A plan replayed from the journal after later commits have
    # landed carries an older set, and writing it as it stands would hand those later records out again;
    # merged with what is already acknowledged, it cannot take one away.
    acked = {offset for offset in _acked(home, partition, positions) | plan.acked if offset >= plan.position}
    position = plan.position
    while position in acked:
        acked.discard(position)
        position += 1
    staged = home / f"{partition:04d}.offset.writing"
    staged.write_text(str(position))
    staged.rename(home / f"{partition:04d}.offset")
    mark_written(home / f"{partition:04d}.offset", directory_changed=True)
    positions.positions[(home, partition)] = position
    positions.acked[(home, partition)] = acked
    acked_path = home / f"{partition:04d}.acked"
    if acked:
        staged = acked_path.with_suffix(".acked.writing")
        staged.write_text(json.dumps(sorted(acked)))
        staged.rename(acked_path)
        mark_written(acked_path, directory_changed=True)
    else:
        acked_path.unlink(missing_ok=True)
    # A committed record is finished with, and so is its count of starts.
    starts = _read_starts(home, partition)
    kept = {offset: tokens for offset, tokens in starts.items()
            if int(offset) >= position and int(offset) not in acked}
    if kept != starts:
        _write_starts(home, partition, kept)


def _read_starts(home: Path, partition: int) -> dict[str, list[str]]:
    path = home / f"{partition:04d}.attempts"
    starts: dict[str, list[str]] = json.loads(path.read_text()) if path.exists() else {}
    return starts


def _write_starts(home: Path, partition: int, starts: dict[str, list[str]]) -> None:
    path = home / f"{partition:04d}.attempts"
    if not starts:
        path.unlink(missing_ok=True)
        return
    staged = path.with_suffix(".attempts.writing")
    staged.write_text(json.dumps(starts))
    staged.rename(path)
    mark_written(path, directory_changed=True)


class MemberState:
    """What an operator has said a consumer may do — HAProxy's server states, for a consumer.

    READY   handed records as usual.
    DRAIN   finishes what it has and takes nothing new. On a COMPETING channel that is the record in
            hand, committed before it asks again, so its partitions go when it next asks. On a KEYED
            channel it is every key it has open — a document half assembled — so it is handed records
            for those keys only, and its partitions go once the last of them closes.
    MAINT   handed nothing, and everything it holds is let go the next time it asks — at once for a
            member waiting on a NEXT, and after the record in hand is committed for one that is
            working, as HAProxy's MAINT leaves a live session to finish. On a KEYED channel a document
            it had open moves to another member, which recovers it mid-flight from the stored reads.
    """

    READY = "ready"
    DRAIN = "drain"
    MAINT = "maint"
    ALL = (READY, DRAIN, MAINT)


class ConsumerGroup:
    """One consumer's membership of a group: the partitions it holds, and its place in each."""

    def __init__(self, topic: Topic, group: str, path: str | Path, *,
                 keeps_its_partitions: bool = True, due_by_header: bool = False,
                 positions: GroupPositions = SHARED) -> None:
        self._topic = topic
        #: Whether this is a retry topic's group, handed only records that are due. See the module notes.
        self._due_by_header = due_by_header
        #: When the next record not yet due falls due, as the last poll found it; None when nothing waits.
        self.next_due_ns: int | None = None
        #: partition -> the offsets this member has been handed on a retry topic, any of which it may
        #: commit, since it commits them one at a time rather than up to a position.
        self._handed_offsets: dict[int, set[int]] = {}
        self._positions = positions
        self._group = group
        self._home = Path(path) / "groups" / group
        self._home.mkdir(parents=True, exist_ok=True)
        self._keeps_its_partitions = keeps_its_partitions
        #: partition -> the open descriptor holding its flock. Ownership lives as long as this object,
        #: and dies with it, which is the point.
        self._owned: dict[int, IO[str]] = {}
        #: READY, DRAIN or MAINT — see `MemberState`. Set by the admin socket.
        self.state = MemberState.READY
        #: Keys this member has been handed and not yet closed, on a channel that keeps its partitions:
        #: the documents it is part way through. What a DRAIN lets it finish.
        self.open_keys: set[bytes] = set()
        #: partition -> the lowest offset a DRAINING member was NOT handed (a key it has not opened).
        #: Its commits stop there, so the member that inherits the partition starts at that record
        #: rather than past it.
        self._held_back: dict[int, int] = {}
        #: partition -> the next offset a DRAINING member reads from. Separate from the committed
        #: position, which the held-back record pins, so the records it WAS handed are not handed
        #: to it again on every poll.
        self._drain_cursor: dict[int, int] = {}
        #: partition -> the highest offset this member has been handed. A commit may not pass it.
        self._handed: dict[int, int] = {}
        #: partition -> the offsets a DRAINING member was handed past its held-back record. A commit
        #: covering them adds them to the group's acked-ahead set, since the position cannot pass them.
        self._handed_past_held_back: dict[int, set[int]] = {}

    @property
    def keeps_its_partitions(self) -> bool:
        """Whether this is a KEYED channel's member, which holds its partitions while it lives."""
        return self._keeps_its_partitions

    def oldest_waiting_ns(self) -> int | None:
        """When the oldest record this group has not read, in this opening's partitions, was written.

        None when nothing is waiting. Only headers are read (`PartitionLog.timestamp_at`); it is how the
        strand a consumer asks first is chosen, so a competing channel is read oldest first across the
        whole server and not only within one strand.
        """
        stamps = [self._topic.log(partition).timestamp_at(self.position(partition))
                  for partition in self._topic.owned_partitions]
        return min((stamp for stamp in stamps if stamp is not None), default=None)

    @property
    def owned(self) -> list[int]:
        """Which partitions this consumer currently holds."""
        return sorted(self._owned)

    def holds(self, partition: int) -> bool:
        """Whether this consumer holds one partition — asked on every produce, so no list is built."""
        return partition in self._owned

    def poll(self, *, limit: int | None = None) -> list[tuple[int, Record]]:
        """Take whatever partitions are going, and read what is new in the ones we hold.

        Returns (partition, record) pairs. Nothing is committed by reading — a consumer that dies
        between reading and committing must see the same records again, which is at-least-once and is
        the only default that does not lose a record.

        ★ IT TAKES PARTITIONS UNTIL IT FINDS WORK, AND THEN STOPS TAKING. A consumer that is busy
        does not reach for more, so a channel spreads over whoever is reading it. What this replaced
        took every unowned partition on its first poll, which on an empty log meant the first of eight
        OCR readers owned all eight and the other seven never saw a page.

        ★ AND A WALK THAT FINDS NOTHING KEEPS ONE PARTITION, on a channel that says it may. The walk
        is how a consumer finds work, so on an idle channel it ends up holding everything it looked
        at — which is the same monopoly again, arrived at politely. Eight readers polling an idle
        channel therefore settle at one partition each: each walks what is free, keeps the first, and
        leaves the rest for the next. Nothing is taken from a live consumer at any point.
        """
        if self.state == MemberState.MAINT:
            self.release()
            return []
        if self.state == MemberState.DRAIN:
            return self._remember(self._poll_while_draining(limit))
        if self._keeps_its_partitions:
            found = self._read_what_we_hold(limit)
            while not found and self._take_one_unowned():
                found = self._read_what_we_hold(limit)
        elif self._due_by_header:
            found = self._read_the_due(limit)
        else:
            found = self._read_the_oldest(limit)
        if not found:
            self._keep_only_what_we_need()
        if self._keeps_its_partitions:
            self.open_keys.update(record.key for _partition, record in found if record.key is not None)
        return self._remember(found)

    def _remember(self, found: list[tuple[int, Record]]) -> list[tuple[int, Record]]:
        """Note the highest offset handed out in each partition, which is how far a commit may reach."""
        for partition, record in found:
            self._handed[partition] = max(self._handed.get(partition, -1), record.offset)
            if self._due_by_header:
                self._handed_offsets.setdefault(partition, set()).add(record.offset)
        return found

    def close_key(self, key: bytes) -> None:
        """This member has finished with a key — a document assembled.

        A draining member whose last key closes gives its partitions up the next time it asks.
        """
        self.open_keys.discard(key)

    def _poll_while_draining(self, limit: int | None) -> list[tuple[int, Record]]:
        """Finish what is in hand; take nothing new; let go once nothing is left.

        A member of a COMPETING channel has nothing in hand by the time it asks again — it commits
        each record before it asks — so it lets go at once. A member of a KEYED channel is handed the
        records of the keys it has open and nothing else, and lets go when the last one closes.
        """
        if not self._keeps_its_partitions or not self.open_keys:
            self.release()
            return []
        found: list[tuple[int, Record]] = []
        for partition in self.owned:
            start = max(self.position(partition), self._drain_cursor.get(partition, 0))
            acked = self.acked(partition)
            for record in self._topic.log(partition).read_from(start, max_bytes=POLL_BUDGET_BYTES):
                self._drain_cursor[partition] = record.offset + 1
                if record.offset in acked:
                    continue
                if record.key in self.open_keys:
                    if partition in self._held_back:
                        self._handed_past_held_back.setdefault(partition, set()).add(record.offset)
                    found.append((partition, record))
                    if limit is not None and len(found) >= limit:
                        return found
                else:
                    self._held_back.setdefault(partition, record.offset)
        return found

    def _read_what_we_hold(self, limit: int | None) -> list[tuple[int, Record]]:
        """Whatever is new in the partitions this consumer already owns."""
        found: list[tuple[int, Record]] = []
        budget = POLL_BUDGET_BYTES
        for partition in self.owned:
            if budget <= 0:
                break
            wanted = None if limit is None else limit - len(found)
            taken = self._read_one_partition(partition, wanted, budget)
            found.extend(taken)
            budget -= sum(len(record.value) for _partition, record in taken)
            if limit is not None and len(found) >= limit:
                return found
        return found

    def _read_one_partition(self, partition: int, limit: int | None,
                            budget: int) -> list[tuple[int, Record]]:
        """What is new in one partition this consumer owns, past what the group has acked ahead."""
        acked = self.acked(partition)
        found: list[tuple[int, Record]] = []
        for record in self._topic.log(partition).read_from(
                self.position(partition), max_bytes=budget, max_records=limit):
            if record.offset in acked:
                continue
            found.append((partition, record))
            if limit is not None and len(found) >= limit:
                break
        return found

    def position(self, partition: int) -> int:
        """The next offset this group should read from a partition. Zero for a group that is new."""
        return _position(self._home, partition, self._positions)

    def acked(self, partition: int) -> set[int]:
        """Offsets past the position that this group has committed, and that are never delivered again."""
        return _acked(self._home, partition, self._positions)

    def commit(self, partition: int, next_offset: int) -> None:
        """Record where to resume. `next_offset` is the NEXT record to read, not the last one read."""
        self.apply(self.plan_commit(partition, next_offset))

    def plan_commit(self, partition: int, next_offset: int) -> CommitPlan:
        """Check a commit, and work out what it does to the group's place, without doing it yet.

        Separate from `apply` because COMPLETE journals the plan before applying it (`server._complete`),
        and a journal replayed after a crash has no member left to ask: what a draining member skipped
        lives only in that member, so the plan carries its effect rather than the offset it was asked for.

        ★ A COMMIT MAY ONLY MOVE PAST WHAT THIS MEMBER WAS HANDED, AND NEVER BACK. It used to check only
        that the partition was held, so a commit naming any offset was written: one far ahead skipped
        records nobody had read, and retention could then delete them; one behind the group's position
        rewound it, and records already committed came round again. Refused, both are answered, and the
        group's position stays where the records it has really handled put it.
        """
        if partition not in self._owned:
            raise PartitionNotHeldError(f"partition {partition} is not held by this consumer")
        if self._due_by_header:
            return self._plan_commit_of_one(partition, next_offset - 1)
        handed = self._handed.get(partition)
        if handed is None or next_offset > handed + 1:
            raise RequestRefusedError(
                f"partition {partition}: a commit to {next_offset} passes what this consumer was handed"
                f" ({'nothing' if handed is None else f'through offset {handed}'})")
        if next_offset < self.position(partition):
            raise RequestRefusedError(
                f"partition {partition}: a commit to {next_offset} would move the group back from"
                f" {self.position(partition)}")
        acked = set(self.acked(partition))
        committing: frozenset[int] = frozenset()
        # A draining member skipped records of keys it had not opened; its position may not pass the
        # first of them, or the member that inherits the partition would never be handed it. What it
        # was handed past that record and is committing now goes into the acked-ahead set instead.
        if partition in self._held_back and next_offset > self._held_back[partition]:
            held_back = self._held_back[partition]
            committing = frozenset(offset for offset in self._handed_past_held_back.get(partition, ())
                                   if held_back <= offset < next_offset)
            acked |= committing
            next_offset = held_back
        # The position absorbs every acked offset it reaches, so the set holds only what lies past a
        # record still to be done.
        while next_offset in acked:
            acked.discard(next_offset)
            next_offset += 1
        return CommitPlan(partition, next_offset,
                          frozenset(offset for offset in acked if offset >= next_offset), committing)

    def _plan_commit_of_one(self, partition: int, offset: int) -> CommitPlan:
        """On a retry topic: commit the one record handed, whatever still waits before it.

        The record is added to the acked-ahead set, and the position absorbs it only when every record
        before it is done, so a record waiting out a long delay holds the position, and retention behind
        it, without holding up a single record behind it.
        """
        if (offset not in self._handed_offsets.get(partition, set()) or offset < self.position(partition)
                or offset in self.acked(partition)):
            raise RequestRefusedError(
                f"partition {partition}: offset {offset} is not a record this consumer is handling")
        acked = set(self.acked(partition)) | {offset}
        position = self.position(partition)
        while position in acked:
            acked.discard(position)
            position += 1
        return CommitPlan(partition, position, frozenset(acked))

    def apply(self, plan: CommitPlan) -> None:
        """Carry out a commit this member planned."""
        remaining = self._handed_past_held_back.get(plan.partition, set()) - plan.committing
        self._handed_past_held_back[plan.partition] = remaining
        write_commit(self._home, plan, self._positions)

    def attempt(self, partition: int, offset: int, token: str | None) -> int:
        """Count one START of a record by this group, and say how many starts it has had.

        ★ ON DISK, BESIDE THE OFFSET, because the count has to outlive whatever it counts. It lived in the
        server's memory, which outlives a consumer — so a page that kills the process reading it is
        still dead-lettered after three starts — but not the server itself: a restart between failures
        began the count again, and a poison record could be retried for ever. The group's directory
        already survives both, so the count sits there, written the same rename-into-place way.

        ★ A START IS COUNTED ONCE, however many times it is ASKED about. A client whose answer was lost
        sends the same request again on a new connection; the token the request carries is what makes
        the second one the same start rather than another, so a record is not dead-lettered early for
        starts it never had. A request without a token counts every time.

        ★ ONLY FOR A RECORD THIS MEMBER HOLDS AND WAS HANDED, and not one already committed. Anything
        else would let one consumer charge another's record with failures and see it dead-lettered
        without its handler ever running.
        """
        if partition not in self._owned:
            raise PartitionNotHeldError(f"partition {partition} is not held by this consumer")
        handed = self._handed.get(partition)
        never_handed = (offset not in self._handed_offsets.get(partition, set()) if self._due_by_header
                        else handed is None or offset > handed)
        if never_handed or offset < self.position(partition) or offset in self.acked(partition):
            raise RequestRefusedError(
                f"partition {partition}: offset {offset} is not a record this consumer is handling")
        starts = self._read_starts(partition)
        tokens = starts.setdefault(str(offset), [])
        if token is None or token not in tokens:
            tokens.append(token or uuid.uuid4().hex)
        self._write_starts(partition, starts)
        return len(tokens)

    def _read_starts(self, partition: int) -> dict[str, list[str]]:
        return _read_starts(self._home, partition)

    def _write_starts(self, partition: int, starts: dict[str, list[str]]) -> None:
        _write_starts(self._home, partition, starts)

    def release(self) -> None:
        """Give up every partition — a clean shutdown. A crash does the same thing, via the kernel."""
        for partition in self.owned:
            self._release_one(partition)
        self._held_back.clear()
        self._drain_cursor.clear()
        self._handed_past_held_back.clear()

    def lag(self) -> dict[int, int]:
        """Records this GROUP has not yet committed, by partition. For the admin socket."""
        return {partition: (self._topic.log(partition).next_offset - self.position(partition)
                            - len(self.acked(partition)))
                for partition in self._topic.owned_partitions}

    def _take_one_unowned(self) -> bool:
        """Acquire ONE partition nobody holds that has something unread in it, and say whether we did.

        Never takes one from a consumer that is alive: the flock is non-blocking, so a partition
        somebody holds simply refuses and we try the next.

        ★ ONLY A PARTITION WITH WORK IN IT. The first version took any free partition, read it, found
        nothing and moved on — so an idle reader, on every poll, took every free partition on the
        channel and handed them all back. With twelve readers and sixty-four partitions that pinned
        the server at 100% of a core with nothing whatever to read. A partition whose end is where the
        group already is costs a comparison of two integers now, and is left alone.
        """
        for partition in self._topic.owned_partitions:
            if partition in self._owned:
                continue
            if self._topic.log(partition).readable_offset <= self.position(partition):
                continue            # nothing unread: taking it would only mean handing it back
            if self._acquire(partition):
                return True
        return False

    def _acquire(self, partition: int) -> bool:
        """Take a partition nobody holds; False when a live consumer holds it."""
        handle = (self._home / f"{partition:04d}.owner").open("a")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()      # held by a live consumer; leave it alone
            return False
        self._owned[partition] = handle
        return True

    def _read_the_oldest(self, limit: int | None) -> list[tuple[int, Record]]:
        """On a COMPETING channel: the oldest record waiting in any partition we hold or nobody holds.

        ★ OLDEST FIRST, ACROSS PARTITIONS. A competing channel promises nothing about order, but its work
        does arrive in order: the stream head publishes a return's pages first to last, round-robin
        across the partitions. A consumer that drained the partition it held read every sixty-fourth
        page, and ten readers worked through a return in scattered stretches from wherever the
        placement happened to start, so the cockpit waited on the front of the return while the middle
        was read (a 286-page return, 2026-09-28). Taken oldest first, the pages are read first to
        last, and each form is settled as soon as its pages are.

        A consumer of a competing channel commits what it was handed before it asks again, so it holds
        nothing in hand here, and letting go of the partitions it is not reading from costs nothing;
        only the record's header is read to learn its age.
        """
        waiting: list[tuple[int, int, int]] = []
        for partition in self._topic.owned_partitions:
            log = self._topic.log(partition)
            position = self.position(partition)
            if log.readable_offset <= position:
                continue
            stamp = log.timestamp_at(position)
            if stamp is not None:
                waiting.append((stamp, position, partition))
        for _stamp, _position, partition in sorted(waiting):
            if partition not in self._owned and not self._acquire(partition):
                continue
            found = self._read_one_partition(partition, limit, POLL_BUDGET_BYTES)
            if found:
                for other in [held for held in self.owned if held != partition]:
                    self._release_one(other)
                return found
        return []

    def _read_the_due(self, limit: int | None, now_ns: int | None = None) -> list[tuple[int, Record]]:
        """On a retry topic: the record whose time to run came first, among those that are due.

        Every unacked record past each partition's position is looked at, in partitions this member holds
        or nobody holds, so the cost of a poll grows with what is waiting to be retried. A retry topic
        holds the few records whose handlers asked to wait; a stage of the pipeline that never raises
        `RetryLater` never writes one. A record due in a partition another member holds is that member's.
        """
        now_ns = time.time_ns() if now_ns is None else now_ns
        due: list[tuple[int, int, int, Record]] = []
        next_due: int | None = None
        for partition in self._topic.owned_partitions:
            log = self._topic.log(partition)
            position = self.position(partition)
            if log.readable_offset <= position:
                continue
            acked = self.acked(partition)
            for record in log.read_from(position, max_bytes=POLL_BUDGET_BYTES):
                if record.offset in acked:
                    continue
                not_before = _not_before(record)
                if not_before <= now_ns:
                    due.append((not_before, record.offset, partition, record))
                else:
                    next_due = not_before if next_due is None else min(next_due, not_before)
        self.next_due_ns = next_due
        for _not_before_ns, _offset, partition, record in sorted(due, key=lambda waiting: waiting[:3]):
            if partition not in self._owned and not self._acquire(partition):
                continue
            for other in [held for held in self.owned if held != partition]:
                self._release_one(other)
            return [(partition, record)][:limit]
        return []

    def _keep_only_what_we_need(self) -> None:
        """Once the walk over the channel has found nothing, hold on to one partition and no more.

        Only where the channel said nothing is mid-assembly behind a partition. The walk above is how
        a consumer FINDS work, so on an idle channel it ends up holding everything it looked at — and
        one reader holding everything is precisely the state that had seven of eight sitting at
        nothing. A reader processes one page at a time and one partition is all it can use, so what it
        keeps is what it can use and the rest goes back for whoever arrives next.

        A consumer that hands partitions back does not rebalance anybody: nothing is taken from anybody,
        the consumer that holds a partition releases it, and only on a channel with no affinity to lose.
        """
        if self._keeps_its_partitions:
            return
        for partition in self.owned[1:]:
            self._release_one(partition)

    def _release_one(self, partition: int) -> None:
        self._handed.pop(partition, None)
        self._handed_offsets.pop(partition, None)
        handle = self._owned.pop(partition)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _not_before(record: Record) -> int:
    """When a retry topic's record may run, in nanoseconds since the epoch; a record without it, at once."""
    try:
        return int(record.headers.get(RETRY_NOT_BEFORE_HEADER, 0))
    except ValueError:
        return 0
