"""A topic: N partitions, and the rule that puts a record in one of them (#450).

    produce(record)  -> (partition, offset)

★ ONE KEY ALWAYS LANDS IN ONE PARTITION, and everything else rests on it. A consumer group gives each
partition to exactly one consumer, so "all of a document's pages reach one aggregator" is not a feature
built on top — it is what partitioning already does, provided the mapping is stable. The moment the
same key could land in two partitions, two consumers are assembling one document and we are back to
2026-09-22.

⚠ SO THE HASH MUST BE STABLE ACROSS PROCESSES, and Python's built-in `hash()` is NOT. It is randomized
per interpreter for bytes and str (PYTHONHASHSEED), so a producer in one process and a producer in
another would disagree about where a key belongs — intermittently, and only in production, where two
processes is the normal case. `crc32` is stable, fast, and in the standard library. Kafka uses murmur2
for the same reason and we would be compatible with it by swapping this one function.

A record with NO key has no home to keep, so it is spread round-robin: an ungrouped channel wants its
work shared out, which is the opposite requirement and the same mechanism.

★ A RESENT PRODUCE IS WRITTEN ONCE. A client that loses the server's answer sends the same request
again, and the server may already have written it: usually the server was killed, which is why the
answer never came. So each produce carries the client's producer id and a sequence number, and the topic
answers a repeat with the placement it gave the first time. The two are written INTO the record, as the
headers `stream.producer` and `stream.sequence`, because a table kept in a file beside it would be a
second write, and a kill between the two is precisely the case this exists for. The topic rebuilds its
table from the last `PRODUCER_MEMORY_SECONDS` of its log when it is opened. Kafka keeps the producer id
and sequence in its record batches for the same reason.
"""
from __future__ import annotations

import itertools
import time
import zlib
from collections import OrderedDict
from collections.abc import Iterable
from pathlib import Path

from .partition_log import PartitionLog
from .record import PRODUCER_HEADER, SEQUENCE_HEADER, Record, Sealer

#: How long a producer's recent placements are remembered. A resend follows the lost answer within
#: seconds, on the client's one reconnect; an hour covers a server that took that long to come back.
PRODUCER_MEMORY_SECONDS = 3600

#: The producer id every journal entry's outputs are appended under begins with this (`journal.py`). Such a
#: producer is remembered until its entry is finished — `forget_producer` — not for PRODUCER_MEMORY_SECONDS:
#: an entry whose outputs keep failing to land, or a server down for longer than that after a crash partway
#: through them, would otherwise have its outputs that did land written a second time when it is finished.
JOURNAL_PRODUCER_PREFIX = "complete."

#: How many recent placements are kept for each producer. A client can have several produces in flight
#: on one connection, and all of them are resent when it drops; this is far more than it ever has.
PLACEMENTS_KEPT = 1024


def partition_for(key: bytes | None, partitions: int, *, spread: int = 0,
                  producer: str | None = None, sequence: int | None = None) -> int:
    """Which partition a record belongs to. Stable for a key, and for a producer's numbered record.

    A keyless record that carries its producer's id and sequence goes round-robin by that sequence,
    starting from a partition the producer's id picks, so a resend of it lands in the partition the first
    attempt did — whichever strand the resend reaches, and after a server restart — and that partition's
    table recognizes it (`Topic.produce_once`). A keyless record with neither, only a test's direct write,
    is spread round-robin.

    ★ ROUND-ROBIN, NOT A HASH OF THE SEQUENCE. The stream head publishes a return's pages in page order,
    and round-robin puts consecutive pages in consecutive partitions. Readers each take the lowest free
    partition with work, so ten readers work through the return in several contiguous stretches at once,
    and the assembler can settle a form as soon as its stretch passes it. A hash scattered the pages: in a
    replay of a 286-page return, at most 4 of its 67 forms had settled by the time half its pages were
    read, against 35 with round-robin (2026-09-28). The producer's id
    sets the starting partition because a journaled COMPLETE numbers its outputs from zero: without it,
    every COMPLETE's first output would land in partition 0.
    """
    if key is not None:
        return zlib.crc32(key) % partitions
    if producer is not None and sequence is not None:
        return (zlib.crc32(producer.encode()) + sequence) % partitions
    return spread % partitions


class Topic:
    """One named stream, stored as `partitions` append-only logs under `path`.

    A strand opens a topic over the partitions it owns and no others (`owned`); every partition number
    keeps its meaning, and asking for a partition another strand owns is a KeyError, never a second
    writer. With no `owned`, the topic is every partition — a tool, a test, the journal replay.
    """

    def __init__(self, path: str | Path, partitions: int = 8, *, seal: Sealer | None,
                 owned: Iterable[int] | None = None, recall_every_journal_output: bool = False) -> None:
        if partitions < 1:
            raise ValueError("a topic needs at least one partition")
        self._path = Path(path)
        self._partitions = partitions
        self._owned = sorted(owned) if owned is not None else list(range(partitions))
        self._logs = {index: PartitionLog(self._path / f"{index:04d}", seal=seal) for index in self._owned}
        #: Only for keyless records with no producer sequence. Nothing depends on WHICH partition such a
        #: record lands in, only that they are spread.
        self._round_robin = itertools.count()
        #: partition -> producer id -> (when last seen, sequence -> offset). A producer's numbered record
        #: always goes to one partition (`partition_for`), so each partition keeps its own table, built
        #: from its own log on first use (`_recall_producers`).
        self._producers: dict[int, dict[str, tuple[float, OrderedDict[int, int]]]] = {}
        #: The journal replay opens topics with this, and recalls every journal entry's outputs still in the
        #: log however old; it runs before any consumer or retention can touch the log, so nothing a pending
        #: entry wrote can have gone. A strand's topic, opened after, recalls only the last hour of them.
        self._recall_every_journal_output = recall_every_journal_output

    @property
    def partitions(self) -> int:
        """How many partitions the topic has in all — the width it was defined with."""
        return self._partitions

    @property
    def owned_partitions(self) -> list[int]:
        """The partitions this opening of the topic holds."""
        return self._owned

    def produce(self, record: Record) -> tuple[int, int]:
        """Append one record to the partition its key names. Returns (partition, offset)."""
        partition = partition_for(record.key, self._partitions, spread=next(self._round_robin))
        return partition, self.log(partition).append(record)

    def produce_to(self, partition: int, record: Record) -> int:
        """Append one record to a partition chosen already. Returns its offset."""
        return self.log(partition).append(record)

    def produce_once(self, record: Record, producer: str, sequence: int) -> tuple[int, int]:
        """Append a record unless this producer already sent this sequence; either way, its placement."""
        partition = partition_for(record.key, self._partitions, producer=producer, sequence=sequence)
        return partition, self.produce_once_to(partition, record, producer, sequence)

    def produce_once_to(self, partition: int, record: Record, producer: str, sequence: int) -> int:
        """`produce_once` into a partition chosen already by `partition_for`. Returns the offset."""
        placements = self._placements_of(partition, producer)
        if sequence in placements:
            return placements[sequence]
        stamped = Record(value=record.value, key=record.key, timestamp_ns=record.timestamp_ns,
                         headers={**record.headers, PRODUCER_HEADER: producer,
                                  SEQUENCE_HEADER: str(sequence)})
        offset = self.log(partition).append(stamped)
        self._remember(partition, producer, sequence, offset)
        return offset

    def _producer_table(self, partition: int) -> dict[str, tuple[float, OrderedDict[int, int]]]:
        if partition not in self._producers:
            self._producers[partition] = self._recall_producers(partition)
        return self._producers[partition]

    def _placements_of(self, partition: int, producer: str) -> OrderedDict[int, int]:
        seen = self._producer_table(partition).get(producer)
        return seen[1] if seen is not None else OrderedDict()

    def _remember(self, partition: int, producer: str, sequence: int, offset: int) -> None:
        table, now = self._producer_table(partition), time.time()
        placements = self._placements_of(partition, producer)
        placements[sequence] = offset
        while len(placements) > PLACEMENTS_KEPT:
            placements.popitem(last=False)
        table[producer] = (now, placements)
        for stale in [name for name, (seen_at, _placements) in table.items()
                      if now - seen_at > PRODUCER_MEMORY_SECONDS
                      and not name.startswith(JOURNAL_PRODUCER_PREFIX)]:
            del table[stale]

    def _recall_producers(self, partition: int) -> dict[str, tuple[float, OrderedDict[int, int]]]:
        """Every producer's recent offsets in one partition, read back out of the records they wrote.

        Read once, when a server that has just started receives its first such produce for this
        partition — which is when a resend is likeliest, since a server restart is what makes a client
        resend.
        """
        since_ns = time.time_ns() - PRODUCER_MEMORY_SECONDS * 1_000_000_000
        recalled: dict[str, tuple[float, OrderedDict[int, int]]] = {}
        log = self.log(partition)
        offset = log.first_offset
        while records := log.read_from(offset, max_bytes=4 * 1024 * 1024):
            for record in records:
                producer = record.headers.get(PRODUCER_HEADER)
                if producer is None or (record.timestamp_ns < since_ns and not (
                        self._recall_every_journal_output
                        and producer.startswith(JOURNAL_PRODUCER_PREFIX))):
                    continue
                seen_at = record.timestamp_ns / 1_000_000_000
                previous_at, placements = recalled.get(producer, (seen_at, OrderedDict()))
                placements[int(record.headers[SEQUENCE_HEADER])] = record.offset
                recalled[producer] = (max(previous_at, seen_at), placements)
            offset = records[-1].offset + 1
        return recalled

    def forget_producer(self, partition: int, producer: str) -> None:
        """Drop a producer from a partition's table — a journal entry that has finished."""
        self._producer_table(partition).pop(producer, None)

    def log(self, partition: int) -> PartitionLog:
        """One partition's log. A KeyError for a partition this opening does not hold."""
        return self._logs[partition]

    def depth(self) -> int:
        """How many records this opening holds in all. For health checks and tests, never a decision."""
        return sum(log.next_offset for log in self._logs.values())
