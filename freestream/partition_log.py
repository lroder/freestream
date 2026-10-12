"""An append-only log, one per partition, kept as a directory of segments — the store (#450).

Kafka's storage model, with the bytes ours. We took the SEMANTICS from something that exists outside
this repo on purpose: a design that lives only in prose Claude wrote is one Claude has already shown it
will drift from. The server seals what the segments hold (`sealing.py`, folded in 2026-09-26),
and `seal` is a required argument here so that no caller writes plaintext without saying so.

    append(record)               -> the offset it was given. Monotonic, gapless, per partition.
    read_from(offset, max_bytes) -> records from there on, in order, up to a byte budget.
    drop_segments_before(offset) -> retention: whole segments nobody needs any more are deleted.

    <partition>/00000000000000000000.log    records 0 .. n
    <partition>/00000000000000004821.log    records 4821 .. — named by the first offset it holds

★ WHY APPEND-ONLY IS THE WHOLE TRICK. A reader's position is a number, so a consumer that dies is
replaced by one that resumes from the last offset it committed. The first version of this queue held a
directory per document until its set completed, which was a retention policy I had to invent and
maintain. An offset replaces that — and a segment is the unit that lets the log SHRINK again once every
reader has passed it, which the single-file version never could: it held every page of every return
it had ever carried, 19 MB for one 286-page scan.

★ A READ SEEKS; IT NEVER SLURPS. The first version called `read_bytes()` on the whole file for every
fetch. On a KEYED channel all of one document's reads land in one partition, so the aggregator re-read a
growing multi-megabyte file every 20 ms and the server spent 57% of a core on it (measured on the 286-page
scanned return, 2026-09-24) — a cost quadratic in the size of the document. Each segment keeps the byte
position of every record it holds, so a read opens the one segment the offset is in, seeks, and reads
exactly the records it hands back.

★ A TORN TAIL IS CUT OFF ON OPEN. The server is the only writer, so bytes past the last whole record can
only be a write the process died in the middle of. Before this, a reopened log remembered where the
whole records ended but appended in "ab" mode AFTER the torn bytes — so the first record written after a
crash sat behind garbage, at a position the index did not point to, and was unreadable. Kafka truncates
on recovery for exactly this reason.

★ A CORRUPT RECORD IS LOST, AND NOTHING ELSE IS. Every record carries a checksum (`record.py`). One that
fails it is whole on disk — its lengths fit, which a torn write's do not — so it is not a torn write, and
cutting the file there would throw away every good record after it. The segment looks for the next record
that passes instead, and that record's own offset — every record stores it — says exactly which offsets
fell in between. Those are LOST: never delivered, logged as an error, and still counted, so every later
offset means what it meant when it was written. With nothing whole after it, the corrupt record is counted
as one lost offset at the extent its lengths claim, so the next append never reuses an offset a group may
already have committed past. Before the checksum, a malformed record stopped the server opening the topic
at all (the adversarial review, 2026-09-25).

★ ORDER IS PER PARTITION AND NOWHERE ELSE, exactly as Kafka promises. Records with one key land in one
partition and are read in the order they were written; records across partitions have no order at all
and nothing may assume one.

"""
from __future__ import annotations

import bisect
import logging
import struct
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .durability import mark_written
from .record import (
    HEADER_BYTES,
    CorruptRecordError,
    Record,
    Sealer,
    TruncatedRecordError,
    frame_timestamp_ns,
)

log = logging.getLogger(__name__)

#: How large the active segment may grow before a new one is started. It is the grain of retention —
#: nothing smaller than a whole segment is ever deleted — so it trades how promptly a consumed log
#: shrinks against how many files a partition holds. At about 68 KB a scanned page, 16 MB is some 240
#: pages: a segment usually rolls once or twice inside one large return.
SEGMENT_BYTES = 16 * 1024 * 1024

#: The width of a segment's filename — Kafka's, which fits any offset a 64-bit counter can reach and
#: keeps `ls` in offset order.
_NAME_DIGITS = 20


#: A record begins with its offset, which is how a segment finds its footing again past a corrupt one.
_OFFSET = struct.Struct("<Q")


@dataclass
class _Segment:
    """One file of the log: the records from `base_offset` on, and where each of them lies."""

    base_offset: int
    path: Path
    #: (start, end) byte range of each offset's record, in offset order; None for an offset LOST to
    #: corruption, which still counts, so the offsets after it keep their meaning.
    spans: list[tuple[int, int] | None] = field(default_factory=list)
    #: where the last whole record ends — and so where the next one is written.
    end: int = 0
    #: What seals the records this segment holds. None only where a caller asked for plaintext.
    seal: Sealer | None = None

    @property
    def next_offset(self) -> int:
        return self.base_offset + len(self.spans)

    @property
    def holds_records(self) -> bool:
        return any(span is not None for span in self.spans)

    @classmethod
    def opened(cls, path: Path, seal: Sealer | None = None) -> _Segment:
        """Index an existing segment, step past a corrupt record, and cut off a torn tail."""
        segment = cls(base_offset=int(path.stem), path=path, seal=seal)
        raw = path.read_bytes()
        at = 0
        while at < len(raw):
            expected = segment.next_offset
            following, found = _record_at(raw, at, expected, seal)
            if found is _Found.WHOLE:
                segment.spans.append((at, following))
                at = following
                continue
            resumed = _next_whole_record(raw, at + 1, expected, seal)
            if resumed is not None:
                # A whole record follows, which a torn write never has: whatever lies between — a
                # checksum that fails, or a damaged length claiming more bytes than the file holds — is
                # lost, and the offset of the record found says how many offsets that was.
                at, resumed_offset = resumed
            elif found is _Found.TRUNCATED:
                break                   # a write the process died inside: everything before it stands
            else:
                # Nothing whole after it. It is not a torn write — its lengths all fit, and a torn
                # write's cannot — so a consumer may already have been handed this offset. It is
                # counted as lost at the extent its lengths claim, and the next append takes the offset
                # after it rather than reusing one a group may have committed past.
                at, resumed_offset = following, expected + 1
            log.error("%s: offset(s) %d to %d are damaged and lost", path, expected, resumed_offset - 1)
            segment.spans.extend([None] * (resumed_offset - expected))
        segment.end = at
        if at < len(raw):
            with path.open("r+b") as handle:
                handle.truncate(at)
        return segment

    def write(self, record: Record) -> int:
        offset = self.next_offset
        framed = record.framed(offset, self.seal)
        with self.path.open("ab") as handle:
            handle.write(framed)
        self.spans.append((self.end, self.end + len(framed)))
        self.end += len(framed)
        return offset

    def read_from(self, offset: int, budget: int | None,
                  max_records: int | None) -> tuple[list[Record], int, int]:
        """Records from `offset` on, up to `budget` bytes and `max_records` records.

        Returns them, the bytes taken, and the offset the read reached — past any LOST offset it
        stepped over, which is why that is not simply `offset + len(records)`.

        Reads exactly the bytes it hands back: the segment knows where every record lies, so the range
        is worked out before anything is read. Never stops before the first record, so a single record
        larger than the budget is still delivered rather than stalling its consumer for ever.
        """
        index = offset - self.base_offset
        chosen: list[tuple[int, int]] = []
        while index < len(self.spans):
            span = self.spans[index]
            if span is not None:
                if chosen and budget is not None and span[1] - chosen[0][0] > budget:
                    break
                if max_records is not None and len(chosen) >= max_records:
                    break
                chosen.append(span)
            index += 1
        if not chosen:
            return [], 0, self.base_offset + index
        start = chosen[0][0]
        with self.path.open("rb") as handle:
            handle.seek(start)
            raw = handle.read(chosen[-1][1] - start)
        records = [Record.unframed(raw, span_start - start, self.seal)[0]
                   for span_start, _span_end in chosen]
        return records, len(raw), self.base_offset + index

    def timestamp_from(self, offset: int) -> int | None:
        """When the first record here at or after `offset` was written, read from its header alone."""
        for span in self.spans[max(offset - self.base_offset, 0):]:
            if span is not None:
                with self.path.open("rb") as handle:
                    handle.seek(span[0])
                    return frame_timestamp_ns(handle.read(HEADER_BYTES))
        return None

    def newest_timestamp_ns(self) -> int | None:
        """When the last record still readable here was written, or None for a segment with none."""
        for span in reversed(self.spans):
            if span is not None:
                with self.path.open("rb") as handle:
                    handle.seek(span[0])
                    return Record.unframed(handle.read(span[1] - span[0]), 0,
                                           self.seal)[0].timestamp_ns
        return None


class _Found(Enum):
    """What `_record_at` found."""

    WHOLE = "whole"
    CORRUPT = "corrupt"
    TRUNCATED = "truncated"


def _record_at(raw: bytes, at: int, expected: int, seal: Sealer | None) -> tuple[int, _Found]:
    """Where the record at `at` ends, and what it is.

    Whole when it passes its checksum and is `expected`; corrupt when it does not; truncated when the
    bytes end inside it, and then the end returned is only the end of the bytes.
    """
    try:
        record, following = Record.unframed(raw, at, seal)
    except TruncatedRecordError:
        return len(raw), _Found.TRUNCATED
    except CorruptRecordError as corrupt:
        return corrupt.end, _Found.CORRUPT
    return following, _Found.WHOLE if record.offset == expected else _Found.CORRUPT


def _next_whole_record(raw: bytes, start: int, expected: int,
                       seal: Sealer | None) -> tuple[int, int] | None:
    """The first position after `start` where a record that passes its checksum begins, and its offset.

    Only offsets past `expected` qualify, since the damaged record was `expected` and every record after
    it has a larger one; a match of eight bytes that merely look like such an offset fails the checksum.
    """
    limit = expected + len(raw) // HEADER_BYTES + 1
    for at in range(start, len(raw) - HEADER_BYTES + 1):
        (stored,) = _OFFSET.unpack_from(raw, at)
        if not expected < stored <= limit:
            continue
        try:
            record, _following = Record.unframed(raw, at, seal)
        except (TruncatedRecordError, CorruptRecordError):
            continue
        return at, record.offset
    return None


class PartitionLog:
    """One partition's records, appended in order and read back by offset, in segment files."""

    def __init__(self, directory: str | Path, *, seal: Sealer | None,
                 segment_bytes: int | None = None) -> None:
        self._directory = Path(directory)
        # Read at construction rather than bound as a default, so the module setting is the one a
        # server actually uses — and the one a test can shrink to watch a real roll and a real delete.
        self._segment_bytes = segment_bytes or SEGMENT_BYTES
        self._adopt_the_single_file_layout()
        self._directory.mkdir(parents=True, exist_ok=True)
        # One writer at a time within a process. Across processes there is no second writer: only the
        # server opens a log, which is the whole reason the server exists.
        self._appending = threading.Lock()
        self._seal = seal
        self._segments = [_Segment.opened(path, seal) for path in sorted(self._directory.glob("*.log"))]
        if not self._segments:
            self._segments.append(self._new_segment(0))
        self._account_for_what_was_cut_off()
        #: Everything found on opening was on the disk already.
        self._readable = self.next_offset

    @property
    def next_offset(self) -> int:
        """The offset the next appended record will be given — and so the log's length."""
        return self._segments[-1].next_offset

    @property
    def first_offset(self) -> int:
        """The oldest offset still held. Everything before it has been retained away."""
        return self._segments[0].base_offset

    @property
    def active_segment(self) -> Path:
        """The file being appended to — for a test that needs to tear a write."""
        return self._segments[-1].path

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    @property
    def bytes_held(self) -> int:
        """How many bytes of whole records the partition's segments hold — for the admin socket."""
        return sum(segment.end for segment in self._segments)

    def append(self, record: Record) -> int:
        """Add one record, and return the offset it was given.

        On a strand, the record becomes readable once the strand's flush has put it on the disk
        (`durability.py`): a consumer handed a record that a power cut then erased would act on it, and the
        producer's resend would be written and handed out again.
        """
        with self._appending:
            active = self._segments[-1]
            new_segment = bool(active.spans and active.end >= self._segment_bytes)
            if new_segment:
                active = self._new_segment(active.next_offset)
                self._segments.append(active)
            offset = active.write(record)
        if not mark_written(active.path, directory_changed=new_segment or offset == active.base_offset,
                            then=lambda: self._now_readable(offset + 1)):
            self._now_readable(offset + 1)
        return offset

    def _now_readable(self, next_offset: int) -> None:
        self._readable = max(self._readable, next_offset)

    @property
    def readable_offset(self) -> int:
        """The offset after the last record a consumer may be handed: the last one on the disk."""
        return self._readable

    def timestamp_at(self, offset: int) -> int | None:
        """When the first readable record at or after `offset` was written; None when there is none.

        Reads that record's header and nothing else, so asking it of every partition costs a few dozen
        bytes apiece however large the records are.
        """
        if offset >= self._readable:
            return None
        offset = max(offset, self.first_offset)
        index = bisect.bisect_right([segment.base_offset for segment in self._segments], offset) - 1
        for segment in self._segments[index:]:
            stamp = segment.timestamp_from(offset)
            if stamp is not None:
                return stamp
        return None

    def read_from(self, offset: int, *, max_bytes: int | None = None,
                  max_records: int | None = None) -> list[Record]:
        """Records from `offset` on, in order, up to about `max_bytes` and at most `max_records`.

        An offset past the end is a caught-up consumer and gets nothing. An offset BEFORE the oldest
        segment still held reads from the oldest one instead: retention only ever drops what every
        group had already committed past, so the only consumer that can ask for it is a group created
        after the fact, and history it never had is not history it lost.

        ★ IT STOPS AT THE FIRST SEGMENT IT DID NOT FINISH. It used to move on to the next segment
        whenever budget was left, and a segment read always takes at least one record — so a read whose
        budget ran out partway through a segment skipped that segment's remaining records and handed
        over the next segment's first one. A consumer that committed that record's offset had passed
        over the skipped ones for good. The records handed back are contiguous now, always.
        """
        if offset >= self._readable:
            return []
        offset = max(offset, self.first_offset)
        # Never past the last record on the disk, however the read began.
        max_records = (self._readable - offset if max_records is None
                       else min(max_records, self._readable - offset))
        index = bisect.bisect_right([segment.base_offset for segment in self._segments], offset) - 1
        records: list[Record] = []
        budget, wanted = max_bytes, max_records
        for segment in self._segments[index:]:
            if offset >= segment.next_offset:
                continue
            taken, used, offset = segment.read_from(offset, budget, wanted)
            records.extend(taken)
            if offset < segment.next_offset:
                break                   # stopped inside this segment: the budget or the count ran out
            if budget is not None:
                budget -= used
                if budget <= 0:
                    break
            if wanted is not None:
                wanted -= len(taken)
                if wanted <= 0:
                    break
        return records

    def drop_segments_before(self, offset: int) -> int:
        """Delete every whole segment whose records all sit below `offset`. Returns how many went.

        Never the active segment, however far `offset` reaches: that is the file being written to, and
        an empty log still needs one to append into.
        """
        with self._appending:
            dropped = 0
            while len(self._segments) > 1 and self._segments[0].next_offset <= offset:
                self._segments.pop(0).path.unlink(missing_ok=True)
                dropped += 1
            return dropped

    def drop_segments_older_than(self, cutoff_ns: int) -> int:
        """Delete every segment whose newest record was written before `cutoff_ns`. Returns how many went.

        Retention by age, for a topic nobody reads past — the dead letters. The active segment goes too
        when everything in it is that old, replaced by an empty one at the same next offset: a topic
        that receives a dead letter a month rarely fills a segment, and its only file would otherwise be
        kept for ever.
        """
        with self._appending:
            dropped = 0
            while self._segments:
                oldest = self._segments[0]
                newest = oldest.newest_timestamp_ns()
                if newest is None or newest >= cutoff_ns:
                    break
                if len(self._segments) == 1:
                    self._segments.append(self._new_segment(oldest.next_offset))
                self._segments.pop(0).path.unlink(missing_ok=True)
                dropped += 1
            return dropped

    def _account_for_what_was_cut_off(self) -> None:
        """Count the offsets a torn or corrupt tail took out of a segment that is not the last.

        Only the active segment is written to, so a segment followed by another was whole when the next
        one began, and every offset up to that one's first existed. A tail cut off here held some of
        them; they are marked LOST so each later offset still means what it meant.
        """
        for segment, following in zip(self._segments, self._segments[1:], strict=False):
            missing = following.base_offset - segment.next_offset
            if missing > 0:
                log.error("%s: offset(s) %d to %d were in a damaged tail and are lost", segment.path,
                          segment.next_offset, following.base_offset - 1)
                segment.spans.extend([None] * missing)

    def _new_segment(self, base_offset: int) -> _Segment:
        path = self._directory / f"{base_offset:0{_NAME_DIGITS}d}.log"
        path.touch()
        return _Segment(base_offset=base_offset, path=path, seal=self._seal)

    def _adopt_the_single_file_layout(self) -> None:
        """A log written before segments existed is one file beside where the directory now goes.

        It becomes the directory's first segment, whole — it starts at offset 0, which is exactly what
        its filename in the new layout says. Nothing is rewritten and no committed offset moves.
        """
        single = self._directory.with_suffix(".log")
        if single.is_file() and not self._directory.exists():
            self._directory.mkdir(parents=True)
            single.rename(self._directory / f"{0:0{_NAME_DIGITS}d}.log")
