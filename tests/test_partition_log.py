"""The record format and the append-only log (#450).

Storage taken from Kafka's model on purpose — see `docs/design/document-read-as-an-event-stream.md`.
The tests that matter here are the ones about a log's TAIL: a reader catching up with a writer, and a
partial write. A log that cannot tell "the end of what is written" from "a damaged record" will read
garbage off the end of a partial append and hand it to a consumer as real work.
"""
import struct
import threading
import zlib

import pytest

from freestream.partition_log import PartitionLog
from freestream.record import HEADER_BYTES, CorruptRecordError, Record, TruncatedRecordError


def _log(tmp_path, name="p0"):
    return PartitionLog(tmp_path / name, seal=None)


# --------------------------------------------------------------------------- the record


def test_a_record_survives_framing_unchanged():
    original = Record(value=b"page bytes", key=b"doc-1", headers={"total": "190"})
    read_back, _ = Record.unframed(original.framed(offset=7))
    assert (read_back.value, read_back.key, read_back.headers) == (b"page bytes", b"doc-1",
                                                                   {"total": "190"})
    assert read_back.offset == 7, "the offset is assigned at framing, not carried by the producer"


def test_a_record_needs_no_key_and_no_headers():
    """An ungrouped channel sends neither. Absent must round-trip as absent, not as empty bytes."""
    read_back, _ = Record.unframed(Record(value=b"just a value").framed(offset=0))
    assert read_back.key is None
    assert read_back.headers == {}


def test_a_value_may_hold_arbitrary_bytes():
    """The body is a single-page PDF, so nothing may assume text — a length prefix, never a sentinel."""
    awkward = bytes(range(256)) + b"\x00\x00" + b"}{not json"
    read_back, _ = Record.unframed(Record(value=awkward).framed(offset=1))
    assert read_back.value == awkward


def test_reading_says_where_the_next_record_starts():
    first = Record(value=b"one").framed(offset=0)
    second = Record(value=b"two").framed(offset=1)
    record, nxt = Record.unframed(first + second)
    assert record.value == b"one"
    assert nxt == len(first)


def test_a_record_cut_short_is_truncated_and_says_so():
    """★ A log must tell these two apart: truncated is the END, corrupt is POISON.

    They call for opposite responses — a reader stops on one and abandons the message on the other —
    so a log that raised the same error for both would either retry the end of the world forever or
    throw away a document because a writer was mid-append.
    """
    whole = Record(value=b"a value long enough to cut").framed(offset=0)
    with pytest.raises(TruncatedRecordError):
        Record.unframed(whole[:-5])
    with pytest.raises(TruncatedRecordError):
        Record.unframed(whole[:HEADER_BYTES - 1])


# --------------------------------------------------------------------------- the log


def test_offsets_are_gapless_and_start_at_zero(tmp_path):
    log = _log(tmp_path)
    assert [log.append(Record(value=str(n).encode())) for n in range(5)] == [0, 1, 2, 3, 4]


def test_records_come_back_in_the_order_they_went_in(tmp_path):
    log = _log(tmp_path)
    for n in range(4):
        log.append(Record(value=f"page {n}".encode()))
    assert [r.value for r in log.read_from(0)] == [b"page 0", b"page 1", b"page 2", b"page 3"]


def test_reading_from_an_offset_skips_what_came_before(tmp_path):
    """★ WHY A REPLACEMENT CONSUMER NEEDS NO RETENTION POLICY. Its position is a number."""
    log = _log(tmp_path)
    for n in range(6):
        log.append(Record(value=str(n).encode()))
    assert [r.offset for r in log.read_from(4)] == [4, 5]


def test_a_caught_up_reader_gets_nothing_rather_than_an_error(tmp_path):
    log = _log(tmp_path)
    log.append(Record(value=b"only one"))
    assert log.read_from(1) == []
    assert log.read_from(99) == [], "a reader ahead of the log is caught up, not broken"


def test_an_empty_log_reads_as_empty(tmp_path):
    assert _log(tmp_path).read_from(0) == []
    assert _log(tmp_path).next_offset == 0


def test_a_reopened_log_knows_where_it_got_to(tmp_path):
    """The log outlives the process that wrote it, which is why a consumer can be replaced at all."""
    first = _log(tmp_path)
    for n in range(3):
        first.append(Record(value=str(n).encode()))

    reopened = _log(tmp_path)

    assert reopened.next_offset == 3, "appending must not overwrite what is already there"
    assert [r.value for r in reopened.read_from(0)] == [b"0", b"1", b"2"]
    assert reopened.append(Record(value=b"3")) == 3


def test_a_partial_write_at_the_tail_is_ignored_rather_than_read(tmp_path):
    """★ THE ONE THAT MATTERS. A writer interrupted mid-append must not hand a reader garbage.

    Everything written whole stands; the half-record at the end is simply not there yet, and appending
    after it does not compound the damage.
    """
    log = _log(tmp_path)
    log.append(Record(value=b"whole one"))
    log.append(Record(value=b"whole two"))
    with log.active_segment.open("ab") as handle:
        handle.write(Record(value=b"interrupted").framed(offset=2)[:9])   # a torn header

    reopened = _log(tmp_path)

    assert [r.value for r in reopened.read_from(0)] == [b"whole one", b"whole two"]
    assert reopened.next_offset == 2, "the torn tail took no offset"


def test_a_write_torn_inside_its_body_is_ignored_too(tmp_path):
    """The likelier tear, and the one a header-length check alone will miss.

    A header is 24 bytes and lands in one go; a body is a whole page and does not. Found by deleting
    the body-length check and watching the header-tear test above still pass — which is what mutation
    is for, and is the only reason this case is here.
    """
    log = _log(tmp_path)
    log.append(Record(value=b"whole one"))
    with log.active_segment.open("ab") as handle:
        handle.write(Record(value=b"x" * 400).framed(offset=1)[:HEADER_BYTES + 12])   # header, part body

    reopened = _log(tmp_path)

    assert [r.value for r in reopened.read_from(0)] == [b"whole one"]
    assert reopened.next_offset == 1, "a record whose body never landed is not a record"


def test_concurrent_appends_do_not_interleave_or_collide(tmp_path):
    """Offsets are assigned under one lock, so two threads appending never share one or tear a record."""
    log = _log(tmp_path)
    given: list[int] = []
    barrier = threading.Barrier(8)

    def appender(n: int) -> None:
        barrier.wait()
        given.append(log.append(Record(value=f"from {n}".encode())))

    threads = [threading.Thread(target=appender, args=(n,)) for n in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(given) == list(range(8)), "every appender got its own offset"
    assert len(log.read_from(0)) == 8, "and every record is readable"
    assert [r.offset for r in log.read_from(0)] == list(range(8))


def test_nothing_reads_the_value_to_decide_where_a_record_goes(tmp_path):
    """★ THE INVARIANT I BROKE ONCE ALREADY. The store never interprets a key or a value.

    Two records whose VALUES are identical and whose keys differ are still two records in one log, in
    the order they arrived. The store has no opinion about what is inside them.
    """
    log = _log(tmp_path)
    log.append(Record(value=b"same", key=b"alpha"))
    log.append(Record(value=b"same", key=b"beta"))
    assert [(r.key, r.offset) for r in log.read_from(0)] == [(b"alpha", 0), (b"beta", 1)]


# --------------------------------------------------------------------------- recovery


def test_a_record_written_after_a_torn_tail_is_readable(tmp_path):
    """★ THE BUG THE TORN-TAIL TESTS DID NOT REACH. They asserted the offset and never appended again.

    A reopened log knew where its whole records ended but opened in "ab" mode, so the next record was
    written AFTER the torn bytes while the index said it started BEFORE them. The first record after a
    crash was unreadable. Recovery now cuts the tail off, as Kafka does.
    """
    log = _log(tmp_path)
    log.append(Record(value=b"before the crash"))
    with log.active_segment.open("ab") as handle:
        handle.write(Record(value=b"interrupted").framed(offset=1)[:HEADER_BYTES + 3])

    reopened = _log(tmp_path)
    assert reopened.append(Record(value=b"after the crash")) == 1

    assert [r.value for r in _log(tmp_path).read_from(0)] == [b"before the crash", b"after the crash"]


# --------------------------------------------------------------------------- segments


def _segmented(tmp_path, segment_bytes=200):
    return PartitionLog(tmp_path / "p0", segment_bytes=segment_bytes, seal=None)


def test_a_log_rolls_into_a_new_segment_named_by_its_first_offset(tmp_path):
    log = _segmented(tmp_path)
    for n in range(12):
        log.append(Record(value=b"x" * 40 + str(n).encode()))

    names = sorted(path.name for path in (tmp_path / "p0").glob("*.log"))
    assert len(names) > 1, "it never rolled"
    assert names[0] == f"{0:020d}.log"
    assert [r.offset for r in log.read_from(0)] == list(range(12)), "reads cross segments seamlessly"


def test_reading_from_the_middle_opens_only_what_it_needs(tmp_path):
    log = _segmented(tmp_path)
    for n in range(12):
        log.append(Record(value=str(n).encode() * 20))

    assert [r.offset for r in log.read_from(9)] == [9, 10, 11]


def test_a_segmented_log_reopens_exactly_where_it_was(tmp_path):
    log = _segmented(tmp_path)
    for _ in range(12):
        log.append(Record(value=b"y" * 50))

    reopened = _segmented(tmp_path)
    assert reopened.next_offset == 12
    assert reopened.append(Record(value=b"z")) == 12
    assert len(reopened.read_from(0)) == 13


def test_a_read_stops_near_its_byte_budget(tmp_path):
    """A read is bounded: a consumer holding a deep partition takes a mouthful, not the lot."""
    log = _log(tmp_path)
    for _ in range(10):
        log.append(Record(value=b"p" * 1000))

    assert 1 <= len(log.read_from(0, max_bytes=2500)) < 10


def test_a_record_larger_than_the_budget_is_still_delivered(tmp_path):
    """Otherwise one big page would stall its partition for ever at every budget smaller than it."""
    log = _log(tmp_path)
    log.append(Record(value=b"q" * 5000))

    assert len(log.read_from(0, max_bytes=100)) == 1


def test_a_read_that_runs_out_inside_a_segment_never_skips_to_the_next(tmp_path):
    """★ IT LOST RECORDS. A budget spent partway through a segment moved the read on to the NEXT
    segment, whose first record is always taken — so the read came back with a gap, and a consumer
    that committed the last record it was handed had skipped the ones in the gap for good."""
    log = _segmented(tmp_path, segment_bytes=400)
    for n in range(12):
        log.append(Record(value=b"r" * 90 + str(n).encode()))
    assert log.segment_count > 1

    offsets = [record.offset for record in log.read_from(0, max_bytes=250)]

    assert offsets == list(range(len(offsets))), f"the read skipped records: {offsets}"


def test_a_read_takes_no_more_records_than_asked_for(tmp_path):
    log = _segmented(tmp_path)
    for n in range(12):
        log.append(Record(value=str(n).encode() * 20))

    assert [r.offset for r in log.read_from(0, max_records=1)] == [0]
    assert [r.offset for r in log.read_from(2, max_records=5)] == [2, 3, 4, 5, 6]


def test_a_read_of_one_record_reads_only_that_record(tmp_path, monkeypatch):
    """A NEXT takes one record, so it reads one record's bytes — not the rest of a 16 MB segment."""
    from pathlib import Path as RealPath
    log = _log(tmp_path)
    for _ in range(10):
        log.append(Record(value=b"s" * 1000))
    read_sizes = []
    real_open = RealPath.open

    def counting_open(self, *args, **kwargs):
        handle = real_open(self, *args, **kwargs)
        real_read = handle.read

        def read(size=-1):
            data = real_read(size)
            read_sizes.append(len(data))
            return data
        handle.read = read
        return handle

    monkeypatch.setattr(RealPath, "open", counting_open)
    [record] = log.read_from(3, max_records=1)
    assert record.offset == 3
    assert max(read_sizes) < 2000, f"read {max(read_sizes)} bytes for one 1 KB record"


# --------------------------------------------------------------------------- retention


def test_retention_drops_only_whole_segments_everyone_has_passed(tmp_path):
    log = _segmented(tmp_path)
    for _ in range(12):
        log.append(Record(value=b"r" * 60))
    before = log.segment_count

    assert log.drop_segments_before(6) >= 1
    assert log.segment_count < before
    assert log.first_offset <= 6, "a segment still holding offset 6 must survive"
    assert [r.offset for r in log.read_from(6)] == list(range(6, 12))


def test_retention_never_drops_the_segment_being_written(tmp_path):
    log = _segmented(tmp_path)
    for _ in range(12):
        log.append(Record(value=b"s" * 60))

    log.drop_segments_before(10_000)

    assert log.segment_count == 1
    assert log.append(Record(value=b"still writable")) == 12


def test_a_consumer_below_the_retained_start_reads_from_what_is_left(tmp_path):
    log = _segmented(tmp_path)
    for _ in range(12):
        log.append(Record(value=b"t" * 60))
    log.drop_segments_before(8)

    assert log.read_from(0)[0].offset == log.first_offset


def test_a_single_file_log_from_before_segments_is_adopted_whole(tmp_path):
    """The dev and staging logs on disk predate segments. They become segment zero, unchanged."""
    legacy = tmp_path / "0000.log"
    legacy.write_bytes(Record(value=b"old").framed(offset=0) + Record(value=b"older").framed(offset=1))

    log = PartitionLog(tmp_path / "0000", seal=None)

    assert [r.value for r in log.read_from(0)] == [b"old", b"older"]
    assert log.append(Record(value=b"new")) == 2
    assert not legacy.exists()


# --------------------------------------------------------------------------- a checksum on every record


def _flip_a_byte(path, at):
    raw = bytearray(path.read_bytes())
    raw[at] ^= 0xFF
    path.write_bytes(bytes(raw))


def _three_records(tmp_path):
    log = _log(tmp_path)
    for value in (b"zero", b"one", b"two"):
        log.append(Record(value=value))
    return log, log.active_segment


def test_a_record_whose_bytes_changed_fails_its_checksum():
    framed = bytearray(Record(value=b"page bytes").framed(offset=0))
    framed[-1] ^= 0x01
    with pytest.raises(CorruptRecordError, match="checksum"):
        Record.unframed(bytes(framed))


def test_a_changed_offset_fails_the_checksum_too():
    """The offset is covered, so a record cannot be read back as some other offset."""
    framed = bytearray(Record(value=b"page bytes").framed(offset=5))
    framed[0] ^= 0x01
    with pytest.raises(CorruptRecordError):
        Record.unframed(bytes(framed))


def test_headers_that_do_not_parse_are_corrupt_even_under_a_good_checksum():
    """A bug of ours could write bad headers and checksum them faithfully; that record is corrupt too."""
    stamp, key, value, headers = 1, b"", b"v", b"{not json"
    checked = struct.pack("<QIII", stamp, len(key), len(value), len(headers)) + key + value + headers
    offset = struct.pack("<Q", 0)
    framed = offset + struct.pack("<I", zlib.crc32(checked, zlib.crc32(offset))) + checked
    with pytest.raises(CorruptRecordError, match="headers"):
        Record.unframed(framed)


def test_a_corrupt_record_in_the_middle_is_lost_and_the_rest_stand(tmp_path):
    """★ TERRA #16. The server used to refuse to open a topic holding one malformed record. The damaged
    record's offset is lost; the one before and the one after are read, and offsets keep their meaning."""
    _log_before, segment = _three_records(tmp_path)
    one = HEADER_BYTES + len(b"zero") + HEADER_BYTES     # into offset 1's value
    _flip_a_byte(segment, one)

    reopened = _log(tmp_path)
    assert [(record.offset, record.value) for record in reopened.read_from(0)] == [
        (0, b"zero"), (2, b"two")]
    assert reopened.next_offset == 3
    assert reopened.append(Record(value=b"three")) == 3


def test_a_record_whose_length_was_damaged_is_stepped_past_to_the_next_whole_one(tmp_path):
    """The lengths themselves can be what changed; the next record that passes is found by its offset."""
    _log_before, segment = _three_records(tmp_path)
    value_len_of_one = HEADER_BYTES + len(b"zero") + 8 + 4 + 8 + 4     # offset 1's value_len field
    _flip_a_byte(segment, value_len_of_one)

    reopened = _log(tmp_path)
    assert [record.offset for record in reopened.read_from(0)] == [0, 2]


def test_a_corrupt_last_record_is_lost_and_its_offset_is_not_reused(tmp_path):
    """It is whole on disk, so it is not a torn write: a consumer may have been handed it and committed
    past it. The next append takes the offset after it."""
    _log_before, segment = _three_records(tmp_path)
    _flip_a_byte(segment, segment.stat().st_size - 1)

    reopened = _log(tmp_path)
    assert [record.offset for record in reopened.read_from(0)] == [0, 1]
    assert reopened.append(Record(value=b"three")) == 3
    assert [record.offset for record in _log(tmp_path).read_from(0)] == [0, 1, 3]


def test_offsets_a_damaged_tail_took_from_an_earlier_segment_are_counted(tmp_path):
    log = _segmented(tmp_path)
    for number in range(12):
        log.append(Record(value=b"x" * 40 + str(number).encode()))
    first = sorted((tmp_path / "p0").glob("*.log"))[0]
    raw = first.read_bytes()
    first.write_bytes(raw[:len(raw) - 10])          # the tail of the first segment, torn off
    reopened = PartitionLog(tmp_path / "p0", segment_bytes=200, seal=None)
    read = [record.offset for record in reopened.read_from(0)]
    assert read == sorted(read) and 11 in read and len(read) == 11, read


# --------------------------------------------------------------------------- retention by age


def test_segments_older_than_the_cutoff_are_dropped_and_the_log_carries_on(tmp_path):
    log = _segmented(tmp_path)
    for number in range(12):
        log.append(Record(value=b"x" * 40 + str(number).encode(), timestamp_ns=1_000 + number))
    segments = log.segment_count
    assert log.drop_segments_older_than(cutoff_ns=10_000) == segments
    assert log.segment_count == 1, "the log keeps one empty segment to append into"
    assert log.read_from(0) == []
    assert log.append(Record(value=b"after")) == 12


def test_a_segment_with_anything_newer_than_the_cutoff_is_kept(tmp_path):
    log = _log(tmp_path)
    log.append(Record(value=b"old", timestamp_ns=1_000))
    log.append(Record(value=b"new", timestamp_ns=50_000))
    assert log.drop_segments_older_than(cutoff_ns=10_000) == 0
    assert [record.value for record in log.read_from(0)] == [b"old", b"new"]


# --------------------------------------------------------------------------- sealed, on disk


def test_a_sealed_segment_on_disk_holds_no_plaintext(tmp_path):
    """Brief R7: what FreeStream stores is sealed (sealing.py); the segment file shows none of it."""
    from freestream.sealing import Seal

    seal = Seal.with_a_new_key()
    log = PartitionLog(tmp_path / "p0", seal=seal)
    log.append(Record(value=b"a fabricated page for 123-45-6789", key=b"doc-1", headers={"page": "3"}))
    on_disk = b"".join(path.read_bytes() for path in (tmp_path / "p0").glob("*.log"))
    assert b"123-45-6789" not in on_disk and b"doc-1" not in on_disk
    assert PartitionLog(tmp_path / "p0", seal=seal).read_from(0)[0].headers == {"page": "3"}
