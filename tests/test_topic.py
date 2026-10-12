"""The partitioner: the rule that decides where a record lands (#450).

★ THE INVARIANT EVERYTHING RESTS ON. A consumer group gives each partition to exactly one consumer, so
"every page of a document reaches one aggregator" is what partitioning already does — PROVIDED one key
always maps to one partition. The moment it does not, two consumers are assembling one document, which
is 2026-09-22 again by a different road.

See `docs/design/document-read-as-an-event-stream.md`.
"""
import ast
import os
import subprocess
import sys

import pytest

from freestream import topic as topic_module
from freestream.record import Record
from freestream.topic import Topic, partition_for


def _topic(tmp_path, partitions=4):
    return Topic(tmp_path / "pages", partitions=partitions, seal=None)


def test_one_key_always_lands_in_one_partition(tmp_path):
    topic = _topic(tmp_path)
    landed = {topic.produce(Record(value=b"page", key=b"doc-1"))[0] for _ in range(20)}
    assert len(landed) == 1, "a document whose pages split across partitions splits across consumers"


def test_a_key_maps_to_the_same_partition_in_a_fresh_process(tmp_path):
    """★ THE ONE THAT CATCHES THE RANDOMIZED-HASH TRAP, and it only shows up across processes.

    Python's built-in `hash()` is seeded per interpreter for bytes and str, so a partitioner built on
    it agrees with itself all day and disagrees with the process next door. A single-process test suite
    never sees that failure. It appears only where two processes produce, which is the normal case in
    production, and it is intermittent when it does.
    """
    here = [partition_for(f"doc-{n}".encode(), 8) for n in range(12)]

    program = (
        "from freestream.topic import partition_for;"
        "print([partition_for(f'doc-{n}'.encode(), 8) for n in range(12)])"
    )
    # A DIFFERENT hash seed on purpose: if the mapping depended on it, this is where it would show. The rest
    # of the environment is kept, because an interpreter may need it to start at all: the CI runner's Python
    # finds libpython through LD_LIBRARY_PATH, and a child given only PATH could not load (2026-09-28).
    finished = subprocess.run([sys.executable, "-c", program], check=True, capture_output=True,
                              text=True, env={**os.environ, "PYTHONHASHSEED": "12345"})

    there = ast.literal_eval(finished.stdout)
    assert there == here, "the partition of a key must not depend on who is asking"


def test_keys_are_spread_rather_than_piled_into_one_partition():
    """A mapping that is stable but lopsided gives one consumer every document and the rest nothing."""
    counts = [0] * 8
    for n in range(400):
        counts[partition_for(f"document-{n}".encode(), 8)] += 1
    assert min(counts) > 400 // 8 * 0.5, f"badly skewed: {counts}"


def test_different_keys_may_share_a_partition_and_that_is_fine(tmp_path):
    """Affinity is per KEY, not per partition. One consumer holding several documents is the design.

    I specified exactly this: a consumer processing page completions that arrive interleaved across
    several documents, holding per-document state. That is a partition owner.
    """
    topic = _topic(tmp_path, partitions=1)
    assert topic.produce(Record(value=b"a", key=b"doc-1"))[0] == 0
    assert topic.produce(Record(value=b"b", key=b"doc-2"))[0] == 0


def test_records_in_one_partition_keep_their_order(tmp_path):
    topic = _topic(tmp_path)
    for page in range(5):
        topic.produce(Record(value=f"page {page}".encode(), key=b"doc-1"))
    partition, _ = topic.produce(Record(value=b"page 5", key=b"doc-1"))
    assert [r.value for r in topic.log(partition).read_from(0)] == [
        b"page 0", b"page 1", b"page 2", b"page 3", b"page 4", b"page 5"]


def test_offsets_are_per_partition_not_global(tmp_path):
    """Kafka's promise, and ours: an offset means something only inside its own partition."""
    topic = _topic(tmp_path, partitions=2)
    first = [topic.produce(Record(value=b"x", key=b"alpha"))[1] for _ in range(3)]
    second = [topic.produce(Record(value=b"y", key=b"beta"))[1] for _ in range(3)]
    assert first == [0, 1, 2]
    assert second == [0, 1, 2], "a second partition starts at zero, it does not continue the first"


def test_a_keyless_record_is_spread_rather_than_kept_together(tmp_path):
    """The ungrouped channel: head-of-stream to OCR readers, where nobody cares who takes it."""
    topic = _topic(tmp_path)
    landed = {topic.produce(Record(value=b"a page"))[0] for _ in range(8)}
    assert len(landed) > 1, "work with no key to keep together should be shared out"


def test_a_topic_needs_at_least_one_partition(tmp_path):
    with pytest.raises(ValueError, match="at least one partition"):
        Topic(tmp_path / "bad", partitions=0, seal=None)


def test_a_reopened_topic_keeps_every_partition_where_it_was(tmp_path):
    first = _topic(tmp_path)
    for page in range(6):
        first.produce(Record(value=str(page).encode(), key=f"doc-{page % 3}".encode()))

    reopened = _topic(tmp_path)
    assert reopened.depth() == 6

    # Where doc-0 belongs, and how much is already there — NOT assumed to be a partition of its own,
    # because keys legitimately share one (see the test above). A test that asserted "offset == 2" here
    # would assert that doc-0, doc-1 and doc-2 hashed apart, which is not a property we have or want.
    home = partition_for(b"doc-0", reopened.partitions)
    already = reopened.log(home).next_offset

    partition, offset = reopened.produce(Record(value=b"next", key=b"doc-0"))

    assert partition == home
    assert offset == already, "it appended after what was already in that partition, not over it"


# ------------------------------------------------------------- a resent produce is written once


def test_a_repeated_sequence_from_one_producer_is_answered_with_the_first_placement(tmp_path):
    topic = Topic(tmp_path, partitions=4, seal=None)
    first = topic.produce_once(Record(value=b"a page"), "producer-1", 0)
    again = topic.produce_once(Record(value=b"a page"), "producer-1", 0)
    assert again == first
    assert topic.depth() == 1


def test_another_producer_or_another_sequence_is_a_new_record(tmp_path):
    topic = Topic(tmp_path, partitions=4, seal=None)
    topic.produce_once(Record(value=b"a page"), "producer-1", 0)
    topic.produce_once(Record(value=b"a page"), "producer-1", 1)
    topic.produce_once(Record(value=b"a page"), "producer-2", 0)
    assert topic.depth() == 3


def test_a_topic_opened_again_recalls_its_producers_from_the_log(tmp_path):
    """★ THE CASE THIS EXISTS FOR. The server is killed after writing a record and before answering; the
    client resends to the server that replaced it, which has an empty table. It reads the producer and
    sequence back out of the record and answers the resend with the original placement."""
    first = Topic(tmp_path, partitions=4, seal=None).produce_once(Record(value=b"a page"), "producer-1", 7)
    reopened = Topic(tmp_path, partitions=4, seal=None)
    assert reopened.produce_once(Record(value=b"a page"), "producer-1", 7) == first
    assert reopened.depth() == 1


def test_a_producer_quiet_past_the_memory_is_forgotten(tmp_path, monkeypatch):
    topic = Topic(tmp_path, partitions=1, seal=None)
    topic.produce_once(Record(value=b"old"), "quiet", 0)
    monkeypatch.setattr(topic_module, "PRODUCER_MEMORY_SECONDS", -1)
    topic.produce_once(Record(value=b"new"), "busy", 0)
    assert "quiet" not in topic._producer_table(0)


def test_only_the_last_placements_of_a_producer_are_kept(tmp_path, monkeypatch):
    monkeypatch.setattr(topic_module, "PLACEMENTS_KEPT", 2)
    topic = Topic(tmp_path, partitions=1, seal=None)
    for sequence in range(3):
        topic.produce_once(Record(value=b"x"), "producer-1", sequence)
    assert list(topic._placements_of(0, "producer-1")) == [1, 2]


def test_a_record_older_than_the_memory_is_not_recalled(tmp_path, monkeypatch):
    Topic(tmp_path, partitions=1, seal=None).produce_once(Record(value=b"old"), "producer-1", 0)
    monkeypatch.setattr(topic_module, "PRODUCER_MEMORY_SECONDS", -1)
    assert Topic(tmp_path, partitions=1, seal=None)._producer_table(0) == {}


def test_a_producers_numbered_record_always_goes_to_the_same_partition(tmp_path):
    """★ SO A RESEND REACHES THE SAME STRAND. The partition comes from the producer and sequence, not a
    counter, so any strand, or a restarted server, sends the resend where the first attempt went."""
    page = Record(value=b"page")
    first = Topic(tmp_path, partitions=8, seal=None).produce_once(page, "producer-1", 41)
    assert Topic(tmp_path, partitions=8, seal=None).produce_once(page, "producer-1", 41) == first
    assert partition_for(None, 8, producer="producer-1", sequence=41) == first[0]


def test_a_topic_opened_over_some_partitions_holds_only_those(tmp_path):
    view = Topic(tmp_path, partitions=4, owned=[1, 3], seal=None)
    assert view.owned_partitions == [1, 3]
    view.produce_to(3, Record(value=b"x"))
    with pytest.raises(KeyError):
        view.log(0)
    assert Topic(tmp_path, partitions=4, seal=None).log(3).next_offset == 1


def test_a_journal_entrys_producer_is_remembered_until_forgotten_not_for_an_hour(tmp_path, monkeypatch):
    """★ BRIEF R5. An entry whose outputs keep failing, or a server down past the hour, would otherwise have
    the outputs that did land written again when the entry is finished."""
    topic = Topic(tmp_path, partitions=1, seal=None)
    topic.produce_once(Record(value=b"output"), "complete.entry-1", 0)
    monkeypatch.setattr(topic_module, "PRODUCER_MEMORY_SECONDS", -1)
    topic.produce_once(Record(value=b"other"), "busy", 0)
    assert "complete.entry-1" in topic._producer_table(0)
    topic.forget_producer(0, "complete.entry-1")
    assert "complete.entry-1" not in topic._producer_table(0)


def test_the_replay_recalls_journal_outputs_however_old_and_a_strand_does_not(tmp_path, monkeypatch):
    old = Record(value=b"output", timestamp_ns=1)
    Topic(tmp_path, partitions=1, seal=None).produce_once(old, "complete.entry-1", 0)
    Topic(tmp_path, partitions=1, seal=None).produce_once(Record(value=b"x", timestamp_ns=1), "someone", 0)
    replaying = Topic(tmp_path, partitions=1, seal=None, recall_every_journal_output=True)
    assert set(replaying._producer_table(0)) == {"complete.entry-1"}
    assert Topic(tmp_path, partitions=1, seal=None)._producer_table(0) == {}
