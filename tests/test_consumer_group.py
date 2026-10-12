"""Consumer groups: one owner per partition, and an offset that survives the owner (#450).

These two facts together are what 2026-09-22 lacked. One owner means one consumer assembles a
document, because its key picks its partition. An offset means a replacement resumes where the dead one
stopped, with no retention policy for anyone to maintain.

See `docs/design/document-read-as-an-event-stream.md`.
"""
import subprocess
import sys
import textwrap

import pytest

from freestream import consumer_group as consumer_group_module
from freestream.consumer_group import ConsumerGroup, MemberState, forget_positions
from freestream.record import Record
from freestream.registry import RequestRefusedError
from freestream.topic import Topic

GROUP = "aggregators"


def _topic(tmp_path, partitions=4):
    return Topic(tmp_path, partitions=partitions, seal=None)


def _consumer(topic, tmp_path):
    return ConsumerGroup(topic, GROUP, tmp_path)


def _fill(topic, pages=6, key=b"doc-1"):
    for page in range(pages):
        topic.produce(Record(value=f"page {page}".encode(), key=key))


# --------------------------------------------------------------------------- one owner


def test_two_consumers_never_hold_the_same_partition(tmp_path):
    """★ THE INVARIANT. Whatever one holds, the other does not — decided by the kernel, not by us."""
    topic = _topic(tmp_path)
    _fill(topic, pages=4, key=None)             # keyless: one record in each of the four partitions
    first, second = _consumer(topic, tmp_path), _consumer(topic, tmp_path)

    first.poll()
    second.poll()

    assert first.owned and second.owned
    assert set(first.owned) & set(second.owned) == set(), "a partition went to two consumers"


def test_a_document_is_never_split_between_two_consumers(tmp_path):
    """The invariant as the pipeline meets it: one key, one partition, one owner, all of the pages."""
    topic = _topic(tmp_path)
    _fill(topic, pages=8, key=b"one-document")
    first, second = _consumer(topic, tmp_path), _consumer(topic, tmp_path)

    by_first = first.poll()
    by_second = second.poll()

    assert not (by_first and by_second), (
        f"one document reached two consumers: {len(by_first)} pages and {len(by_second)}")
    assert len(by_first or by_second) == 8, "and whoever got it got all of it"


def test_an_arriving_consumer_takes_nothing_from_a_live_one(tmp_path):
    """★ ASSIGN ON FIRST SIGHT, NEVER REBALANCE. Our keys are mortal, so nobody has to be interrupted."""
    topic = _topic(tmp_path)
    _fill(topic, pages=4, key=None)
    established = _consumer(topic, tmp_path)
    established.poll()
    held = set(established.owned)
    assert held

    arriving = _consumer(topic, tmp_path)
    arriving.poll()

    assert set(established.owned) == held, "a partition changed hands under a working consumer"
    assert not set(arriving.owned) & held


def test_a_consumer_that_lets_go_hands_its_partitions_on(tmp_path):
    topic = _topic(tmp_path)
    _fill(topic, pages=1, key=None)             # one record, in partition 0
    leaving = _consumer(topic, tmp_path)
    leaving.poll()
    assert leaving.owned == [0]

    leaving.release()
    arriving = _consumer(topic, tmp_path)
    assert arriving.poll(), "the record the leaving consumer never committed is still there"

    assert arriving.owned == [0]
    assert leaving.owned == []


def test_a_consumer_that_dies_hands_its_partitions_on(tmp_path):
    """★ THE LIVENESS STORY, AND THERE ISN'T ONE. A real process is killed; the kernel does the rest.

    Nothing heartbeats, nothing times out, nothing notices. The flock goes when the process does, so
    the next consumer to look simply succeeds. Done with a real SIGKILL rather than by dropping an
    object, because the claim is about what the KERNEL does and only a real death tests that.
    """
    topic = _topic(tmp_path)
    _fill(topic)

    holder = subprocess.Popen(
        [sys.executable, "-c", textwrap.dedent(f"""
            import time
            from freestream.topic import Topic
            from freestream.consumer_group import ConsumerGroup
            topic = Topic({str(tmp_path)!r}, partitions=4, seal=None)
            group = ConsumerGroup(topic, {GROUP!r}, {str(tmp_path)!r})
            group.poll()
            print("held", flush=True)
            time.sleep(60)
        """)], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        blocked = _consumer(topic, tmp_path)
        assert blocked.poll() == [], "the live holder's pages must not reach a second consumer"
        held_by_the_dying = {
            partition for partition in range(topic.partitions) if partition not in blocked.owned}
    finally:
        holder.kill()
        holder.wait(timeout=10)

    replacement = _consumer(topic, tmp_path)
    assert replacement.poll(), "the dead consumer's work must not die with it"
    assert set(replacement.owned) & held_by_the_dying, "it took nothing the dead one had held"


# --------------------------------------------------------------------------- offsets


def test_a_new_group_starts_at_the_beginning(tmp_path):
    topic = _topic(tmp_path)
    _fill(topic)
    assert len(_consumer(topic, tmp_path).poll()) == 6


def test_reading_does_not_commit(tmp_path):
    """At-least-once, the only default that loses nothing: a consumer that dies before it commits sees
    the record again."""
    topic = _topic(tmp_path)
    _fill(topic)
    consumer = _consumer(topic, tmp_path)

    assert len(consumer.poll()) == 6
    assert len(consumer.poll()) == 6, "polling twice without committing must return the same work"


def test_the_committed_offset_is_the_next_one_to_read(tmp_path):
    """★ A SILENT OFF-BY-ONE. A consumer that commits "the last one I read" is handed it again forever."""
    topic = _topic(tmp_path)
    _fill(topic, pages=5, key=b"doc-1")
    consumer = _consumer(topic, tmp_path)
    partition = consumer.poll()[0][0]

    consumer.commit(partition, 5)           # every one of offsets 0..4 is done

    assert consumer.poll() == [], "a fully-read partition must hand back nothing"


def test_a_replacement_resumes_where_the_last_one_stopped(tmp_path):
    """The reason a replacement needs no retention policy: its position is a number on disk."""
    topic = _topic(tmp_path)
    _fill(topic, pages=6, key=b"doc-1")
    first = _consumer(topic, tmp_path)
    partition = first.poll()[0][0]
    first.commit(partition, 4)
    first.release()

    resumed = _consumer(topic, tmp_path).poll()

    assert [record.value for _p, record in resumed] == [b"page 4", b"page 5"]


def test_a_consumer_cannot_commit_a_partition_it_does_not_hold(tmp_path):
    """Otherwise a consumer that lost a partition could still move somebody else's position."""
    topic = _topic(tmp_path)
    with pytest.raises(RequestRefusedError, match="not held"):
        _consumer(topic, tmp_path).commit(0, 3)


def test_offsets_are_kept_per_group_so_two_groups_read_the_same_records(tmp_path):
    """Channel 3 is broadcast — several groups over one topic, each with its own position."""
    topic = _topic(tmp_path)
    _fill(topic, pages=3, key=b"doc-1")

    aggregators = ConsumerGroup(topic, "aggregators", tmp_path)
    partition = aggregators.poll()[0][0]
    aggregators.commit(partition, 3)
    aggregators.release()

    auditors = ConsumerGroup(topic, "auditors", tmp_path)

    assert aggregators.position(partition) == 3
    assert len(auditors.poll()) == 3, "a second group reads the topic from its own beginning"


# --------------------------------------------------------- spreading over the consumers reading it


def test_an_idle_channel_costs_nothing(tmp_path):
    """★ THE SERVER WAS AT 100% OF A CORE WITH NOTHING TO READ. An idle reader took every free partition
    on each poll — open, lock, read, find nothing — and handed them all back: twelve readers, sixty-four
    partitions, twenty polls a second. A partition with nothing unread in it is not taken now, so an
    idle poll opens no lock file at all."""
    topic = _topic(tmp_path, partitions=8)
    readers = [ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False) for _ in range(8)]

    for _ in range(5):
        for reader in readers:
            assert reader.poll() == []

    assert all(reader.owned == [] for reader in readers)
    assert not list((tmp_path / "groups" / GROUP).glob("*.owner")), "an idle poll opened a lock"


def test_eight_readers_share_a_channel_that_has_work_one_partition_each(tmp_path):
    """★ THE BUG THE FIRST POLICY FIXED, STILL FIXED. Eight readers and eight partitions with work: each
    takes one. When a reader took every free partition at once, whichever polled first got all eight."""
    topic = _topic(tmp_path, partitions=8)
    _fill(topic, pages=8, key=None)
    readers = [ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False) for _ in range(8)]

    for reader in readers:
        assert reader.poll(), "a reader with work waiting came back empty"

    assert sorted(len(reader.owned) for reader in readers) == [1] * 8


def test_a_reader_that_arrives_late_is_given_the_work_nobody_holds(tmp_path):
    topic = _topic(tmp_path, partitions=4)
    _fill(topic, pages=4, key=None)
    early = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)
    early.poll()

    late = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)
    assert late.poll(), "the late reader was given nothing while unread partitions sat free"
    assert not set(late.owned) & set(early.owned)


def test_a_channel_that_keeps_its_partitions_never_hands_one_back(tmp_path):
    """★ AND THE ASSEMBLER MUST NOT DO THIS. A KEYED channel has documents mid-assembly behind its
    partitions, so letting one go hands somebody a half-read return. It is the channel's declaration
    that decides, not a setting anyone tunes."""
    topic = _topic(tmp_path, partitions=4)
    _fill(topic, pages=4, key=None)
    keeping = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=True)

    while found := keeping.poll():
        for partition, record in found:
            keeping.commit(partition, record.offset + 1)
    for _ in range(5):
        keeping.poll()

    assert keeping.owned == list(range(4)), "an idle keyed consumer let a partition go"


def test_a_busy_consumer_does_not_reach_for_more(tmp_path):
    """Work is what stops the taking. A consumer that found something leaves the rest for somebody."""
    topic = _topic(tmp_path, partitions=8)
    # Keyless, which is what a COMPETING channel publishes: they spread round-robin from partition 0,
    # so the first partition this reader takes already has work on it.
    _fill(topic, pages=8, key=None)
    reader = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)

    assert reader.poll(), "it should have found the pages waiting"
    assert reader.owned == [0], "it kept taking after it had work"


def _pages_in_order(reader, pages):
    """Poll and commit until the channel is empty, and return the pages in the order they were read."""
    read = []
    while found := reader.poll(limit=1):
        for partition, record in found:
            read.append(record.value.decode())
            reader.commit(partition, record.offset + 1)
        assert len(read) <= pages, "a page was handed over twice"
    return read


def test_a_competing_reader_takes_the_oldest_page_across_partitions(tmp_path):
    """★ FIRST TO LAST, NOT EVERY FOURTH PAGE. Round-robin puts consecutive pages in consecutive
    partitions, so a reader that drained the partition it held read pages 0, 4, 1, 5, …; the cockpit then
    waited on the front of a return while its middle was read (2026-09-28)."""
    topic = _topic(tmp_path, partitions=4)
    _fill(topic, pages=8, key=None)
    reader = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)

    assert _pages_in_order(reader, 8) == [f"page {page}" for page in range(8)]


def test_competing_readers_split_the_oldest_work_without_sharing_a_partition(tmp_path):
    """Each takes the oldest page nobody else holds, so two readers between them read the front first."""
    topic = _topic(tmp_path, partitions=4)
    _fill(topic, pages=8, key=None)
    first = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)
    second = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)

    [(first_partition, first_record)] = first.poll(limit=1)
    [(second_partition, second_record)] = second.poll(limit=1)

    assert first_partition != second_partition
    assert {first_record.value, second_record.value} == {b"page 0", b"page 1"}
    assert len(first.owned) == 1 and len(second.owned) == 1, "a reader kept a partition it was not reading"


def test_the_age_of_a_waiting_record_is_read_from_its_header_even_when_sealed(tmp_path):
    from freestream.sealing import Seal
    topic = Topic(tmp_path, partitions=1, seal=Seal.with_a_new_key())
    topic.produce(Record(value=b"page 0", timestamp_ns=1_000))
    topic.produce(Record(value=b"page 1", timestamp_ns=2_000))
    log = topic.log(0)

    assert log.timestamp_at(0) == 1_000
    assert log.timestamp_at(1) == 2_000
    assert log.timestamp_at(2) is None, "nothing is waiting past the end"


# --------------------------------------------------------------------------- what an operator says


def _one_partition(tmp_path):
    return Topic(tmp_path, partitions=1, seal=None)


def _keyed(topic, key, pages):
    for page in pages:
        topic.produce(Record(value=f"{key.decode()} page {page}".encode(), key=key))


def test_a_consumer_in_maintenance_is_handed_nothing_and_lets_everything_go(tmp_path):
    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1, 2])
    member, next_one = _consumer(topic, tmp_path), _consumer(topic, tmp_path)
    assert member.poll()

    member.state = MemberState.MAINT

    assert member.poll() == [] and member.owned == []
    assert [record.value for _p, record in next_one.poll()] == [b"a page 1", b"a page 2"]


def test_a_draining_competing_consumer_lets_go_at_its_next_fetch(tmp_path):
    """A COMPETING consumer commits each record before it asks for more, so by its next fetch there is
    nothing in its hands to finish."""
    topic = _one_partition(tmp_path)
    topic.produce(Record(value=b"a page"))
    member = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)
    assert member.poll()

    member.state = MemberState.DRAIN

    assert member.poll() == [] and member.owned == []


def test_a_draining_assembler_finishes_its_open_documents_and_takes_no_new_one(tmp_path):
    """★ C, AT THE CONSUMER. Document `a` is half assembled here when the operator says drain. `a`'s last
    page and the whole of `b` then land in the same partition. The draining member is handed `a`'s page
    and not `b`'s; its commit stops at `b`'s first record; and once `a` closes it lets the partition go,
    so the next member starts at `b` — neither losing it nor re-reading `a`."""
    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1])
    member = _consumer(topic, tmp_path)
    [(partition, first)] = member.poll()
    member.commit(partition, first.offset + 1)
    assert member.open_keys == {b"a"}

    member.state = MemberState.DRAIN
    _keyed(topic, b"b", [1])
    _keyed(topic, b"a", [2])
    _keyed(topic, b"b", [2])

    handed = member.poll()
    assert [record.value for _p, record in handed] == [b"a page 2"]
    assert member.poll() == [], "a record already handed over was handed over again"
    member.commit(partition, handed[-1][1].offset + 1)
    assert member.position(partition) == 1, "the commit ran past b's first record"

    member.close_key(b"a")
    assert member.poll() == [] and member.owned == []

    arriving = _consumer(topic, tmp_path)
    handed_on = arriving.poll()
    assert [record.value for _p, record in handed_on] == [b"b page 1", b"b page 2"], (
        "a page the draining member committed was handed to the next one again")

    # As the inheritor commits b, the position absorbs a's acked record and ends past all three.
    for handed_partition, record in handed_on:
        arriving.commit(handed_partition, record.offset + 1)
    assert arriving.position(partition) == 4 and arriving.acked(partition) == set()
    assert not (tmp_path / "groups" / GROUP / f"{partition:04d}.acked").exists()


def test_the_acked_ahead_set_survives_the_server_restarting(tmp_path):
    """The set is on disk beside the offset, so a server that restarts mid-drain still skips what was
    committed — the positions table in memory is only a cache of the files."""
    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1])
    member = _consumer(topic, tmp_path)
    [(partition, first)] = member.poll()
    member.commit(partition, first.offset + 1)
    member.state = MemberState.DRAIN
    _keyed(topic, b"b", [1])
    _keyed(topic, b"a", [2])
    [(_p, a_page_2)] = member.poll()
    member.commit(partition, a_page_2.offset + 1)
    member.close_key(b"a")
    member.poll()

    consumer_group_module.forget_positions(tmp_path / "groups" / GROUP)   # a restart
    arriving = _consumer(topic, tmp_path)
    assert [record.value for _p, record in arriving.poll()] == [b"b page 1"]


def test_an_acked_offset_below_the_position_is_the_trace_of_a_crash_and_is_dropped(tmp_path):
    """The position is written before the set, so a crash between the two leaves offsets the position
    already covers. They are ignored on reading, never delivered, and never counted as lag."""
    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1, 2, 3])
    home = tmp_path / "groups" / GROUP
    home.mkdir(parents=True)
    (home / "0000.offset").write_text("2")
    (home / "0000.acked").write_text("[0, 1]")
    member = _consumer(topic, tmp_path)
    assert member.acked(0) == set()
    assert [record.value for _p, record in member.poll()] == [b"a page 3"]


def test_a_draining_assembler_with_nothing_open_lets_go_at_once(tmp_path):
    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1])
    member = _consumer(topic, tmp_path)
    member.poll()
    member.close_key(b"a")

    member.state = MemberState.DRAIN

    assert member.poll() == [] and member.owned == []


def test_a_member_set_ready_again_carries_on_where_the_group_is(tmp_path):
    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1, 2])
    member = _consumer(topic, tmp_path)
    member.state = MemberState.MAINT
    assert member.poll() == []

    member.state = MemberState.READY

    assert len(member.poll()) == 2


def test_lag_is_what_the_group_has_not_committed(tmp_path):
    topic = _topic(tmp_path, partitions=2)
    for partition_record in (b"x", b"y", b"z"):
        topic.produce(Record(value=partition_record))
    member = ConsumerGroup(topic, GROUP, tmp_path, keeps_its_partitions=False)
    taken = member.poll()
    member.commit(taken[0][0], taken[0][1].offset + 1)

    assert sum(member.lag().values()) == 2


def test_a_deleted_group_made_again_starts_from_the_beginning(tmp_path):
    """The in-memory positions are what a poll reads, so deleting the files alone would leave a group
    made again under the same name at the old one's offsets."""
    import shutil

    topic = _one_partition(tmp_path)
    _keyed(topic, b"a", [1, 2])
    member = _consumer(topic, tmp_path)
    member.poll()
    member.commit(0, 2)
    member.release()

    home = tmp_path / "groups" / GROUP
    forget_positions(home)
    shutil.rmtree(home)

    assert _consumer(topic, tmp_path).position(0) == 0


# --------------------------------------------------------------------------- counting starts


def _handling(tmp_path, topic):
    """A member that has been handed offset 0 of partition 0 and not yet committed it."""
    consumer = _consumer(topic, tmp_path)
    [(partition, record)] = consumer.poll(limit=1)
    return consumer, partition, record.offset


def test_a_start_counts_once_however_often_it_is_asked_about(tmp_path):
    """★ A RESENT ATTEMPT IS THE SAME START. A client whose answer was lost asks again with the same
    token; counting it twice dead-lettered a record early for starts it never had."""
    topic = _topic(tmp_path)
    _fill(topic, pages=1, key=b"doc-1")
    consumer, partition, offset = _handling(tmp_path, topic)
    assert consumer.attempt(partition, offset, "start-1") == 1
    assert consumer.attempt(partition, offset, "start-1") == 1
    assert consumer.attempt(partition, offset, "start-2") == 2
    assert consumer.attempt(partition, offset, None) == 3


def test_the_count_outlives_the_server_that_kept_it(tmp_path):
    """★ ON DISK. The count lived in the server's memory, so a restart between failures began it again
    and a poison record could be retried for ever. A new member of the group — as after a restart —
    finds the starts already made."""
    topic = _topic(tmp_path)
    _fill(topic, pages=1, key=b"doc-1")
    first, partition, offset = _handling(tmp_path, topic)
    first.attempt(partition, offset, "start-1")
    first.attempt(partition, offset, "start-2")
    first.release()
    second, partition, offset = _handling(tmp_path, topic)
    assert second.attempt(partition, offset, "start-3") == 3


def test_a_start_is_refused_on_a_record_this_member_is_not_handling(tmp_path):
    """Otherwise one consumer could charge another's record with failures and see it dead-lettered."""
    topic = _topic(tmp_path)
    _fill(topic, pages=2, key=b"doc-1")
    consumer, partition, offset = _handling(tmp_path, topic)
    with pytest.raises(RequestRefusedError, match="not a record"):
        consumer.attempt(partition, offset + 1, "t")       # not yet handed
    with pytest.raises(RequestRefusedError, match="not held"):
        _consumer(topic, tmp_path).attempt(partition, offset, "t")   # another member of the group


def test_a_commit_forgets_the_starts_of_what_it_passed(tmp_path):
    topic = _topic(tmp_path)
    _fill(topic, pages=1, key=b"doc-1")
    consumer, partition, offset = _handling(tmp_path, topic)
    consumer.attempt(partition, offset, "start-1")
    assert list((tmp_path / "groups").rglob("*.attempts"))
    consumer.commit(partition, offset + 1)
    assert not list((tmp_path / "groups").rglob("*.attempts"))
