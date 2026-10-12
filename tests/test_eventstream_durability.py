"""The stream answers only for what is on the disk, and many answers share one flush — brief R5 (#450).

Brief R5, 2026-09-26: "A system/process crash cannot result in a lost or duplicate per-record output."
A power cut takes whatever the kernel had not written yet, so the server flushes before it answers a
produce, before it hands a record to a consumer, and before a COMPLETE's journal entry lets anything
happen. These tests hold the flush back with a gate and check that nothing is answered or readable until
it lets go.
"""
import asyncio
import os
import threading
from pathlib import Path

import pytest

from freestream import durability as durability_module
from freestream.client import FreeStreamClient, Output
from freestream.durability import Durability, flush_now, mark_written
from freestream.server import EventStreamServer
from tests.eventually import eventually


class FlushGate:
    """Stands in for `flush_now`: records each flush, and holds it until the test opens the gate."""

    def __init__(self) -> None:
        self.opened = threading.Event()
        self.flushes: list[tuple[set, set]] = []
        self.started = threading.Event()

    def __call__(self, files, directories=()) -> None:
        self.flushes.append((set(files), set(directories)))
        self.started.set()
        assert self.opened.wait(10), "the test never opened the flush gate"


@pytest.fixture
def gate(monkeypatch):
    held = FlushGate()
    monkeypatch.setattr(durability_module, "flush_now", held)
    return held


async def _until(condition, seconds=5.0):
    deadline = asyncio.get_running_loop().time() + seconds
    while not condition():
        assert asyncio.get_running_loop().time() < deadline, "the condition never came true"
        await asyncio.sleep(0.005)


# --------------------------------------------------------------------------- one strand's barrier


async def test_writes_marked_during_a_flush_share_the_next_one(gate, tmp_path):
    """★ GROUP COMMIT. One flush is in flight at a time, and everything written meanwhile waits for the
    next, so a busy strand answers many requests per flush."""
    durability = Durability(asyncio.get_running_loop())
    durability.mark(tmp_path / "a")
    first = asyncio.create_task(durability.barrier())
    await _until(gate.started.is_set)
    for name in ("b", "c", "d"):
        durability.mark(tmp_path / name, directory_changed=True)
    later = [asyncio.create_task(durability.barrier()) for _ in range(3)]
    await asyncio.sleep(0.02)
    assert not first.done() and not any(task.done() for task in later)
    gate.opened.set()
    await asyncio.gather(first, *later)
    assert gate.flushes == [({tmp_path / "a"}, set()),
                            ({tmp_path / "b", tmp_path / "c", tmp_path / "d"}, {tmp_path})]
    assert durability.passes == 2


async def test_a_barrier_with_nothing_marked_waits_for_the_flush_in_flight(gate, tmp_path):
    """A request whose write another request marked must not answer before that write's flush ends."""
    durability = Durability(asyncio.get_running_loop())
    durability.mark(tmp_path / "a")
    await _until(gate.started.is_set)
    waiting = asyncio.create_task(durability.barrier())
    await asyncio.sleep(0.02)
    assert not waiting.done()
    gate.opened.set()
    await waiting


async def test_a_barrier_with_nothing_written_returns_at_once(tmp_path):
    await asyncio.wait_for(Durability(asyncio.get_running_loop()).barrier(), 1)


async def test_what_follows_a_flush_runs_only_after_it(gate, tmp_path):
    durability = Durability(asyncio.get_running_loop())
    ran = []
    durability.mark(tmp_path / "a", then=lambda: ran.append("readable"))
    durability.when_flushed(lambda: ran.append("offered"))
    await _until(gate.started.is_set)
    durability.when_flushed(lambda: ran.append("after the flush in flight"))
    assert ran == []
    gate.opened.set()
    await durability.barrier()
    await _until(lambda: len(ran) == 3)
    assert ran == ["readable", "offered", "after the flush in flight"]
    durability.when_flushed(lambda: ran.append("at once"))
    assert ran[-1] == "at once"


async def test_a_failed_flush_stops_the_server(monkeypatch, tmp_path):
    """★ AFTER A FAILED FSYNC NOBODY KNOWS WHAT THE DISK HOLDS, so the server stops and recovers from it
    rather than answer from memory. Nothing that waited on the flush is told it succeeded."""
    stopped = []
    monkeypatch.setattr(durability_module, "the_flush_failed", stopped.append)

    def failing(files, directories=()):
        raise OSError(5, "Input/output error")
    monkeypatch.setattr(durability_module, "flush_now", failing)
    durability = Durability(asyncio.get_running_loop())
    ran = []
    durability.mark(tmp_path / "a", then=lambda: ran.append("readable"))
    with pytest.raises(OSError, match="Input/output"):
        await durability.barrier()
    assert [failure.errno for failure in stopped] == [5]
    assert ran == []


def test_the_server_stops_with_its_own_exit_status(monkeypatch):
    exits = []
    monkeypatch.setattr(durability_module.os, "_exit", exits.append)
    durability_module.the_flush_failed(OSError(5, "Input/output error"))
    assert exits == [durability_module.EXIT_FLUSH_FAILED]


def test_a_thread_without_a_barrier_marks_nothing(tmp_path):
    """A tool or the journal replay writes before any strand runs, and flushes for itself."""
    assert threading.current_thread() is threading.main_thread()
    assert mark_written(tmp_path / "a") is False


# --------------------------------------------------------------------------- flush_now


def test_flush_now_fsyncs_every_file_then_every_directory(monkeypatch, tmp_path):
    for name in ("a", "b"):
        (tmp_path / name).write_bytes(b"x")
    synced = []
    real_fsync = os.fsync

    def recording(descriptor):
        synced.append(str(Path(f"/proc/self/fd/{descriptor}").readlink()))
        real_fsync(descriptor)
    monkeypatch.setattr(durability_module.os, "fsync", recording)
    flush_now([tmp_path / "a", tmp_path / "b", tmp_path / "gone"], [tmp_path])
    assert sorted(synced[:2]) == [str(tmp_path / "a"), str(tmp_path / "b")]
    assert synced[2:] == [str(tmp_path)]


def test_flush_now_raises_a_failure_after_every_fsync_has_run(monkeypatch, tmp_path):
    for name in ("a", "b"):
        (tmp_path / name).write_bytes(b"x")
    synced = []

    def failing_on_a(descriptor):
        path = str(Path(f"/proc/self/fd/{descriptor}").readlink())
        synced.append(path)
        if path in (str(tmp_path / "a"), str(tmp_path)):
            raise OSError(5, "Input/output error")
    monkeypatch.setattr(durability_module.os, "fsync", failing_on_a)
    with pytest.raises(OSError, match="Input/output"):
        flush_now([tmp_path / "a", tmp_path / "b"])
    assert len(synced) == 2
    with pytest.raises(OSError, match="Input/output"):
        flush_now([], [tmp_path])


# --------------------------------------------------------------------------- through the server


@pytest.fixture
async def served(tmp_path):
    running = EventStreamServer(tmp_path / "log", strands=2)
    running.define_topic("work", partitions=1, delivery="competing")
    running.define_topic("done", partitions=2, delivery="competing")
    ready = asyncio.Event()
    serving = asyncio.create_task(running.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    yield running, tmp_path / "es.sock"
    serving.cancel()
    await asyncio.gather(serving, return_exceptions=True)


async def test_a_produce_is_answered_and_readable_only_once_it_is_on_the_disk(served, gate):
    _running, socket_path = served
    producer, reader = FreeStreamClient(socket_path), FreeStreamClient(socket_path)
    try:
        answer = asyncio.create_task(producer.produce("work", value=b"page 1"))
        await _until(gate.started.is_set)
        await asyncio.sleep(0.05)
        assert not answer.done()
        assert await reader.next("work", "readers", wait=False) is None
        gate.opened.set()
        assert await answer == (0, 0)
        partition, record = await reader.next("work", "readers", wait=True)
        assert (partition, record.value) == (0, b"page 1")
    finally:
        gate.opened.set()
        await producer.close()
        await reader.close()


async def test_a_connections_produces_share_a_flush_and_keep_their_order(served, gate):
    """The connection reads its next produce while the last waits for its flush, so a producer that keeps
    several in flight is answered by far fewer flushes than records, in the order it sent them.

    The first flush is held until all 200 records are written behind it, so the count is the mechanism's
    and not the disk's: the held flush, and one more for everything written during it. It used to count
    free-running flushes under 100, which a fast enough disk could exceed."""
    running, socket_path = served
    owner = running.strand_for("work", 0)
    log = owner._topic("work").log(0)
    passes_before = owner.durability.passes
    producer = FreeStreamClient(socket_path)
    try:
        sending = asyncio.gather(
            *(producer.produce("work", value=f"record {number}".encode()) for number in range(200)))
        await eventually(lambda: log.next_offset >= 200, "the records were never all written")
        gate.opened.set()
        placements = await sending
    finally:
        gate.opened.set()
        await producer.close()
    assert [offset for _partition, offset in placements] == list(range(200))
    assert owner.durability.passes - passes_before <= 2


async def test_a_completes_outputs_are_on_the_disk_when_it_is_answered(served, gate):
    """★ AN OUTPUT IS RECORDED AS LANDED ONLY ONCE ITS OWN STRAND HAS FLUSHED IT: a replay skips a landed
    output, and must never skip one a power cut erased."""
    running, socket_path = served
    gate.opened.set()
    producer, worker = FreeStreamClient(socket_path), FreeStreamClient(socket_path)
    try:
        await producer.produce("work", value=b"page 1")
        partition, record = await worker.next("work", "workers", wait=True)
        gate.opened.clear()
        gate.started.clear()
        completing = asyncio.create_task(worker.complete(
            "work", "workers", partition, record.offset + 1, [Output("done", b"read")]))
        await _until(gate.started.is_set)
        await asyncio.sleep(0.05)
        assert not completing.done()
        gate.opened.set()
        await completing
        output = await producer.next("done", "readers", wait=False)
        assert output is not None and output[1].value == b"read"
    finally:
        gate.opened.set()
        await producer.close()
        await worker.close()
    await _until(lambda: not list((running.root / ".journal").glob("*.complete")))


async def test_an_output_that_failed_leaves_the_ones_before_it_flushed_and_recorded(served, monkeypatch):
    """When the second output's append fails, the first is flushed and written into the entry as landed, and
    the strand finishing the entry sends only the second."""
    from freestream import strand as strand_module
    from freestream.journal import pending_entries
    running, socket_path = served
    monkeypatch.setattr(strand_module, "FINISH_FIRST_WAIT_SECONDS", 0.5)
    real_append, failures = strand_module.Strand.append, []

    def failing_once_on_the_second(self, channel, partition, record, producer, sequence):
        if channel == "done" and sequence == 1 and not failures:
            failures.append(sequence)
            raise OSError(28, "No space left on device")
        return real_append(self, channel, partition, record, producer, sequence)
    monkeypatch.setattr(strand_module.Strand, "append", failing_once_on_the_second)
    producer, worker = FreeStreamClient(socket_path), FreeStreamClient(socket_path)
    try:
        await producer.produce("work", value=b"page 1")
        partition, record = await worker.next("work", "workers", wait=True)
        await worker.complete("work", "workers", partition, record.offset + 1,
                              [Output("done", b"first"), Output("done", b"second")])
        recorded = [sorted(entry.landed) for _path, entry in pending_entries(running.root, running.seal)]
        await _until(lambda: not list((running.root / ".journal").glob("*.complete")))
        outputs = []
        for _ in range(3):
            taken = await producer.next("done", "readers", wait=False)
            if taken is None:
                break
            outputs.append(taken[1].value)
            await producer.commit("done", "readers", taken[0], taken[1].offset + 1)
    finally:
        await producer.close()
        await worker.close()
    assert failures == [1]
    assert recorded == [[0]]
    assert sorted(outputs) == [b"first", b"second"]
