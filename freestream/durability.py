"""The stream puts what it writes on the disk before it answers, and many answers share one flush (#450).

The brief's R5, 2026-09-26: "A system/process crash cannot result in a lost or duplicate
per-record output." A process crash leaves the page cache behind, and the kernel writes it out; a power cut
or a kernel panic does not. Until this, nothing in the stream flushed, so after a power cut the filesystem was
free to keep a journal entry's removal and a group's commit while it lost the output records they relied on.

So each strand keeps a `Durability`. A write marks its file (and, for a rename or a new file, the directory)
as needing a flush, and a request that must be durable before it is answered awaits `barrier()`. The strand
keeps at most one flush in flight: while a worker thread flushes one batch, the strand's loop goes on taking
requests, and whatever they write waits for the next flush, which starts the moment the current one ends. A
busy strand therefore answers many requests per flush, and an idle one flushes a single write at once.

We measured the costs on the development box's NVMe drive under ext4, 2026-09-26. The drive commits ext4's
journal once per fsync, at about 6.5 ms, and a thread that fsyncs sixteen files one after another pays that
sixteen times, 100 ms. Sixteen threads that fsync the same sixteen files at once share one journal commit and
take 13.7 ms. So `flush_now` fsyncs a batch's files together from a pool of threads, never one by one.

A writer that waits for each answer before it sends the next is held to one record per flush. The stream
head publishes a return's pages that way, at about 80 a second, and a stage worker completes its records
that way, at about 26 a second in the multithreading test, since a COMPLETE waits on two flushes. OCR reads
about ten pages a second across the dev box's ten readers, and the quick soak's undisturbed round took
166 s after this change against 158 s and 159 s before it (2026-09-26).

A thread with no `Durability` — a test, a tool, the journal replay before the strands start — marks nothing,
and flushes, where it must, with `flush_now`.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

log = logging.getLogger(__name__)

_CURRENT = threading.local()

#: The threads that fsync, shared by every strand. ext4 folds fsyncs that arrive together into one journal
#: commit, so this pool is sized for a batch's files and directories rather than for the machine's cores.
FSYNC_THREADS = 32
_FSYNC_POOL = ThreadPoolExecutor(max_workers=FSYNC_THREADS, thread_name_prefix="eventstream-fsync")


class Durability:
    """One strand's flush barrier: marks what was written, and keeps one flush in flight at a time."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        #: What was written since the flush in flight began, and what to run once it is on the disk — a log
        #: making its records readable.
        self._files: set[Path] = set()
        self._directories: set[Path] = set()
        self._then: list[Callable[[], None]] = []
        #: Resolved by the flush that will cover what is marked now; None while nothing is marked.
        self._next: asyncio.Future[None] | None = None
        #: Resolved when the flush in flight ends; None while none is.
        self._in_flight: asyncio.Future[None] | None = None
        #: How many flushes have run, for the tests.
        self.passes = 0

    def install(self) -> None:
        """Make this the barrier for writes made on the calling thread — the strand's own."""
        _CURRENT.durability = self

    def mark(self, path: Path, *, directory_changed: bool = False,
             then: Callable[[], None] | None = None) -> None:
        self._files.add(path)
        if directory_changed:
            self._directories.add(path.parent)
        if then is not None:
            self._then.append(then)
        if self._next is None:
            # A write nobody awaits is flushed as soon as the strand can, so the records it made readable do
            # not wait for the next request that happens to need a barrier.
            self._next = self._loop.create_future()
            if self._in_flight is None:
                self._loop.call_soon(self._start_flush)

    def when_flushed(self, follow_up: Callable[[], None]) -> None:
        """Run `follow_up` once everything marked so far is on the disk; at once when nothing is waiting."""
        if self._next is not None:
            self._then.append(follow_up)
        elif self._in_flight is not None:
            def after(flushed: asyncio.Future[None]) -> None:
                if flushed.exception() is None:
                    follow_up()
            self._in_flight.add_done_callback(after)
        else:
            follow_up()

    async def barrier(self) -> None:
        """Return once everything marked so far is on the disk."""
        if self._next is not None:
            await asyncio.shield(self._next)
        # Nothing marked since the flush in flight began, so that flush covers every earlier write; it may
        # have been marked by another request, and this one must not answer before it ends.
        elif self._in_flight is not None:
            await asyncio.shield(self._in_flight)

    def _start_flush(self) -> None:
        if self._in_flight is not None or self._next is None:
            return
        files, directories, then, done = self._files, self._directories, self._then, self._next
        self._files, self._directories, self._then, self._next = set(), set(), [], None
        self._in_flight = done
        self.passes += 1
        self._loop.create_task(self._flush(files, directories, then, done))

    async def _flush(self, files: set[Path], directories: set[Path], then: list[Callable[[], None]],
                     done: asyncio.Future[None]) -> None:
        try:
            await self._loop.run_in_executor(None, flush_now, files, directories)
        except OSError as failure:
            the_flush_failed(failure)
            done.set_exception(failure)
        else:
            for follow_up in then:
                follow_up()
            done.set_result(None)
        finally:
            self._in_flight = None
            if self._next is not None:
                self._start_flush()


#: The exit status of a server stopped by a failed flush; systemd restarts it (`Restart=on-failure`).
EXIT_FLUSH_FAILED = 3


def the_flush_failed(failure: OSError) -> None:
    """Stop the whole server at once: after a failed fsync, nobody knows what the disk holds.

    Linux marks the pages of a failed fsync clean, so a second fsync can succeed over data the disk never
    took, and Postgres stops for the same reason. We stop too, rather than answer from memory that may no
    longer match the disk. The server that systemd starts next recovers each partition from what its
    segment files really hold and replays the journal, and every client resends what went unanswered with
    its producer sequence unchanged, so a record already on the disk is recognized and not written twice.
    """
    log.critical("a flush failed, so the event stream server stops and recovers from the disk: %s", failure)
    logging.shutdown()
    os._exit(EXIT_FLUSH_FAILED)


def mark_written(path: Path, *, directory_changed: bool = False,
                 then: Callable[[], None] | None = None) -> bool:
    """Note a write on the calling thread's barrier. False when the thread has none; then `then` never ran."""
    durability: Durability | None = getattr(_CURRENT, "durability", None)
    if durability is None:
        return False
    durability.mark(path, directory_changed=directory_changed, then=then)
    return True


def flush_now(files: Iterable[Path], directories: Iterable[Path] = ()) -> None:
    """Fsync the files together, then the directories whose entries changed. A file gone since is skipped."""
    for fsync_one, paths in ((_fsync, files), (_fsync_directory, directories)):
        # Every fsync of the group ends before a failure is raised: the list waits for all of them.
        failures = [failure for failure in _FSYNC_POOL.map(fsync_one, list(paths)) if failure is not None]
        if failures:
            raise failures[0]


def _fsync(path: Path) -> OSError | None:
    return _fsync_opened(path, os.O_RDONLY)


def _fsync_directory(path: Path) -> OSError | None:
    return _fsync_opened(path, os.O_RDONLY | os.O_DIRECTORY)


def _fsync_opened(path: Path, flags: int) -> OSError | None:
    """Fsync one path; the failure is returned rather than raised, so every other fsync still runs."""
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None
    try:
        os.fsync(descriptor)
    except OSError as failure:
        return failure
    finally:
        os.close(descriptor)
    return None
