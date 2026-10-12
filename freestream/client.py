"""The client that talks to the FreeStream server over its socket (#450).

`FreeStreamTransport` and the consumer loop under it are written against these calls. The pipeline is
not: it holds an `EventStreamTransport` (`freestream.interface.transport`), and moving to another queue means
another transport beside this package rather than a change to this file.

    async for partition, record in client.subscribe("pages.to_read", "page.readers"):
        ...                                         # awaits: nothing is asked for while idle

★ A CONSUMER AWAITS ITS NEXT RECORD (2026-09-24). `next` sends one request and the server answers
it when there is a record, the way `read()` on a socket returns when there are bytes. How the records
arrive is this file's business and the server's: a Kafka client's `subscribe` would sit on its own fetch
loop under the same signature, which is why callers never see which.

★ MANY REQUESTS IN FLIGHT ON ONE CONNECTION, matched by id. A held NEXT can be outstanding for minutes,
and meanwhile the same consumer commits, counts attempts and closes keys — the assembler closes a key
from its assembly task while its consumer waits for the next page. So each request carries an `id`,
the server answers with it as `re`, and one task reads every answer off the socket and hands each to
whoever asked. It has to be one connection: a consumer's membership of its group IS its connection, so
a close sent over a second socket would reach a member that does not exist.

★ A DROPPED CONNECTION IS RECONNECTED AND THE CALL RETRIED ONCE. The server restarting under a deploy
is ordinary and a producer should not see it. Retried ONCE rather than looped: a second failure is the
server being down rather than a connection being stale, and a caller that waits forever on that is a
document nobody is told about.

★ EVERY CONNECTION SAYS WHO IT IS FIRST — a HELLO with its role, a name, its pid and host — and says it
again after a reconnect, because to the server a reconnect is a stranger. That is what `show consumers`
on the admin socket lists. It identifies; it authorizes nothing.
"""
from __future__ import annotations

import asyncio
import contextlib
import itertools
import os
import socket
import uuid
from collections.abc import AsyncIterator, Iterator
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from .interface.transport import EventStreamUnavailableError
from .protocol import PREFIX_BYTES, Frame, read_prefix
from .record import STREAM_HEADER_PREFIX, Record
from .registry import (
    PartitionNotHeldError,
    RequestRefusedError,
    TopicConflictError,
    TopicError,
    UnknownTopicError,
)

#: A refusal the server answered with, by the name it gave, raised here as the same exception.
_REFUSALS: dict[str, type[TopicError]] = {
    refusal.__name__: refusal for refusal in (TopicError, UnknownTopicError, TopicConflictError,
                                              RequestRefusedError, PartitionNotHeldError)}


class HandlingEndedError(RuntimeError):
    """A produce made in the name of a record whose handling has already finished.

    Raised rather than written straight to the log: an output belongs to the record its handler was
    handed, and one that arrives after that record was completed has lost the step that made it happen
    once. The case that brought it about is a task started inside a handler, which carries the handler's
    context with it and outlives it.
    """


@dataclass(slots=True)
class Output:
    """One record a handler produced, held until its input is completed."""

    channel: str
    value: bytes
    key: str | None = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class Handling:
    """The outputs of one record's handler, collected while it runs. See `FreeStreamClient.handling`."""

    client: FreeStreamClient
    outputs: list[Output] = field(default_factory=list)
    ended: bool = False


#: The handling in progress in this context, if any. A context variable so that every produce made while
#: a handler runs, however deep in the call stack, joins its record's COMPLETE without being told to.
_HANDLING: ContextVar[Handling | None] = ContextVar("event_stream_handling", default=None)


class _Connection:
    """One open socket, and the requests waiting on an answer from it."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader, self.writer = reader, writer
        #: request id -> the future its answer resolves.
        self.waiting: dict[int, asyncio.Future[Frame]] = {}
        #: One writer at a time, so two requests' frames never interleave on the socket.
        self.writing = asyncio.Lock()
        self.answers: asyncio.Task[None] | None = None

    @property
    def alive(self) -> bool:
        return not self.writer.is_closing() and self.answers is not None and not self.answers.done()

    async def read_answers(self) -> None:
        """Hand each answer to the request it names, until the socket closes."""
        try:
            while True:
                prefix = await self.reader.readexactly(PREFIX_BYTES)
                header_len, body_len = read_prefix(prefix)
                answer = Frame.decoded(prefix, await self.reader.readexactly(header_len + body_len))
                request_id = answer.header.get("re")
                waiting = self.waiting.pop(request_id, None) if isinstance(request_id, int) else None
                # No one waiting is a NEXT that was canceled while the server held it. Its record
                # was not committed, so the next NEXT is handed it again; dropping it loses nothing.
                if waiting is not None and not waiting.done():
                    waiting.set_result(answer)
        except (OSError, asyncio.IncompleteReadError) as closed:
            self.fail_everything(closed)

    def fail_everything(self, cause: BaseException) -> None:
        for waiting in self.waiting.values():
            if not waiting.done():
                waiting.set_exception(ConnectionResetError(f"the server connection closed: {cause}"))
        self.waiting.clear()


class FreeStreamClient:
    """One connection to the server, opened on first use and re-opened when it drops."""

    def __init__(self, socket_path: str | Path, *, role: str = "client", name: str = "") -> None:
        self._socket_path = str(socket_path)
        self._hello = {"op": "hello", "role": role, "name": name, "pid": os.getpid(),
                       "host": socket.gethostname()}
        #: The number the server gave this connection — what `set consumer <id>` takes.
        self.connection_id: int | None = None
        self._connection: _Connection | None = None
        self._connecting = asyncio.Lock()
        self._ids = itertools.count(1)
        #: Set by `close`. A NEXT still held when the client closes fails, and without this its retry
        #: would open a fresh connection for a consumer that is shutting down.
        self._closed = False
        #: Who this client is to the server as a producer, for as long as the object lives — across
        #: reconnects, which is where a resend comes from. See `Topic.produce_once`.
        self._producer_id = uuid.uuid4().hex
        self._sequences = itertools.count()

    async def hello(self) -> None:
        """Connect now and say who this client is, rather than at the first call. Raises if nobody answers."""
        try:
            await self._connected()
        except (OSError, asyncio.IncompleteReadError) as unreachable:
            raise EventStreamUnavailableError(
                f"the event stream server at {self._socket_path} did not answer") from unreachable

    async def produce(self, channel: str, *, value: bytes, key: str | None = None,
                      headers: dict[str, str] | None = None) -> tuple[int, int] | None:
        """Append one record. Returns the (partition, offset) it was given.

        The topic must have been defined; the server refuses one it does not know (`UnknownTopicError`).

        ★ INSIDE A HANDLER IT IS HELD, NOT SENT, and returns None. The consumer sends everything its
        handler produced together with the commit of the record it handled, as one COMPLETE, so a
        handler that dies partway leaves nothing in the log, and the retry's outputs are the only ones.
        There is no placement to return until then. The caller writes the same call either way.
        """
        handling = _HANDLING.get()
        if handling is not None and handling.client is self:
            if handling.ended:
                raise HandlingEndedError(
                    f"a record for {channel} was produced after the record it belongs to was completed")
            handling.outputs.append(Output(channel, value, key, dict(headers or {})))
            return None
        # The same header goes out on the one resend `_call` makes after a drop, sequence and all, so a
        # server that wrote the first attempt answers the second with the same placement.
        answer = await self._call(
            {"op": "produce", "channel": channel, "key": key, "headers": headers or {},
             "producer": self._producer_id, "sequence": next(self._sequences)},
            body=value)
        return answer.header["partition"], answer.header["offset"]

    async def next(self, channel: str, group: str, *,
                   wait: bool = True) -> tuple[int, Record] | None:
        """This consumer's next record, as (partition, record) — awaited until there is one.

        The record is the first one the GROUP has not committed in the partitions this consumer holds,
        so a record whose handler failed comes back as the next one. `wait=False` answers at once, and
        answers None when there is nothing. A topic that is defined but has no producer is ordinary: the
        NEXT is simply held until one arrives.
        """
        answer = await self._call({"op": "next", "channel": channel, "group": group, "wait": wait})
        if answer.header.get("empty"):
            return None
        record, _end = Record.unframed(answer.body)
        # The stream's own headers (`Topic.produce_once`) are how the server recognizes a resend; they
        # say nothing about the record, so a handler never sees them.
        own = [name for name in record.headers if name.startswith(STREAM_HEADER_PREFIX)]
        if own:
            record = replace(record, headers={name: value for name, value in record.headers.items()
                                              if name not in own})
        return answer.header["partition"], record

    async def subscribe(self, channel: str, group: str) -> AsyncIterator[tuple[int, Record]]:
        """Every record this consumer is given, for as long as it keeps asking. Idle costs nothing.

        Ask for the next one only after the last is committed or given up on — the server hands over
        the first uncommitted record, so asking sooner is asking for the same one again.
        """
        while True:
            taken = await self.next(channel, group)
            if taken is not None:
                yield taken

    async def attempt(self, channel: str, group: str, partition: int, offset: int) -> int:
        """Tell the server a record is about to run, and learn how many times that makes it."""
        # One token per START: a request resent after a lost answer carries the same one, and the
        # server counts it once (`ConsumerGroup.attempt`).
        answer = await self._call({"op": "attempt", "channel": channel, "group": group,
                                   "partition": partition, "offset": offset, "token": uuid.uuid4().hex})
        attempts: int = answer.header["attempt"]
        return attempts

    async def commit(self, channel: str, group: str, partition: int, next_offset: int) -> None:
        """Record where to resume. `next_offset` is the NEXT record to read, not the last one read."""
        await self._call({"op": "commit", "channel": channel, "group": group,
                          "partition": partition, "next_offset": next_offset})

    @contextlib.contextmanager
    def handling(self) -> Iterator[Handling]:
        """Collect what this client produces while one record is handled; `Consumer.handle` calls it.

        On leaving, the handling is marked ended: a task the handler started keeps a reference to it
        through its context, and a produce from that task raises instead of being lost.
        """
        handling = Handling(self)
        token = _HANDLING.set(handling)
        try:
            yield handling
        finally:
            handling.ended = True
            _HANDLING.reset(token)

    async def complete(self, channel: str, group: str, partition: int, next_offset: int,
                       outputs: list[Output]) -> None:
        """Append a handler's outputs and commit its record, as one step on the server or not at all."""
        await self._call(
            {"op": "complete", "channel": channel, "group": group, "partition": partition,
             "next_offset": next_offset,
             "outputs": [{"channel": output.channel, "key": output.key, "headers": output.headers,
                          "length": len(output.value)} for output in outputs]},
            body=b"".join(output.value for output in outputs))

    async def close_key(self, channel: str, group: str, key: str) -> None:
        """Tell the server this consumer has finished with a key — on a keyed channel, a document done.

        A DRAINING member is handed records only for the keys it has open, and lets its partitions go
        when the last of them is closed; this is how it says one is.
        """
        await self._call({"op": "close", "channel": channel, "group": group, "key": key})

    async def members(self, channel: str, group: str) -> int:
        """How many READY members a group has right now. Zero means nobody would read what is sent."""
        answer = await self._call({"op": "members", "channel": channel, "group": group})
        ready: int = answer.header["ready"]
        return ready

    async def close(self) -> None:
        self._closed = True
        async with self._connecting:
            await self._drop()

    # ------------------------------------------------------------------ inner

    async def _call(self, header: dict[str, Any], *, body: bytes = b"") -> Frame:
        answer = await self._call_once_more_on_a_drop(header, body)
        if "error" in answer.header:
            refusal = _REFUSALS.get(answer.header.get("kind", ""), TopicError)
            raise refusal(answer.header["error"])
        return answer

    async def _call_once_more_on_a_drop(self, header: dict[str, Any], body: bytes) -> Frame:
        try:
            return await self._request(await self._connected(), header, body)
        except (OSError, asyncio.IncompleteReadError):
            async with self._connecting:
                await self._drop()          # stale connection — the server restarted, most likely
        try:
            return await self._request(await self._connected(), header, body)
        except (OSError, asyncio.IncompleteReadError) as unreachable:
            async with self._connecting:
                await self._drop()
            raise EventStreamUnavailableError(
                f"the event stream server at {self._socket_path} did not answer") from unreachable

    async def _request(self, connection: _Connection, header: dict[str, Any], body: bytes) -> Frame:
        request_id = next(self._ids)
        answered: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        connection.waiting[request_id] = answered
        try:
            async with connection.writing:
                connection.writer.write(Frame(header={**header, "id": request_id}, body=body).encoded())
                await connection.writer.drain()
            return await answered
        finally:
            connection.waiting.pop(request_id, None)

    async def _connected(self) -> _Connection:
        async with self._connecting:
            if self._closed:
                raise EventStreamUnavailableError("this server client has been closed")
            if self._connection is None or not self._connection.alive:
                await self._drop()
                reader, writer = await asyncio.open_unix_connection(self._socket_path)
                connection = _Connection(reader, writer)
                connection.answers = asyncio.create_task(connection.read_answers())
                self._connection = connection
                answer = await self._request(connection, self._hello, b"")
                self.connection_id = answer.header["connection"]
            return self._connection

    async def _drop(self) -> None:
        connection, self._connection = self._connection, None
        if connection is None:
            return
        if connection.answers is not None:
            connection.answers.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await connection.answers
        connection.fail_everything(ConnectionResetError("closed by this client"))
        if not connection.writer.is_closing():
            connection.writer.close()
            # Already gone is exactly what we were trying to achieve.
            with contextlib.suppress(OSError, asyncio.IncompleteReadError):
                await connection.writer.wait_closed()
