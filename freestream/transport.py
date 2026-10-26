"""FreeStream as a transport: the interface in `freestream.interface.transport`, kept by our own server.

    freestream:///var/lib/freestream/eventstream.sock                 a stage, or the API
    freestream:var/eventstream.sock                                the same, relative to where it runs
    freestream:///var/lib/freestream/eventstream.sock?admin=/var/lib/freestream/eventstream-admin.sock
                                                                    the deploy step, which defines topics

The address is a socket path, because the server listens on a unix socket and is permissioned by the
filesystem; it has no host, so an address with one is refused rather than read as something it is not.
The console's socket rides in the query, and only the deploy step needs it: an operator defines topics,
and a stage never does.

★ EVERY GUARANTEE IS KEPT HERE, by the server and by the consumer loop in `consumer.py`: one member
holds a key until it closes it, a handler's outputs go with its commit as one COMPLETE, a resend is
recognized by its producer id and sequence, a record started `max_attempts` times is dead-lettered and
closed out, and a record whose handler raised `RetryLater` waits on its channel's retry topic, which the
server hands out only once each record is due.
"""
from __future__ import annotations

import asyncio
from collections.abc import Iterable
from pathlib import Path
from typing import Self
from urllib.parse import parse_qs, urlsplit

from .client import FreeStreamClient
from .consumer import Registration, retry_channel, run_consumers
from .interface.channel import Channel, Delivery
from .interface.transport import (
    DEFAULT_MAX_ATTEMPTS,
    EventStreamTransport,
    EventStreamUnavailableError,
    Exhausted,
    Guarantee,
    Handler,
)

#: How long `define` waits for the console to answer — deploy restarts the server just before it runs.
WAIT_FOR_CONSOLE_SECONDS = 30.0


class FreeStreamAddressError(ValueError):
    """A `freestream:` address that does not name a socket path."""


def socket_paths(url: str) -> tuple[str, str | None]:
    """The server's socket path, and the console's when the query names one."""
    parts = urlsplit(url)
    if parts.netloc:
        raise FreeStreamAddressError(
            f"{url!r} names a host, and FreeStream is reached by a socket path: write "
            f"freestream:///absolute/path.sock or freestream:relative/path.sock")
    if not parts.path:
        raise FreeStreamAddressError(f"{url!r} names no socket path")
    admin = parse_qs(parts.query).get("admin")
    return parts.path, admin[0] if admin else None


class FreeStreamTransport(EventStreamTransport):
    """One connection to the FreeStream server, and the handlers that read through it."""

    guarantees = frozenset(Guarantee)

    def __init__(self, client: FreeStreamClient, *, admin_socket: str | Path | None = None) -> None:
        #: The connection itself. The tests and the soak tool reach it to drive the server's own calls;
        #: the pipeline never sees it.
        self.client = client
        self._admin_socket = str(admin_socket) if admin_socket is not None else None
        self._registrations: list[Registration] = []

    @classmethod
    def open(cls, url: str, *, role: str) -> Self:
        socket_path, admin_socket = socket_paths(url)
        return cls(FreeStreamClient(socket_path, role=role), admin_socket=admin_socket)

    @property
    def registrations(self) -> list[Registration]:
        """The handlers declared on this transport, in the order they were declared."""
        return list(self._registrations)

    async def ensure_reachable(self) -> None:
        await self.client.hello()

    async def _produce(self, channel: Channel, *, value: bytes, key: str | None,
                       headers: dict[str, str]) -> None:
        await self.client.produce(channel.name, value=value, key=key, headers=headers)

    def register(self, channel: Channel, *, group: str, handler: Handler,
                 on_exhausted: Exhausted | None = None, max_attempts: int = DEFAULT_MAX_ATTEMPTS,
                 delayed_retry: bool = False) -> None:
        """Run `handler` on the channel's records; with `delayed_retry`, on its retry topic's as well.

        The retry topic is read by a registration of its own, in the same group, on this connection: the
        server hands its records out only once they are due, so this member waits on both at once.
        """
        self._registrations.append(Registration(
            channel=channel, group=group, handler=handler, on_exhausted=on_exhausted,
            max_attempts=max_attempts, delayed_retry=delayed_retry))
        if delayed_retry:
            retries = Channel(retry_channel(channel.name), Delivery.COMPETING, partitions=channel.partitions)
            self._registrations.append(Registration(
                channel=retries, group=group, handler=handler, on_exhausted=on_exhausted,
                max_attempts=max_attempts, delayed_retry=True, retries_of=channel.name))

    async def run(self, *, stop: asyncio.Event | None = None) -> None:
        await run_consumers(self.client, self._registrations, stop=stop)

    async def close_key(self, channel: Channel, *, group: str, key: str) -> None:
        await self.client.close_key(channel.name, group, key)

    async def define(self, channels: Iterable[Channel]) -> list[str]:
        """Define each channel as a topic, over the console, as an operator would with `create topic`."""
        if self._admin_socket is None:
            raise FreeStreamAddressError(
                "defining topics needs the console: add ?admin=<the console's socket> to the address")
        await _await_the_console(self._admin_socket, WAIT_FOR_CONSOLE_SECONDS)
        answers = []
        for channel in channels:
            answer = await _say(self._admin_socket,
                                f"create topic {channel.name} {channel.partitions} {channel.delivery}")
            if answer.startswith("refused"):
                raise RuntimeError(f"{channel.name}: {answer}")
            answers.append(answer)
        return answers

    async def close(self) -> None:
        await self.client.close()


async def _say(admin_socket: str, line: str) -> str:
    reader, writer = await asyncio.open_unix_connection(admin_socket)
    try:
        writer.write(line.encode() + b"\n")
        await writer.drain()
        return (await reader.read()).decode().strip()
    finally:
        writer.close()


async def _await_the_console(admin_socket: str, seconds: float) -> None:
    """Wait until the console ANSWERS, not until its file exists.

    A server being restarted leaves its old socket file in place until the new one binds, and a
    connection to that file is refused — which is how the first real run of the topics step found no
    topics.
    """
    for _ in range(int(seconds / 0.1)):
        try:
            _reader, writer = await asyncio.open_unix_connection(admin_socket)
        except OSError:
            await asyncio.sleep(0.1)
            continue
        writer.close()
        return
    raise EventStreamUnavailableError(f"no event stream server console at {admin_socket} after "
                                      f"{seconds:.0f}s")
