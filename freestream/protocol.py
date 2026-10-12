"""The frames a client and the server exchange (#450).

★ WHY THERE IS A SERVER AT ALL (2026-09-22). The first version of this store was a library: every
producer opened the log file and appended to it. That works while one process writes, and hop 2 is the
case where it does not — twelve OCR consumers finish pages of one document at once, and the key routes
all of them to the SAME partition. Offsets assigned from one process's memory collide, and a 30 KB body
can be split across write() calls and interleave with another writer's.

Kafka, NATS and Redpanda do not have that problem because they are all brokers: producers SEND, and one
process appends. When we imitated their semantics and kept the library shape, we kept the semantics and
threw away the thing that makes them hold. So: one process owns the log, and concurrent publishes stop being
a question anyone has to answer.

A UNIX socket rather than AF_INET, because the server and its clients are on one box. It is faster, it
is permissioned by the filesystem rather than by anything we write, and there is no port to collide.
A page reader on a box of its own will reach the server through a TCP listener with TLS beside the
socket, speaking the same protocol.

FRAME — one shape for both directions, because a response carrying records is the same problem as a
request carrying a page:

    u32 header_len | u32 body_len | header (JSON, utf-8) | body (raw bytes)

The header says what this is; the body is content and is never encoded.

★ EVERY REQUEST CARRIES AN `id` AND ITS REPLY CARRIES IT BACK AS `re`, because a reply no longer comes
back in the order the requests went. A consumer AWAITS its next record: the server holds that reply
until a record exists, which may be minutes, and meanwhile the same connection commits, counts
attempts and closes keys. The id is what matches each answer to its question, as Kafka's correlation
id does. A page is a 30 KB PDF and
base64 would cost 33% of it for nothing, which is the same reason `Record` is framed rather than
serialized to JSON.
"""
from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from typing import Any, Self

_PREFIX = struct.Struct("<II")
PREFIX_BYTES = _PREFIX.size

#: A frame larger than this is refused rather than allocated. A page is tens of KB; the ceiling exists
#: so a malformed length cannot make the server try to reserve gigabytes on one bad read.
MAX_FRAME_BYTES = 64 * 1024 * 1024

#: Room kept, below the frame limit, for the JSON header of the answer that delivers a record — its
#: partition and the request id it answers. A record is refused at produce unless it fits beside it.
ANSWER_HEADER_ALLOWANCE = 4096


class ProtocolError(ValueError):
    """A frame that cannot be read. Poison — the connection is dropped rather than resynchronised."""


@dataclass(frozen=True, slots=True)
class Frame:
    """One message in either direction: what it is, and optionally the bytes it carries."""

    header: dict[str, Any] = field(default_factory=dict)
    body: bytes = b""

    def encoded(self) -> bytes:
        head = json.dumps(self.header, separators=(",", ":")).encode()
        if len(head) + len(self.body) > MAX_FRAME_BYTES:
            raise ProtocolError(f"frame of {len(head) + len(self.body)} bytes is over the ceiling")
        return _PREFIX.pack(len(head), len(self.body)) + head + self.body

    @classmethod
    def decoded(cls, prefix: bytes, rest: bytes) -> Self:
        """Rebuild a frame from its prefix and the bytes that followed it."""
        header_len, body_len = read_prefix(prefix)
        if len(rest) != header_len + body_len:
            raise ProtocolError(f"expected {header_len + body_len} bytes after the prefix, "
                                f"got {len(rest)}")
        try:
            header = json.loads(rest[:header_len])
        except ValueError as malformed:
            raise ProtocolError(f"unreadable frame header: {malformed}") from malformed
        if not isinstance(header, dict):
            raise ProtocolError("a frame header must be an object")
        return cls(header=header, body=rest[header_len:])


def read_prefix(prefix: bytes) -> tuple[int, int]:
    """The two lengths at the front of a frame, checked before anything is allocated for them."""
    if len(prefix) != PREFIX_BYTES:
        raise ProtocolError(f"a frame prefix is {PREFIX_BYTES} bytes, got {len(prefix)}")
    header_len, body_len = _PREFIX.unpack(prefix)
    if header_len + body_len > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame claims {header_len + body_len} bytes, over the ceiling")
    if header_len == 0:
        raise ProtocolError("a frame with no header says nothing about itself")
    return header_len, body_len
