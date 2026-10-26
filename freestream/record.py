"""One record on the wire, framed the way a log can read it back (#450).

Shaped after Kafka's record rather than invented: offset, timestamp, key, value, headers. That is not
imitation for its own sake — it is how the semantics stay OUTSIDE this repo. A format of my own would
mean the meaning of a record lived in prose I wrote, which is the arrangement that failed on
2026-09-22.

    offset(8) | crc32(4) | timestamp_ns(8) | key_len(4) | value_len(4) | headers_len(4)
    | key | value | headers

Length-prefixed and fixed-width in the header, so a reader can skip a record it does not want without
decoding it, and can tell a truncated tail from a short record — a log that cannot distinguish those
will happily read garbage off the end of a partial write.

★ EVERY RECORD CARRIES A CHECKSUM, a CRC32 over its offset and everything after the checksum itself —
Kafka's record batches carry one for the same reason. Without it a record whose bytes changed on disk,
or that a bug of ours wrote badly, was PARSED: its headers failed as JSON and the server could not open
the topic, or worse, its value was handed to a consumer as though it were what had been written (the
adversarial review, 2026-09-25). A record that fails the check raises `CorruptRecordError`, and
`partition_log` decides what that means where it was found.

★ THE KEY IS BYTES THE PRODUCER CHOSE, and nothing here interprets it. The partition it lands in is a
hash of it and no more. A store that reached into `value` for something identifier-shaped would be a
store that knows what our documents are (2026-09-22).
"""
from __future__ import annotations

import json
import struct
import time
import zlib
from dataclasses import dataclass, field
from typing import Protocol, Self

#: offset, crc32, timestamp_ns, key_len, value_len, headers_len
_HEADER = struct.Struct("<QIQIII")
HEADER_BYTES = _HEADER.size
#: The offset, then the checksum, then everything the checksum covers along with the offset.
_OFFSET = struct.Struct("<Q")
_CHECKED_FROM = _OFFSET.size + 4
#: Inside a sealed record: key_len, value_len, headers_len, then the three.
_INNER = struct.Struct("<III")


class Sealer(Protocol):
    """What `sealing.Seal` offers a record. A Protocol, so the record format imports no cipher."""

    def seal(self, plaintext: bytes, bound_to: bytes) -> bytes: ...

    def open(self, sealed: bytes, bound_to: bytes) -> bytes: ...

#: The headers the server writes into a record to recognize a resent produce (`Topic.produce_once`).
#: The stream owns them: a produce's own headers under this prefix are dropped, since a processor copying
#: its input's headers forward would otherwise carry another producer's identity into its output, and the
#: client removes them before a handler sees the record.
STREAM_HEADER_PREFIX = "stream."
PRODUCER_HEADER = "stream.producer"
SEQUENCE_HEADER = "stream.sequence"

#: The headers a record carries on a retry topic (`<topic>.retry`), written by the consumer loop when a
#: handler raises `RetryLater`. Not under `stream.`, because a produce's own headers there are dropped and
#: these are produced, inside a COMPLETE, like any other output. The consumer loop removes them before the
#: handler sees the record again.
RETRY_HEADER_PREFIX = "retry."
#: When the record may run again, in nanoseconds since the epoch. The server hands it out no sooner.
RETRY_NOT_BEFORE_HEADER = "retry.not_before"
#: How many times the record was started before it was written here, so the attempt limit spans the hops.
RETRY_ATTEMPTS_HEADER = "retry.attempts"
#: The key the record carried, kept here because a retry topic is COMPETING and refuses a key.
RETRY_KEY_HEADER = "retry.key"


class CorruptRecordError(ValueError):
    """A whole record is there, and it is not what was written.

    Its checksum fails, or its headers do not parse. Its framing may be intact or may be the damaged part,
    so a reader cannot trust the lengths it claims; `partition_log` looks for the next record that passes
    before it decides anything. `end` is where the record's own lengths say it ends, which is all there
    is to go on when nothing after it passes.
    """

    def __init__(self, message: str, *, end: int) -> None:
        super().__init__(message)
        self.end = end


class TruncatedRecordError(ValueError):
    """The bytes end mid-record — a partial write, or a reader that has run off the end of the log.

    Named rather than folded into a generic parse error because the two call for opposite responses: a
    CORRUPT record is poison and gets abandoned, a TRUNCATED one is simply the end of what has been
    written so far and the reader stops rather than retries.
    """


def frame_timestamp_ns(header: bytes) -> int:
    """When a framed record was written, read from its header alone.

    The header is never sealed, so this needs neither the key nor the rest of the record: a consumer
    choosing the oldest record waiting across partitions reads a few dozen bytes of each, not the pages.
    """
    _offset, _crc, stamp, _key_len, _value_len, _headers_len = _HEADER.unpack_from(header, 0)
    return int(stamp)


@dataclass(frozen=True, slots=True)
class Record:
    """One event. `offset` is assigned by the log on append, and is meaningless before that."""

    value: bytes
    key: bytes | None = None
    headers: dict[str, str] = field(default_factory=dict)
    offset: int = -1
    timestamp_ns: int = 0

    def framed_size(self) -> int:
        """How many bytes `framed` will produce, without producing them."""
        headers = json.dumps(self.headers, separators=(",", ":")).encode() if self.headers else b""
        return HEADER_BYTES + len(self.key or b"") + len(self.value) + len(headers)

    def framed(self, offset: int, seal: Sealer | None = None) -> bytes:
        """This record as the log stores it, stamped with the offset it is being given.

        With a `seal`, the key, value and headers are sealed together, bound to the offset, and stored as
        the frame's value; the frame header stays readable (`sealing.py`). Without one — a record crossing
        the socket to a client — they are stored as they are.
        """
        key = self.key or b""
        headers = json.dumps(self.headers, separators=(",", ":")).encode() if self.headers else b""
        stamp = self.timestamp_ns or time.time_ns()
        offset_bytes = _OFFSET.pack(offset)
        if seal is not None:
            sealed = seal.seal(_INNER.pack(len(key), len(self.value), len(headers)) + key + self.value
                               + headers, offset_bytes)
            key, value, headers = b"", sealed, b""
        else:
            value = self.value
        checked = struct.pack("<QIII", stamp, len(key), len(value), len(headers)) + key + value + headers
        crc = zlib.crc32(checked, zlib.crc32(offset_bytes))
        return offset_bytes + struct.pack("<I", crc) + checked

    @classmethod
    def unframed(cls, buffer: memoryview | bytes, at: int = 0,
                 seal: Sealer | None = None) -> tuple[Self, int]:
        """Read one record starting at `at`, returning it and where the next one begins.

        Raises `TruncatedRecordError` when the buffer ends inside this record — which is the ordinary
        way a reader discovers it has caught up with the writer, not a fault — and `CorruptRecordError`
        when a whole record is there and fails its checksum, does not open under `seal`, or has headers
        that do not parse.
        """
        view = memoryview(buffer)
        if len(view) - at < HEADER_BYTES:
            raise TruncatedRecordError(f"{len(view) - at} bytes left, a header needs {HEADER_BYTES}")
        offset, crc, stamp, key_len, value_len, headers_len = _HEADER.unpack_from(view, at)
        start = at + HEADER_BYTES
        end = start + key_len + value_len + headers_len
        if len(view) < end:
            raise TruncatedRecordError(f"record at {at} wants {end - at} bytes, {len(view) - at} left")
        expected = zlib.crc32(view[at + _CHECKED_FROM:end], zlib.crc32(view[at:at + _OFFSET.size]))
        if crc != expected:
            raise CorruptRecordError(f"record at {at} fails its checksum", end=end)
        if seal is None:
            key = bytes(view[start:start + key_len]) or None
            value = bytes(view[start + key_len:start + key_len + value_len])
            raw_headers = bytes(view[start + key_len + value_len:end])
        else:
            key, value, raw_headers = cls._opened(view, at, start, end, key_len, headers_len, offset,
                                                  seal)
        try:
            headers = json.loads(raw_headers) if raw_headers else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as unreadable:
            raise CorruptRecordError(f"record at {at} has headers that do not parse",
                                     end=end) from unreadable
        return cls(value=value, key=key, headers=headers, offset=offset, timestamp_ns=stamp), end

    @staticmethod
    def _opened(view: memoryview, at: int, start: int, end: int, key_len: int, headers_len: int,
                offset: int, seal: Sealer) -> tuple[bytes | None, bytes, bytes]:
        """The key, value and headers of a sealed record, opened; corrupt when they do not open."""
        if key_len or headers_len:
            raise CorruptRecordError(f"record at {at} is not sealed", end=end)
        try:
            inner = seal.open(bytes(view[start:end]), _OFFSET.pack(offset))
        except ValueError as unopenable:
            raise CorruptRecordError(f"record at {at} does not open", end=end) from unopenable
        if len(inner) < _INNER.size:
            raise CorruptRecordError(f"record at {at} opens to too little", end=end)
        inner_key, inner_value, inner_headers = _INNER.unpack_from(inner)
        body = inner[_INNER.size:]
        if len(body) != inner_key + inner_value + inner_headers:
            raise CorruptRecordError(f"record at {at} opens to the wrong length", end=end)
        return (body[:inner_key] or None, body[inner_key:inner_key + inner_value],
                body[inner_key + inner_value:])
