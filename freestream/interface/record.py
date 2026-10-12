"""What a handler is handed: one record's key, value and headers, and nothing about where it was kept (#450).

A transport keeps far more than this about a record — its partition, its offset, when it was written, how
many times it has been started — and none of it belongs to the handler. A handler that could see its
partition would eventually make a decision from it, and the page reader must not be able to care which
partition a page came from. So the pipeline types its handlers against this Protocol, and each transport
hands over its own record type, which satisfies it by having these three attributes.
"""
from __future__ import annotations

from typing import Protocol


class StreamRecord(Protocol):
    """One record as its handler sees it."""

    #: The bytes the producer chose to group by, or None. On a KEYED channel every record carrying one
    #: key reaches one consumer; nothing else about the key is interpreted.
    @property
    def key(self) -> bytes | None: ...

    #: The record's content, exactly as it was produced.
    @property
    def value(self) -> bytes: ...

    #: Small strings ABOUT the record (which document, which page), readable by anything that can read
    #: the channel. A transport's own bookkeeping headers are removed before a handler sees them.
    @property
    def headers(self) -> dict[str, str]: ...
