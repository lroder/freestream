"""What a channel IS — declared once, by the people who own the hop (#450).

Our three hops are deliberately not alike, and a hop that needs nothing must be able to say so rather
than inherit machinery for someone else's problem:

    head of stream -> OCR readers    COMPETING   any consumer, any order, no key
    OCR readers    -> aggregator     KEYED       every record of one key to one consumer
    aggregator     -> broker         BROADCAST   every group sees everything, and may lose it

★ A DECLARATION IS A REQUIREMENT, NOT A HINT. The same ruling as `confidential` (2026-09-21): the
caller states what the hop needs and the machinery either meets it or REFUSES. A KEYED channel handed a
record with no key does not quietly spread it — it raises, because a record that lands in the wrong
partition is a document assembled by two consumers, which is not slower, it is wrong.

★ AND A BROADCAST CHANNEL MUST NOT BE AWAITABLE. This is the caveat the blueprint carries: one set of
utilities across all three hops is good, one set of GUARANTEES across them is not. A nudge we are happy
to lose must never be able to block a pipeline that is not — so `produce` on a broadcast channel
returns nothing to wait on, by construction rather than by convention.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Delivery(StrEnum):
    """How a channel's records reach consumers. The one axis every hop must state."""

    #: Any consumer may take any record; nothing is kept together. Work shared out.
    COMPETING = "competing"
    #: Every record carrying one key reaches ONE consumer. Affinity, not ordering across keys.
    KEYED = "keyed"
    #: Every consumer group sees every record, and none of them is promised delivery.
    BROADCAST = "broadcast"


class ChannelContractError(Exception):
    """A record does not satisfy what its channel declared — refused rather than sent anyway."""


@dataclass(frozen=True, slots=True)
class Channel:
    """One named stream and the shape it requires. Declared where the hop is, read everywhere else."""

    name: str
    delivery: Delivery
    #: More partitions is more consumers that can work in parallel; it is also the ceiling on that.
    #: Only meaningful for KEYED and COMPETING — a broadcast group reads all of them regardless.
    partitions: int = 8

    def __post_init__(self) -> None:
        if self.partitions < 1:
            raise ValueError(f"channel {self.name!r} needs at least one partition")

    @property
    def requires_key(self) -> bool:
        """Whether a record without a key is a contract violation rather than an ordinary record."""
        return self.delivery is Delivery.KEYED

    @property
    def refuses_key(self) -> bool:
        """Whether a key on a record of this channel is a contract violation.

        The mirror of `requires_key`, and it exists because I broke this channel's own declaration
        within an hour of writing it. COMPETING says "any consumer, any order, no key" — and the head
        published each page keyed by its document, so all 286 pages of a return hashed to one
        partition, reached one reader, and were read one at a time while seven readers sat idle. It
        cost nothing to detect and was invisible until I looked at which offset files existed.

        A key is not a hint here either: on a channel whose whole point is that work is shared out,
        supplying one silently un-shares it.
        """
        return self.delivery is Delivery.COMPETING

    @property
    def keeps_its_partitions(self) -> bool:
        """Whether a consumer of this channel must hold on to what it was assigned.

        A KEYED channel has documents mid-assembly behind its partitions, so letting one go would hand
        a half-read document to somebody else. A COMPETING channel has no affinity at all, so a
        consumer holding partitions it is not using should let them go and let a reader that started
        later have them — which is not a rebalance, because nothing is taken from anyone.
        """
        return self.delivery is not Delivery.COMPETING

    @property
    def is_droppable(self) -> bool:
        """Whether losing a record is acceptable — and so whether anything may wait on delivery."""
        return self.delivery is Delivery.BROADCAST

    def check_key(self, key: str | None) -> None:
        """Refuse a record whose key breaks this channel's declaration, before any transport sees it.

        Checked here, on the producing side, because this is the last place the caller's intent is still
        visible. A transport sees a key or no key and does the only thing it can with either, so a
        keyless record on a KEYED channel would land wherever the transport put it, and the document it
        belongs to would be assembled by two consumers.
        """
        if self.requires_key and key is None:
            raise ChannelContractError(
                f"channel {self.name!r} is {self.delivery} and this record has no key — a record that "
                f"lands in the wrong partition is a document assembled twice")
        if self.refuses_key and key is not None:
            raise ChannelContractError(
                f"channel {self.name!r} is {self.delivery} and this record has a key — every record "
                f"sharing it would reach one consumer, and this channel exists to share its work out")
