"""The topics the server knows about, defined before anyone produces to them (#450).

    registry = TopicRegistry(root)
    registry.define("pages.to_read", partitions=64, delivery=Delivery.COMPETING)
    registry.get("pages.to_read")        # -> its definition, or UnknownTopicError

★ A TOPIC EXISTS BECAUSE SOMEBODY DEFINED IT, NOT BECAUSE SOMEBODY NAMED IT (2026-09-24). The server
used to create a topic the first time any request mentioned one — Kafka's `auto.create.topics.enable`,
which Kafka shops turn off in production for the reasons it bit here: a consumer subscribed to a typo
waited forever on a topic nobody would ever write to, the width came from whichever request happened to
arrive first, and every consumer told the server on every request whether it kept its partitions. Now a
topic is defined once, its definition lives with its data, and a consumer that arrives before any
producer comes online and waits, because the topic is known to exist.

Definitions come from two places, and both end here: the deploy step that ensures the topics the code
declares (`EventStreamTransport.define`, which a product runs on every deploy), and an operator on the
server's web page or console.

★ A NAME BECOMES A DIRECTORY, so it is checked before it is used. Until this existed a topic name from
any client was joined onto the log root, and `../` in one would have written outside it.

★ WIDTH AND DELIVERY ARE FIXED ONCE DEFINED. On a keyed topic the key decides the partition, so a
different width sends a document's pages somewhere else and two consumers assemble one return. When
somebody defines a topic again with the same shape, the registry changes nothing — which is what makes
the deploy step safe to run every time — and it refuses a different shape.

★ A DEAD-LETTER TOPIC IS DERIVED, not defined: `<topic>.dead`, one partition, for every defined topic.
A consumer that gives up on a record must always have somewhere to put it.

★ SO IS A RETRY TOPIC: `<topic>.retry`, COMPETING, as wide as its topic (2026-10-12). A handler that raises
`RetryLater` has its record written here with the time it may run again, and the server hands a record
of a retry topic out only once that time has passed, while the records behind it go on being handed
out (`ConsumerGroup`, `due_by_header`). It is as wide as its topic so that retries spread over the group
as the first attempts did; a retry topic nobody writes to is never opened and costs nothing.
"""
from __future__ import annotations

import json
import re
import shutil
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from .durability import flush_now
from .interface.channel import Delivery

#: What a topic name may be: lower-case words joined by dots, dashes or underscores. It becomes a
#: directory name, so nothing that could climb out of the log root, and nothing a shell would mangle.
_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*(\.[a-z0-9][a-z0-9_-]*)*$")

#: The most partitions a topic may have. Each is a directory and a lock file; this is a sanity bound on
#: an operator's typo, not a tuning knob.
MAX_PARTITIONS = 1024

DEAD_SUFFIX = ".dead"
RETRY_SUFFIX = ".retry"
#: The suffixes a topic's derived topics carry, which no defined topic may end in.
_DERIVED_SUFFIXES = (DEAD_SUFFIX, RETRY_SUFFIX)
_DEFINITION_FILE = "topic.json"


class TopicError(Exception):
    """A request about topics that cannot be carried out, with the reason in words."""


class UnknownTopicError(TopicError):
    """Nobody has defined a topic by that name."""


class TopicConflictError(TopicError):
    """A topic by that name exists with a different shape."""


class RequestRefusedError(TopicError):
    """A request the server will not carry out, answered with the reason rather than a dropped line.

    A commit past what the member was handed, a record that breaks its topic's key rule, a group name
    that is not a name: each is the caller's mistake, and the caller is told which. A partition the
    member no longer holds has a subclass of its own, because a reconnect causes it without any mistake.
    """


class PartitionNotHeldError(RequestRefusedError):
    """The member asked about a partition it does not hold — most often one it held until a reconnect.

    A consumer that reconnects, after the server restarted or its connection dropped, is a new member
    that holds nothing, while the record it was handed on the old connection may still be running. Its
    commit or its attempt is refused with this, and the record goes to whichever member holds the
    partition now: Kafka's `CommitFailedException` after a rebalance, and the same at-least-once answer.
    """


def is_valid_name(name: str) -> bool:
    """Whether a name may become a directory under the log: a topic's, or a consumer group's."""
    return bool(_NAME.match(name))


@dataclass(frozen=True, slots=True)
class TopicDefinition:
    """What a topic is: its name, how many partitions, and how its records reach consumers."""

    name: str
    partitions: int
    delivery: Delivery
    defined_at: float = 0.0

    @property
    def keeps_its_partitions(self) -> bool:
        """Whether a consumer must hold what it was given — anything but COMPETING. See `Channel`."""
        return self.delivery is not Delivery.COMPETING

    @property
    def is_dead_letters(self) -> bool:
        return self.name.endswith(DEAD_SUFFIX)

    @property
    def is_retries(self) -> bool:
        """Whether this is a topic's retry topic, whose records are handed out only once they are due."""
        return self.name.endswith(RETRY_SUFFIX)

    @property
    def is_derived(self) -> bool:
        """Whether this topic exists because another was defined: its dead letters or its retries."""
        return self.is_dead_letters or self.is_retries


class TopicRegistry:
    """Every defined topic, read from the log root at startup and written through on every change.

    ★ READ BY EVERY STRAND, CHANGED ONLY BY THE MAIN THREAD, AND NEVER IN PLACE. A definition changes when
    an operator or deploy defines or removes a topic, on the thread that runs the consoles; each change
    builds a new table and replaces the old one whole, so a strand reading it sees one table or the other,
    never one being changed. A dead-letter or retry topic's definition is derived when it is asked for, not
    written, so no read changes anything.
    """

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._defined: dict[str, TopicDefinition] = {}
        for definition_file in sorted(self._root.glob(f"*/{_DEFINITION_FILE}")):
            fields = json.loads(definition_file.read_text())
            definition = TopicDefinition(name=fields["name"], partitions=int(fields["partitions"]),
                                         delivery=Delivery(fields["delivery"]),
                                         defined_at=float(fields.get("defined_at", 0.0)))
            self._defined[definition.name] = definition

    def get(self, name: str) -> TopicDefinition:
        """The topic's definition. A defined topic's dead-letter and retry topics are derived from it."""
        defined = self._defined
        if name in defined:
            return defined[name]
        if name.endswith(DEAD_SUFFIX) and (base := defined.get(name[:-len(DEAD_SUFFIX)])) is not None:
            return TopicDefinition(name, 1, Delivery.COMPETING, base.defined_at)
        if name.endswith(RETRY_SUFFIX) and (base := defined.get(name[:-len(RETRY_SUFFIX)])) is not None:
            return TopicDefinition(name, base.partitions, Delivery.COMPETING, base.defined_at)
        raise UnknownTopicError(f"no topic named {name!r} has been defined")

    def all(self) -> list[TopicDefinition]:
        """Every defined topic, and the dead-letter and retry topics each has."""
        defined = self._defined
        derived = {name + suffix: self.get(name + suffix)
                   for name in defined for suffix in _DERIVED_SUFFIXES}
        return sorted({**derived, **defined}.values(), key=lambda definition: definition.name)

    def define(self, name: str, *, partitions: int, delivery: Delivery | str) -> TopicDefinition:
        """Define a topic, or confirm one already defined with this shape. Refuses a different shape."""
        delivery = Delivery(delivery)
        if not _NAME.fullmatch(name) or len(name) > 120:
            raise TopicError(f"{name!r} is not a topic name: lower-case letters, digits, '-' and '_', "
                             f"in words joined by '.'")
        for suffix in _DERIVED_SUFFIXES:
            if name.endswith(suffix):
                raise TopicError(f"{name!r} ends in {suffix!r}, which is kept for a topic's derived topics")
        if not 1 <= partitions <= MAX_PARTITIONS:
            raise TopicError(f"a topic has between 1 and {MAX_PARTITIONS} partitions, not {partitions}")
        existing = self._defined.get(name)
        if existing is not None:
            if (existing.partitions, existing.delivery) != (partitions, delivery):
                raise TopicConflictError(
                    f"{name!r} is already {existing.delivery} with {existing.partitions} partition(s); "
                    f"a topic's width and delivery cannot change under the data it holds")
            return existing
        on_disk = self._root / name
        widths = [child for child in on_disk.iterdir() if child.is_dir() and child.name.isdigit()] \
            if on_disk.is_dir() else []
        if widths and len(widths) != partitions:
            raise TopicConflictError(
                f"{name!r} already has {len(widths)} partition(s) on disk from before topics were "
                f"defined; define it with that width")
        return self._write(TopicDefinition(name, partitions, delivery, time.time()))

    def forget(self, name: str) -> TopicDefinition:
        """Stop defining a topic, its dead letters and its retries, and return what it was. Deletes nothing.

        The first step of removing a topic (`EventStreamServer.remove_topic`): from here on a produce or a
        NEXT that names it is refused, so nothing reopens its partitions while they are being deleted.
        """
        if name not in self._defined:
            raise UnknownTopicError(f"no topic named {name!r} has been defined")
        definition = self._defined[name]
        self._defined = {kept: kept_definition for kept, kept_definition in self._defined.items()
                         if kept not in (name, *(name + suffix for suffix in _DERIVED_SUFFIXES))}
        return definition

    def remove(self, name: str) -> None:
        """Forget a topic and delete all it holds — for a tool on one thread, with no server running."""
        self.forget(name)
        for doomed in (name, *(name + suffix for suffix in _DERIVED_SUFFIXES)):
            shutil.rmtree(self._root / doomed, ignore_errors=True)

    def _write(self, definition: TopicDefinition) -> TopicDefinition:
        home = self._root / definition.name
        home.mkdir(parents=True, exist_ok=True)
        staged = home / (_DEFINITION_FILE + ".writing")
        staged.write_text(json.dumps({**asdict(definition), "delivery": str(definition.delivery)},
                                     indent=2))
        staged.rename(home / _DEFINITION_FILE)
        flush_now([home / _DEFINITION_FILE], [home, self._root])
        self._defined = {**self._defined, definition.name: definition}
        return definition
