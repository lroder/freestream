"""The journal that makes a COMPLETE happen once: its commit, and every output landing exactly once (#450).

A COMPLETE is a consumer saying, in one request, "these are the records my handler produced, and the
record it handled is done." The server has to append each output to its topic and move the group past
the input, and those are several writes to several files. A server killed between them would leave
some outputs written and the input not committed: the input is handed out again, the handler runs
again, and its outputs go out a second time, which is the duplicate COMPLETE exists to prevent.

So the server writes the whole COMPLETE into one journal file first, by rename, and that rename is the
point at which it happened. Then it applies the entry and deletes the file. A server that starts and
finds an entry applies it again, and the second application changes nothing the first one made:

  · each output is appended with `Topic.produce_once` under the producer id `complete.<entry id>` and
    its position in the entry as the sequence, so an output the first application already wrote is
    recognized in the log and not written again;
  · the commit is written as the effect the member worked out (`CommitPlan`), not the offset it asked
    for, because what a DRAINING member skipped lives only in that member, which is gone by the time a
    replay runs; and a plan behind where the group already is changes nothing (`write_commit`).

    <root>/.journal/<entry id>.complete     one entry: an 8-byte length, a JSON header, the output bodies

The directory name starts with a dot, which no topic name may (`registry.is_valid_name`), so it can
never collide with a topic.
"""
from __future__ import annotations

import json
import struct
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from .consumer_group import CommitPlan
from .durability import mark_written
from .record import Record, Sealer
from .topic import JOURNAL_PRODUCER_PREFIX

JOURNAL_DIRECTORY = ".journal"
_LENGTH = struct.Struct("<Q")


@dataclass(frozen=True, slots=True)
class JournalOutput:
    """One record a COMPLETE appends, and the topic it goes to."""

    channel: str
    record: Record


@dataclass(frozen=True, slots=True)
class JournalEntry:
    """Everything one COMPLETE does: the outputs to append, then the commit that finishes its input."""

    entry_id: str
    channel: str
    group: str
    plan: CommitPlan
    outputs: tuple[JournalOutput, ...]
    #: output number -> (partition, offset) for each output already appended. Written into the entry as each
    #: lands (`EventStreamServer.append_outputs`), so finishing the entry later — a retry, or the replay after
    #: a crash — skips what landed without reading it back from a log that retention may have trimmed.
    landed: dict[int, tuple[int, int]] = field(default_factory=dict)

    @classmethod
    def new(cls, channel: str, group: str, plan: CommitPlan,
            outputs: tuple[JournalOutput, ...]) -> JournalEntry:
        return cls(uuid.uuid4().hex, channel, group, plan, outputs)

    @property
    def producer(self) -> str:
        """The producer id this entry's outputs are appended under, the same on every replay."""
        return f"{JOURNAL_PRODUCER_PREFIX}{self.entry_id}"

    def encoded(self) -> bytes:
        header = {
            "entry_id": self.entry_id, "channel": self.channel, "group": self.group,
            "partition": self.plan.partition, "position": self.plan.position,
            "acked": sorted(self.plan.acked),
            "landed": {str(number): list(placement) for number, placement in self.landed.items()},
            "outputs": [{"channel": output.channel,
                         "key": output.record.key.decode() if output.record.key is not None else None,
                         "headers": output.record.headers, "length": len(output.record.value)}
                        for output in self.outputs]}
        head = json.dumps(header, separators=(",", ":")).encode()
        return _LENGTH.pack(len(head)) + head + b"".join(output.record.value for output in self.outputs)

    @classmethod
    def decoded(cls, data: bytes) -> JournalEntry:
        (head_length,) = _LENGTH.unpack_from(data)
        header = json.loads(data[_LENGTH.size:_LENGTH.size + head_length])
        at = _LENGTH.size + head_length
        outputs = []
        for described in header["outputs"]:
            value = data[at:at + described["length"]]
            at += described["length"]
            key = described["key"]
            outputs.append(JournalOutput(described["channel"], Record(
                value=value, key=key.encode() if key is not None else None,
                headers=described["headers"])))
        plan = CommitPlan(header["partition"], header["position"], frozenset(header["acked"]))
        landed = {int(number): (placement[0], placement[1])
                  for number, placement in header.get("landed", {}).items()}
        return cls(header["entry_id"], header["channel"], header["group"], plan, tuple(outputs), landed)


#: What a sealed journal entry is bound to, so a sealed record could never be taken for one.
_JOURNAL_BINDING = b"freestream journal entry"


def write_entry(root: Path, entry: JournalEntry, seal: Sealer | None) -> Path:
    """Write an entry by rename, which is the moment its COMPLETE happened. Returns its path.

    Sealed whole when the server has a seal: an entry carries the outputs' bodies, which are pages.
    """
    directory = root / JOURNAL_DIRECTORY
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{entry.entry_id}.complete"
    staged = path.with_suffix(".writing")
    encoded = entry.encoded()
    staged.write_bytes(seal.seal(encoded, _JOURNAL_BINDING) if seal is not None else encoded)
    staged.rename(path)
    mark_written(path, directory_changed=True)
    return path


def pending_entries(root: Path, seal: Sealer | None) -> list[tuple[Path, JournalEntry]]:
    """Every entry written and not yet applied — what a server that was killed left behind.

    A `.writing` file is an entry the server was killed while staging: its COMPLETE never happened, the
    consumer was never answered, and it is deleted.
    """
    directory = root / JOURNAL_DIRECTORY
    if not directory.is_dir():
        return []
    for staged in directory.glob("*.writing"):
        staged.unlink()
    entries = []
    for path in sorted(directory.glob("*.complete")):
        stored = path.read_bytes()
        entries.append((path, JournalEntry.decoded(
            seal.open(stored, _JOURNAL_BINDING) if seal is not None else stored)))
    return entries
