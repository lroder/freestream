"""What the server can say about itself, as plain data — for the console and the web page alike (#450).

The console (`admin.py`) renders these as aligned tables for `nc`; the web page (`web.py`) sends them as
JSON. Both read the same functions, so a number means the same thing on either, and neither keeps a
count of its own: every figure is read from the server when it is asked for.
"""
from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .consumer_group import MemberState
from .record import STREAM_HEADER_PREFIX
from .registry import DEAD_SUFFIX, RETRY_SUFFIX

if TYPE_CHECKING:
    from .server import EventStreamServer

#: How many dead letters are listed per topic, newest last. The count given is all of them.
DEAD_LETTERS_SHOWN = 50


@dataclass(frozen=True, slots=True)
class EventStreamStatus:
    """Read-outs of one live server, and the few changes an operator may make to it.

    Every read is gathered from the strands by message (`EventStreamServer.gather`), since each strand
    alone touches its partitions and its consumers.
    """

    event_stream_server: EventStreamServer

    def info(self) -> dict[str, Any]:
        stats = self.event_stream_server.partition_stats()
        free_threaded = hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()
        return {
            "pid": os.getpid(),
            "uptime_seconds": round(time.time() - self.event_stream_server.started_at),
            "python": f"{sys.version.split()[0]} {'free-threaded' if free_threaded else 'GIL'}",
            "strands": len(self.event_stream_server.strands),
            "root": str(self.event_stream_server.root),
            "topics": sum(1 for name in stats if not name.endswith((DEAD_SUFFIX, RETRY_SUFFIX))),
            "connections": len(self.event_stream_server.connections()),
            "memberships": len(self.event_stream_server.memberships()),
            "bytes_held": sum(partition["bytes"] for partitions in stats.values()
                              for partition in partitions.values()),
        }

    def topics(self) -> list[dict[str, Any]]:
        """Every defined topic: its shape, what it holds, and how many dead letters it has."""
        stats = self.event_stream_server.partition_stats()
        rows = []
        for definition in self.event_stream_server.registry.all():
            if definition.is_derived:
                continue                    # counted in its topic's `dead` and `retrying`, not beside it
            partitions = stats.get(definition.name, {}).values()
            dead = stats.get(definition.name + DEAD_SUFFIX, {}).values()
            retrying = stats.get(definition.name + RETRY_SUFFIX, {}).values()
            rows.append({
                "name": definition.name,
                "delivery": str(definition.delivery),
                "partitions": definition.partitions,
                "records": sum(partition["next"] - partition["first"] for partition in partitions),
                "bytes": sum(partition["bytes"] for partition in partitions),
                "oldest": min((partition["first"] for partition in partitions), default=0),
                "segments": sum(partition["segments"] for partition in partitions),
                "dead": sum(partition["next"] for partition in dead),
                "retrying": sum(partition["next"] - partition["first"] for partition in retrying),
                "defined_at": definition.defined_at,
            })
        return rows

    def groups(self, channel: str | None = None) -> list[dict[str, Any]]:
        """Every group on disk or in use: its members, how many are READY, and how far behind it is."""
        states: dict[tuple[str, str], list[str]] = {}
        for definition in self.event_stream_server.registry.all():
            if channel is not None and definition.name != channel:
                continue
            home = self.event_stream_server.root / definition.name / "groups"
            for group_home in sorted(home.iterdir()) if home.is_dir() else []:
                states.setdefault((definition.name, group_home.name), [])
        for membership in self.event_stream_server.memberships():
            if channel is None or membership["channel"] == channel:
                states.setdefault((membership["channel"], membership["group"]), []).append(
                    membership["state"])
        rows = []
        for (held_channel, group), member_states in sorted(states.items()):
            lag = self.event_stream_server.lag(held_channel, group)
            rows.append({"channel": held_channel, "group": group, "members": len(member_states),
                         "ready": member_states.count(MemberState.READY), "lag": sum(lag.values()),
                         "partitions_behind": sum(1 for waiting in lag.values() if waiting),
                         "lag_by_partition": lag})
        return rows

    def consumers(self) -> list[dict[str, Any]]:
        """Every connection: who it said it is, its state, and each membership it holds."""
        held: dict[int, list[dict[str, Any]]] = {}
        for membership in self.event_stream_server.memberships():
            held.setdefault(membership["connection"], []).append({
                "channel": membership["channel"], "group": membership["group"],
                "partitions": membership["partitions"], "open_keys": membership["open_keys"],
                # `waiting` is a member holding a NEXT with nothing to hand it; `working` has a record.
                "doing": "waiting" if membership["waiting"] else "working",
            })
        return [{"id": seen.connection_id, "role": seen.role, "name": seen.name, "pid": seen.pid,
                 "host": seen.host, "state": seen.state,
                 "seen_seconds_ago": round(time.time() - seen.last_seen, 1),
                 "connected_at": seen.connected_at, "requests": seen.requests,
                 "memberships": held.get(seen.connection_id, [])}
                for seen in self.event_stream_server.connections()]

    def backlog(self, channel: str | None = None, *,
                groups: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        """Every partition with records a group has not committed, most behind first."""
        rows = [{"waiting": waiting, "channel": group["channel"], "group": group["group"],
                 "partition": partition}
                for group in (groups if groups is not None else self.groups(channel))
                for partition, waiting in group["lag_by_partition"].items() if waiting]
        return sorted(rows, key=lambda row: (-row["waiting"], row["channel"], row["group"],
                                             row["partition"]))

    def dead(self, channel: str | None = None) -> dict[str, Any]:
        """The records given up on, newest last, with how many attempts and whose they were.

        A record's own headers are shown as it carried them. The stream knows nothing of what its
        callers put there, so it names none of them; the `dead.` headers are its own.
        """
        letters, total = [], 0
        for definition in self.event_stream_server.registry.all():
            if not definition.is_dead_letters:
                continue
            source = definition.name[:-len(DEAD_SUFFIX)]
            if channel is not None and source != channel:
                continue
            records = self.event_stream_server.dead_letters(definition.name)
            total += len(records)
            letters += [{"channel": source, "offset": record.headers.get("dead.from_offset"),
                         "attempts": record.headers.get("dead.attempts"),
                         "group": record.headers.get("dead.group"),
                         "headers": {name: value for name, value in record.headers.items()
                                     if not name.startswith(("dead.", STREAM_HEADER_PREFIX))}}
                        for record in records[-DEAD_LETTERS_SHOWN:]]
        return {"total": total, "letters": letters}

    def snapshot(self) -> dict[str, Any]:
        """Everything the web page shows, in one answer — a view costs one read, however much it holds."""
        groups = self.groups()
        return {"info": self.info(), "topics": self.topics(), "groups": groups,
                "consumers": self.consumers(), "backlog": self.backlog(groups=groups),
                "states": list(MemberState.ALL)}

    # ------------------------------------------------------------------ changes

    def set_state(self, connection: int, state: str) -> None:
        self.event_stream_server.set_state(connection, state)

    def delete_group(self, channel: str, group: str) -> None:
        self.event_stream_server.delete_group(channel, group)

    def define_topic(self, name: str, partitions: int, delivery: str) -> None:
        self.event_stream_server.define_topic(name, partitions=partitions, delivery=delivery)

    def remove_topic(self, name: str) -> None:
        self.event_stream_server.remove_topic(name)
