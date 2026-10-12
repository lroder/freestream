"""The server's console: plain text over its own socket, in HAProxy's vocabulary (#450).

    echo "show consumers" | nc -U /var/lib/freestream/eventstream-admin.sock
    echo "show backlog; show dead" | socat stdio unix-connect:/var/lib/freestream/eventstream-admin.sock
    echo "set consumer 7 state drain" | nc -U /var/lib/freestream/eventstream-admin.sock
    echo "create topic document.bakes 8 competing" | nc -U /var/lib/freestream/eventstream-admin.sock

★ MODELED ON HAPROXY'S RUNTIME API BECAUSE LEE RUNS ONE. Its stats socket lets an operator see every
frontend and backend and drain, disable or re-enable a server without a restart, and everything it
does is a line of text a person can type. This is the same shape for the event stream: `show …` to
look, `set consumer … state` to drain one or take it out of service, several commands on a line
separated by `;`, and each answer ends with a blank line so a script can tell where one stops. The web
page (`web.py`) offers the same read-outs and the same changes, for a browser.

★ ITS OWN SOCKET. The data socket speaks length-prefixed frames that no person should have to type, and
nothing speaking frames should ever reach a command that changes what a consumer is given. Two
sockets keep the two audiences from meeting. Access is the socket file's permissions, as it is for the
data socket.

★ THE TABLES ARE WHITESPACE-ALIGNED WITH A `#` HEADER, like HAProxy's `show servers state`, so they read
cleanly by eye and still split cleanly with `awk`. Every figure comes from `status.py`, read from the
server when the command runs.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from .consumer_group import MemberState
from .registry import TopicError
from .status import EventStreamStatus

if TYPE_CHECKING:
    from .server import EventStreamServer

log = logging.getLogger(__name__)

#: How long the console waits for a line before it hangs up — `nc -U` left open with nothing typed.
CONSOLE_IDLE_SECONDS = 10.0

HELP = """\
show info                              the server: uptime, interpreter, topics, connections, bytes held
show topics                            each topic: delivery, partitions, records, bytes, dead letters
show groups [topic]                    each group: members, how many are ready, how far behind it is
show consumers                         each connection: who, its state, waiting or working, what it holds
show backlog [topic]                   each partition with records waiting, most behind first
show dead [topic]                      records given up on, with how many attempts and whose they were
create topic <name> <n> <delivery>     define a topic: n partitions, competing | keyed | broadcast
del topic <name>                       forget a topic and all it holds; refused while it has members
set consumer <id> state <state>        ready · drain (finish what it has, take nothing new) · maint
del group <topic> <group>              forget a retired group, so it stops holding segments
help                                   this
several commands may share a line, separated by ';'"""


def _table(header: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """A `#` header and whitespace-aligned rows — readable by eye, splittable by `awk`."""
    cells = [[str(cell) for cell in row] for row in rows]
    widths = [max([len(header[column]) + (2 if column == 0 else 0)]
                  + [len(row[column]) for row in cells]) for column in range(len(header))]
    lines = ["# " + "  ".join(name.ljust(widths[column] - (2 if column == 0 else 0))
                              for column, name in enumerate(header)).rstrip()]
    lines += ["  ".join(cell.ljust(widths[column]) for column, cell in enumerate(row)).rstrip()
              for row in cells]
    return "\n".join(lines)


def _ago(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


def _bytes(count: int) -> str:
    if count < 1024:
        return f"{count}B"
    scaled = count / 1024
    for unit in ("KB", "MB"):
        if scaled < 1024:
            return f"{scaled:.1f}{unit}"
        scaled /= 1024
    return f"{scaled:.1f}GB"


class AdminConsole:
    """Answers one line of commands against a live server."""

    def __init__(self, event_stream_server: EventStreamServer) -> None:
        self._status = EventStreamStatus(event_stream_server)
        self._commands: dict[tuple[str, ...], Callable[[list[str]], str]] = {
            ("help",): lambda _args: HELP,
            ("show", "info"): self._show_info,
            ("show", "topics"): self._show_topics,
            ("show", "groups"): self._show_groups,
            ("show", "consumers"): self._show_consumers,
            ("show", "backlog"): self._show_backlog,
            ("show", "dead"): self._show_dead,
            ("create", "topic"): self._create_topic,
            ("del", "topic"): self._del_topic,
            ("set", "consumer"): self._set_consumer,
            ("del", "group"): self._del_group,
        }

    def execute(self, line: str) -> str:
        """Every command on the line, each answer followed by a blank line."""
        answers = []
        for command in (part.strip() for part in line.split(";")):
            if command:
                answers.append(self._one(command.split()))
        return "".join(answer.rstrip("\n") + "\n\n" for answer in answers)

    def _one(self, words: list[str]) -> str:
        for length in (2, 1):
            handler = self._commands.get(tuple(word.lower() for word in words[:length]))
            if handler is not None:
                try:
                    return handler(words[length:])
                except (ValueError, KeyError, RuntimeError, TopicError) as refused:
                    return f"refused: {refused}"
        return f"unknown command: {' '.join(words)} — try 'help'"

    # ------------------------------------------------------------------ show

    def _show_info(self, _args: list[str]) -> str:
        info = self._status.info()
        rows = [("pid", info["pid"]), ("uptime", _ago(info["uptime_seconds"])),
                ("python", info["python"]), ("strands", info["strands"]), ("root", info["root"]),
                ("topics", info["topics"]),
                ("connections", info["connections"]), ("memberships", info["memberships"]),
                ("bytes held", _bytes(info["bytes_held"]))]
        return _table(("field", "value"), rows)

    def _show_topics(self, _args: list[str]) -> str:
        rows = [(topic["name"], topic["delivery"], topic["partitions"], topic["records"],
                 _bytes(topic["bytes"]), topic["oldest"], topic["segments"], topic["dead"])
                for topic in self._status.topics()]
        if not rows:
            return "no topics defined — create one with 'create topic <name> <n> <delivery>'"
        return _table(("topic", "delivery", "partitions", "records", "bytes", "oldest", "segments",
                       "dead"), rows)

    def _show_groups(self, args: list[str]) -> str:
        rows = [(group["channel"], group["group"], group["members"], group["ready"], group["lag"],
                 group["partitions_behind"])
                for group in self._status.groups(args[0] if args else None)]
        # `ready` is the number the API asks for before it publishes a scan: zero at either hop, and
        # the scan reads in-process.
        return _table(("topic", "group", "members", "ready", "lag", "partitions behind"), rows)

    def _show_consumers(self, _args: list[str]) -> str:
        rows = []
        for seen in self._status.consumers():
            memberships = seen["memberships"] or [
                {"channel": "-", "group": "-", "doing": "-", "partitions": [], "open_keys": 0}]
            for held in memberships:
                rows.append((seen["id"], seen["role"], seen["pid"] or "-", seen["state"],
                             held["doing"], held["channel"], held["group"],
                             ",".join(str(p) for p in held["partitions"]) or "-", held["open_keys"],
                             _ago(seen["seen_seconds_ago"]), seen["requests"], seen["name"] or "-"))
        return _table(("id", "role", "pid", "state", "doing", "topic", "group", "partitions",
                       "open keys", "seen", "requests", "name"), rows)

    def _show_backlog(self, args: list[str]) -> str:
        rows = [(row["waiting"], row["channel"], row["group"], row["partition"])
                for row in self._status.backlog(args[0] if args else None)]
        return _table(("waiting", "topic", "group", "partition"), rows) if rows else "no backlog"

    def _show_dead(self, args: list[str]) -> str:
        dead = self._status.dead(args[0] if args else None)
        if not dead["letters"]:
            return "no dead letters"
        rows = [(letter["channel"], letter["offset"] or "-", letter["attempts"] or "-",
                 letter["group"] or "-",
                 " ".join(f"{name}={value}" for name, value in letter["headers"].items()) or "-")
                for letter in dead["letters"]]
        return _table(("topic", "offset", "attempts", "group", "headers"), rows) + \
            f"\n# {dead['total']} dead letter(s) in all"

    # ------------------------------------------------------------------ change

    def _create_topic(self, args: list[str]) -> str:
        if len(args) != 3:
            raise ValueError("usage: create topic <name> <partitions> <competing|keyed|broadcast>")
        self._status.define_topic(args[0], int(args[1]), args[2].lower())
        return f"topic {args[0]} is defined: {args[2].lower()}, {args[1]} partition(s)"

    def _del_topic(self, args: list[str]) -> str:
        if len(args) != 1:
            raise ValueError("usage: del topic <name>")
        self._status.remove_topic(args[0])
        return f"topic {args[0]} removed, with everything it held"

    def _set_consumer(self, args: list[str]) -> str:
        if len(args) != 3 or args[1].lower() != "state":
            raise ValueError("usage: set consumer <id> state ready|drain|maint")
        connection, state = int(args[0]), args[2].lower()
        self._status.set_state(connection, state)
        log.warning("admin: consumer %d set to %s", connection, state)
        what = {MemberState.READY: "handed records as usual",
                MemberState.DRAIN: "finishes what it has, then lets go",
                MemberState.MAINT: "handed nothing; lets its partitions go when it next asks"}[state]
        return f"consumer {connection} is {state} — {what}"

    def _del_group(self, args: list[str]) -> str:
        if len(args) != 2:
            raise ValueError("usage: del group <topic> <group>")
        self._status.delete_group(args[0], args[1])
        log.warning("admin: group %s/%s deleted", args[0], args[1])
        return f"group {args[0]}/{args[1]} deleted — it no longer holds any segment"


async def serve_admin(event_stream_server: EventStreamServer,
                      socket_path: str | Path) -> asyncio.base_events.Server:
    """Listen for console connections. Each connection is one line of commands and its answer."""
    from .server import _refuse_a_path_the_kernel_cannot_hold

    path = Path(socket_path)
    _refuse_a_path_the_kernel_cannot_hold(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    console = AdminConsole(event_stream_server)

    async def answer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=CONSOLE_IDLE_SECONDS)
            writer.write(console.execute(line.decode(errors="replace")).encode())
            await writer.drain()
        except (TimeoutError, ConnectionResetError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionResetError, BrokenPipeError):
                await writer.wait_closed()

    server = await asyncio.start_unix_server(answer, path=str(path))
    log.info("event stream console listening on %s", path)
    return server
