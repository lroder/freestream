"""Run the event stream server as its own process.

    python -m freestream --socket var/eventstream.sock --root var/eventstream \
        --admin-socket var/eventstream-admin.sock --web-port 8260

★ ITS OWN INTERPRETER, AND THAT IS THE POINT (2026-09-22). Everything this module imports, all
the way down, is one of our own modules, the standard library, or `cryptography` — which seals what the
server writes and ships a free-threaded wheel — so it runs on free-threaded Python (3.14t) even where the
product around it cannot: where FreeStream was built, a PDF library re-enables the GIL on import, so the
API and the OCR workers keep the lock and the server is the one component nothing blocks.

★ AND THE SERVER USES IT: it is fully multithreaded (2026-09-25), running its work on strands, one
thread each with its own loop, each owning a share of the partitions (`strand.py`). It ran on 3.14t for
days as a single asyncio loop that used one core, until I found it had never been built to use more.
`tests/test_eventstream_multithreaded.py` measures it against this interpreter on every CI run.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import sysconfig

from .sealing import load_key
from .server import DEFAULT_STRANDS, EventStreamServer

#: What a log line looks like as text: when, how bad, who, and what.
_TEXT_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


def configure_logging(level: str = "INFO", log_format: str = "text") -> None:
    """Send log lines to stdout at `level`, as text or as one JSON object per line."""
    name = level.upper()
    if name not in logging.getLevelNamesMapping():
        raise ValueError(f"LOG_LEVEL must be a logging level name (got {level!r})")
    if log_format not in ("text", "json"):
        raise ValueError(f"LOG_FORMAT must be text or json (got {log_format!r})")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter() if log_format == "json" else logging.Formatter(_TEXT_FORMAT))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(name)


class _JsonFormatter(logging.Formatter):
    """One JSON object per line, for a log collector."""

    def format(self, record: logging.LogRecord) -> str:
        line = {"time": self.formatTime(record), "level": record.levelname, "logger": record.name,
                "message": record.getMessage()}
        if record.exc_info:
            line["exception"] = self.formatException(record.exc_info)
        return json.dumps(line)


def runs_free_threaded() -> bool:
    """Whether this interpreter is a free-threaded build with the GIL off, after every import."""
    gil_enabled = getattr(sys, "_is_gil_enabled", lambda: True)   # before 3.13 there is only the GIL
    return bool(sysconfig.get_config_var("Py_GIL_DISABLED")) and not gil_enabled()


#: The variable that sets the strand count, from a product's development environment or a systemd unit's
#: drop-in in production. The server reads this one variable and never a product's .env, which holds
#: settings it has no use for.
STRANDS_VARIABLE = "EVENTSTREAM_STRANDS"


def strands_from_the_environment() -> int | None:
    """The strand count `EVENTSTREAM_STRANDS` sets, or None when it is unset or blank."""
    value = os.environ.get(STRANDS_VARIABLE, "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        raise SystemExit(f"refused: {STRANDS_VARIABLE}={value!r} is not a whole number of strands") from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="eventstream", description=__doc__)
    parser.add_argument("--socket", required=True, help="unix socket to listen on")
    parser.add_argument("--root", required=True, help="directory the logs live under")
    parser.add_argument("--admin-socket", default=None,
                        help="unix socket for the operator's console (`echo help | nc -U <path>`)")
    parser.add_argument("--web-port", type=int, default=None,
                        help="loopback port for the web page (http://127.0.0.1:<port>/)")
    parser.add_argument("--key-file", required=True,
                        help="the key everything written to disk is sealed under "
                             "(make one: python -m freestream.sealing <path>)")
    parser.add_argument("--strands", type=int, default=None,
                        help="how many strands (threads) run the work; at least two (default "
                             f"${STRANDS_VARIABLE}, or {DEFAULT_STRANDS} when that is not set)")
    args = parser.parse_args(argv)
    # The server reads its log settings from the environment, as it reads its strands: the unit sets
    # LOG_LEVEL, and the server reads no product's settings.
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"), os.environ.get("LOG_FORMAT", "text"))
    strands = args.strands if args.strands is not None else strands_from_the_environment()
    if strands is not None and strands < 2:
        print(f"refused: the event stream server runs at least two strands, and was given {strands}",
              file=sys.stderr, flush=True)
        return 2

    free_threaded = runs_free_threaded()
    print(f"event stream server · python {sys.version.split()[0]} · "
          f"{'free-threaded' if free_threaded else 'GIL'}", file=sys.stderr, flush=True)
    # ★ FULLY MULTITHREADED, OR NOT AT ALL (2026-09-25). A server with the GIL on runs its strands one
    # at a time on one core, which is the failure the requirement exists to rule out, and it would do so
    # quietly: pointed at the API's 3.12 interpreter it imports and serves as if nothing were wrong. And on a
    # free-threaded build, a C extension that has not declared support turns the GIL back on when imported.
    # Everything the server imports is imported by now, so either way a GIL that is on here is refused.
    if not free_threaded:
        print("refused: the event stream server must run free-threaded, with the GIL off "
              "(build its interpreter: uv venv --python 3.14t .venv-3.14t)",
              file=sys.stderr, flush=True)
        return 2

    event_stream_server = EventStreamServer(args.root, strands=strands,
                                            key=load_key(args.key_file))
    try:
        asyncio.run(event_stream_server.serve(args.socket, admin_socket=args.admin_socket,
                                 web_port=args.web_port))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
