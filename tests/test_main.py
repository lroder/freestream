"""The server's entry point: what `python -m freestream` builds and runs.

It is the only code that decides what a deployed server IS — its socket, its log, its console, how many
strands it runs — and a systemd unit is its one caller, so nothing else would catch a wrong default here.
"""
import pytest

from freestream import __main__ as broker_entry
from freestream.sealing import new_key_file
from freestream.server import EventStreamServer

# --------------------------------------------------------------------------- the server process


def test_the_broker_serves_what_it_was_given(monkeypatch, tmp_path):
    """The socket, the root, the console and the web page — and no width: a topic's width is its own."""
    built = {}

    class _Recording(EventStreamServer):
        def __init__(self, root, **options):
            built["root"] = root
            super().__init__(root, **options)

        async def serve(self, socket_path, **options):
            built["socket"], built["options"] = socket_path, options

    monkeypatch.setattr(broker_entry, "EventStreamServer", _Recording)
    monkeypatch.setattr(broker_entry, "runs_free_threaded", lambda: True)
    new_key_file(tmp_path / "es.key")
    assert broker_entry.main(["--socket", str(tmp_path / "es.sock"), "--root", str(tmp_path / "log"),
                              "--key-file", str(tmp_path / "es.key"),
                              "--admin-socket", str(tmp_path / "admin.sock"), "--web-port", "8260"]) == 0
    assert built == {"root": str(tmp_path / "log"), "socket": str(tmp_path / "es.sock"),
                     "options": {"admin_socket": str(tmp_path / "admin.sock"), "web_port": 8260}}


def _strands_it_ran(monkeypatch, tmp_path, *arguments):
    """How many strands `main` gave the server, with the server's serving stubbed out."""
    built = {}

    class _Counting(EventStreamServer):
        async def serve(self, socket_path, **options):
            built["strands"] = len(self.strands)

    monkeypatch.setattr(broker_entry, "EventStreamServer", _Counting)
    monkeypatch.setattr(broker_entry, "runs_free_threaded", lambda: True)
    new_key_file(tmp_path / "k")
    status = broker_entry.main(["--socket", str(tmp_path / "s"), "--root", str(tmp_path / "l"),
                                "--key-file", str(tmp_path / "k"), *arguments])
    return status, built.get("strands")


def test_the_strand_count_comes_from_the_environment(monkeypatch, tmp_path):
    """Set in backend/.env and carried to the server as one variable: the unit's drop-in in production."""
    monkeypatch.setenv("EVENTSTREAM_STRANDS", "6")
    assert _strands_it_ran(monkeypatch, tmp_path) == (0, 6)


def test_the_command_line_strand_count_wins_over_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("EVENTSTREAM_STRANDS", "6")
    assert _strands_it_ran(monkeypatch, tmp_path, "--strands", "3") == (0, 3)


def test_with_no_strand_count_the_server_takes_its_default(monkeypatch, tmp_path):
    monkeypatch.setenv("EVENTSTREAM_STRANDS", " ")
    assert _strands_it_ran(monkeypatch, tmp_path) == (0, broker_entry.DEFAULT_STRANDS)


def test_fewer_than_two_strands_is_refused(monkeypatch, tmp_path):
    """★ ONE STRAND IS ONE CORE, the failure brief R1 exists to rule out, so the server does not start."""
    monkeypatch.setenv("EVENTSTREAM_STRANDS", "1")
    assert _strands_it_ran(monkeypatch, tmp_path) == (2, None)


def test_a_strand_count_that_is_not_a_number_is_refused(monkeypatch, tmp_path):
    monkeypatch.setenv("EVENTSTREAM_STRANDS", "four")
    with pytest.raises(SystemExit, match="not a whole number"):
        _strands_it_ran(monkeypatch, tmp_path)


def test_a_broker_interrupted_at_the_keyboard_exits_quietly(monkeypatch, tmp_path):
    def _interrupted(coroutine):
        coroutine.close()
        raise KeyboardInterrupt
    monkeypatch.setattr(broker_entry.asyncio, "run", _interrupted)
    monkeypatch.setattr(broker_entry, "runs_free_threaded", lambda: True)
    new_key_file(tmp_path / "k")
    assert broker_entry.main(["--socket", str(tmp_path / "s"), "--root", str(tmp_path / "l"),
                              "--key-file", str(tmp_path / "k")]) == 0
