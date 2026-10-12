"""What FreeStream writes to disk is sealed — brief R7 (#450).

We folded it in on 2026-09-26. Records and journal entries are sealed with AES-GCM under the server's key
as they are written, and opened as they are read; what crosses the socket is plaintext.
"""
import stat
import sys
import sysconfig

import pytest

from freestream import __main__ as eventstream_entry
from freestream.consumer_group import CommitPlan
from freestream.journal import JournalEntry, JournalOutput, pending_entries, write_entry
from freestream.partition_log import PartitionLog
from freestream.record import CorruptRecordError, Record
from freestream.sealing import Seal, UnopenableError, load_key, new_key_file

PLAINTEXT = b"Form 1040 page 3 for a fabricated taxpayer, 123-45-6789"


def test_a_sealed_segment_holds_no_plaintext_and_reads_back_whole(tmp_path):
    seal = Seal.with_a_new_key()
    log = PartitionLog(tmp_path / "p0", seal=seal)
    log.append(Record(value=PLAINTEXT, key=b"doc-1", headers={"page": "3"}))
    on_disk = b"".join(path.read_bytes() for path in (tmp_path / "p0").glob("*.log"))
    assert PLAINTEXT not in on_disk and b"doc-1" not in on_disk and b"page" not in on_disk
    [record] = PartitionLog(tmp_path / "p0", seal=seal).read_from(0)
    assert (record.value, record.key, record.headers) == (PLAINTEXT, b"doc-1", {"page": "3"})


def test_a_record_sealed_at_one_offset_does_not_open_at_another():
    seal = Seal.with_a_new_key()
    framed = bytearray(Record(value=PLAINTEXT).framed(offset=4, seal=seal))
    framed[:8] = (5).to_bytes(8, "little")          # moved to offset 5, checksum recomputed below
    import struct
    import zlib
    checked = bytes(framed[12:])
    framed[8:12] = struct.pack("<I", zlib.crc32(checked, zlib.crc32(bytes(framed[:8]))))
    with pytest.raises(CorruptRecordError, match="does not open"):
        Record.unframed(bytes(framed), 0, seal)


def test_a_log_opened_under_another_key_loses_its_records_rather_than_failing(tmp_path):
    PartitionLog(tmp_path / "p0", seal=Seal.with_a_new_key()).append(Record(value=PLAINTEXT))
    reopened = PartitionLog(tmp_path / "p0", seal=Seal.with_a_new_key())
    assert reopened.read_from(0) == [] and reopened.next_offset == 1


def test_an_unsealed_record_does_not_pass_for_a_sealed_one():
    framed = Record(value=b"v", key=b"k").framed(offset=0)
    with pytest.raises(CorruptRecordError, match="not sealed"):
        Record.unframed(framed, 0, Seal.with_a_new_key())


def test_a_journal_entry_is_sealed_on_disk_and_opens_under_its_key(tmp_path):
    seal = Seal.with_a_new_key()
    entry = JournalEntry.new("to.read", "readers", CommitPlan(0, 1, frozenset()),
                             (JournalOutput("work", Record(value=PLAINTEXT)),))
    path = write_entry(tmp_path, entry, seal)
    assert PLAINTEXT not in path.read_bytes()
    assert pending_entries(tmp_path, seal) == [(path, entry)]
    with pytest.raises(UnopenableError):
        pending_entries(tmp_path, Seal.with_a_new_key())


def test_a_key_file_is_the_owners_alone_and_is_never_overwritten(tmp_path):
    path = tmp_path / "keys" / "es.key"
    new_key_file(path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert len(load_key(path)) == 32
    with pytest.raises(FileExistsError):
        new_key_file(path)


@pytest.mark.parametrize(("content", "complaint"), [("zz", "hex"), ("abcd", "holds 2 bytes")])
def test_a_key_file_that_is_not_a_key_is_refused(tmp_path, content, complaint):
    (tmp_path / "bad.key").write_text(content)
    with pytest.raises(ValueError, match=complaint):
        load_key(tmp_path / "bad.key")


def test_a_seal_needs_a_whole_key_and_sealed_bytes_need_a_nonce():
    with pytest.raises(ValueError, match="32 bytes"):
        Seal(b"short")
    with pytest.raises(UnopenableError, match="too short"):
        Seal.with_a_new_key().open(b"tiny", b"")


def test_an_interpreter_with_the_gil_on_refuses_to_start(tmp_path, capsys):
    """★ FULLY MULTITHREADED, OR NOT AT ALL. These tests run on the API's interpreter, with the GIL on — the
    one a unit pointed at the wrong venv would start the server under. It refuses rather than serve on one
    core."""
    new_key_file(tmp_path / "es.key")
    assert eventstream_entry.main(["--socket", str(tmp_path / "s"), "--root", str(tmp_path / "l"),
                                   "--key-file", str(tmp_path / "es.key")]) == 2
    assert "must run free-threaded" in capsys.readouterr().err


def test_a_free_threaded_build_that_an_import_turned_the_gil_on_in_is_not_free_threaded(monkeypatch):
    monkeypatch.setattr(sysconfig, "get_config_var", lambda name: 1 if name == "Py_GIL_DISABLED" else None)
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: True, raising=False)
    assert eventstream_entry.runs_free_threaded() is False
    monkeypatch.setattr(sys, "_is_gil_enabled", lambda: False, raising=False)
    assert eventstream_entry.runs_free_threaded() is True
