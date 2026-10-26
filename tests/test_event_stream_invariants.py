"""The invariants FreeStream was designed around, as tests.

★ WHY THIS FILE EXISTS. We settled the design in conversation on 2026-09-22 and I contradicted it
twice within the hour — once by reaching into a message for a grouping key the producer was supposed to
supply, and then by writing a confident comment explaining why that was right. Prose I author cannot
police prose I author. A test can.

This is a reflexion model in the sense Murphy, Notkin and Sullivan meant: the intended architecture is
stated somewhere other than the code, and something mechanical reports where the code diverges. An
import linter can hold the import-shaped rules; every invariant that broke that day was not import-shaped,
which is the gap this closes.

ONE TEST PER NUMBERED INVARIANT, named for it. A test here failing means the CODE is wrong, or the
design changed — and in the second case the design is restated first, not quietly changed by whoever is
mid-keystroke.

They were first written against the work queue's port, and the event stream replaced that transport
on 2026-09-24; the invariants did not change, so the tests were aimed at what now carries them.

Invariants 4, 5 and 7 are about the product built on the stream, not the stream, so they are tested
there and not here.
"""
import ast
import inspect

import pytest

from freestream import (
    admin,
    client,
    consumer,
    consumer_group,
    partition_log,
    record,
    registry,
    server,
    status,
    topic,
    transport,
    web,
)
from freestream.consumer_group import ConsumerGroup
from freestream.interface import channel
from freestream.interface import transport as stream_interface
from freestream.interface.channel import Channel, ChannelContractError, Delivery
from freestream.record import Record
from freestream.topic import Topic, partition_for


def _without_docstrings(source: str) -> str:
    """The module's executable code, with every docstring removed.

    `producer.py` shows a caller writing `key=document_id.bytes`, which is the invariant being obeyed
    rather than broken — the producer naming its own key. If we checked the raw text we would flag that
    example and push the next person to rename a variable in prose to satisfy a test, which teaches the
    wrong lesson about what the rule is for.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        holds_a_docstring = isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        if holds_a_docstring and ast.get_docstring(node) is not None:
            node.body = node.body[1:] or [ast.Pass()]
    return ast.unparse(tree)


# --------------------------------------------------------------- 1. one owner per key

def test_invariant_1_only_one_consumer_owns_a_key_at_a_time(tmp_path):
    """★ THE INVARIANT EVERYTHING ELSE SERVES. Two consumers, one document: one of them gets all of it.

    A key names one partition, and a partition is held by one member of a group at a time — held by an
    flock, so the kernel decides it rather than a coroutine's good luck.
    """
    keyed = Topic(tmp_path / "page.reads", partitions=8, seal=None)
    for page in range(6):
        keyed.produce(Record(value=f"page {page}".encode(), key=b"one-document"))
    first, second = (ConsumerGroup(keyed, "assemblers", tmp_path / "page.reads") for _ in range(2))

    by_first, by_second = first.poll(), second.poll()

    assert by_first or by_second, "somebody must get the work"
    assert not (by_first and by_second), (
        f"one document went to two consumers: {len(by_first)} pages to one, "
        f"{len(by_second)} to the other")


def test_invariant_1_a_dead_owner_hands_the_document_on(tmp_path):
    """The other half: exclusive is not the same as stuck. An owner that goes releases its document.

    Nothing heartbeats and nothing times out — the kernel drops the flock when the holder goes, so the
    next consumer to look simply succeeds.
    """
    keyed = Topic(tmp_path / "page.reads", partitions=8, seal=None)
    keyed.produce(Record(value=b"page 1", key=b"one-document"))
    died = ConsumerGroup(keyed, "assemblers", tmp_path / "page.reads")
    assert died.poll()
    died.release()                          # the connection ends; it tells nobody

    replacement = ConsumerGroup(keyed, "assemblers", tmp_path / "page.reads")
    assert replacement.poll()


# --------------------------------------------------------------- 2. the producer chooses the key

def test_invariant_2_the_producer_chooses_the_key_not_the_transport(tmp_path):
    """★ THE ONE I BROKE. Two records with the SAME body, keyed differently, go where their KEYS say.

    If the transport were reaching into the message for something identifier-shaped, the two would land
    together; they land in the partitions their keys hash to, which is written from the outside so it
    cannot be satisfied by a comment.
    """
    keyed = Topic(tmp_path / "t", partitions=8, seal=None)
    body = b'{"document": "the-same-one"}'
    alpha, beta = b"alpha", b"beta"
    assert partition_for(alpha, 8) != partition_for(beta, 8), "pick two keys that part"
    placed = [keyed.produce(Record(value=body, key=key))[0] for key in (alpha, beta)]
    assert placed == [partition_for(alpha, 8), partition_for(beta, 8)]


def test_invariant_2_the_event_stream_never_reads_an_identifier_out_of_a_message():
    """Structural, because how it behaves alone would not catch a transport using the key AND the id.

    A product's pipeline is deliberately NOT checked: the stream head choosing a document id as its key is the
    producer doing exactly what this invariant reserves to it.
    """
    for module in (admin, server, channel, client, consumer, consumer_group, partition_log, record,
                   registry, status, stream_interface, topic, transport, web):
        code = _without_docstrings(inspect.getsource(module))
        for pipeline_field in ("subject_id", "document_id", "client_id", "tenant_id"):
            assert pipeline_field not in code, (
                f"{module.__name__} reads {pipeline_field!r} — the key is the producer's to choose")


# --------------------------------------------------------------- 3. declare, and refuse on mismatch

class _NeverCalled:
    async def produce(self, *_args, **_kwargs):
        raise AssertionError("a record that broke its channel's contract reached the stream")


@pytest.mark.parametrize(("delivery", "key", "why"), [
    (Delivery.KEYED, None, "no key"),
    (Delivery.COMPETING, "a-document", "has a key"),
])
async def test_invariant_3_a_record_that_breaks_its_channel_is_refused_not_degraded(delivery, key, why):
    """★ REFUSED, NOT DEGRADED. A keyless record on a keyed channel is a document assembled by several
    consumers — not slower, WRONG — and a keyed record on a competing one un-shares the work."""
    with pytest.raises(ChannelContractError, match=why):
        await transport.FreeStreamTransport(_NeverCalled()).produce(Channel("t", delivery), value=b"x",
                                                                    key=key)


# --------------------------------------------------------------- 6. durable means surviving a process

def test_invariant_6_the_log_does_not_fsync_every_record():
    """Durable means surviving a PROCESS, not a power cut (2026-09-22).

    A record the log loses to a power cut is a page of a document that is itself stored in a database, so
    a re-read regenerates it — and an fsync per record would slow the common path to insure against the
    rarer failure. Stated as a test because the tempting "fix" for a flake is to add one, and the cost
    would not show up in any suite.
    """
    source = inspect.getsource(partition_log)
    assert "fsync" not in _without_docstrings(source), (
        "see invariant 6 — this is a deliberate trade, not an oversight")
