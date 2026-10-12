"""Topics are defined before they are used, and the definition is the topic's own (#450)."""
import pytest

from freestream.interface.channel import Delivery
from freestream.registry import (
    TopicConflictError,
    TopicError,
    TopicRegistry,
    UnknownTopicError,
)
from freestream.server import EventStreamServer


def test_a_definition_survives_the_broker_restarting(tmp_path):
    TopicRegistry(tmp_path).define("pages.to_read", partitions=64, delivery="competing")

    definition = TopicRegistry(tmp_path).get("pages.to_read")

    assert (definition.partitions, definition.delivery) == (64, Delivery.COMPETING)
    assert not definition.keeps_its_partitions


def test_defining_it_again_with_the_same_shape_is_a_no_op(tmp_path):
    """What makes the deploy step safe to run on every deploy."""
    registry = TopicRegistry(tmp_path)
    first = registry.define("page.reads", partitions=8, delivery="keyed")
    assert registry.define("page.reads", partitions=8, delivery="keyed") == first


@pytest.mark.parametrize(("partitions", "delivery"), [(16, "keyed"), (8, "competing")])
def test_a_topic_cannot_change_shape_under_its_data(tmp_path, partitions, delivery):
    """On a keyed topic a new width sends a document's pages somewhere else, and two consumers
    assemble one return."""
    registry = TopicRegistry(tmp_path)
    registry.define("page.reads", partitions=8, delivery="keyed")
    with pytest.raises(TopicConflictError, match="cannot change"):
        registry.define("page.reads", partitions=partitions, delivery=delivery)


@pytest.mark.parametrize("name", ["../escape", "a/b", "Pages", ".hidden", "trailing.", "sp ace", "",
                                  "x" * 121, "page.reads.dead"])
def test_a_name_that_is_not_a_topic_name_is_refused(tmp_path, name):
    """★ A NAME BECOMES A DIRECTORY. Until topics were defined, `../` in a name from any client was
    joined onto the log root."""
    with pytest.raises(TopicError):
        TopicRegistry(tmp_path).define(name, partitions=1, delivery="competing")
    assert not (tmp_path.parent / "escape").exists()


@pytest.mark.parametrize("partitions", [0, 1025])
def test_a_width_out_of_bounds_is_refused(tmp_path, partitions):
    with pytest.raises(TopicError, match="between 1 and"):
        TopicRegistry(tmp_path).define("work", partitions=partitions, delivery="competing")


def test_an_unknown_delivery_is_refused(tmp_path):
    with pytest.raises(ValueError):
        TopicRegistry(tmp_path).define("work", partitions=1, delivery="sideways")


def test_a_topic_on_disk_from_before_definitions_is_adopted_at_its_width(tmp_path):
    """The deployed server already holds `pages.to_read` with 64 partition directories and no
    definition. The registry adopts it, records and all, when it is defined at that width, and refuses
    another width."""
    for partition in range(4):
        (tmp_path / "work" / f"{partition:04d}").mkdir(parents=True)
    registry = TopicRegistry(tmp_path)
    with pytest.raises(TopicConflictError, match="4 partition"):
        registry.define("work", partitions=8, delivery="competing")
    assert registry.define("work", partitions=4, delivery="competing").partitions == 4


def test_a_dead_letter_topic_is_derived_and_written_down(tmp_path):
    registry = TopicRegistry(tmp_path)
    registry.define("work", partitions=4, delivery="keyed")

    dead = registry.get("work.dead")

    assert (dead.partitions, dead.delivery, dead.is_dead_letters) == (1, Delivery.COMPETING, True)
    assert TopicRegistry(tmp_path).get("work.dead") == dead
    with pytest.raises(UnknownTopicError):
        registry.get("nothing.dead")


def test_removing_a_topic_removes_its_records_and_its_dead_letters(tmp_path):
    registry = TopicRegistry(tmp_path)
    registry.define("work", partitions=1, delivery="competing")
    registry.get("work.dead")

    registry.remove("work")

    assert not (tmp_path / "work").exists() and not (tmp_path / "work.dead").exists()
    with pytest.raises(UnknownTopicError):
        registry.get("work")
    with pytest.raises(UnknownTopicError):
        registry.remove("work")


async def test_the_broker_will_not_remove_a_topic_somebody_is_reading(tmp_path):
    import asyncio

    from freestream.client import FreeStreamClient

    server = EventStreamServer(tmp_path / "log")
    server.define_topic("work", partitions=1, delivery="competing")
    ready = asyncio.Event()
    serving = asyncio.create_task(server.serve(tmp_path / "es.sock", ready=ready))
    await ready.wait()
    reader = FreeStreamClient(tmp_path / "es.sock")
    try:
        await reader.next("work", "g", wait=False)
        with pytest.raises(TopicError, match="live members"):
            await asyncio.to_thread(server.remove_topic, "work")
        await reader.close()
        for _ in range(50):
            if not server.memberships():
                break
            await asyncio.sleep(0.02)
        await asyncio.to_thread(server.remove_topic, "work")
        assert "work" not in {definition.name for definition in server.registry.all()}
    finally:
        await reader.close()
        serving.cancel()
        await asyncio.gather(serving, return_exceptions=True)
