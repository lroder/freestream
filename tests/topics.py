"""Define topics on a test's server, as a product's deploy step defines them on the real one."""
from freestream.interface.channel import Channel, Delivery
from freestream.server import EventStreamServer

#: The three topics the tests define when they name none: the shapes bosstax-web's document pipeline runs
#: on, where FreeStream was built — a wide competing topic, a keyed one, and a narrow competing one.
SAMPLE_CHANNELS = (
    Channel("pages.to_read", Delivery.COMPETING, partitions=64),
    Channel("page.reads", Delivery.KEYED, partitions=8),
    Channel("documents.assembled", Delivery.COMPETING, partitions=8),
)


def define(server: EventStreamServer, *channels: Channel) -> EventStreamServer:
    """Define each channel as a topic; with none named, the sample topics."""
    for channel in channels or SAMPLE_CHANNELS:
        server.define_topic(channel.name, partitions=channel.partitions, delivery=channel.delivery)
    return server
