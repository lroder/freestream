# FreeStream

A partitioned event stream server for free-threaded Python, and the transport interface a product reaches
it through. It was built inside a document-processing product, where it carries a document's pages from
the reader to the assembler, and it became its own project so that several products can each run their
own instance from one codebase. Each product runs its own server, with its own socket, log and key, and
they share code but never an instance.

The server keeps a partitioned log on disk, sealed with AES-256-GCM, and hands each record to one member
of a consumer group. It is the design Kafka and Redpanda share, sized for one host: producers and
consumers reach it over a unix socket, and the filesystem permissions on that socket decide who may. It
runs on free-threaded Python 3.14t, as strands, each a thread with its own event loop that owns a share
of the partitions, so it uses every core it is given.

## What a product imports

A product's code imports `freestream.interface` and nothing else of FreeStream, so moving to another queue
later is a transport beside this one and a change of address:

```python
from freestream.interface.channel import Channel, Delivery
from freestream.interface.transport import Guarantee, RetryLater, open_transport

JOBS = Channel("jobs", Delivery.COMPETING, partitions=8)

transport = open_transport("freestream:///var/lib/freestream/eventstream.sock", role="worker",
                           requires={Guarantee.OUTPUTS_WITH_COMMIT, Guarantee.DEAD_LETTER})
transport.register(JOBS, group="workers", handler=handle)
await transport.run(stop=stop)
```

- **The address names the transport** by its scheme, the way a database URL names its driver.
  `freestream:///absolute/path.sock` or `freestream:relative/path.sock` is this server. The step that
  defines topics is also told the console: `freestream:///path.sock?admin=/path-admin.sock`.
- **A channel declares how its records reach consumers**: `COMPETING` (any member takes any record),
  `KEYED` (every record of one key reaches one member until it closes the key) or `BROADCAST`. A record
  that breaks its channel's declaration is refused before any transport sees it.
- **The guarantees are the interface.** `open_transport` refuses a transport that does not keep every
  guarantee the caller requires: `KEY_AFFINITY`, `OUTPUTS_WITH_COMMIT` (what a handler produced lands with
  the commit of its record, or not at all), `PRODUCED_ONCE`, `DEAD_LETTER` and `DELAYED_RETRY`.
- **A handler returns, raises, or asks to wait.** Returning commits the record; raising retries it at once
  and, after `max_attempts` starts, writes it to `<channel>.dead` and runs `on_exhausted`. A handler
  registered with `delayed_retry=True` may raise `RetryLater(timedelta(...))`: the record waits on
  `<channel>.retry` and is handed back no sooner than the delay, and nothing behind it waits meanwhile.
  `retry.` and `dead.` are FreeStream's header prefixes.

## Running the server

```sh
uv venv --python 3.14t .venv-3.14t
uv pip install --python .venv-3.14t/bin/python "freestream @ git+https://github.com/lroder/freestream.git@v1.0.1"
.venv-3.14t/bin/python -m freestream.sealing /var/lib/freestream/eventstream.key   # once
.venv-3.14t/bin/python -m freestream --socket /var/lib/freestream/eventstream.sock \
    --root /var/lib/freestream/eventstream --key-file /var/lib/freestream/eventstream.key \
    --admin-socket /var/lib/freestream/eventstream-admin.sock --web-port 8260
```

The server refuses to start on an interpreter with the GIL on, and with fewer than two strands
(`--strands`, or `EVENTSTREAM_STRANDS`). Its console speaks plain text on the admin socket
(`echo help | nc -U /var/lib/freestream/eventstream-admin.sock`), in the manner of HAProxy's runtime API, and
`--web-port` serves a page on loopback with the same view.

## Releases

`main` takes new work. Each version is cut from it as a release branch (`release/1.0`), and its tags go on
that branch: `v1.0.0`, then `v1.0.1` for a fix. A fix lands on `main` first and is cherry-picked onto the
release branch, so no fix exists only on an old version. Pin a tag, never a branch, in each requirements
file that installs FreeStream, and take a fix by moving the pin:

```
freestream @ git+https://github.com/lroder/freestream.git@v1.0.1
```

The name `freestream` on PyPI belongs to an unrelated project, so FreeStream installs from this repository.

## Developing

```sh
uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e ".[test]"
.venv/bin/ruff check freestream tests && .venv/bin/mypy freestream && .venv/bin/python -m pytest
uv venv --python 3.14t .venv-3.14t && uv pip install --python .venv-3.14t/bin/python -e .
.venv/bin/python -m pytest -m system      # measures the server's multithreading on this machine
```

## Contributing

Development happens in a private repository, and https://github.com/lroder/freestream is a mirror of it.
Issues are welcome there; a pull request is applied upstream by hand and reaches GitHub through the mirror,
so it shows as closed rather than merged.

## License

MIT — see [LICENSE](LICENSE).
