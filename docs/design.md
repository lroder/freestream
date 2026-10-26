# Design of FreeStream

This note sets out why FreeStream exists, the decisions I made while building it, and the measurements
behind each of them; the README covers how to run it. The figures were measured on the machines named
beside them, and I include them as the evidence for a decision rather than as a benchmark.

---

## Problem it was built for

I built FreeStream inside a document-processing product, where it carries a large scanned document's
pages from the step that splits the file to the readers that OCR each page, and from those readers to the
step that assembles the document again. A 286-page scan took minutes to read because every page was OCR'd
in turn, so we parallelized the OCR. Our first attempt turned one document into three: a re-upload started
three concurrent passes over the same file, each form in it was carved three to five times, and the
progress counter froze for thirty seconds at a stretch.

All of those symptoms came from a single omission, which is that we built the fan-out and never built the
fan-in. Nothing owned a document, so nothing could say "I already have that page", and the only guard
against doing the work twice was a Python set that disappeared with the process holding it.

It follows that the requirement under everything below is that exactly one consumer owns a key at a time.
A partitioned log with consumer groups provides that by construction: a key hashes to a partition, and a
group gives each partition to exactly one consumer, so grouping comes from partitioning itself. A
consumer's committed offset also serves as its progress, so a replacement resumes where a dead consumer
stopped, and nobody has to maintain a retention policy to make that happen.

Kafka and Redpanda share that model, and running one of them was the obvious first choice; they are
widely deployed, specified in detail, and tested by far more people than will ever test this code. Two
facts about our hosts ruled them out, however. Redpanda reserves roughly 2 GB of memory per core, which a
shared development box and a small VPS (virtual private server) cannot spare, and a Kafka-family broker
writes its log segments to disk in the clear, on a host that presently has no disk encryption. So I took
their semantics, which someone else has specified and tested and which therefore cannot drift when I lose
track of a detail, and I kept the storage our own: sealed, and sized for one host. FreeStream runs in under
70 MB of memory.

## One process owns the log

My first version of the store was a library, in which every producer opened the log file and appended to
it. That holds while one process writes. It stopped holding as soon as twelve OCR workers finished pages of
one document at the same moment and the key routed all of them to the same partition: offsets assigned from
each process's own memory collided, and a 30 KB body could be split across `write()` calls and interleave
with another writer's.

Kafka, NATS and Redpanda avoid that problem because they are brokers, in which producers send and one process
appends. When I imitated their semantics in a library, I kept the semantics and discarded the property that
makes them hold. FreeStream is therefore a server: one process owns the log, and clients reach it over a unix
socket, which is faster than TCP on one machine, takes its access control from the filesystem permissions on
the socket file, and has no port to collide with anything else. The protocol has eight requests, which is what
our pipeline stages and the console need; Kafka's has dozens, because it serves a far wider range of clients.

    PRODUCE  channel, key, headers, producer, sequence + body  ->  partition, offset (a resend: the same)
    NEXT     channel, group, wait               ->  one record and its partition, WHEN there is one
    COMMIT   channel, group, partition, next_offset   (refused past what was handed, or backward)
    COMPLETE channel, group, partition, next_offset, outputs + bodies  ->  their placements (all or none)
    ATTEMPT  channel, group, partition, offset, token ->  how many distinct starts it has had
    HELLO    role, name, pid, host              ->  this connection's number
    CLOSE    channel, group, key                    a consumer has finished with a key
    MEMBERS  channel, group                     ->  how many READY members the group has

Each request carries an `id`, and each answer carries it back (Kafka's correlation id), because the server
answers a held NEXT whenever a record arrives, and so answers no longer return in the order the requests
went out.

## GIL, and the process boundary around it

In the original product a page read had two halves, and only one of them ran in parallel:

| | | |
|---|---|---|
| rasterize the page at 300 DPI | **79 ms** | in-process, in a PDF library |
| tesseract | **974 ms** | in a subprocess |

The PDF library holds the GIL (Python's global interpreter lock) while it rasterizes. I rasterized the same
page from 1, 4 and 12 threads and measured 12.8, 12.8 and 12.7 pages a second, and twelve tesseract workers
top out at roughly 12.3 pages a second, so at twelve workers the rasterizing was already a co-bottleneck
that more workers could not relieve.

I tried free-threaded Python as the way past it, and closed that avenue the same day. The library has not
declared free-threading support, so importing it turns the GIL back on under 3.14t, and when I forced the GIL
off with `PYTHON_GIL=0` the process dumped core.

The process boundary that the stream already puts in place answered it instead. Each reader is a separate
process, handed one page over the socket, with an interpreter and a GIL of its own. The server's only
third-party import is `cryptography`, which ships a free-threaded wheel, so the server can also run on a
different interpreter from the product around it, and the two processes share only a socket.

## Strands

My requirement was that the event stream be fully multithreaded; that is the reason I chose 3.14t for it.
The server ran on free-threaded Python from its first day and used one core, because I had written it as a
single asyncio loop and nothing checked the difference. It now follows the design Redpanda runs on (Seastar's
thread per core), under a name of our own: a strand is one thread with its own event loop
(`freestream/strand.py`).

- Every partition of every topic belongs to exactly one strand, chosen by a hash of the topic plus the
  partition. That strand alone touches the partition's log and every group's state for it, so within a
  strand nothing interleaves, exactly as within the single loop it replaced.
- Every connection is homed on one strand, which the server assigns round-robin as it accepts connections.
- Strands reach each other only by message. A strand runs a function on another strand's loop and awaits the
  answer, and no lock is shared between them, so no strand ever waits on another strand's lock.

The home strand of a connection holds its NEXT and asks first the strands where the consumer already holds
partitions, then the rest. A strand with nothing to offer registers the consumer as waiting in the same step,
so the consumer does not miss a record produced a moment later. One record answers one NEXT: the home strand
accepts the first offer and declines the others, and a declined record stays with that strand's share of the
consumer, which the home strand asks first the next time.

I measured the change with `tests/test_eventstream_multithreaded.py` (8 load processes, 64 KB records, before
the durability work described below). The single loop used 0.97 cores and took about 5,500 records a second;
four strands use 3.75 cores and take about 11,000, in 68 MB resident. CI fails if the GIL is on after the
server's imports, and the system test fails if the server spends no more CPU seconds than wall seconds under
load. The server also refuses to start with fewer than two strands, so it is multithreaded wherever it runs.

## Sealed storage

I first left out encryption at rest, then folded it in, because the alternative (dm-crypt on the host) would
end up being something I forget about over time. It is in the code instead (`freestream/sealing.py`):

- The server seals each record's key, value and headers together with AES-256-GCM, bound to the record's
  offset. The frame header (offset, checksum, lengths and timestamp) stays readable, and that lets a segment
  step past a damaged record.
- The server seals a journal entry whole, since an entry carries its outputs' bodies.
- What crosses the socket is plaintext. The server is the only process that touches the log, so it is the
  only one that holds a key, and producers and consumers never handle a cipher.
- The key is a file readable by its owner alone. If the key is lost, what is already written cannot be read.

I measured `cryptography` before letting it in: the GIL stayed off, four threads sealed 3.8 times what one
thread did, and one cipher object served eight threads without an error. With sealing on, the
multithreading test still used 3.66 cores.

## Durability through a power cut

My requirement here, in my own words: "A system/process crash cannot result in a lost or duplicate
per-record output." A process crash leaves the page cache behind, and the kernel writes it out anyway; a power
cut or a kernel panic takes whatever the kernel had not yet written. The server therefore flushes
(`freestream/durability.py`):

- The server answers for a write only once it is on the disk. It answers a produce after the record is
  flushed, hands a consumer a record only after the record is flushed, and does nothing for a COMPLETE until
  its journal entry is flushed.
- Each strand keeps one flush in flight, and everything written in the meantime shares the next one. A
  worker thread flushes one batch while the strand goes on taking requests, and a connection sends its next
  produce while the previous one waits for its flush.
- The server flushes the files of a batch together. On an NVMe drive under ext4, one fsync costs a journal
  commit of about 6.5 ms, so one thread that fsyncs sixteen files in turn pays 100 ms, while sixteen threads
  that fsync the same files at once share the commits and take 13.7 ms. The server hands a batch's files to
  a pool of threads for that reason.
- A failed flush stops the server. Linux marks the pages of a failed fsync clean, so a second fsync can
  report success over data the disk never took (Postgres stops for the same reason). The next server to start
  recovers each partition from what its segment files actually hold and replays the journal, and each client
  resends what went unanswered under the same producer sequence.

On a 24-core development box, four producer processes wrote about 5,700 records of 64 KB a second between
them before this change, and about 3,400 a second with every record flushed, while the server used 2.2
cores. A consumer that completes one record at a time waits on two flushes per COMPLETE and completes about
26 records a second. OCR takes about a second a page, so neither figure slowed the pipeline.

Every record also carries a CRC32, as Kafka's record batches do. When a segment meets a damaged record, it
looks for the next record that passes, and that record's stored offset says exactly which offsets fell in
between. The server counts those offsets as lost, logs them as an error, and never hands them out, so no
offset a group has committed past is ever delivered again. Only a damaged run at the very end of a segment,
with bytes missing, is a torn write, and the server cuts off only that.

## Consumers await their next record

The console showed me the idle cost: each reader asked for work about 21 times a second and was told there
was none, some 250 requests a second from twelve readers doing nothing. The first proposal was Kafka's long
poll, a fetch the server holds for up to a timeout. A process in the kernel simply awaits, however (a blocked
`read()` on a socket returns when there are bytes), and I could see no reason for a consumer to poll at all.

A consumer now sends one NEXT and awaits it. The server answers at once when it has a record for that
member, and otherwise holds the request until a produce, a departing consumer, an operator's state change or
a closed key makes a record available. The request has no timeout, because the connection itself is the
liveness check: a consumer that goes away closes its socket, and the server releases what it held.

Idle, each reader now makes two requests in its lifetime (its HELLO and one NEXT), and the server used no
CPU over ten seconds, against 41 ticks before. Seventeen readers waiting on an idle channel cost nothing
measurable.

I made the produce path cheaper in the same pass. The server used to re-poll every waiter on the channel
after every produce, with each waiter walking every partition, which came to seven polls for one record with
four readers waiting. One record answers at most one waiter per group, so a produce now asks the member that
holds its partition (or, when nobody holds it, the waiters in turn) and stops at the first that takes it.

The server drops a client that stops reading. It writes a held NEXT's answer without waiting on the socket,
so a client that stopped reading would otherwise keep a whole record in the server's memory indefinitely,
with the record uncommitted behind it. When a connection still holds unsent bytes 30 seconds after they were
written, the server drops it, frees the memory, and hands the record to another member.

## Delivery guarantees, and what a handler owns

My standard is that a queue we set up guarantees unique output by its own design, so that a consumer does
not have to catch duplicates the transport let through. I drew the line where Google Pub/Sub draws it: a
record a group has completed is never delivered to that group again, and a record is written once however
many times its produce is sent. A handler that dies before completing a record is handed the record again,
since nothing can know whether its work finished, and the handler is responsible for making idempotent
whatever it wrote outside the stream.

Duplicates could enter the stream at three points, and the server closes each of them:

| Where a duplicate came from | How the server closes it |
|---|---|
| A produce resent after its answer was lost | Idempotent producer. Each client has a producer id and numbers its produces, and the server writes both into the record and answers a repeat with the first placement. I put them in the record rather than in a file beside it, because a kill between two writes is precisely the case to cover. Kafka keeps the same pair in its record batches. |
| A handler that produced its output and died before committing its input | COMPLETE. The client holds every produce a handler makes (in a context variable, so the handler's code is unchanged) and sends them with the commit of the handler's record. The server journals the whole step in one file by rename, applies it, and deletes the entry, and a server that starts and finds an entry finishes it. Kafka transactions and Flink's checkpointed sinks do the same. |
| A draining member's commits, pinned behind the first record it skipped | Acked-ahead set. A group's place is its offset plus the offsets committed past it; the server delivers nothing in the set again, and the offset absorbs the set as it reaches it. Kafka share groups keep the same per-record state. |

When a record has been started `max_attempts` times, the consumer writes it to `<channel>.dead` and closes
it out. A dead-letter topic exists for every defined topic, and since no group reads one as a pipeline stage,
no group's commits would ever let it shrink; the server sweeps it by age instead, keeping thirty days.

## Guarantees as the interface

A product imports `freestream.interface` and nothing else of FreeStream, so moving to another queue would
mean writing a new transport beside this one and changing an address. The scheme of that address picks the
transport, the way a database URL names its driver.

Every queue offers `produce` and some way of handing out records. What differs between queues, and what a
product is written against, is what each queue promises about those records, so the interface names the
promises: `KEY_AFFINITY`, `OUTPUTS_WITH_COMMIT`, `PRODUCED_ONCE`, `DEAD_LETTER` and `DELAYED_RETRY`. A
transport states which of them it keeps, the product states which it requires, and `open_transport` refuses
a transport that lacks one. A transport that spread one document over three consumers assembling it would
make the pipeline wrong, and I wanted that mismatch reported when the product starts.

The retry loop belongs to the transport. NATS counts deliveries itself, Kafka's share groups count
acquisitions, and FreeStream counts starts on its server, so a handler only returns, raises, or asks to be
tried again later.

## Delayed retry

The second product built on FreeStream delivers webhooks to its customers' own HTTPS endpoints, and an
endpoint can be down for hours. Deliveries back off from thirty seconds to six hours, each wait as long as the
delivery has already waited, and give up after 13 attempts, about fifteen hours after the first; one
customer's dead endpoint must never hold up another customer's deliveries on the same partition.

A handler raises `RetryLater(delay)` on a registration that opted in with `delayed_retry=True`. The consumer
writes the record to the channel's retry topic, `<channel>.retry`, in the same COMPLETE that commits the
original, with the time it may run, its starts so far and its key. The server hands a retry topic's records
out only once they are due, earliest first, from anywhere past the group's position, and commits each one
into the acked-ahead set, so a record waiting out six hours holds up nothing behind it. A timer wakes a held
NEXT when the next record falls due, since nothing is produced when a delay runs out.

Kafka keeps a retry topic per delay (`.retry.30s`, `.retry.1m`, and so on), because a Kafka consumer cannot
skip a record it is not ready for, and so every record in one tier has to share one delay. FreeStream can
skip a record, through the same acked-ahead set a draining member uses, so it needs one retry topic per
channel and rounds no delay.

## Partitions and consumers

My requirement for the server was that n subscribers to a channel each be handed records nobody else got.
Twelve reader processes against one server and 400 records now see 400 records, all of them distinct, and
three conditions had to hold for that, of which only the first held at the start:

1. One partition reaches one consumer. A file lock always provided this.
2. A consumer is a connection. I first keyed membership by channel and group for the server's whole life, so
   the server held the locks on behalf of every client, and every client was handed every record. Eight
   readers did eight copies of the work, and 42 consumed pages turned into 336 page reads, a factor of eight
   that matched the reader count.
3. Every consumer gets a partition. With eight partitions and twelve readers, four readers read nothing for
   the entire run while every other figure looked correct.

A channel's partition count is therefore the ceiling on its consumers, and I treat it as a capacity decision:
a partition nobody reads costs one empty file, whereas a reader with no partition costs a core and reports
nothing. The OCR channel declares 64. The server also logs a warning when a group has more members than its
channel has partitions, since those extra members would otherwise sit idle without a sign.

Each consumer takes partitions only until it finds work, and on a channel with no key affinity it keeps one
and hands the rest back. My first version took every free partition on the first poll, so the first reader
to look at an empty log owned all of them; eight readers now settle at one partition each.

## Defined topics

A producer names a topic and a consumer subscribes to it, so, as in Kafka, a topic has to exist before
either one arrives, or a consumer that comes up first has nothing to wait on. The server used to create a
topic the first time any request named it (Kafka's `auto.create.topics.enable`), so a consumer of a misspelled
topic waited forever, and the topic's width came from whichever request happened to arrive first.

A topic now exists because someone defined it (`freestream/registry.py`), and its definition (name, width and
delivery) is stored beside its data. A product defines its topics on every deploy, as it would apply a
migration: the server changes nothing for a topic it already holds at the same shape, and refuses a different
one, because a keyed topic cannot change width under the data it holds. The server checks a name before the
name becomes a directory, so `../` in a topic name cannot write outside the log root.

## Console, modeled on HAProxy

I drain, disable and re-enable backend servers with HAProxy's runtime API every day, and all of it is a line
of text typed into a socket, so I gave the server the same kind of console, on a socket of its own beside the
data socket (`freestream/admin.py`):

    echo "show consumers; show backlog" | nc -U /var/lib/freestream/eventstream-admin.sock
    echo "set consumer 13 state drain"  | nc -U /var/lib/freestream/eventstream-admin.sock

Answers are whitespace-aligned tables under a `#` header, as in HAProxy's `show servers state`, so a person
can read them and `awk` can split them. The console has its own socket so that no client speaking frames can
reach a command that changes what a consumer is handed, and its access control is that socket file's
permissions.

The three consumer states are HAProxy's. READY is handed records. MAINT is handed nothing, and lets every
partition go the next time it asks. DRAIN finishes what it has and takes nothing new, and what it has depends
on the channel. On a competing channel it is the one record in hand. On a keyed channel it is every key the
consumer is part way through, and only the server knows which keys those are: the server opens a key when it
hands a record over, and the consumer closes it (with CLOSE) when it is done with it. The server hands a
draining consumer records for its open keys and nothing else, and the consumer lets its partition go when its
last key closes. I drained the only assembler 40 seconds into a 286-page read; it finished the document in
117.1 s, against 116.1 s undrained, and then let its partition go.

`MEMBERS` lets a product ask, before it publishes work, whether any consumer is ready to take it, and fall
back to doing the work in-process when none is, so the stream does not hold the work until it times out.

The web page (`freestream/web.py`) is the same console in a browser, served by the server itself in the
manner of HAProxy's `stats uri`, so it works even when the product does not. It listens on loopback only and
is reached through an SSH tunnel. It checks the Host header (against DNS rebinding), requires its own header
on any change (which a cross-site form cannot set), and holds one Server-Sent Events stream that resends the
state after each change, at most twice a second.

## Invariants

The tests in `tests/test_event_stream_invariants.py` check these mechanically, because I found I could not
hold a design that lived only in prose: within an hour of settling it, I twice wrote code that contradicted
it.

1. Exactly one consumer owns a key at a time, and everything else here depends on it.
2. The producer chooses the key. A transport that reached into a message for something resembling an
   identifier would know what the product's data is.
3. A consumer declares what it requires, a transport declares what it keeps, and a mismatch raises an error.
   A record that breaks its channel's declaration (a key on a competing channel, or no key on a keyed one) is
   refused, by the client and again by the server.
4. The server answers for a write only once it is on the disk.
5. The server is fully multithreaded, as checked against the real interpreter on every CI run.
6. The stream delivers each record to a group once, and each handler makes its own effects idempotent.

## Deliberately deferred

- A rebalance. Kafka has to rebalance because a partition's work never ends. In the workloads I built
  FreeStream for, keys are short-lived (a document finishes and its key retires), so a new consumer takes the
  next keys and nobody's key moves while it is in flight. The server assigns a key on first sight and leaves
  it there.
- Fairness between tenants. The server serves records in the order they arrived, each partition first in,
  first out, so a small job behind a large one waits for it. A fairness key in the server would have needed
  per-record acknowledgment, would not have survived a move to another queue, and would have taught the
  server what a tenant is.
- Strict global order. A key's records stay on one partition, while records of different keys (or of a
  keyless channel) spread over all of them. One admission sequence for every record would guarantee an order
  nothing uses, through a gate every record would have to pass.
- A network listener. Clients presently reach the server over a unix socket on the same host. A TCP listener
  with TLS beside the socket is the plan for reaching it from another machine, and I have not written it yet.
- A conformance suite against a real broker. I intend to run one behavioral suite against both Redpanda and
  FreeStream, so that a divergence from the model shows up as a failing test. I have not written it yet, and
  the present suite drives FreeStream's own server.

## Lessons from the first real runs

The first end-to-end run read half a page a second, slower than the in-process pool it replaced, and three
mistakes of mine accounted for it, none visible until I looked at which files existed under the log. I
published every page with the document's id as its key, on a channel that had declared it wanted no key, so
all 286 pages went to one reader. Consumer-group membership was shared by every client, so all eight readers
read every page. Partition assignment was greedy, so the first reader owned every partition. Once I fixed the
three, the read went from 0.5 pages a second to about 2.3. The channel contract is now checked in both
directions, since the direction that went unchecked was the one my code broke.

My first CPU readings also missed most of the work. `utime + stime` from `/proc/<pid>/stat` does not count a
reaped child, and tesseract runs as a child, so my first reading put the readers at 2.3x parallelism while
missing 80% of the work; `cutime + cstime` (fields 14 and 15) are the rest of it. Measured properly, sixteen
readers read the 286-page scan in 59.2 s (4.8 pages a second, 12.2x parallelism out of a possible 16), while
the server used no measurable CPU. The same code run in-process took 141 s.

When I changed consumers to await their next record, the change exposed a bug that could lose records. When a
read's byte budget ran out partway through a log segment, the read moved on and took the next segment's first
record, so it came back with a gap, and a consumer that committed the last record it was handed skipped the
gap for good. A read now stops at the first segment it does not finish, and it reads exactly the bytes of the
records it hands back.

The server used to hang on the way down while any client was connected. `serve_forever`, when canceled, waits
for every connection to close, and a connected consumer never closes its own connection, since it is waiting
for its next answer. The server now closes its clients' sockets itself on the way down, and a client whose
socket closes reconnects, as it does after any restart.

Finally, `sockaddr_un.sun_path` is 108 bytes on Linux, and a developer's checkout path can get long. Over the
limit, every client that connects fails with `AF_UNIX path too long`, and none of those clients chose the
path, so the server checks the length when it binds and reports the limit.
