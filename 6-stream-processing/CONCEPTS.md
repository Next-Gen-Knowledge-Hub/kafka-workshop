# Stream Processing Concepts

## What stream processing is

Everything up to this stage treated Kafka as a transport: a producer emits an event, a consumer
reacts to it, once, independently. **Stream processing** is what happens when you build a
continuous computation *on top of* a stream — not "react to this one event" but "maintain a
running answer over every event that's ever arrived, as more keep arriving." Word counts, moving
averages, joins between two live streams, sessionization — these all require the processor to
remember something between events, which single-event consumers don't need to do.

The canonical Java tool for this is **Kafka Streams** — a library (not a separate cluster) that
runs inside your own application process, reading from and writing to Kafka topics, with
built-in support for local state and fault tolerance. There is no direct Go equivalent; the
`Go` programs in this stage hand-roll the same *concepts* Kafka Streams automates, specifically
so those concepts are visible rather than hidden behind a DSL. See the note on tooling at the
bottom of this page.

## Streams and tables: two views of the same log

A Kafka topic can be read two ways:

- As a **stream** (`KStream` in Kafka Streams terms) — every record is an independent event.
  Two records with the same key are two separate facts ("user 42 clicked", "user 42 clicked
  again"), not an update to one.
- As a **table** (`KTable`) — every record is an *update*, keyed. A new record for an existing
  key replaces the previous value for that key in your mental model of "current state." This is
  exactly what [log compaction](../3-internals-and-reliability/INTERNALS.md#physical-storage)
  is built for: a compacted topic *is* a changelog for a table.

This is the **stream-table duality**: a stream of updates and a table of current values are two
representations of the same information. Aggregating a stream (counting, summing) naturally
produces a table — the running count *is* a continuously updated value per key. And a table can
be read as a stream of the changes that produced it. [`go/wordcount`](./go/wordcount/) makes
this concrete: incrementing a word's count is a stream-to-table aggregation, and every updated
count republished to `word-count-output` is that table's changelog.

## State

Any computation that needs "what happened before" — a running total, a join, a window —needs
**state**: memory that survives across events. Kafka Streams keeps this in an embedded local
store (RocksDB by default) *and* backs it with a compacted changelog topic, so if the process
restarts, it can rebuild its state by replaying the changelog instead of losing it. The
`Go` examples here use a plain in-memory map to keep the concept visible — see each example's
comments for what a production version would need on top (persistence, changelog backup).

## Time

Stream processing has to pick which clock it cares about:

- **Event time** — when the event actually happened (a field in the payload, or the Kafka
  record's own embedded timestamp).
- **Processing time** — when your application happens to see the record.

These diverge under any real-world delay: a mobile client buffering events offline, a slow
upstream pipeline stage. Windowing and out-of-order handling need to be explicit about which
clock they're using — this workshop's [`go/windowedaggregation`](./go/windowedaggregation/)
uses event time on purpose, because "the 10:00–10:05 window" should mean events that happened
in that range, not events that happened to arrive during it.

## Windows

A **window** buckets a stream into fixed time spans so aggregation has a boundary — "average
price per minute" is meaningless without deciding what a minute *is* relative to the data.

- **Tumbling windows** — fixed-size, non-overlapping (`[0-5s) [5-10s) [10-15s)`). Every event
  belongs to exactly one window. What [`go/windowedaggregation`](./go/windowedaggregation/)
  implements.
- **Hopping windows** — fixed-size, overlapping, advanced by a hop smaller than the window size
  (a 5-minute window, advancing every 1 minute) — an event can land in several windows.
- **Session windows** — dynamically sized, bounded by a gap of inactivity rather than a fixed
  clock — "this user's session ended after 30 minutes of no events."

## Joins

- **Stream-stream join** — correlate two live streams within a time window (e.g. an ad
  impression and a click, if the click happens within N minutes of the impression).
- **Stream-table join** — enrich each stream event with the *current* value from a table (e.g.
  attach the customer's current profile to every order event). This never waits for a window —
  it looks up whatever the table's latest value is at the moment the stream event arrives.
  [`go/streamtablejoin`](./go/streamtablejoin/) implements exactly this: an `orders` stream
  joined against a `customer-profiles` table materialized from its own compacted changelog
  topic.
- **Table-table join** — a relational-style join kept continuously up to date as either side's
  changelog updates.

## Out-of-order events and reprocessing

Because consumers can lag and clients can buffer, event-time processing has to tolerate records
arriving "late" relative to the window they belong to — either accept a bounded amount of
lateness (buffer windows open a little longer) or accept that very late events get dropped or
routed to a side output. And because Kafka retains data, **reprocessing** — replaying a topic
from an earlier offset through updated logic — is a first-class operation, not a special case;
it's the same mechanism [manual offset commits](../2-producers-and-consumers/CONSUMERS.md) give
you, applied at the scale of "start this consumer group over from the beginning."

## A note on tooling

Kafka Streams (Java) gives you `KStream`/`KTable` as typed, composable objects with `.filter()`,
`.map()`, `.groupByKey()`, `.windowedBy()`, `.join()` — the DSL *Kafka Streams in Action* teaches
end to end, backed by automatic state-store fault tolerance via changelog topics. If your team is
on the JVM, that library — not a hand-rolled Go equivalent — is almost always the right choice
for this class of problem. This stage's Go examples exist to make the underlying concepts
legible; treat them as "what the DSL is doing for you," not as a replacement for it.

Continue to the runnable examples: [`go/wordcount`](./go/wordcount/),
[`go/windowedaggregation`](./go/windowedaggregation/), [`go/streamtablejoin`](./go/streamtablejoin/).
