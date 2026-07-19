# Kafka Producers: Writing Messages to Kafka

## The write path

Sending a message is not a single network call. A producer:

1. Serializes the key and value to bytes.
2. Runs the **partitioner** to decide which partition of the topic the message goes to.
3. Appends the message to an in-memory batch for that topic-partition.
4. A background I/O thread flushes full (or aged-out) batches to the broker that leads that
   partition.
5. The leader broker appends the batch to its log, replicates it to followers, and responds
   once the configured acknowledgment level is satisfied.

Batching happens whether you "send one message at a time" from the application's point of view
or not — the client library batches transparently. This is the main reason Kafka producers can
sustain very high throughput: the fixed cost of a network round trip and a fsync is amortized
across many records.

## Synchronous vs. asynchronous sends

- **Fire-and-forget** — call send and don't wait for the result. Highest throughput, but you
  can silently lose messages on error.
- **Synchronous** — call send and block until the broker acknowledges. Simple and safe, but
  throughput is capped by one in-flight request's round-trip latency (unless you parallelize
  producers). See [`go/syncproducer`](./go/syncproducer/).
- **Asynchronous with callback** — call send, register a callback, and move on; the callback
  fires (usually on a different goroutine) when the broker responds, so you can log/retry/count
  failures without blocking the producing goroutine. This is what you want for high-throughput
  pipelines. See [`go/asyncproducer`](./go/asyncproducer/).

## `acks`: how durable is "sent"?

`acks` controls how many replicas must confirm a write before the broker reports success back
to the producer:

| `acks` | Meaning | Durability | Latency |
|---|---|---|---|
| `0` | Don't wait for any broker response | None — a broker restart can lose the batch | lowest |
| `1` | Wait for the partition leader only | Survives losing followers, not the leader | low |
| `all` / `-1` | Wait for all in-sync replicas (ISR) | Survives losing any single broker (with `min.insync.replicas` set correctly) | highest |

`acks=all` is what [stage 3](../3-internals-and-reliability/RELIABILITY.md) builds reliable
delivery on top of.

## Partitioning

- **No key** — the default partitioner distributes records round-robin (in "sticky batches",
  to keep batching effective) across all partitions.
- **With a key** — the key is hashed and the result maps to a partition. Every message with
  the same key always lands on the same partition, which means: (a) all messages for that key
  stay in relative order, and (b) a consumer processing that partition sees every event for
  that key. This is the mechanism behind "partition by user ID", "partition by order ID", etc.
- **Custom partitioner** — you can supply your own partition-selection logic, e.g. to route
  VIP customers to a dedicated partition, or to implement sticky/rack-aware placement. See
  [`go/keyedpartitioning`](./go/keyedpartitioning/).

## Compression

Producers can compress whole batches (`gzip`, `snappy`, `lz4`, `zstd`) before sending. Because
compression happens on the already-assembled batch, bigger batches compress better — another
reason to let the client batch instead of forcing synchronous sends of tiny messages.

## Retries and idempotence

Transient errors (a leader election in progress, a broker restart) are retried by the client
automatically. Retrying naively can reorder or duplicate messages — Kafka's **idempotent
producer** (`enable.idempotence=true`, the default in modern clients) solves this by tagging
each message with a producer ID and sequence number so the broker can drop duplicates. Covered
in depth in [stage 3](../3-internals-and-reliability/RELIABILITY.md).

## Serializers

The producer needs to turn your key/value types into bytes. Options, roughly in order of how
much this workshop uses them:

- **Raw bytes / strings** — what we use throughout for simplicity.
- **JSON** — human-readable, but has no compact binary form and only "documentation" for a
  schema. Good enough for the stream-processing examples in stage 6.
- **Avro/Protobuf + Schema Registry** — the production answer: compact binary encoding, and a
  registry that enforces schema compatibility as producers and consumers evolve independently.
  Not required for this workshop, but know that it exists before you build a real pipeline.

Continue to [2.2 Consumers](./CONSUMERS.md).
