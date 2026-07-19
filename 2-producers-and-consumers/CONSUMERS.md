# Kafka Consumers: Reading Data from Kafka

## Consumers and consumer groups

A consumer subscribes to one or more topics and pulls messages by **polling** the broker — the
broker never pushes to consumers. Every consumer belongs to a **consumer group**, identified
by a `group.id`. Kafka guarantees that within a group, each partition is assigned to exactly
one consumer at a time:

```
Topic "orders" — 6 partitions, consumer group "inventory-service" with 3 members

consumer-1: partitions 0, 1
consumer-2: partitions 2, 3
consumer-3: partitions 4, 5
```

This is Kafka's load-balancing mechanism: add more consumers to the group (up to the partition
count) to increase parallelism, and Kafka rebalances partitions across them automatically. Two
different groups reading the same topic are fully independent — each group gets its own copy
of every message and tracks its own offsets. That's how you fan a topic out to multiple
unrelated applications (e.g. `inventory-service` and `analytics-service` both consuming
`orders` at their own pace). See [`go/consumergroup`](./go/consumergroup/).

## Rebalancing

When a consumer joins or leaves a group (including crashing, or being killed by a slow poll
loop exceeding `max.poll.interval.ms`), the group **rebalances**: partitions are reassigned
among the remaining members. Rebalancing stops-the-world for the group briefly, which is why
consumer applications should poll promptly and keep per-record processing fast — offload slow
work instead of doing it inline in the poll loop.

## The poll loop

```go
for {
    msg, err := reader.ReadMessage(ctx)
    // ... process msg ...
}
```

Under the hood this is a request/response loop against the broker: fetch a batch of records
from the assigned partitions, hand them to the application, repeat. `fetch.min.bytes` and
`fetch.max.wait.ms` (exposed via `kafka.ReaderConfig.MinBytes`/`MaxWait` in `kafka-go`) trade
off latency against network efficiency — wait for more data to accumulate before responding,
or respond immediately with whatever's available.

## Offsets and commits

An **offset** is the consumer's bookmark: "I've processed everything in this partition up to
and including offset N." Offsets are themselves stored in Kafka, in an internal topic called
`__consumer_offsets`, keyed by `(group.id, topic, partition)`.

- **Automatic commit** — the client commits the latest offset periodically in the background
  (e.g. every 5s). Simple, but you can lose or double-process messages: if the process crashes
  between "processed the message" and "the next auto-commit tick", the next consumer to take
  that partition starts from the last *committed* offset, not the last *processed* one.
- **Manual commit** — the application decides exactly when an offset is safe to commit,
  typically *after* the side effect (write to a database, call an API) has definitely
  succeeded. This gives you "at-least-once" delivery: on a crash-and-restart, you might
  reprocess a few messages, but you never silently skip one. See
  [`go/manualcommit`](./go/manualcommit/).

```
        at-least-once (commit after processing)          at-most-once (commit before processing)
process -> commit                                          commit -> process
(crash before commit => reprocess on restart, safe)         (crash before process => message lost)
```

True **exactly-once** processing needs more than offset placement — see the transactional
producer in [stage 3](../3-internals-and-reliability/RELIABILITY.md).

## Standalone consumers

You don't always want group-managed partition assignment — sometimes you want a consumer to
own a *specific* partition regardless of group membership (e.g. a stateful service that's
sharded by partition number itself). `kafka-go`'s `Reader` supports this by setting `Partition`
directly instead of `GroupID`. *Kafka: The Definitive Guide* calls this pattern the "standalone
consumer."

Continue to [stage 3: Internals & Reliable Delivery](../3-internals-and-reliability/INTERNALS.md).
