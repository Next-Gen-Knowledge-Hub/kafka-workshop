# Reliable Data Delivery

Kafka doesn't have a single "reliability" switch — it's a set of independent knobs on the
broker, the producer, and the consumer that you combine to get the guarantee you need.

## Reliability guarantees

Kafka can give you, in increasing order of what you have to opt into:

- **At-most-once** — messages may be lost, never redelivered. What you get with
  fire-and-forget producing and auto-commit-before-processing on the consumer.
- **At-least-once** — messages are never lost, but may be redelivered/reprocessed. What you
  get with `acks=all` producing plus manual commit-after-processing on the consumer (see
  [`2-producers-and-consumers/go/manualcommit`](../2-producers-and-consumers/go/manualcommit/)).
  This is the right default for most systems, provided your processing is **idempotent** (safe
  to apply twice — e.g. `SET balance = 100` rather than `balance += 10`).
- **Exactly-once** — every message affects the final state exactly once, even across
  failures/restarts. Built from the idempotent producer + transactions, covered below.

## Broker-side: replication factor and `min.insync.replicas`

- **`replication.factor`** (per topic) — how many copies of each partition exist. `3` is the
  standard production default: it tolerates one broker failure while still having a majority
  for leader election, and two failures without losing data (assuming they were all in-sync).
- **`min.insync.replicas`** (broker/topic config) — combined with producer `acks=all`, this is
  what actually gives you durability. It sets the minimum ISR size required for a produce
  request to succeed. With `replication.factor=3` and `min.insync.replicas=2`, a write
  succeeds as soon as 2 of the 3 replicas have it — and any surviving replica after a single
  broker failure is guaranteed to have every acknowledged write.
- **Unclean leader election** (`unclean.leader.election.enable`) — if every in-sync replica is
  down, should Kafka elect an out-of-sync replica as the new leader (availability, but you can
  lose already-acknowledged messages) or refuse to elect a leader until an in-sync replica
  comes back (consistency over availability)? Defaults to `false` (favor consistency) since
  Kafka 0.11.

## Producer-side: acks, retries, and idempotence

`acks=all` alone is not enough — a naive retry-on-timeout can still duplicate or reorder
messages (the first attempt may have actually succeeded on the broker before the timeout; the
retried attempt then creates a duplicate). The **idempotent producer**
(`enable.idempotence=true`) fixes this: the producer is assigned a `producer.id`, and every
message on a partition gets a monotonic sequence number. The broker deduplicates by
`(producer.id, sequence number)`, so retries are safe — see
[`go/idempotentproducer`](./go/idempotentproducer/).

## Exactly-once with transactions

Idempotence guarantees exactly-once *per partition, per producer session*. **Transactions**
extend that to "exactly-once across multiple partitions/topics, atomically" — the classic case
being a "consume from topic A, produce to topic B" application, where you want the input
offset commit and the output message to be all-or-nothing.

```go
w := &kafka.Writer{ /* ... */ Transport: &kafka.Transport{ /* ... */ } }
// pseudocode of the pattern (see go/transactionalproducer for a runnable example
// built on kafka-go's transactional Client APIs):
tx.Begin()
tx.Produce(outputTopic, result)
tx.SendOffsetsToTransaction(inputOffsets, consumerGroupID)
tx.Commit() // both the produced message and the consumed offset become visible atomically
```

Consumers that only want to see committed data set `isolation.level=read_committed`, which
filters out messages from aborted transactions. See
[`go/transactionalproducer`](./go/transactionalproducer/).

## Validating reliability

Two separate things need validating before you trust a pipeline in production:

- **Configuration** — `kafka-topics --describe` to check replication factor and ISR size,
  `kafka-configs --describe` to check `min.insync.replicas` (see
  [stage 4](../4-cluster-administration/ADMIN.md)).
- **Behavior under failure** — actually kill a broker while producing/consuming and confirm no
  messages are lost or duplicated beyond what your chosen guarantee allows. "It's configured
  correctly" and "it behaves correctly when a broker dies" are different claims; only the
  second one is the one that matters at 3am.

Continue to [stage 4: Cluster Administration & Monitoring](../4-cluster-administration/ADMIN.md).
