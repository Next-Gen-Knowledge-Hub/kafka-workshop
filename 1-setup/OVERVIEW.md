# What is Kafka?

## Publish/subscribe messaging

Publish/subscribe (pub/sub) messaging decouples the system that produces a piece of data (a
*message*, or in Kafka terms a *record*) from the system(s) that consume it. A **producer**
publishes to a named channel; any number of **consumers** subscribe to that channel and each
receives a copy of every message, independent of the others.

Kafka grew out of a very concrete pain point at LinkedIn: dozens of point-to-point pipelines
shipping activity data, metrics, and log lines between systems, each hand-wired and each with
its own reliability story. Kafka replaced all of it with a single, central, durable log that
any number of producers and consumers could plug into.

## Messages, batches, and schemas

- A **message** (record) is the unit of data: an optional key, a value, optional headers, and
  a timestamp. The key is used for partitioning and log compaction, not just as metadata.
- Producers group messages into **batches** for efficiency — one round trip over the network
  can carry many records, amortizing the cost of compression and the network hop.
- Kafka treats message content as opaque bytes; it never parses it. Because of that, teams
  need a shared **schema** so producers and consumers agree on the structure of a value (JSON,
  Avro, Protobuf, plus a schema registry are the common answers). This workshop uses JSON for
  clarity; production systems at scale usually move to Avro/Protobuf + Confluent Schema
  Registry for compact, evolvable payloads.

## Topics and partitions

Messages are organized into **topics**, and every topic is split into one or more
**partitions**. A partition is an ordered, append-only, immutable sequence of messages — a
commit log. Kafka only guarantees message order *within* a partition, not across an entire
topic.

Partitioning is what makes Kafka scale: a topic with 12 partitions can be written to and read
from by up to 12 producers/consumers in parallel, spread across the whole cluster, while each
individual partition still gives you a strict, replayable order. Each message within a
partition gets a monotonically increasing id called its **offset**.

```
Topic: orders (3 partitions)

Partition 0:  [msg0][msg1][msg2][msg3] ...   -> offset increases left to right
Partition 1:  [msg0][msg1][msg2] ...
Partition 2:  [msg0][msg1][msg2][msg3][msg4] ...
```

## Producers and consumers

- **Producers** create messages. By default, a producer with no explicit key round-robins
  across partitions; with a key, it hashes the key to pick a partition, so all messages with
  the same key always land in the same partition (and therefore keep their relative order).
- **Consumers** subscribe to one or more topics and read messages in partition order, tracking
  their position via **offsets**. Consumers are organized into **consumer groups**: Kafka
  divides a topic's partitions among the members of a group so each partition is read by
  exactly one consumer in the group at a time — this is how Kafka gets consumer-side
  parallelism while still giving each group its own full view of the topic.

## Brokers and clusters

A single Kafka server is called a **broker**. A broker receives messages from producers,
assigns them offsets, and commits them to disk; it also serves fetch requests from consumers.
A group of brokers working together is a **cluster**. One broker in the cluster additionally
acts as the **controller**, responsible for partition leadership and cluster metadata (see
[stage 3](../3-internals-and-reliability/INTERNALS.md)).

Every partition is replicated across multiple brokers for durability. One replica is the
**leader** — all reads and writes for that partition go through it — and the rest are
**followers** that passively replicate the leader's log so one of them can take over if the
leader fails.

## Why Kafka

- **Multiple producers / multiple consumers** — many independent teams can write to and read
  from the same topic without coordinating with each other.
- **Disk-based retention** — unlike a traditional message queue, Kafka keeps messages on disk
  for a configurable retention period (or forever, with log compaction) even after they've
  been consumed. Consumers can rewind and replay history; new consumers can be added later and
  still see the full backlog.
- **Scalable & fault-tolerant** — partitions and replication let a topic scale horizontally
  across a cluster and survive broker failures without data loss.
- **High throughput, low latency** — sequential disk I/O, zero-copy sends, and batching let a
  single broker sustain very high message rates.

## Where this fits

Kafka sits between "just a message queue" and "a distributed log / storage system." It
overlaps with:

- **Enterprise messaging** (ActiveMQ, RabbitMQ) — similar pub/sub semantics, but Kafka is a
  distributed, horizontally-scalable cluster from day one, not a single hand-wired broker.
- **ETL / data integration** — Kafka Connect (stage 5) moves data in and out of Kafka the way
  ETL tools move data between systems, but Kafka inverts the coupling: sources and sinks talk
  to Kafka, not to each other.
- **Stream processing** — because Kafka retains data and preserves order per partition, you
  can build continuous computations directly on top of a topic (stage 6), instead of only
  reacting to individual messages.

Continue to [1.2 Run Kafka locally with Docker](./README.md).
