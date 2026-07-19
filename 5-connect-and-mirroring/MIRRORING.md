# Cross-Cluster Data Mirroring

## Why mirror between clusters at all

A single Kafka cluster is usually confined to one datacenter/region for latency reasons, but
real systems often need data in more than one place: disaster recovery, geo-local reads for a
globally distributed application, or isolating a noisy/experimental workload from production by
giving it its own cluster fed from a mirror. **Cross-cluster mirroring** copies topics from one
Kafka cluster to another, continuously.

## Multi-cluster architectures

- **Hub-and-spoke** — many "spoke" clusters (e.g. one per datacenter) all mirror into one
  central "hub" cluster. Good when downstream consumers (analytics, reporting) mostly need a
  global view. Simple to reason about, but the hub is a single point of aggregation.
- **Active-active** — two (or more) clusters, each with local producers and consumers, mirroring
  *into each other* so both have the full dataset. Gives every region low-latency local reads
  and writes. The hard part: if the same logical entity can be written in both regions, you
  need an application-level strategy for conflicts (last-write-wins, CRDTs, partitioning writes
  by entity so only one region ever owns a given key).
- **Active-standby** — one cluster is live, a second mirrors it and sits idle, ready to take
  over on a regional failure. Simplest operationally, at the cost of the standby's resources
  sitting mostly unused, and a cutover procedure you need to actually rehearse.
- **Stretch clusters** — a single Kafka cluster with brokers spread across regions/AZs instead
  of separate clusters. Trades replication simplicity for being extremely latency-sensitive to
  inter-region network conditions, since ISR replication is now crossing that link.

## Apache Kafka's MirrorMaker 2

MirrorMaker 2 (MM2) is built on Kafka Connect — a mirror is just a pair of specialized
source/sink connectors — so it gets Connect's scaling, offset tracking, and distributed-mode
fault tolerance for free. Point it at a source and a target cluster and it replicates topics,
consumer group offsets, and topic configuration/ACLs, prefixing mirrored topic names with the
source cluster's alias by default (`us-west.orders` on the target for an `orders` topic
mirrored from a cluster aliased `us-west`) so mirrored and local topics never collide.

```properties
# mm2.properties (sketch)
clusters = source, target
source.bootstrap.servers = source-kafka:9092
target.bootstrap.servers = target-kafka:9092

source->target.enabled = true
source->target.topics  = orders|payments

sync.topic.configs.enabled = true
emit.checkpoints.enabled   = true   # replicate consumer group offsets too
```

`emit.checkpoints.enabled` is what lets a consumer group failing over from the source cluster
to the target cluster resume roughly where it left off, instead of starting from the beginning
or the end.

## Alternatives

- **Uber's uReplicator** and **Confluent's Replicator** predate/complement MM2, solving the same
  problem with different trade-offs (uReplicator focused on rebalance stability at very large
  scale; Replicator adds tighter Confluent Platform/Schema Registry integration). MM2 has since
  absorbed most of what made those attractive, and is the default choice for anyone starting
  fresh on open-source Kafka today.

Continue to [stage 6: Stream Processing](../6-stream-processing/CONCEPTS.md).
