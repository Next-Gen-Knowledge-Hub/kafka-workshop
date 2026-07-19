# Kafka Internals

## Cluster membership and the controller

Every broker registers itself with the cluster's metadata store (ZooKeeper in the classic
deployment, or the KRaft controller quorum in modern Kafka) on startup, and gets a unique
`broker.id`. One broker is elected the **controller**. The controller is responsible for:

- Watching for brokers joining/leaving the cluster.
- Electing a new partition leader whenever the current leader fails.
- Propagating metadata changes (new topics, new partitions, leadership changes) to every other
  broker.

If the controller itself fails, the remaining brokers race to elect a new one — this is one of
the main things KRaft mode speeds up versus ZooKeeper, since the controller is now part of the
same Raft quorum instead of a separate ZooKeeper election.

## Replication

Every partition has `N` replicas, one of which is the **leader**; the rest are **followers**.
All producer writes and consumer reads for that partition go through the leader. Followers
continuously fetch from the leader to stay caught up — this looks just like a consumer reading
from that partition. A follower that is caught up (within `replica.lag.time.max.ms`) is
considered **in-sync** and is part of the partition's **ISR** (in-sync replica set).

```
Partition "orders-0", replication factor 3

Broker 1 (leader)    [msg0][msg1][msg2][msg3]
Broker 2 (follower)  [msg0][msg1][msg2][msg3]   <- in ISR
Broker 3 (follower)  [msg0][msg1]               <- lagging, may fall out of ISR
```

If the leader fails, the controller picks a new leader from the ISR — this is why ISR
membership (not just "any replica") matters for durability. See
[stage 3.2](./RELIABILITY.md) for how `acks` and `min.insync.replicas` build on top of this.

## Request processing

Brokers handle three main request types:

- **Produce requests** — append a batch to a partition's log, and (depending on `acks`) wait
  for the required number of ISR members to replicate it before responding.
- **Fetch requests** — from both consumers and follower replicas, reading a range of a
  partition's log starting at a given offset.
- **Metadata / admin requests** — topic creation, partition/config changes, group coordination.

## Physical storage

Each partition is stored on disk as a sequence of **segment** files, rolled over by size or
time (`log.segment.bytes`, `log.roll.ms`). Kafka relies on the OS page cache and sequential
disk I/O rather than fine-grained per-message bookkeeping — this is a big part of why it can
sustain very high throughput on commodity disks.

- Each segment has an accompanying **offset index** and **timestamp index** so the broker can
  binary-search to a given offset or timestamp without scanning the whole segment.
- **Retention** deletes whole segments once they age out (`retention.ms`) or the partition
  exceeds a size limit (`retention.bytes`).
- **Log compaction** (`cleanup.policy=compact`) is a different retention strategy: instead of
  deleting old messages by age, Kafka retains only the *latest* value for each key, forever.
  This is what turns a Kafka topic into a changelog for a table — the same idea the `KTable`
  concept in [stage 6](../6-stream-processing/CONCEPTS.md) is built on. A **tombstone**
  (message with a `nil` value) marks a key for deletion during the next compaction pass.

```
Before compaction (offset order):           After compaction:
(k1,v1) (k2,v1) (k1,v2) (k3,v1) (k1,v3)     (k2,v1) (k3,v1) (k1,v3)
```

Continue to [3.2 Reliable Data Delivery](./RELIABILITY.md).
