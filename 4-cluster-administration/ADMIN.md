# Administering Kafka

## Topic operations

Everything about a topic — partition count, replication factor, retention, compaction — is
mutable after creation *except* going from more partitions to fewer (partition count can only
increase, never decrease, because decreasing would break the key → partition mapping that
ordering guarantees depend on).

```bash
# create
kafka-topics --bootstrap-server localhost:9093 --create \
  --topic clicks --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2 --config retention.ms=604800000

# add partitions (never remove)
kafka-topics --bootstrap-server localhost:9093 --alter --topic clicks --partitions 12

# list / describe
kafka-topics --bootstrap-server localhost:9093 --list
kafka-topics --bootstrap-server localhost:9093 --describe --topic clicks

# delete
kafka-topics --bootstrap-server localhost:9093 --delete --topic clicks
```

`--describe` is what you reach for to check replication health directly:

```
Topic: clicks   PartitionCount: 6   ReplicationFactor: 3
  Partition: 0  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 1  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  ...
```

`Isr` shorter than `Replicas` means a replica has fallen behind — exactly the signal
[monitoring](./MONITORING.md) alerts on.

## Dynamic configuration

Per-topic and per-client-id configuration overrides can be changed without a restart:

```bash
kafka-configs --bootstrap-server localhost:9093 --alter \
  --entity-type topics --entity-name clicks \
  --add-config min.insync.replicas=2,cleanup.policy=compact

kafka-configs --bootstrap-server localhost:9093 --describe --entity-type topics --entity-name clicks
```

## Consumer group management

```bash
kafka-consumer-groups --bootstrap-server localhost:9093 --list
kafka-consumer-groups --bootstrap-server localhost:9093 --describe --group inventory-service
kafka-consumer-groups --bootstrap-server localhost:9093 --delete --group old-service
```

`--describe` on a group shows current offset, log-end-offset, and the derived `LAG` per
partition — the same numbers [`go/lagmonitor`](./go/lagmonitor/) computes programmatically for
alerting.

## Partition management

- **Preferred replica election** — after a broker comes back from a restart, leadership for
  its partitions doesn't automatically move back to it; `--preferred-replica-election`
  rebalances leadership back to the first replica in each partition's replica list, which
  spreads leader load evenly again.
- **Reassigning partitions** — moving a partition's replicas to different brokers (e.g. to
  rebalance disk usage after adding a broker) is a two-step dance: generate a reassignment
  plan with `kafka-reassign-partitions --generate`, then execute it with `--execute`. This
  streams the partition's data to its new replicas in the background before switching over.
- **Changing replication factor** — there's no single flag for this; it's done by writing a
  reassignment JSON that lists more (or fewer) replicas per partition than currently exist.

## ACLs

```bash
kafka-acls --bootstrap-server localhost:9093 --add \
  --allow-principal User:inventory-service \
  --operation Read --operation Write --topic orders

kafka-acls --bootstrap-server localhost:9093 --list
```

ACLs only take effect once the broker's `authorizer.class.name` is configured (out of scope
for a local workshop cluster, but this is the command surface — see
[7.2 Security](../7-testing-and-security/SECURITY.md)).

## Doing this from `Go`

The Kafka CLI tools above wrap the same Admin API a client library exposes. See
[`go/topicadmin`](./go/topicadmin/) for programmatic topic creation, listing, config
inspection, and consumer group description using
[`twmb/franz-go/pkg/kadm`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm) — a purpose-
built admin client that wraps the same broker requests the CLI tools use.

Continue to [4.3 Monitoring Kafka](./MONITORING.md).
