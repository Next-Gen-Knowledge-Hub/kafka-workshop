# 4. Cluster Administration & Monitoring

Read first:

- [4.2 Administering Kafka](./ADMIN.md) — topic ops, dynamic config, consumer groups, ACLs.
- [4.3 Monitoring Kafka](./MONITORING.md) — under-replicated partitions, consumer lag.

## 4.1 A 3-broker cluster

Stages 1–3 ran against a single broker, which is enough to learn the client-side mechanics but
can't show you leader election, ISR shrink/grow, or partition reassignment — those need
multiple brokers to actually move data between. Bring up a 3-broker cluster (stop the
single-broker one first, they'd collide on the ZooKeeper/host ports):

```bash
cd ../1-setup && docker compose down
cd ../4-cluster-administration && docker compose -f docker-compose.cluster.yml up -d
```

- Brokers: `localhost:9093`, `localhost:9094`, `localhost:9095`
- Kafka UI: http://localhost:8080

`KAFKA_AUTO_CREATE_TOPICS_ENABLE` is deliberately `false` here — topics are created explicitly
in this stage, with `replication.factor=3` / `min.insync.replicas=2`, so you can watch ISR
behavior for real.

Try killing a broker and watching leadership move:

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9093 --describe --topic admin-demo-topic
docker stop kafka-2
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9093 --describe --topic admin-demo-topic
# any partition led by broker 2 now has a new leader from its ISR, and 2 is
# missing from Isr entirely until you `docker start kafka-2`
docker start kafka-2
```

## What's here

| Path | Demonstrates |
|---|---|
| [`go/topicadmin`](./go/topicadmin/) | Create topic + config, describe partitions/ISR, read/alter dynamic config, list consumer groups |
| [`go/lagmonitor`](./go/lagmonitor/) | Per-group, per-topic consumer lag via the Admin API |

```bash
cd go/topicadmin && go run . localhost:9093,localhost:9094,localhost:9095
cd ../lagmonitor  && go run . localhost:9093,localhost:9094,localhost:9095
```

Both default to `localhost:9092` (the stage 1 single-broker cluster) if you don't pass a
broker list — handy for a quick check without bringing the 3-broker stack up.
