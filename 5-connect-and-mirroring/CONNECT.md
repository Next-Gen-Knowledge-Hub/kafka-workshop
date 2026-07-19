# Kafka Connect

## The problem Connect solves

A huge fraction of "get data into/out of Kafka" work is the same handful of patterns over and
over: tail a database's change log into a topic, dump a topic into a data warehouse, copy files
from a directory into a topic. Writing a bespoke producer or consumer for every one of these is
needless boilerplate, and each one reinvents its own answer to offset tracking, retries,
serialization, and scaling.

**Kafka Connect** is a framework, bundled with Kafka, for running reusable **connectors**:

- A **source connector** pulls data from an external system and produces it to a topic (a
  database's binlog, a directory of files, an API you poll).
- A **sink connector** consumes a topic and writes it to an external system (a data warehouse,
  a search index, another database).

You configure connectors with JSON; Connect handles distributing the work across **tasks**,
tracking source offsets (e.g. "which file position/binlog position have we read up to"),
retrying, and scaling out. This is the same category of tool as traditional ETL software,
inverted: sources and sinks only ever have to know how to talk to Kafka, not to each other.

## Standalone vs. distributed mode

- **Standalone** — a single process, config on local disk. Good for local development, edge
  deployments, or connectors tied to a specific host (like tailing a local log file).
- **Distributed** — a cluster of Connect worker processes coordinating through Kafka itself
  (much like a consumer group). Connectors and their tasks are spread across workers and
  survive a worker crashing. This is what production deployments use, and what
  [`docker-compose.connect.yml`](./docker-compose.connect.yml) runs.

## When to use Connect vs. a custom producer/consumer

Reach for Connect when:

- You're moving data between Kafka and a well-known system type (database, search index, cloud
  storage, another message queue) — check the [connector
  hub](https://www.confluent.io/hub/) before writing custom code.
- You want offset tracking, scaling, and retries handled for you.

Write your own client (the `Go` programs throughout this workshop) when:

- The logic is genuinely custom business logic, not just data movement.
- You need tight control over batching, partitioning, or transactional semantics (stage 3).
- The "external system" is another one of your own services, better reached over its own API.

## Running it

```bash
docker compose -f docker-compose.connect.yml up -d
# Connect REST API -> http://localhost:8083
# Kafka UI (both clusters + this Connect worker) -> http://localhost:8080
```

This brings up two independent single-broker clusters — `source` (host port `9092`) and
`target` (host port `9096`) — plus one Connect worker. The connectors you'll submit are
MirrorMaker 2's own connector classes (`org.apache.kafka.connect.mirror.*`), which ship in the
base Kafka Connect distribution — no extra plugin install needed. This doubles as the hands-on
half of [5.2 Cross-Cluster Mirroring](./MIRRORING.md): submitting a `MirrorSourceConnector` to
a normal Connect worker *is* how you run MM2 without its separate dedicated-process mode.

```bash
# create a topic on the SOURCE cluster and write a couple of messages to it
docker exec kafka-source kafka-topics --bootstrap-server localhost:9092 \
  --create --topic mirror-demo --partitions 3 --replication-factor 1
docker exec -i kafka-source kafka-console-producer --bootstrap-server localhost:9092 --topic mirror-demo <<'EOF'
hello from the source cluster
this message gets mirrored
EOF

# start mirroring source -> target
curl -s -X POST -H 'Content-Type: application/json' \
  --data @connectors/mirror-source.json \
  http://localhost:8083/connectors | jq

# a few seconds later (refresh.topics.interval.seconds=5), the TARGET cluster has
# "source.mirror-demo" -- MM2 prefixes mirrored topics with the source alias so
# they never collide with a same-named local topic
docker exec kafka-target kafka-topics --bootstrap-server localhost:9096 --list
docker exec kafka-target kafka-console-consumer --bootstrap-server localhost:9096 \
  --topic source.mirror-demo --from-beginning --max-messages 2 --timeout-ms 10000
```

Optionally, also mirror consumer group offsets so a group can fail over from source to target
and resume roughly where it left off (see `emit.checkpoints.enabled` in
[MIRRORING.md](./MIRRORING.md)):

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  --data @connectors/mirror-checkpoint.json \
  http://localhost:8083/connectors | jq
```

### Inspecting connectors

```bash
curl -s http://localhost:8083/connectors | jq
curl -s http://localhost:8083/connectors/mirror-source/status | jq
curl -s -X DELETE http://localhost:8083/connectors/mirror-source
```

## A deeper look: Single Message Transforms (SMTs)

Connect lets you attach lightweight, stateless transformations to a connector's config —
masking a field, renaming it, dropping it, routing to a different topic based on content —
without writing a custom connector. They run inline on each record as it passes through Connect,
which is the right tool for simple reshaping; anything stateful (joins, aggregation, windowing)
belongs in [stream processing](../6-stream-processing/CONCEPTS.md) instead.

Continue to [5.2 Cross-Cluster Mirroring](./MIRRORING.md).
