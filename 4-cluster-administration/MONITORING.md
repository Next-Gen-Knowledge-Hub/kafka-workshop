# Monitoring Kafka

## Where the metrics come from

Kafka brokers and Java clients expose metrics via JMX. Since this workshop's clients are `Go`
programs (no JMX), we monitor the two things that matter most from *outside* the process:
broker-reported partition/replication state, and consumer group offsets — both available
through the same Admin API [`go/topicadmin`](./go/topicadmin/) already uses.

## The metric that matters most: under-replicated partitions

A partition is **under-replicated** when its ISR is smaller than its replica count — some
replica has fallen behind the leader. This is *the* broker health signal to alert on: it means
you're one more failure away from either unavailability (if it drops below
`min.insync.replicas`) or data loss (if the lagging replica is later elected leader via unclean
leader election). Everything else broker-side (request latency, log flush latency, throughput)
is useful for capacity planning; under-replicated partitions is the "wake someone up" metric.

```bash
kafka-topics --bootstrap-server localhost:9093 --describe --under-replicated-partitions
```

## Client-side: producer and consumer metrics

- **Producer** — request latency, batch size, error/retry rate, buffer-available-bytes (if
  this drops to zero, the app is producing faster than the broker can absorb, and `Send` calls
  will start blocking).
- **Consumer** — poll rate, records-consumed-rate, and above all: **lag**.

## Consumer lag

Lag is the gap between a partition's log-end-offset (the newest message) and a consumer
group's committed offset for that partition — how far behind the consumer is. It's the single
best indicator of whether a streaming pipeline is keeping up:

```
log-end-offset:      1,000,000
committed offset:      998,500
lag:                      1,500   <- messages waiting to be processed
```

Rising lag over time means the group can't keep up with the topic's write rate — either add
more consumers (up to the partition count) or speed up per-record processing. Flat, non-zero
lag that never grows is often fine; it just means the group's poll loop has a steady-state
delay.

```bash
kafka-consumer-groups --bootstrap-server localhost:9093 --describe --group inventory-service
```

[`go/lagmonitor`](./go/lagmonitor/) computes the same numbers programmatically and prints total
lag per group/topic — the shape of a check you'd wire into Prometheus/alerting in production.

## End-to-end monitoring

Broker and client metrics tell you the pipeline is *healthy*; they don't tell you a specific
message actually made it from producer to consumer within an acceptable time. For that, teams
often run a synthetic "canary" producer/consumer pair that continuously round-trips a message
through a dedicated topic and measures the elapsed time — catching problems (like a
misconfigured ACL silently dropping a consumer) that per-component metrics can miss entirely.

Continue to [stage 5: Data Pipelines & Cross-Cluster Mirroring](../5-connect-and-mirroring/CONNECT.md).
