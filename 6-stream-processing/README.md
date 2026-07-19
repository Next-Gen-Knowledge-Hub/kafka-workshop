# 6. Stream Processing

Read [6.1 Concepts](./CONCEPTS.md) first — streams vs. tables, state, time, windows, joins, and
a note on why this stage uses hand-rolled `Go` instead of the Kafka Streams DSL (Java-only).

Run against the cluster from [stage 1](../1-setup/):

```bash
cd go/wordcount
go run . seed
go run . run

cd ../windowedaggregation
go run . seed
go run . run

cd ../streamtablejoin
go run . seed-table
go run . seed-stream
go run . run
```

Each example follows the same shape: `seed` writes synthetic input, `run` processes it and
prints/publishes the result, then exits once its input goes idle for a few seconds — no need to
Ctrl+C.

## What's here

| Path | Demonstrates |
|---|---|
| [`go/wordcount`](./go/wordcount/) | Stream → table aggregation: a running count per key, republished as a changelog |
| [`go/windowedaggregation`](./go/windowedaggregation/) | Event-time tumbling windows, computed per key |
| [`go/streamtablejoin`](./go/streamtablejoin/) | A live stream joined against a table materialized from a compacted-style changelog topic |

## What's next

[Stage 7](../7-testing-and-security/TESTING.md) covers testing pipelines like these against a
real broker, and the security controls (SASL, TLS, ACLs) you'd put in front of them in
production. [Stage 8](../8-capstone-order-pipeline/) is the capstone: a small but complete
`Go` microservice pipeline pulling together producers, consumer groups, and the
enrichment/join pattern from this stage.
