# 3. Internals & Reliable Delivery

Read first:

- [3.1 Broker internals](./INTERNALS.md) — controller, replication, ISR, storage, compaction.
- [3.2 Reliable data delivery](./RELIABILITY.md) — acks, `min.insync.replicas`, idempotence,
  transactions.

## A note on client libraries in this stage

Every other stage in this workshop uses
[`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) — it's simple and pure Go, which
is exactly what you want for producer/consumer basics. But `kafka-go`'s public API does not
implement the wire-level idempotent-producer or transaction protocol (no producer ID/sequence
number management, no `InitProducerId`/`AddPartitionsToTxn` orchestration on the write path).

So this stage uses [`twmb/franz-go`](https://github.com/twmb/franz-go) instead — another pure
Go client, with idempotent production enabled by default and a purpose-built
`GroupTransactSession` helper for the "consume, transform, produce" exactly-once pattern. Both
are legitimate choices in the Go Kafka ecosystem; picking the right one for the job (and
knowing *why* `kafka-go` isn't enough here) is itself part of what this stage teaches. In a
Java codebase this distinction doesn't exist — the standard client supports all of this out of
the box, which is one reason it remains the reference implementation in both source books.

## What's here

| Path | Demonstrates |
|---|---|
| [`go/idempotentproducer`](./go/idempotentproducer/) | Idempotent production (enabled by default in `franz-go`): duplicate-free delivery under client-side retries |
| [`go/transactionalproducer`](./go/transactionalproducer/) | Exactly-once consume → transform → produce using `GroupTransactSession` |

## Run it

```bash
cd go/idempotentproducer && go run .

cd ../transactionalproducer
go run . seed    # writes sample input orders to orders-exactly-once-in
go run . run     # consumes them transactionally, produces to orders-exactly-once-out
```

Watch `orders-exactly-once-out` with Kafka UI (http://localhost:8080) filtering on
`isolation.level=read_committed` semantics — only committed transactions' output ever becomes
visible to a `read_committed` consumer, which is exactly what `transactionalproducer` uses to
verify its own output.
