# 2. Producers & Consumers

This stage is about the client-side mechanics of talking to Kafka: how a producer decides
where a message goes and when it's considered "sent", and how a consumer group divides up
partitions and tracks its progress.

Read first:

- [2.1 Producer internals](./PRODUCERS.md)
- [2.2 Consumer internals](./CONSUMERS.md)

Then run, against the cluster from [stage 1](../1-setup/):

```bash
cd go/syncproducer      && go run .
cd ../asyncproducer      && go run .
cd ../keyedpartitioning  && go run .
```

For the consumer-group demo, open two or three terminals so you can see partitions being
assigned across multiple consumer instances in the same group:

```bash
# terminal 1, 2, 3 ...
cd go/consumergroup && go run .
```

Watch the log lines: each instance only receives messages from the partitions it was assigned,
and if you kill one, the others pick up its partitions after a short rebalance pause.

```bash
cd ../manualcommit && go run .
```

`manualcommit` demonstrates committing offsets only after a (simulated) side effect succeeds,
and shows what happens on restart if you comment out the commit call.

## What's here

| Path | Demonstrates |
|---|---|
| [`go/syncproducer`](./go/syncproducer/) | Blocking send, checking the result of every write |
| [`go/asyncproducer`](./go/asyncproducer/) | Non-blocking send with a completion callback |
| [`go/keyedpartitioning`](./go/keyedpartitioning/) | Keyed messages landing on the same partition; a custom partitioner |
| [`go/consumergroup`](./go/consumergroup/) | Multiple consumers in one group splitting partitions, live rebalancing |
| [`go/manualcommit`](./go/manualcommit/) | At-least-once processing via explicit offset commits |
