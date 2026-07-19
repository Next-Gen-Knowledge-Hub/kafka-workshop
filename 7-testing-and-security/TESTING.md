# Testing a Kafka Application

## What actually needs testing

Split your Kafka-touching code into two categories, and test them differently:

- **Business logic that happens to run inside a message handler** — masking a field, computing
  a total, deciding whether an order is valid. This has nothing to do with Kafka; extract it
  into a plain function and unit test it exactly like any other Go function, no broker
  involved.
- **The actual integration with Kafka** — did I subscribe to the right topic, does my consumer
  commit offsets at the right time, does my producer partition the way I expect, does my
  exactly-once transaction actually roll back on failure. This *does* need a real (or
  real-enough) broker, because the thing under test is the interaction with Kafka's protocol,
  which no amount of mocking faithfully reproduces — this is the same lesson *Kafka Streams in
  Action* applies via its topology test driver and embedded-broker integration tests.

## Integration testing against a real broker

This workshop doesn't reach for a mocking library or an embedded/in-memory broker — every
example so far has run against the actual `cp-kafka` container from [stage
1](../1-setup/docker-compose.yml), and the integration test in
[`go/kafkatest`](./go/kafkatest/) does the same thing:

```bash
cd ../1-setup && docker compose up -d   # if it isn't already running
cd ../7-testing-and-security/go/kafkatest
go test -v ./...
```

The test dials `localhost:9092` first and calls `t.Skip` with a clear message if nothing
answers — so `go test ./...` from the repo root doesn't explode in an environment with no
broker running (a laptop with Docker not started, a CI job that hasn't provisioned Kafka yet),
but still runs for real whenever a broker is available. That's a deliberate middle ground
between "always mocked, never actually proves anything" and "hard-fails everywhere Kafka isn't
running."

## What the test actually verifies

1. Creates a throwaway topic with a random suffix (so parallel test runs never collide).
2. Produces a known set of messages with `acks=all`.
3. Consumes them back with a fresh consumer group and asserts every message arrived, with the
   right key/value and no duplicates.
4. Cleans the topic up afterward.

This is the same round-trip shape as [stage 3's idempotent producer
verification](../3-internals-and-reliability/go/idempotentproducer/) — "does what I produced
match what I can read back" is the core assertion behind almost every Kafka integration test
you'll write, whether you're testing a raw producer/consumer or a full stream-processing
topology.

## Testing stream processing specifically

For a real Kafka Streams (Java) topology, `TopologyTestDriver` lets you feed input records and
assert output records *without* a running broker at all — it drives the topology's processing
logic directly. There's no Go equivalent (no Go stream-processing DSL to test), but the
principle carries over directly to the hand-rolled examples in [stage
6](../6-stream-processing/): the aggregation/windowing/join logic is plain Go functions
operating on plain data structures, so unit-test *that* directly, and reserve integration tests
(like this stage's) for "does it actually read and write Kafka correctly."

Continue to [7.2 Securing Kafka](./SECURITY.md).
