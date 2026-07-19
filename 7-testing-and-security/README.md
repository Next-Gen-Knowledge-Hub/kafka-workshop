# 7. Testing & Security

Read first:

- [7.1 Testing a Kafka application](./TESTING.md) — what to unit test vs. integration test, and
  why this workshop tests against a real broker instead of mocking one.
- [7.2 Securing Kafka](./SECURITY.md) — TLS, SASL, ACLs, and how little of your client code
  changes when you turn them on.

## Run the integration test

```bash
cd ../1-setup && docker compose up -d   # if it isn't already running
cd ../7-testing-and-security/go/kafkatest
go test -v ./...
```

## What's here

| Path | Demonstrates |
|---|---|
| [`go/kafkatest`](./go/kafkatest/) | An integration test that produces, consumes, and verifies a round trip against a real broker — skips cleanly if none is reachable |

This closes out the "how do I trust this pipeline" half of the workshop. [Stage
8](../8-capstone-order-pipeline/) is the capstone: a small `Go` microservice pipeline that
exercises everything from stages 1–7 at once.
