# 8. Capstone: Order Processing Pipeline

Three small `Go` microservices, talking only through Kafka, pulling together everything from
stages 1–7:

```
                 POST /orders                    orders topic (6 partitions)
   client  ───────────────────▶  orders-service  ───────────────────▶  (Kafka)
                                                                            │
                                                          consumer group "inventory-service"
                                                                            ▼
                                                                  inventory-service
                                                            (checks/decrements stock per SKU)
                                                                     │            │
                                                        orders-reserved      orders-failed
                                                                     │            │
                                                                     ▼            ▼
                                                          consumer group "notification-service"
                                                                     │
                                                            notification-service
                                                        (simulated flaky notify + retry)
                                                                     │
                                                          (exhausted retries)
                                                                     ▼
                                                               orders-dlq
```

- **orders-service** — [2. Producers](../2-producers-and-consumers/PRODUCERS.md): an HTTP
  handler that produces one `orders` event per request, keyed by order ID.
- **inventory-service** — [2. Consumer groups](../2-producers-and-consumers/CONSUMERS.md) +
  [manual commits](../2-producers-and-consumers/go/manualcommit/): consumes `orders`, makes a
  real decision (enough stock or not), and only commits after successfully publishing the
  outcome.
- **notification-service** — the retry/dead-letter-queue pattern hinted at in [stage
  6's stream-table join](../6-stream-processing/go/streamtablejoin/): consumes both outcome
  topics, retries a flaky "send notification" step, and routes exhausted messages to
  `orders-dlq` instead of dropping them or looping forever.

## Run it

Reuse the cluster from [stage 1](../1-setup/) if it's already up, or bring up a standalone one:

```bash
docker compose up -d   # only if 1-setup's cluster isn't already running
```

Then, in three separate terminals:

```bash
cd go/inventoryservice    && go run .
cd go/notificationservice && go run .
cd go/ordersservice       && go run .   # starts the HTTP API on :8090
```

And place some orders:

```bash
curl -s -X POST localhost:8090/orders -d '{"customerId":"cust-1","sku":"widget","quantity":3,"amount":29.97}'
curl -s -X POST localhost:8090/orders -d '{"customerId":"cust-2","sku":"gadget","quantity":10,"amount":199.90}'
# gadget only has 5 in stock — watch this one come back through orders-failed instead
curl -s -X POST localhost:8090/orders -d '{"customerId":"cust-3","sku":"unknown-sku","quantity":1,"amount":9.99}'
```

Watch the three services' logs: an order flows through `orders` → `inventory-service`'s
decision → `orders-reserved`/`orders-failed` → `notification-service`'s (occasionally retried)
notification attempt. Run enough orders and you'll eventually see one hit `orders-dlq` when the
simulated notification failure persists through all retries. [`go/lagmonitor`](../4-cluster-administration/go/lagmonitor/)
from stage 4 works unmodified against this pipeline's three consumer groups — point it here to
watch lag in a live multi-service pipeline instead of a single toy consumer.

## What's here

| Path | Role |
|---|---|
| [`go/internal/events`](./go/internal/events/) | The shared message schema every service produces/consumes |
| [`go/ordersservice`](./go/ordersservice/) | HTTP → Kafka producer |
| [`go/inventoryservice`](./go/inventoryservice/) | Consumer group → business decision → producer, manual commits |
| [`go/notificationservice`](./go/notificationservice/) | Consumer group → retry → dead-letter queue |
| [`docker-compose.yml`](./docker-compose.yml) | Standalone single-broker cluster, same shape as stage 1's |

## Where to go from here

You've now built the full loop this workshop set out to teach: topics and partitions
([1](../1-setup/)), producers and consumer groups ([2](../2-producers-and-consumers/)),
reliability guarantees ([3](../3-internals-and-reliability/)), running and administering a real
cluster ([4](../4-cluster-administration/)), moving data in/out with Connect and between
clusters with MirrorMaker ([5](../5-connect-and-mirroring/)), stateful stream processing
([6](../6-stream-processing/)), and testing/securing all of it
([7](../7-testing-and-security/)). From here, the natural next steps are the same ones *Kafka:
The Definitive Guide* and *Kafka Streams in Action* point to next: run this against a real
multi-broker production cluster, add schema management (Avro/Protobuf + a schema registry), and
if you're on the JVM, learn the Kafka Streams DSL this workshop's stage 6 approximated by hand.
