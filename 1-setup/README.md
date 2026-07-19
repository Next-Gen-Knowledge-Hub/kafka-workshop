# Setup a simple Kafka cluster

Read [1.1 What is Kafka?](./OVERVIEW.md) first if you haven't — this page is only about
getting a broker running locally.

## Option A: ZooKeeper mode (`docker-compose.yml`)

This is the classic Kafka deployment model described in *Kafka: The Definitive Guide*: a
ZooKeeper ensemble stores cluster metadata (broker list, topic config, ACLs, and — critically
— elects the controller), and Kafka brokers register themselves with it.

```bash
docker compose up -d
docker compose ps
```

- Broker (from your host machine): `localhost:9092`
- Broker (from another container on the compose network): `kafka:29092`
- ZooKeeper: `localhost:2181`
- Kafka UI: http://localhost:8080

## Option B: KRaft mode (`docker-compose.kraft.yml`)

Since Kafka 3.3+ (Confluent Platform 7.5+), Kafka can run without ZooKeeper: the brokers
themselves manage cluster metadata via the Raft consensus protocol (KRaft = "Kafka Raft").
Fewer moving parts, faster controller failover, one process to reason about. Everything from
stage 2 onward works identically against either mode — only the compose file and infra
changes.

```bash
docker compose -f docker-compose.kraft.yml up -d
```

- Broker: `localhost:9092` (host) / `kafka-kraft:29092` (containers)
- Kafka UI: http://localhost:8080

> Pick one mode and stick with it for the rest of the workshop to avoid port clashes on 9092 —
> stop one stack (`docker compose down`) before starting the other.

## Pre-create the workshop's topics

Auto-creation (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`) is on for convenience, but the very
first write to a brand-new topic can race the partition leader election and fail once with
`Leader Not Available`. Avoid that — and get sane partition counts — by pre-creating every
topic this workshop uses:

```bash
cd ..   # repo root
./scripts/create-topics.sh
```

Re-run it any time; it skips topics that already exist.

## Verifying the cluster from the CLI

The `cp-kafka` image ships the full Kafka CLI toolkit. You can exec into the running broker
container to use it without installing anything locally:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic greetings --partitions 3 --replication-factor 1

docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic greetings
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings --from-beginning
```

Or point [kcat](https://github.com/edenhill/kcat) at `localhost:9092` if you have it installed
locally. Kafka UI (http://localhost:8080) gives you the same operations with a browser.

## Connecting with `Go`

We use [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go) throughout this workshop.
It's a pure-Go client — no `librdkafka`/cgo toolchain required, which keeps `go run` working
out of the box on any machine with Go installed.

```go
import "github.com/segmentio/kafka-go"

w := &kafka.Writer{
    Addr:     kafka.TCP("localhost:9092"),
    Topic:    "greetings",
    Balancer: &kafka.LeastBytes{},
}
defer w.Close()

err := w.WriteMessages(context.Background(), kafka.Message{
    Key:   []byte("hello"),
    Value: []byte("world"),
})
```

## 1.3 Hello World

Now write and read your first message with Go: [`helloworld/`](./helloworld/).

```bash
cd helloworld
go run ./producer
go run ./consumer
```

## Stop / clean up

```bash
docker compose down          # stop, keep data volumes
docker compose down -v       # stop and wipe all topic data + ZK/KRaft metadata
```
