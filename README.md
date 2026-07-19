# Kafka Workshop

A hands-on workshop around Apache Kafka: its architecture, its guarantees, and how to build
real event-driven systems on top of it with `Go`. The material follows *Kafka: The Definitive
Guide* (Narkhede, Shapira, Palino) for broker/client internals and operations, and *Kafka
Streams in Action* (Bejeck) for stream-processing concepts. Every stage runs against a real
Kafka cluster in Docker, and every concept has a runnable `Go` program next to it.

This repository contains the following topics

1. [Setup](./1-setup/)
    - 1.1 [What is Kafka? (pub/sub, topics, partitions, brokers, clusters)](./1-setup/OVERVIEW.md)
    - 1.2 [Run Kafka locally with Docker (ZooKeeper mode & KRaft mode)](./1-setup/README.md)
    - 1.3 [Hello World: first producer & consumer in `Go`](./1-setup/helloworld/)
2. [Producers & Consumers](./2-producers-and-consumers/)
    - 2.1 [Producer internals: batching, acks, partitioner, compression, serializers](./2-producers-and-consumers/PRODUCERS.md)
      - 2.1.1 [Synchronous send](./2-producers-and-consumers/go/syncproducer/)
      - 2.1.2 [Asynchronous send with callbacks](./2-producers-and-consumers/go/asyncproducer/)
      - 2.1.3 [Keyed messages & custom partitioning](./2-producers-and-consumers/go/keyedpartitioning/)
    - 2.2 [Consumer internals: groups, rebalancing, offsets, the poll loop](./2-producers-and-consumers/CONSUMERS.md)
      - 2.2.1 [Consumer groups & parallelism](./2-producers-and-consumers/go/consumergroup/)
      - 2.2.2 [Manual offset commits](./2-producers-and-consumers/go/manualcommit/)
3. [Internals & Reliable Delivery](./3-internals-and-reliability/)
    - 3.1 [Broker internals: controller, replication, ISR, log segments, compaction](./3-internals-and-reliability/INTERNALS.md)
    - 3.2 [Reliability guarantees: acks, min.insync.replicas, idempotence, transactions](./3-internals-and-reliability/RELIABILITY.md)
      - 3.2.1 [Idempotent producer](./3-internals-and-reliability/go/idempotentproducer/)
      - 3.2.2 [Transactional (exactly-once) producer](./3-internals-and-reliability/go/transactionalproducer/)
4. [Cluster Administration & Monitoring](./4-cluster-administration/)
    - 4.1 [A 3-broker cluster in Docker](./4-cluster-administration/docker-compose.cluster.yml)
    - 4.2 [Topic operations, consumer group management, ACLs](./4-cluster-administration/ADMIN.md)
      - 4.2.1 [Admin client in `Go`](./4-cluster-administration/go/topicadmin/)
    - 4.3 [Monitoring: broker/client metrics, under-replicated partitions, consumer lag](./4-cluster-administration/MONITORING.md)
      - 4.3.1 [Consumer lag monitor in `Go`](./4-cluster-administration/go/lagmonitor/)
5. [Data Pipelines & Cross-Cluster Mirroring](./5-connect-and-mirroring/)
    - 5.1 [Kafka Connect: source/sink connectors, when to use Connect vs. a custom client](./5-connect-and-mirroring/CONNECT.md)
    - 5.2 [Cross-cluster mirroring: MirrorMaker 2, hub-and-spoke, active-active](./5-connect-and-mirroring/MIRRORING.md)
6. [Stream Processing](./6-stream-processing/)
    - 6.1 [Concepts: streams vs. tables, state, time, windows, joins](./6-stream-processing/CONCEPTS.md)
      - 6.1.1 [Word count (stateful aggregation)](./6-stream-processing/go/wordcount/)
      - 6.1.2 [Windowed aggregation (stock ticker stats)](./6-stream-processing/go/windowedaggregation/)
      - 6.1.3 [Stream-table join (order enrichment)](./6-stream-processing/go/streamtablejoin/)
7. [Testing & Security](./7-testing-and-security/)
    - 7.1 [Testing producers/consumers with a real broker in Docker](./7-testing-and-security/TESTING.md)
      - 7.1.1 [Integration test example](./7-testing-and-security/go/kafkatest/)
    - 7.2 [Securing Kafka: SASL, TLS, ACLs](./7-testing-and-security/SECURITY.md)
8. [Capstone: Order Processing Pipeline (`Go` microservices)](./8-capstone-order-pipeline/)
    - [X] 8.1 `orders-service` (producer, REST -> Kafka)
    - [X] 8.2 `inventory-service` (consumer group + producer, reserves stock, emits results)
    - [X] 8.3 `notification-service` (consumer, dead-letter handling)
    - [X] 8.4 Standalone docker-compose cluster (Kafka + kafka-ui) to run the three services against

## Prerequisites

- Docker & Docker Compose
- Go 1.23+
- (optional) `kcat`/`kafkacat` and the Kafka CLI tools bundled in the `cp-kafka` image for
  poking at the cluster from the command line

## Quick start

```bash
cd 1-setup
docker compose up -d
# Kafka UI  -> http://localhost:8080
# Broker    -> localhost:9092 (host) / kafka:29092 (from other containers)

cd ..
./scripts/create-topics.sh   # pre-create every topic the workshop uses

cd 1-setup/helloworld
go run ./producer
go run ./consumer
```

Each numbered stage is self-contained: it has its own `README.md`, its own `docker-compose`
file where one is needed, and runnable `Go` programs under `go/`. Work through them in order —
stage 4 assumes stage 1's cluster concepts, stage 6 assumes stage 2's producer/consumer
mechanics, and the capstone in stage 8 pulls every previous stage together.

Feel free to use and make any change ;)
