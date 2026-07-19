# 5. Data Pipelines & Cross-Cluster Mirroring

Read first:

- [5.1 Kafka Connect](./CONNECT.md) — source/sink connectors, standalone vs. distributed mode,
  when to use Connect instead of a custom producer/consumer.
- [5.2 Cross-Cluster Mirroring](./MIRRORING.md) — MirrorMaker 2, hub-and-spoke, active-active,
  active-standby.

This stage's compose file spins up two independent clusters plus a Connect worker (separate
from stages 1/4), so it's fine to run standalone:

```bash
docker compose -f docker-compose.connect.yml up -d
# source cluster         -> localhost:9092
# target cluster         -> localhost:9096
# Kafka Connect REST API -> http://localhost:8083
# Kafka UI (both clusters) -> http://localhost:8080
```

Then walk through the source → target mirroring example in [CONNECT.md](./CONNECT.md#running-it)
using the connector configs in [`connectors/`](./connectors/).

## What's here

| Path | Purpose |
|---|---|
| [`docker-compose.connect.yml`](./docker-compose.connect.yml) | Two single-broker clusters (`source`, `target`) + a Kafka Connect worker + Kafka UI wired to both |
| [`connectors/mirror-source.json`](./connectors/mirror-source.json) | `MirrorSourceConnector`: replicates the `mirror-demo` topic from `source` to `target` |
| [`connectors/mirror-checkpoint.json`](./connectors/mirror-checkpoint.json) | `MirrorCheckpointConnector`: replicates consumer group offsets so a group can fail over |

No `Go` code in this stage — Connect and MirrorMaker are configuration-driven infrastructure,
not client libraries. That said, everything they do (produce/consume with offset tracking) is
exactly what you already built by hand in [stage 2](../2-producers-and-consumers/); Connect's
value is not reinventing that per integration.
