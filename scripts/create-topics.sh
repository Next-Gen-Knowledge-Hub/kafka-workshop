#!/usr/bin/env bash
# Pre-creates every topic used across the workshop against the broker started by
# 1-setup/docker-compose.yml (or docker-compose.kraft.yml). Run this once after
# bringing the cluster up:
#
#   ./scripts/create-topics.sh
#
# Relying on auto.create.topics.enable works too, but the first producer to touch
# a brand-new topic can hit a transient "Leader Not Available" while the partition
# leader election finishes — creating topics up front (with deliberate partition
# counts) avoids that race and mirrors how you'd actually provision topics in a
# real cluster (see stage 4, ADMIN.md).
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP_SERVER:-localhost:9092}"

# name:partitions:replication-factor
TOPICS=(
  "greetings:3:1"
  "page-views:3:1"
  "customer-events:4:1"
  "orders:6:1"
  "order-events:3:1"
  "orders-idempotent:3:1"
  "orders-exactly-once-in:3:1"
  "orders-exactly-once-out:3:1"
  "word-count-input:3:1"
  "word-count-output:3:1"
  "stock-ticks:3:1"
  "stock-window-output:3:1"
  "customer-profiles:3:1"
  "enriched-orders:3:1"
  "connect-demo:3:1"
  "orders-reserved:3:1"
  "orders-failed:3:1"
  "orders-dlq:3:1"
)

for entry in "${TOPICS[@]}"; do
  IFS=':' read -r name partitions rf <<<"$entry"
  if docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" >/dev/null 2>&1; then
    echo "exists:  $name"
  else
    docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" \
      --create --topic "$name" --partitions "$partitions" --replication-factor "$rf" >/dev/null
    echo "created: $name (partitions=$partitions, rf=$rf)"
  fi
done

docker exec "$CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
