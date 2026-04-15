#!/bin/bash
# docker/kafka-init.sh
# Creates Kafka topics for crypto streams after Kafka is healthy.

set -e

KAFKA_HOST="${KAFKA_ADVERTISED_HOST:-kafka}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
KAFKA="kafka-topics.sh --bootstrap-server ${KAFKA_HOST}:${KAFKA_PORT}"

echo "[kafka-init] Waiting for Kafka to be ready..."
until $KAFKA --list &>/dev/null; do
  sleep 2
done

echo "[kafka-init] Kafka is ready. Creating crypto topics..."

# crypto_trades — 6 partitions, replication factor 1
$KAFKA --create --if-not-exists \
  --topic crypto_trades \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000

# crypto_klines — 6 partitions, replication factor 1
$KAFKA --create --if-not-exists \
  --topic crypto_klines \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "[kafka-init] Crypto topics created:"
$KAFKA --list | grep crypto

echo "[kafka-init] Done."
