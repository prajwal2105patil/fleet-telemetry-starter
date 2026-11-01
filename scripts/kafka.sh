#!/usr/bin/env bash
# Kafka helper commands (run from the repo root, with `docker compose up -d` running).
#
#   ./scripts/kafka.sh topics                     list topics
#   ./scripts/kafka.sh describe telemetry.raw     partitions, leaders
#   ./scripts/kafka.sh consume telemetry.raw      print messages (with keys) from the beginning, Ctrl+C to stop
#   ./scripts/kafka.sh consume telemetry.dlq
#   ./scripts/kafka.sh lag                        consumer-group lag per partition for group telemetry-writer
#   ./scripts/kafka.sh reset-offsets              rewind telemetry-writer to earliest (stop your consumer first)
set -euo pipefail
K="docker compose exec -T kafka /opt/kafka/bin"
B="--bootstrap-server localhost:9092"
GROUP="${GROUP:-telemetry-writer}"

case "${1:-help}" in
  topics)   $K/kafka-topics.sh $B --list ;;
  describe) $K/kafka-topics.sh $B --describe --topic "${2:?topic}" ;;
  consume)  $K/kafka-console-consumer.sh $B --topic "${2:?topic}" --from-beginning \
              --property print.key=true --property print.partition=true --property key.separator=" | " ;;
  lag)      $K/kafka-consumer-groups.sh $B --describe --group "$GROUP" ;;
  reset-offsets) $K/kafka-consumer-groups.sh $B --group "$GROUP" --reset-offsets --to-earliest --all-topics --execute ;;
  *) sed -n '2,9p' "$0" ;;
esac
