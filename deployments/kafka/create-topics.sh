#!/usr/bin/env bash
# Creates the application topics. Safe to re-run (--if-not-exists).
# Partition counts are the upper bound on consumer parallelism per group;
# see docs/adr/0002-kafka.md.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

create() { # name partitions
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

create sms.express 6
create sms.normal 12
create sms.retry 6
create sms.dlq 3
create sms.status 6

"$KT" --bootstrap-server "$BOOTSTRAP" --list
