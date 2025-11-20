#!/bin/bash

KAFKA_CONTAINER="assignmentproject-kafka-1"

echo "Creating Kafka topics..."

docker exec -it $KAFKA_CONTAINER kafka-topics --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

docker exec -it $KAFKA_CONTAINER kafka-topics --create \
  --topic orders-dlq \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it $KAFKA_CONTAINER kafka-topics --create \
  --topic orders-aggregates \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

echo "Listing all topics:"
docker exec -it $KAFKA_CONTAINER kafka-topics --list --bootstrap-server localhost:9092
