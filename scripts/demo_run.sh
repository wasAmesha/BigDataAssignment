#!/bin/bash

echo "Starting Kafka demonstration..."

echo "1. Starting Kafka containers..."
docker-compose up -d

echo "2. Creating required topics..."
bash scripts/create_topics.sh

echo "3. Starting Consumer..."
python consumer.py &
CONSUMER_PID=$!

sleep 3

echo "4. Starting Producer..."
python producer.py

echo "Producer finished sending messages."
echo "Consumer is still running with PID $CONSUMER_PID"

echo "To stop the consumer, run: kill $CONSUMER_PID"
