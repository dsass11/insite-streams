#!/bin/bash

# Determine script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"

echo "Reading from Kafka topic pii-output..."
echo "To exit, press Ctrl+C"
echo "Waiting for messages..."

# Check if Kafka container is running
if ! docker ps | grep -q kafka; then
  echo "Error: Kafka container is not running"
  echo "Start Kafka using: cd docker && docker-compose up -d"
  exit 1
fi

# Offer options for reading
FROM_BEGINNING=""
if [ "$1" == "--from-beginning" ] || [ "$1" == "-b" ]; then
  FROM_BEGINNING="--from-beginning"
  echo "Reading all messages from the beginning"
else
  echo "Reading only new messages (use --from-beginning or -b to read all)"
fi

# Read from Kafka output topic
docker exec kafka kafka-console-consumer --topic pii-output --bootstrap-server kafka:9092 $FROM_BEGINNING