#!/bin/bash

# Determine script and directory locations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"

# Check if we're in a directory called 'scripts' inside 'playground'
if [[ "$SCRIPT_DIR" == */playground/scripts ]]; then
  # We're in scripts directory inside playground
  EVENTS_DIR="$PLAYGROUND_DIR/events"
elif [[ "$SCRIPT_DIR" == */playground ]]; then
  # We're directly in playground directory
  EVENTS_DIR="$SCRIPT_DIR/events"
else
  # We might be somewhere else - try to guess based on current directory name
  if [[ "$(basename "$(pwd)")" == "scripts" ]] && [[ "$(basename "$(dirname "$(pwd)")")" == "playground" ]]; then
    EVENTS_DIR="$(dirname "$(pwd)")/events"
  elif [[ "$(basename "$(pwd)")" == "playground" ]]; then
    EVENTS_DIR="$(pwd)/events"
  else
    # Default to assuming events is a subdirectory of current location
    EVENTS_DIR="./events"
    echo "Warning: Unable to determine exact location. Assuming events directory is at $EVENTS_DIR"
  fi
fi

# Check if file argument provided
if [ -z "$1" ]; then
  echo "Usage: $0 <event-json-file>"
  echo "Available events in $EVENTS_DIR:"
  ls -la "$EVENTS_DIR"/*.json 2>/dev/null || echo "No JSON files found"
  exit 1
fi

# Determine file path - if it's a relative path that doesn't exist, try to find it in events dir
EVENT_FILE="$1"
if [ ! -f "$EVENT_FILE" ] && [[ "$EVENT_FILE" != /* ]]; then
  # Try in events directory
  if [ -f "$EVENTS_DIR/$(basename "$EVENT_FILE")" ]; then
    EVENT_FILE="$EVENTS_DIR/$(basename "$EVENT_FILE")"
  elif [ -f "$EVENTS_DIR/$EVENT_FILE" ]; then
    EVENT_FILE="$EVENTS_DIR/$EVENT_FILE"
  fi
fi

# Check if file exists
if [ ! -f "$EVENT_FILE" ]; then
  echo "Error: Event file not found: $1"
  echo "Available events in $EVENTS_DIR:"
  ls -la "$EVENTS_DIR"/*.json 2>/dev/null || echo "No JSON files found"
  exit 1
fi

# Get the file content
echo "Reading event from $EVENT_FILE"
EVENT_DATA=$(cat "$EVENT_FILE")

# Check if we have content
if [ -z "$EVENT_DATA" ]; then
  echo "Error: Event file is empty: $EVENT_FILE"
  exit 1
fi

# Send to Kafka
echo "Sending event to Kafka topic pii-input..."
echo "$EVENT_DATA" | tr -d '\n' | docker exec -i kafka kafka-console-producer --topic pii-input --bootstrap-server kafka:9092

if [ $? -eq 0 ]; then
  echo "Event sent successfully!"
else
  echo "Error: Failed to send event to Kafka"
  echo "Make sure Kafka is running: docker ps | grep kafka"
  exit 1
fi