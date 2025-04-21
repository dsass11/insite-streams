#!/bin/bash

# Determine script and directory locations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"

# Check if we're in a directory called 'scripts' inside 'playground'
if [[ "$SCRIPT_DIR" == */playground/scripts ]]; then
  # We're in scripts directory inside playground
  cd "$PLAYGROUND_DIR"
elif [[ "$SCRIPT_DIR" == */playground ]]; then
  # We're directly in playground directory
  cd "$SCRIPT_DIR"
else
  # We might be somewhere else - try to guess based on current directory name
  if [[ "$(basename "$(pwd)")" == "scripts" ]] && [[ "$(basename "$(dirname "$(pwd)")")" == "playground" ]]; then
    cd "$(dirname "$(pwd)")"
  elif [[ "$(basename "$(pwd)")" == "playground" ]]; then
    # Already in playground directory
    :
  else
    echo "Warning: Not in playground directory. Creating playground structure here."
    mkdir -p playground
    cd playground
  fi
fi

echo "Initializing playground in $(pwd)"

# Create necessary directories if they don't exist
mkdir -p docker
mkdir -p schemas
mkdir -p events
mkdir -p scripts

# Check if docker-compose.yml exists
if [ ! -f "docker/docker-compose.yml" ]; then
  echo "Error: docker-compose.yml not found in docker directory"
  echo "Please make sure you've copied the docker-compose.yml file to the docker directory"
  exit 1
fi

# Check if schema files exist in schemas directory
if [ ! -f "schemas/pii-schema.json" ]; then
  echo "Checking for sample schema files..."
  if [ -f "events/pii-schema.json" ]; then
    echo "Copying sample schema file from events directory..."
    cp events/pii-schema.json schemas/
  else
    echo "Warning: Schema file not found. Please create schemas/pii-schema.json manually."
  fi
fi

# Check if sample event files exist
if [ ! -f "events/sample-event.json" ]; then
  echo "Warning: No sample event files found in events directory."
  echo "Please create sample event files (e.g., events/sample-event.json) to test with."
fi

# Start Docker containers
echo "Starting Docker containers..."
cd docker
docker compose up -d
if [ $? -ne 0 ]; then
  echo "Error: Failed to start Docker containers"
  echo "Check that Docker is running and docker-compose.yml is valid"
  exit 1
fi
cd ..

# Check if containers are running
sleep 5
if ! docker ps | grep -q kafka; then
  echo "Warning: Kafka container not detected after startup"
  echo "Check Docker logs: docker logs kafka"
else
  echo "Kafka container running successfully"
fi

echo ""
echo "Playground environment initialized!"
echo "----------------------------------------"
echo "To start the Flink application: ./scripts/start-pii-stream.sh"
echo "To send events: ./scripts/send-event.sh events/sample-event.json"
echo "To read output: ./scripts/read-output.sh"
echo "To update schema: ./scripts/update-schema.sh schemas/pii-schema-updated.json"
echo "----------------------------------------"
echo "Kafka UI available at: http://localhost:9021"
echo "----------------------------------------"