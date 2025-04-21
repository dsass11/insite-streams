#!/bin/bash

# Determine script and directory locations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$PLAYGROUND_DIR")"

# Go to project root
cd "$PROJECT_ROOT"
echo "Building project at $(pwd)..."

# Build the main project
mvn clean install -DskipTests

# Create Kafka topics
cd "$PLAYGROUND_DIR"
echo "Setting up Kafka topics..."
docker exec kafka kafka-topics --create --if-not-exists --topic pii-input --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --create --if-not-exists --topic pii-output --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1

# Make sure schema directory exists
mkdir -p schemas

# Copy the schema file if it doesn't exist
if [ ! -f "schemas/pii-schema.json" ]; then
  echo "Setting up schema file..."
  if [ -f "events/pii-schema.json" ]; then
    cp events/pii-schema.json schemas/
  else
    echo "Warning: Schema file not found. Please create it manually."
  fi
fi

# Create runner directory if it doesn't exist
mkdir -p "$PLAYGROUND_DIR/runner"

# Copy the runner POM file if it doesn't exist
if [ ! -f "$PLAYGROUND_DIR/runner/pom.xml" ]; then
  echo "Setting up runner POM file..."
  cp "$SCRIPT_DIR/runner-pom.xml" "$PLAYGROUND_DIR/runner/pom.xml"
fi

# Run using the runner POM
cd "$PLAYGROUND_DIR/runner"
echo "Running the application with explicit dependencies..."

mvn exec:java -Dexec.mainClass="com.insite.streams.common.StreamRunner" \
  -Dexec.args="com.insite.streams.pii.PIIStream bootstrap-servers=localhost:29092 input-topic=pii-input output-topic=pii-output schema-path=$PLAYGROUND_DIR/schemas/pii-schema.json group-id=pii-processor-1 parallelism=1 schema.refresh.interval.minutes=1" \
  -Dexec.cleanupDaemonThreads=false \
  -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"

# debug command
# mvn exec:java \
#  -Dexec.mainClass="com.insite.streams.common.StreamRunner" \
#  -Dexec.args="com.insite.streams.pii.PIIStream bootstrap-servers=localhost:29092 input-topic=pii-input output-topic=pii-output schema-path=$PLAYGROUND_DIR/schemas/pii-schema.json group-id=pii-processor-1 parallelism=1 schema.refresh.interval.minutes=1" \
#  -Dexec.cleanupDaemonThreads=false \
#  -Dexec.jvmArgs="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005 --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED"
