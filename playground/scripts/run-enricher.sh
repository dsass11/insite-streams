#!/bin/bash

# Determine script and directory locations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$PLAYGROUND_DIR")"

# Default configuration values
DEFAULT_CONFIG="bootstrap-servers=localhost:29092"
DEFAULT_CONFIG="$DEFAULT_CONFIG input-topic=pii-output"
DEFAULT_CONFIG="$DEFAULT_CONFIG group-id=enricher-processor-1"
DEFAULT_CONFIG="$DEFAULT_CONFIG schema-path=$PLAYGROUND_DIR/schemas/trail_schema.json"
DEFAULT_CONFIG="$DEFAULT_CONFIG iceberg-catalog=hadoop_catalog"
DEFAULT_CONFIG="$DEFAULT_CONFIG iceberg-database=prod"
DEFAULT_CONFIG="$DEFAULT_CONFIG iceberg-table=prod.enrichment_ice"
DEFAULT_CONFIG="$DEFAULT_CONFIG parallelism=1"
DEFAULT_CONFIG="$DEFAULT_CONFIG schema.refresh.interval.seconds=300"
DEFAULT_CONFIG="$DEFAULT_CONFIG checkpoint.interval=10000"
DEFAULT_CONFIG="$DEFAULT_CONFIG checkpoint.dir=file:///tmp/flink-checkpoints"
DEFAULT_CONFIG="$DEFAULT_CONFIG filter.invalid.records=true"
DEFAULT_CONFIG="$DEFAULT_CONFIG catalog.property.warehouse=$PLAYGROUND_DIR/iceberg-warehouse"

# Go to project root
cd "$PROJECT_ROOT"
echo "Building project at $(pwd)..."

# Build the project
mvn clean package -DskipTests

# Create Iceberg warehouse directory
ICEBERG_WAREHOUSE="$PLAYGROUND_DIR/iceberg-warehouse"
if [ ! -d "$ICEBERG_WAREHOUSE" ]; then
    echo "Creating Iceberg warehouse directory at $ICEBERG_WAREHOUSE"
    mkdir -p "$ICEBERG_WAREHOUSE"
fi

# Run the EnricherStream application
cd "$PROJECT_ROOT"
echo "Starting EnricherStream application from $(pwd)..."

# Merge default config with user-provided parameters
CONFIG="$DEFAULT_CONFIG"
for PARAM in "$@"; do
    CONFIG="$CONFIG $PARAM"
done

java -cp enricher/target/enricher-1.0-SNAPSHOT.jar com.insite.streams.common.StreamRunner \
  com.insite.streams.enricher.EnricherStream \
  $CONFIG
