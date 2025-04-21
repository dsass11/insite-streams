#!/bin/bash

# Determine script and directory locations
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"

# Check if we're in a directory called 'scripts' inside 'playground'
if [[ "$SCRIPT_DIR" == */playground/scripts ]]; then
  # We're in scripts directory inside playground
  SCHEMAS_DIR="$PLAYGROUND_DIR/schemas"
elif [[ "$SCRIPT_DIR" == */playground ]]; then
  # We're directly in playground directory
  SCHEMAS_DIR="$SCRIPT_DIR/schemas"
else
  # We might be somewhere else - try to guess based on current directory name
  if [[ "$(basename "$(pwd)")" == "scripts" ]] && [[ "$(basename "$(dirname "$(pwd)")")" == "playground" ]]; then
    SCHEMAS_DIR="$(dirname "$(pwd)")/schemas"
  elif [[ "$(basename "$(pwd)")" == "playground" ]]; then
    SCHEMAS_DIR="$(pwd)/schemas"
  else
    # Default to assuming schemas is a subdirectory of current location
    SCHEMAS_DIR="./schemas"
    echo "Warning: Unable to determine exact location. Assuming schemas directory is at $SCHEMAS_DIR"
  fi
fi

# Check if file argument provided
if [ -z "$1" ]; then
  echo "Usage: $0 <schema-json-file>"
  exit 1
fi

# Make sure schemas directory exists
mkdir -p "$SCHEMAS_DIR"
echo "Schemas directory: $SCHEMAS_DIR"

# Copy the new schema file
echo "Updating schema..."
cp "$1" "$SCHEMAS_DIR/pii-schema.json"

# Verify the file was copied successfully
if [ -f "$SCHEMAS_DIR/pii-schema.json" ]; then
  echo "Schema updated! The application will detect the change on the next refresh cycle."
  echo "Default refresh is set to 1 minute."
else
  echo "Error: Failed to update schema file at $SCHEMAS_DIR/pii-schema.json"
  exit 1
fi