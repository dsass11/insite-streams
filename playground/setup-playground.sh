#!/bin/bash

# Create necessary directories
mkdir -p schemas
mkdir -p logs

# Copy initial schema files
cp schemas/pii-schema.json schemas/

# Start Docker containers
cd docker
docker-compose up -d
cd ..

echo "Playground environment is ready!"
echo "To start the Flink application, run: ./scripts/start-pii-stream.sh"