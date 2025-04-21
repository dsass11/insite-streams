#!/bin/bash

# A script to check if all Flink dependencies are properly defined in pom.xml

# Determine project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLAYGROUND_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$PLAYGROUND_DIR")"

cd "$PROJECT_ROOT"
echo "Checking dependencies in project at $(pwd)..."

# Check for Flink dependencies in common module
echo "=== Checking Flink dependencies in common module ==="
cd common
mvn dependency:tree | grep -i flink

# Check for Flink dependencies in pii module
echo ""
echo "=== Checking Flink dependencies in pii module ==="
cd ../pii
mvn dependency:tree | grep -i flink

# Check for scope="provided" which might cause runtime issues
echo ""
echo "=== Checking for provided scope (may cause runtime issues) ==="
cd ..
grep -r "scope.*provided" --include="pom.xml" .

echo ""
echo "=== Checking for deployment profile ==="
grep -r "profile" --include="pom.xml" .

echo ""
echo "Done checking dependencies."