#!/bin/bash

set -e

CONFIG_FILE="config.example.yaml"
QUERY="${1:-SELECT 1 as test}"
ITERATIONS="${2:-5}"
PROFILE="${3:-prod}"

echo "=========================================="
echo "Running benchmarks for all Snowflake clients"
echo "Query: $QUERY"
echo "Iterations: $ITERATIONS"
echo "Profile: $PROFILE"
echo "=========================================="
echo ""

echo "1. Benchmarking ADBC driver..."
cargo run --bin adbc-cli -- --config "$CONFIG_FILE" benchmark --query "$QUERY" --client adbc --iterations "$ITERATIONS" --profile "$PROFILE"
echo ""

# echo "2. Benchmarking snowflake-connector-rs..."
# cargo run --bin adbc-cli -- --config "$CONFIG_FILE" benchmark --query "$QUERY" --client snowflake-connector-rs --iterations "$ITERATIONS" --profile "$PROFILE"
# echo ""

echo "3. Benchmarking snowflake-api (Arrow format)..."
cargo run --bin adbc-cli -- --config "$CONFIG_FILE" benchmark --query "$QUERY" --client snowflake-api-arrow --iterations "$ITERATIONS" --profile "$PROFILE"
echo ""

echo "4. Benchmarking snowflake-api (JSON format)..."
echo "Note: Using SHOW TABLES query for JSON format (non-SELECT queries return JSON)"
SHOW_QUERY="${4:-SHOW TABLES}"
cargo run --bin adbc-cli -- --config "$CONFIG_FILE" benchmark --query "$SHOW_QUERY" --client snowflake-api-json --iterations "$ITERATIONS" --profile "$PROFILE"
echo ""

echo "=========================================="
echo "All benchmarks completed!"
echo "=========================================="

