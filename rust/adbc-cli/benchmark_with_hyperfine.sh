#!/bin/bash

set -e

CONFIG_FILE="config.example.yaml"
QUERY="${1:-SELECT 1 as test}"
ITERATIONS="${2:-10}"
PROFILE="${3:-prod}"

echo "=========================================="
echo "Building adbc-cli in release mode..."
echo "=========================================="
cargo build --release --bin adbc-cli

BINARY="/Users/alexkreidler/arrow-adbc/rust/target/release/adbc-cli"

if [ ! -f "$BINARY" ]; then
    echo "Error: Binary not found at $BINARY"
    exit 1
fi

echo ""
echo "=========================================="
echo "Benchmarking with hyperfine"
echo "Query: $QUERY"
echo "Profile: $PROFILE"
echo "=========================================="
echo ""

if ! command -v hyperfine &> /dev/null; then
    echo "Error: hyperfine is not installed"
    echo "Install it with: cargo install hyperfine"
    exit 1
fi

echo "1. Benchmarking ADBC driver..."
hyperfine \
    --warmup 1 \
    --min-runs "$ITERATIONS" \
    "$BINARY --config $CONFIG_FILE benchmark --query '$QUERY' --client adbc --iterations 1 --profile $PROFILE"

echo ""
echo "2. Benchmarking snowflake-api (Arrow format)..."
hyperfine \
    --warmup 1 \
    --min-runs "$ITERATIONS" \
    "$BINARY --config $CONFIG_FILE benchmark --query '$QUERY' --client snowflake-api-arrow --iterations 1 --profile $PROFILE"

echo ""
echo "3. Benchmarking snowflake-api (JSON format)..."
SHOW_QUERY="${4:-SHOW TABLES}"
hyperfine \
    --warmup 1 \
    --min-runs "$ITERATIONS" \
    "$BINARY --config $CONFIG_FILE benchmark --query '$SHOW_QUERY' --client snowflake-api-json --iterations 1 --profile $PROFILE"

echo ""
echo "=========================================="
echo "All benchmarks completed!"
echo "=========================================="


