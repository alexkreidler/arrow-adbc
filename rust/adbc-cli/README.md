# ADBC CLI

A command-line tool for running queries against Snowflake using ADBC (Arrow Database Connectivity).

## Features

- Execute SQL queries against Snowflake
- Support for private key authentication (JWT)
- Interactive mode for running multiple queries
- Configurable connection profiles
- Pretty-printed query results
- Benchmark different Snowflake clients (ADBC, snowflake-connector-rs, snowflake-api)

## Usage

### Basic Usage

Run a single query:
```bash
cargo run --bin adbc-cli -- --config config.yaml --query "SELECT 1 as test"
```

Run in interactive mode:
```bash
cargo run --bin adbc-cli -- --config config.yaml
```

Specify a profile:
```bash
cargo run --bin adbc-cli -- --config config.yaml --profile prod --query "SELECT * FROM my_table LIMIT 10"
```

### Benchmarking

Benchmark different Snowflake clients to compare performance:

```bash
# Benchmark ADBC driver
cargo run --bin adbc-cli -- --config config.yaml benchmark --query "SELECT * FROM my_table LIMIT 1000" --client adbc --iterations 5

# Benchmark snowflake-connector-rs
cargo run --bin adbc-cli -- --config config.yaml benchmark --query "SELECT * FROM my_table LIMIT 1000" --client snowflake-connector-rs --iterations 5

# Benchmark snowflake-api with Arrow results
cargo run --bin adbc-cli -- --config config.yaml benchmark --query "SELECT * FROM my_table LIMIT 1000" --client snowflake-api-arrow --iterations 5

# Benchmark snowflake-api with JSON results (typically for non-SELECT queries)
cargo run --bin adbc-cli -- --config config.yaml benchmark --query "SHOW TABLES" --client snowflake-api-json --iterations 5
```

Supported clients:
- `adbc`: ADBC Snowflake driver (default)
- `snowflake-connector-rs`: snowflake-connector-rs library
- `snowflake-api-arrow`: snowflake-api library with Arrow results (for SELECT queries)
- `snowflake-api-json`: snowflake-api library with JSON results (for non-SELECT queries)

Note: `snowflake-api-arrow` expects Arrow results (SELECT queries), while `snowflake-api-json` expects JSON results (typically non-SELECT queries like SHOW, DESCRIBE, etc.).

### Configuration File Format

The configuration file is a YAML file with named profiles. Each profile contains Snowflake connection settings:

```yaml
prod:
  type: snowflake
  account: FRDMZMO-XHB72082
  user: AIRBYTE_USER
  private_key: |
    -----BEGIN PRIVATE KEY-----
    MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDEZC0KLMa30pCI
    ...
    -----END PRIVATE KEY-----
  role: AIRBYTE_ROLE
  warehouse: ANALYTICS_WAREHOUSE
  database: PROJECT_ANALYTICS
  schema: SHOULDNEVERHAPPEN
  client_session_keep_alive: false
```

### Configuration Options

- `type`: Must be `snowflake`
- `account`: Snowflake account identifier
- `user`: Snowflake username
- `private_key`: RSA private key in PEM format (for JWT authentication)
- `password`: Password (alternative to private key)
- `role`: Snowflake role to use
- `warehouse`: Snowflake warehouse name
- `database`: Database name
- `schema`: Schema name
- `client_session_keep_alive`: Keep session alive after connection closes (boolean)

### Example

See `config.example.yaml` for a complete example configuration file.

## Building

```bash
cd rust/adbc-cli
cargo build --release
```

The binary will be available at `target/release/adbc-cli`.

## Requirements

- Rust 1.81 or later
- ADBC Snowflake driver (must be available in the system library path or built with the `bundled` feature)
- For benchmarking: `snowflake-connector-rs` and `snowflake-api` crates (automatically included as dependencies)

