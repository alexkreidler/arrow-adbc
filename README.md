<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# ADBC: Arrow Database Connectivity

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/apache/arrow-adbc/blob/master/LICENSE.txt)

ADBC is an API standard (version 1.0.0) for database access libraries ("drivers") in C, Go, and Java that uses Arrow for result sets and query parameters.
Instead of writing code to convert to and from Arrow data for each individual database, applications can build against the ADBC APIs, and link against drivers that implement the standard.
Additionally, a JDBC/ODBC-style driver manager is provided. This also implements the ADBC APIs, but dynamically loads drivers and dispatches calls to them.

Like JDBC/ODBC, the goal is to provide a generic API for multiple databases. ADBC, however, is focused on bulk columnar data retrieval and ingestion through an Arrow-based API rather than attempting to replace JDBC/ODBC in all use cases.
Hence, ADBC is complementary to those existing standards.

Like [Arrow Flight SQL][flight-sql], ADBC is an Arrow-based way to work with databases.
However, Flight SQL is a _protocol_ defining a wire format and network transport as opposed to an _API specification_. Flight SQL requires a database to specifically implement support for it, while ADBC is a client API specification for wrapping existing database protocols which could be Arrow-native or not.
Together, ADBC and Flight SQL offer a fully Arrow-native solution for clients and database vendors.

For more about ADBC, see the [introductory blog post][arrow-blog].

[arrow-blog]: https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/
[flight-sql]: https://arrow.apache.org/docs/format/FlightSql.html

## Status

ADBC versions the API standard and the implementing libraries separately.

The API standard (version 1.0.0) is considered stable, but enhancements may be made.

Libraries are under development.
For more details, see the [documentation](https://arrow.apache.org/adbc/main/driver/status.html), or read the [changelog](CHANGELOG.md).

## Installation

Please see the [documentation](https://arrow.apache.org/adbc/main/driver/installation.html).

## Documentation

The core API definitions can be read in `adbc.h`.
User documentation can be found at https://arrow.apache.org/adbc

## Benchmark
In `rust/adbc-cli` you can run `./run_all_benchmarks.sh` (which will build for release target) and `benchmark_with_hyperfine.sh` (which will get measurements)

```
==========================================
Benchmarking with hyperfine
Query: SELECT 1 as test
Profile: prod
==========================================

1. Benchmarking ADBC driver...
Benchmark 1: /arrow-adbc/rust/target/release/adbc-cli --config config.example.yaml benchmark --query 'SELECT 1 as test' --client adbc --iterations 1 --profile prod
  Time (mean ± σ):      1.100 s ±  0.094 s    [User: 0.052 s, System: 0.030 s]
  Range (min … max):    0.995 s …  1.299 s    10 runs
 

2. Benchmarking snowflake-api (Arrow format)...
Benchmark 1: /arrow-adbc/rust/target/release/adbc-cli --config config.example.yaml benchmark --query 'SELECT 1 as test' --client snowflake-api-arrow --iterations 1 --profile prod
  Time (mean ± σ):     721.5 ms ±  53.1 ms    [User: 84.8 ms, System: 24.7 ms]
  Range (min … max):   657.4 ms … 813.1 ms    10 runs
 

3. Benchmarking snowflake-api (JSON format)...
Benchmark 1: /arrow-adbc/rust/target/release/adbc-cli --config config.example.yaml benchmark --query 'SHOW TABLES' --client snowflake-api-json --iterations 1 --profile prod
  Time (mean ± σ):      1.288 s ±  0.204 s    [User: 0.083 s, System: 0.022 s]
  Range (min … max):    1.007 s …  1.711 s    10 runs
 

==========================================
All benchmarks completed!
==========================================
```

Overall lesson is that Snowflake is slow / has a lot of latency, regardless of client. (may also be location-related)

## Development and Contributing

For detailed instructions on how to build the various ADBC libraries, see [CONTRIBUTING.md](CONTRIBUTING.md).
