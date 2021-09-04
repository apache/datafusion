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

# Ballista Examples

This directory contains examples for executing distributed queries with Ballista.

For background information on the Ballista architecture, refer to 
the [Ballista README](../ballista/README.md).

## Start a standalone cluster

From the root of the arrow-datafusion project, build release binaries.

```bash
cargo build --release
```

Start a Ballista scheduler process in a new terminal session.

```bash
RUST_LOG=info ./target/release/ballista-scheduler
```

Start one or more Ballista executor processes in new terminal sessions. When starting more than one 
executor, a unique port number must be specified for each executor.

```bash
RUST_LOG=info ./target/release/ballista-executor -c 4
```

## Running the examples

Refer to the instructions in [DEVELOPERS.md](../DEVELOPERS.md) to define the `ARROW_TEST_DATA` and
`PARQUET_TEST_DATA` environment variables so that the examples can find the test data files.

The examples can be run using the `cargo run --bin` syntax. 

```bash
cargo run --release --bin ballista-dataframe
```

