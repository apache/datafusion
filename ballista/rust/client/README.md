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

# Ballista: Distributed Scheduler for Apache Arrow DataFusion

Ballista is a distributed compute platform primarily implemented in Rust, and powered by Apache Arrow and
DataFusion. It is built on an architecture that allows other programming languages (such as Python, C++, and
Java) to be supported as first-class citizens without paying a penalty for serialization costs.

The foundational technologies in Ballista are:

- [Apache Arrow](https://arrow.apache.org/) memory model and compute kernels for efficient processing of data.
- [Apache Arrow Flight Protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficient
  data transfer between processes.
- [Google Protocol Buffers](https://developers.google.com/protocol-buffers) for serializing query plans.
- [Docker](https://www.docker.com/) for packaging up executors along with user-defined code.

Ballista can be deployed as a standalone cluster and also supports [Kubernetes](https://kubernetes.io/). In either
case, the scheduler can be configured to use [etcd](https://etcd.io/) as a backing store to (eventually) provide
redundancy in the case of a scheduler failing.

## Starting a cluster

There are numerous ways to start a Ballista cluster, including support for Docker and
Kubernetes. For full documentation, refer to the
[DataFusion User Guide](https://github.com/apache/arrow-datafusion/tree/master/docs/user-guide)

A simple way to start a local cluster for testing purposes is to use cargo to install
the scheduler and executor crates.

```bash
cargo install ballista-scheduler
cargo install ballista-executor
```

With these crates installed, it is now possible to start a scheduler process.

```bash
RUST_LOG=info ballista-scheduler
```

The scheduler will bind to port 50050 by default.

Next, start an executor processes in a new terminal session with the specified concurrency
level.

```bash
RUST_LOG=info ballista-executor -c 4
```

The executor will bind to port 50051 by default. Additional executors can be started by
manually specifying a bind port. For example:

```bash
RUST_LOG=info ballista-executor --bind-port 50052 -c 4
```

## Executing a query

Ballista provides a `BallistaContext` as a starting point for creating queries. DataFrames can be created
by invoking the `read_csv`, `read_parquet`, and `sql` methods.

The following example runs a simple aggregate SQL query against a CSV file from the
[New York Taxi and Limousine Commission](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
data set.

```rust,no_run
use ballista::prelude::*;
use datafusion::arrow::util::pretty;
use datafusion::prelude::CsvReadOptions;

#[tokio::main]
async fn main() -> Result<()> {
   // create configuration
   let config = BallistaConfig::builder()
       .set("ballista.shuffle.partitions", "4")
       .build()?;

   // connect to Ballista scheduler
   let ctx = BallistaContext::remote("localhost", 50050, &config);

   // register csv file with the execution context
   ctx.register_csv(
       "tripdata",
       "/path/to/yellow_tripdata_2020-01.csv",
       CsvReadOptions::new(),
   )?;

   // execute the query
   let df = ctx.sql(
       "SELECT passenger_count, MIN(fare_amount), MAX(fare_amount), AVG(fare_amount), SUM(fare_amount)
       FROM tripdata
       GROUP BY passenger_count
       ORDER BY passenger_count",
   )?;

   // collect the results and print them to stdout
   let results = df.collect().await?;
   pretty::print_batches(&results)?;
   Ok(())
}
```
