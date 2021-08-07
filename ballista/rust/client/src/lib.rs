// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Ballista is a distributed compute platform primarily implemented in Rust, and powered by Apache Arrow and
//! DataFusion. It is built on an architecture that allows other programming languages (such as Python, C++, and
//! Java) to be supported as first-class citizens without paying a penalty for serialization costs.
//!
//! The foundational technologies in Ballista are:
//!
//! - [Apache Arrow](https://arrow.apache.org/) memory model and compute kernels for efficient processing of data.
//! - [Apache Arrow Flight Protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficient
//!   data transfer between processes.
//! - [Google Protocol Buffers](https://developers.google.com/protocol-buffers) for serializing query plans.
//! - [Docker](https://www.docker.com/) for packaging up executors along with user-defined code.
//!
//! Ballista can be deployed as a standalone cluster and also supports [Kubernetes](https://kubernetes.io/). In either
//! case, the scheduler can be configured to use [etcd](https://etcd.io/) as a backing store to (eventually) provide
//! redundancy in the case of a scheduler failing.
//!
//! ## Starting a cluster
//!
//! There are numerous ways to start a Ballista cluster, including support for Docker and
//! Kubernetes. For full documentation, refer to the
//! [DataFusion User Guide](https://github.com/apache/arrow-datafusion/tree/master/docs/user-guide)
//!
//! A simple way to start a local cluster for testing purposes is to use cargo to install
//! the scheduler and executor crates.
//!
//! ```bash
//! cargo install ballista-scheduler
//! cargo install ballista-executor
//! ```
//!
//! With these crates installed, it is now possible to start a scheduler process.
//!
//! ```bash
//! RUST_LOG=info ballista-scheduler
//! ```
//!
//! The scheduler will bind to port 50050 by default.
//!
//! Next, start an executor processes in a new terminal session with the specified concurrency
//! level.
//!
//! ```bash
//! RUST_LOG=info ballista-executor -c 4
//! ```
//!
//! The executor will bind to port 50051 by default. Additional executors can be started by
//! manually specifying a bind port. For example:
//!
//! ```bash
//! RUST_LOG=info ballista-executor --bind-port 50052 -c 4
//! ```
//!
//! ## Executing a query
//!
//! Ballista provides a `BallistaContext` as a starting point for creating a DataFrame from a CSV
//! or Parquet file and then performing transformations on the DataFrame. This usage is almost
//! identical to DataFusion except that the starting point is a `BallistaContext` instead of a
//! DataFusion `ExecutionContext`.
//!
//! ```no_run
//! use ballista::prelude::*;
//! use datafusion::arrow::util::pretty;
//! use datafusion::prelude::{col, lit};
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let config = BallistaConfig::builder()
//!         .set("ballista.shuffle.partitions", "4")
//!         .build()?;
//!
//!     // connect to Ballista scheduler
//!     let ctx = BallistaContext::remote("localhost", 50050, &config);
//!
//!     let testdata = datafusion::arrow::util::test_util::parquet_test_data();
//!
//!     let filename = &format!("{}/alltypes_plain.parquet", testdata);
//!
//!     // define the query using the DataFrame trait
//!     let df = ctx
//!         .read_parquet(filename)?
//!         .select_columns(&["id", "bool_col", "timestamp_col"])?
//!         .filter(col("id").gt(lit(1)))?;
//!
//!     let results = df.collect().await?;
//!     pretty::print_batches(&results)?;
//!
//!     Ok(())
//! }
//! ```
//!
//! SQL is also supported as demonstrated in the following example:
//!
//! ```no_run
//! use ballista::prelude::*;
//! use datafusion::arrow::util::pretty;
//! use datafusion::prelude::CsvReadOptions;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let config = BallistaConfig::builder()
//!         .set("ballista.shuffle.partitions", "4")
//!         .build()?;
//!
//!     // connect to Ballista scheduler
//!     let ctx = BallistaContext::remote("localhost", 50050, &config);
//!
//!     let testdata = datafusion::arrow::util::test_util::arrow_test_data();
//!
//!     // register csv file with the execution context
//!     ctx.register_csv(
//!         "aggregate_test_100",
//!         &format!("{}/csv/aggregate_test_100.csv", testdata),
//!         CsvReadOptions::new(),
//!     )?;
//!
//!     // execute the query
//!     let df = ctx.sql(
//!         "SELECT c1, MIN(c12), MAX(c12) \
//!         FROM aggregate_test_100 \
//!         WHERE c11 > 0.1 AND c11 < 0.9 \
//!         GROUP BY c1",
//!     )?;
//!
//!     let results = df.collect().await?;
//!     pretty::print_batches(&results)?;
//!
//!     Ok(())
//! }
//! ```

pub mod columnar_batch;
pub mod context;
pub mod prelude;
