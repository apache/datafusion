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
#![warn(missing_docs, clippy::needless_borrow)]

//! [DataFusion] is an extensible query engine written in Rust that
//! uses [Apache Arrow] as its in-memory format. DataFusion's [use
//! cases] include building very fast database and analytic systems,
//! customized to particular workloads.
//!
//! "Out of the box," DataFusion quickly runs complex [SQL] and
//! [`DataFrame`] queries using a sophisticated query planner, a columnar,
//! multi-threaded, vectorized execution engine, and partitioned data
//! sources (Parquet, CSV, JSON, and Avro).
//!
//! DataFusion can also be easily customized to support additional
//! data sources, query languages, functions, custom operators and
//! more.
//!
//! [DataFusion]: https://arrow.apache.org/datafusion/
//! [Apache Arrow]: https://arrow.apache.org
//! [use cases]: https://arrow.apache.org/datafusion/user-guide/introduction.html#use-cases
//! [SQL]: https://arrow.apache.org/datafusion/user-guide/sql/index.html
//! [`DataFrame`]: dataframe::DataFrame
//!
//! # Examples
//!
//! The main entry point for interacting with DataFusion is the
//! [`SessionContext`].
//!
//! [`SessionContext`]: execution::context::SessionContext
//!
//! ## DataFrame
//!
//! To execute a query against data stored
//! in a CSV file using a [`DataFrame`]:
//!
//! ```rust
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use datafusion::arrow::record_batch::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//!
//! // create the dataframe
//! let df = ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?;
//!
//! // create a plan
//! let df = df.filter(col("a").lt_eq(col("b")))?
//!            .aggregate(vec![col("a")], vec![min(col("b"))])?
//!            .limit(0, Some(100))?;
//!
//! // execute the plan
//! let results: Vec<RecordBatch> = df.collect().await?;
//!
//! // format the results
//! let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?
//!    .to_string();
//!
//! let expected = vec![
//!     "+---+----------------+",
//!     "| a | MIN(?table?.b) |",
//!     "+---+----------------+",
//!     "| 1 | 2              |",
//!     "+---+----------------+"
//! ];
//!
//! assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
//! # Ok(())
//! # }
//! ```
//!
//! ## SQL
//!
//! To execute a query against a CSV file using [SQL]:
//!
//! ```
//! # use datafusion::prelude::*;
//! # use datafusion::error::Result;
//! # use datafusion::arrow::record_batch::RecordBatch;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let ctx = SessionContext::new();
//!
//! ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new()).await?;
//!
//! // create a plan
//! let df = ctx.sql("SELECT a, MIN(b) FROM example WHERE a <= b GROUP BY a LIMIT 100").await?;
//!
//! // execute the plan
//! let results: Vec<RecordBatch> = df.collect().await?;
//!
//! // format the results
//! let pretty_results = arrow::util::pretty::pretty_format_batches(&results)?
//!   .to_string();
//!
//! let expected = vec![
//!     "+---+----------------+",
//!     "| a | MIN(example.b) |",
//!     "+---+----------------+",
//!     "| 1 | 2              |",
//!     "+---+----------------+"
//! ];
//!
//! assert_eq!(pretty_results.trim().lines().collect::<Vec<_>>(), expected);
//! # Ok(())
//! # }
//! ```
//!
//! ## More Examples
//!
//! There are many additional annotated examples of using DataFusion in the [datafusion-examples] directory.
//!
//! [datafusion-examples]: https://github.com/apache/arrow-datafusion/tree/main/datafusion-examples
//!
//! ## Customization and Extension
//!
//! DataFusion supports extension at many points:
//!
//! * read from any datasource ([`TableProvider`])
//! * define your own catalogs, schemas, and table lists ([`CatalogProvider`])
//! * build your own query langue or plans using the ([`LogicalPlanBuilder`])
//! * declare and use user-defined scalar functions ([`ScalarUDF`])
//! * declare and use user-defined aggregate functions ([`AggregateUDF`])
//! * add custom optimizer rewrite passes ([`OptimizerRule`] and [`PhysicalOptimizerRule`])
//! * extend the planner to use user-defined logical and physical nodes ([`QueryPlanner`])
//!
//! You can find examples of each of them in the [datafusion-examples] directory.
//!
//! [`TableProvider`]: crate::datasource::TableProvider
//! [`CatalogProvider`]: crate::catalog::catalog::CatalogProvider
//! [`LogicalPlanBuilder`]: datafusion_expr::logical_plan::builder::LogicalPlanBuilder
//! [`ScalarUDF`]: physical_plan::udf::ScalarUDF
//! [`AggregateUDF`]: physical_plan::udaf::AggregateUDF
//! [`QueryPlanner`]: execution::context::QueryPlanner
//! [`OptimizerRule`]: datafusion_optimizer::optimizer::OptimizerRule
//! [`PhysicalOptimizerRule`]: crate::physical_optimizer::optimizer::PhysicalOptimizerRule
//!
//! # Code Organization
//!
//! ## Overview  Presentations
//!
//! The following presentations offer high level overviews of the
//! different components and how they interact together.
//!
//! - [Apr 2023]: The Apache Arrow DataFusion Architecture talks
//!   - _Query Engine_: [recording](https://youtu.be/NVKujPxwSBA) and [slides](https://docs.google.com/presentation/d/1D3GDVas-8y0sA4c8EOgdCvEjVND4s2E7I6zfs67Y4j8/edit#slide=id.p)
//!   - _Logical Plan and Expressions_: [recording](https://youtu.be/EzZTLiSJnhY) and [slides](https://docs.google.com/presentation/d/1ypylM3-w60kVDW7Q6S99AHzvlBgciTdjsAfqNP85K30)
//!   - _Physical Plan and Execution_: [recording](https://youtu.be/2jkWU3_w6z0) and [slides](https://docs.google.com/presentation/d/1cA2WQJ2qg6tx6y4Wf8FH2WVSm9JQ5UgmBWATHdik0hg)
//! - [February 2021]: How DataFusion is used within the Ballista Project is described in \*Ballista: Distributed Compute with Rust and Apache Arrow: [recording](https://www.youtube.com/watch?v=ZZHQaOap9pQ)
//! - [July 2022]: DataFusion and Arrow: Supercharge Your Data Analytical Tool with a Rusty Query Engine: [recording](https://www.youtube.com/watch?v=Rii1VTn3seQ) and [slides](https://docs.google.com/presentation/d/1q1bPibvu64k2b7LPi7Yyb0k3gA1BiUYiUbEklqW1Ckc/view#slide=id.g11054eeab4c_0_1165)
//! - [March 2021]: The DataFusion architecture is described in _Query Engine Design and the Rust-Based DataFusion in Apache Arrow_: [recording](https://www.youtube.com/watch?v=K6eCAVEk4kU) (DataFusion content starts [~ 15 minutes in](https://www.youtube.com/watch?v=K6eCAVEk4kU&t=875s)) and [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)
//! - [February 2021]: How DataFusion is used within the Ballista Project is described in \*Ballista: Distributed Compute with Rust and Apache Arrow: [recording](https://www.youtube.com/watch?v=ZZHQaOap9pQ)
//!
//! ## Architecture
//!
//! DataFusion is a fully fledged query engine capable of performing complex operations.
//! Specifically, when DataFusion receives an SQL query, there are different steps
//! that it passes through until a result is obtained. Broadly, they are:
//!
//! 1. The string is parsed to an Abstract syntax tree (AST) using [sqlparser].
//! 2. The planner [`SqlToRel`] converts logical expressions on the AST to logical expressions [`Expr`]s.
//! 3. The planner [`SqlToRel`] converts logical nodes on the AST to a [`LogicalPlan`].
//! 4. [`OptimizerRule`]s are applied to the [`LogicalPlan`] to optimize it.
//! 5. The [`LogicalPlan`] is converted to an [`ExecutionPlan`] by a [`PhysicalPlanner`]
//! 6. The [`ExecutionPlan`]is executed against data through the [`SessionContext`]
//!
//! With the [`DataFrame`] API, steps 1-3 are not used as the DataFrame builds the [`LogicalPlan`] directly.
//!
//! Phases 1-5 are typically cheap when compared to phase 6, and thus DataFusion puts a
//! lot of effort to ensure that phase 6 runs efficiently and without errors.
//!
//! DataFusion's planning is divided in two main parts: logical planning and physical planning.
//!
//! ### Logical planning
//!
//! Logical planning yields [`LogicalPlan`]s and logical [`Expr`]
//! expressions which are [`Schema`]aware and represent statements
//! whose result is independent of how it should physically be
//! executed.
//!
//! A [`LogicalPlan`] is a Directed Acyclic Graph (DAG) of other
//! [`LogicalPlan`]s, and each node contains [`Expr`]s.  All of these
//! are located in [`datafusion_expr`] module.
//!
//! ### Physical planning
//!
//! An [`ExecutionPlan`] (sometimes referred to as a "physical plan")
//! is a plan that can be executed against data. Compared to a
//! logical plan, the physical plan has concrete information about how
//! calculations should be performed (e.g. what Rust functions are
//! used) and how data should be loaded into memory.
//!
//! [`ExecutionPlan`]s uses the [Apache Arrow] format as its in-memory
//! representation of data, through the [arrow] crate. The [arrow]
//! crate documents how the memory is physically represented.
//!
//! A [`ExecutionPlan`] is composed by nodes (which each implement the
//! [`ExecutionPlan`] trait). Each node can contain physical
//! expressions ([`PhysicalExpr`]) or aggreagate expressions
//! ([`AggregateExpr`]).  All of these are located in the
//! [`physical_plan`] module.
//!
//! Broadly speaking,
//!
//! * an [`ExecutionPlan`] receives a partition number and
//!   asynchronously returns an iterator over [`RecordBatch`] (a
//!   node-specific struct that implements [`RecordBatchReader`])
//! * a [`PhysicalExpr`] receives a [`RecordBatch`]
//!   and returns an [`Array`]
//! * an [`AggregateExpr`] receives a series of [`RecordBatch`]es
//!   and returns a [`RecordBatch`] of a single row(*)
//!
//! (*) Technically, it aggregates the results on each partition and then merges the results into a single partition.
//!
//! The following physical nodes are currently implemented:
//!
//! * Projection: [`ProjectionExec`](physical_plan::projection::ProjectionExec)
//! * Filter: [`FilterExec`](physical_plan::filter::FilterExec)
//! * Grouped and non-grouped aggregations: [`AggregateExec`](physical_plan::aggregates::AggregateExec)
//! * Hash Join: [`HashJoinExec`](physical_plan::joins::HashJoinExec)
//! * Cross Join: [`CrossJoinExec`](physical_plan::joins::CrossJoinExec)
//! * Sort Merge Join: [`SortMergeJoinExec`](physical_plan::joins::SortMergeJoinExec)
//! * Union: [`UnionExec`](physical_plan::union::UnionExec)
//! * Sort: [`SortExec`](physical_plan::sorts::sort::SortExec)
//! * Coalesce partitions: [`CoalescePartitionsExec`](physical_plan::coalesce_partitions::CoalescePartitionsExec)
//! * Limit: [`LocalLimitExec`](physical_plan::limit::LocalLimitExec) and [`GlobalLimitExec`](physical_plan::limit::GlobalLimitExec)
//! * Scan CSV: [`CsvExec`](physical_plan::file_format::CsvExec)
//! * Scan Parquet: [`ParquetExec`](physical_plan::file_format::ParquetExec)
//! * Scan Avro: [`AvroExec`](physical_plan::file_format::AvroExec)
//! * Scan newline-delimited JSON: [`NdJsonExec`](physical_plan::file_format::NdJsonExec)
//! * Scan from memory: [`MemoryExec`](physical_plan::memory::MemoryExec)
//! * Explain the plan: [`ExplainExec`](physical_plan::explain::ExplainExec)
//!
//! Future topics (coming soon):
//! * Analyzer Rules
//! * Resource management (memory and disk)
//!
//! [sqlparser]: https://docs.rs/sqlparser/latest/sqlparser
//! [`SqlToRel`]: sql::planner::SqlToRel
//! [`Expr`]: datafusion_expr::Expr
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan
//! [`OptimizerRule`]: optimizer::optimizer::OptimizerRule
//! [`ExecutionPlan`]: physical_plan::ExecutionPlan
//! [`PhysicalPlanner`]: physical_plan::PhysicalPlanner
//! [`Schema`]: arrow::datatypes::Schema
//! [`datafusion_expr`]: datafusion_expr
//! [`PhysicalExpr`]: physical_plan::PhysicalExpr
//! [`AggregateExpr`]: physical_plan::AggregateExpr
//! [`RecordBatch`]: arrow::record_batch::RecordBatch
//! [`RecordBatchReader`]: arrow::record_batch::RecordBatchReader
//! [`Array`]: arrow::array::Array

/// DataFusion crate version
pub const DATAFUSION_VERSION: &str = env!("CARGO_PKG_VERSION");

extern crate core;
extern crate sqlparser;

pub mod avro_to_arrow;
pub mod catalog;
pub mod dataframe;
pub mod datasource;
pub mod error;
pub mod execution;
pub mod physical_optimizer;
pub mod physical_plan;
pub mod prelude;
pub mod scalar;
#[cfg(feature = "scheduler")]
pub mod scheduler;
pub mod variable;

// re-export dependencies from arrow-rs to minimise version maintenance for crate users
pub use arrow;
pub use parquet;

// re-export DataFusion crates
pub use datafusion_common as common;
pub use datafusion_common::config;
pub use datafusion_expr as logical_expr;
pub use datafusion_optimizer as optimizer;
pub use datafusion_physical_expr as physical_expr;
pub use datafusion_row as row;
pub use datafusion_sql as sql;

#[cfg(feature = "jit")]
pub use datafusion_jit as jit;

pub use common::from_slice;

#[cfg(test)]
pub mod test;
pub mod test_util;

#[cfg(doctest)]
doc_comment::doctest!("../../../README.md", readme_example_test);

#[cfg(doctest)]
doc_comment::doctest!(
    "../../../docs/source/user-guide/example-usage.md",
    user_guid_example_tests
);
