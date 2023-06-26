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
//! uses [Apache Arrow] as its in-memory format. DataFusion's many [use
//! cases] help developers build very fast and feature rich database
//! and analytic systems, customized to particular workloads.
//!
//! "Out of the box," DataFusion quickly runs complex [SQL] and
//! [`DataFrame`] queries using a sophisticated query planner, a columnar,
//! multi-threaded, vectorized execution engine, and partitioned data
//! sources (Parquet, CSV, JSON, and Avro).
//!
//! DataFusion is designed for easy customization such as supporting
//! additional data sources, query languages, functions, custom
//! operators and more. See the [Architecture] section for more details.
//!
//! [DataFusion]: https://arrow.apache.org/datafusion/
//! [Apache Arrow]: https://arrow.apache.org
//! [use cases]: https://arrow.apache.org/datafusion/user-guide/introduction.html#use-cases
//! [SQL]: https://arrow.apache.org/datafusion/user-guide/sql/index.html
//! [`DataFrame`]: dataframe::DataFrame
//! [Architecture]: #architecture
//!
//! # Examples
//!
//! The main entry point for interacting with DataFusion is the
//! [`SessionContext`]. [`Expr`]s represent expressions such as `a + b`.
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
//! DataFusion is designed to be a "disaggregated" query engine.  This
//! means that developers can mix and extend the parts of DataFusion
//! they need for their usecase. For example, just the
//! [`ExecutionPlan`] operators, or the [`SqlToRel`] SQL planner and
//! optimizer.
//!
//! In order to achieve this, DataFusion supports extension at many points:
//!
//! * read from any datasource ([`TableProvider`])
//! * define your own catalogs, schemas, and table lists ([`CatalogProvider`])
//! * build your own query langue or plans using the ([`LogicalPlanBuilder`])
//! * declare and use user-defined functions: ([`ScalarUDF`], and [`AggregateUDF`])
//! * add custom optimizer rewrite passes ([`OptimizerRule`] and [`PhysicalOptimizerRule`])
//! * extend the planner to use user-defined logical and physical nodes ([`QueryPlanner`])
//!
//! You can find examples of each of them in the [datafusion-examples] directory.
//!
//! [`TableProvider`]: crate::datasource::TableProvider
//! [`CatalogProvider`]: crate::catalog::CatalogProvider
//! [`LogicalPlanBuilder`]: datafusion_expr::logical_plan::builder::LogicalPlanBuilder
//! [`ScalarUDF`]: physical_plan::udf::ScalarUDF
//! [`AggregateUDF`]: physical_plan::udaf::AggregateUDF
//! [`QueryPlanner`]: execution::context::QueryPlanner
//! [`OptimizerRule`]: datafusion_optimizer::optimizer::OptimizerRule
//! [`PhysicalOptimizerRule`]: crate::physical_optimizer::optimizer::PhysicalOptimizerRule
//!
//! # Architecture
//!
//! <!-- NOTE: The goal of this section is to provide a high level
//! overview of how DataFusion is organized and then link to other
//! sections of the docs with more details -->
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
//! - [July 2022]: DataFusion and Arrow: Supercharge Your Data Analytical Tool with a Rusty Query Engine: [recording](https://www.youtube.com/watch?v=Rii1VTn3seQ) and [slides](https://docs.google.com/presentation/d/1q1bPibvu64k2b7LPi7Yyb0k3gA1BiUYiUbEklqW1Ckc/view#slide=id.g11054eeab4c_0_1165)
//! - [March 2021]: The DataFusion architecture is described in _Query Engine Design and the Rust-Based DataFusion in Apache Arrow_: [recording](https://www.youtube.com/watch?v=K6eCAVEk4kU) (DataFusion content starts [~ 15 minutes in](https://www.youtube.com/watch?v=K6eCAVEk4kU&t=875s)) and [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)
//! - [February 2021]: How DataFusion is used within the Ballista Project is described in _Ballista: Distributed Compute with Rust and Apache Arrow_: [recording](https://www.youtube.com/watch?v=ZZHQaOap9pQ)
//!
//! ## Query Planning and Execution Overview
//!
//! ### SQL
//!
//! ```text
//!                 Parsed with            SqlToRel creates
//!                 sqlparser              initial plan
//! ┌───────────────┐           ┌─────────┐             ┌─────────────┐
//! │   SELECT *    │           │Query {  │             │Project      │
//! │   FROM ...    │──────────▶│..       │────────────▶│  TableScan  │
//! │               │           │}        │             │    ...      │
//! └───────────────┘           └─────────┘             └─────────────┘
//!
//!   SQL String                 sqlparser               LogicalPlan
//!                              AST nodes
//! ```
//!
//! 1. The query string is parsed to an Abstract Syntax Tree (AST)
//! [`Statement`] using [sqlparser].
//!
//! 2. The AST is converted to a [`LogicalPlan`] and logical
//! expressions [`Expr`]s to compute the desired result by the
//! [`SqlToRel`] planner.
//!
//! [`Statement`]: https://docs.rs/sqlparser/latest/sqlparser/ast/enum.Statement.html
//!
//! ### DataFrame
//!
//! When executing plans using the [`DataFrame`] API, the process is
//! identical as with SQL, except the DataFrame API builds the
//! [`LogicalPlan`] directly using [`LogicalPlanBuilder`]. Systems
//! that have their own custom query languages typically also build
//! [`LogicalPlan`] directly.
//!
//! ### Planning
//!
//! ```text
//!             AnalyzerRules and      PhysicalPlanner          PhysicalOptimizerRules
//!             OptimizerRules         creates ExecutionPlan    improve performance
//!             rewrite plan
//! ┌─────────────┐        ┌─────────────┐      ┌───────────────┐        ┌───────────────┐
//! │Project      │        │Project(x, y)│      │ProjectExec    │        │ProjectExec    │
//! │  TableScan  │──...──▶│  TableScan  │─────▶│  ...          │──...──▶│  ...          │
//! │    ...      │        │    ...      │      │    ParquetExec│        │    ParquetExec│
//! └─────────────┘        └─────────────┘      └───────────────┘        └───────────────┘
//!
//!  LogicalPlan            LogicalPlan         ExecutionPlan             ExecutionPlan
//! ```
//!
//! To process large datasets with many rows as efficiently as
//! possible, significant effort is spent planning and
//! optimizing, in the following manner:
//!
//! 1. The [`LogicalPlan`] is checked and rewritten to enforce
//! semantic rules, such as type coercion, by [`AnalyzerRule`]s
//!
//! 2. The [`LogicalPlan`] is rewritten by [`OptimizerRule`]s, such as
//! projection and filter pushdown, to improve its efficiency.
//!
//! 3. The [`LogicalPlan`] is converted to an [`ExecutionPlan`] by a
//! [`PhysicalPlanner`]
//!
//! 4. The [`ExecutionPlan`] is rewritten by
//! [`PhysicalOptimizerRule`]s, such as sort and join selection, to
//! improve its efficiency.
//!
//! ## Data Sources
//!
//! ```text
//! Planning       │
//! requests       │            TableProvider::scan
//! information    │            creates an
//! such as schema │            ExecutionPlan
//!                │
//!                ▼
//!   ┌─────────────────────────┐         ┌──────────────┐
//!   │                         │         │              │
//!   │impl TableProvider       │────────▶│ParquetExec   │
//!   │                         │         │              │
//!   └─────────────────────────┘         └──────────────┘
//!         TableProvider
//!         (built in or user provided)    ExecutionPlan
//! ```
//!
//! DataFusion includes several built in data sources for common use
//! cases, and can be extended by implementing the [`TableProvider`]
//! trait. A [`TableProvider`] provides information for planning and
//! an [`ExecutionPlan`]s for execution.
//!
//! 1. [`ListingTable`]: Reads data from Parquet, JSON, CSV, or AVRO
//! files.  Supports single files or multiple files with HIVE style
//! partitioning, optional compression, directly reading from remote
//! object store and more.
//!
//! 2. [`MemTable`]: Reads data from in memory [`RecordBatch`]es.
//!
//! 3. [`StreamingTable`]: Reads data from potentially unbounded inputs.
//!
//! [`ListingTable`]: crate::datasource::listing::ListingTable
//! [`MemTable`]: crate::datasource::memory::MemTable
//! [`StreamingTable`]: crate::datasource::streaming::StreamingTable
//!
//! ## Plans
//!
//! Logical planning yields [`LogicalPlan`]s nodes and [`Expr`]
//! expressions which are [`Schema`] aware and represent statements
//! independent of how they are physically executed.
//! A [`LogicalPlan`] is a Directed Acyclic Graph (DAG) of other
//! [`LogicalPlan`]s, each potentially containing embedded [`Expr`]s.
//!
//! An [`ExecutionPlan`] (sometimes referred to as a "physical plan")
//! is a plan that can be executed against data. It a DAG of other
//! [`ExecutionPlan`]s each potentially containing expressions of the
//! following types:
//!
//! 1. [`PhysicalExpr`]: Scalar functions
//!
//! 2. [`AggregateExpr`]: Aggregate functions
//!
//! 2. [`WindowExpr`]: Window functions
//!
//! Compared to a [`LogicalPlan`], an [`ExecutionPlan`] has concrete
//! information about how to perform calculations (e.g. hash vs merge
//! join), and how data flows during execution (e.g. partitioning and
//! sortedness).
//!
//! [`PhysicalExpr`]: crate::physical_plan::PhysicalExpr
//! [`AggregateExpr`]: crate::physical_plan::AggregateExpr
//! [`WindowExpr`]: crate::physical_plan::WindowExpr
//!
//! ## Execution
//!
//! ```text
//!            ExecutionPlan::execute             Calling next() on the
//!            produces a stream                  stream produces the data
//!
//! ┌───────────────┐      ┌─────────────────────────┐         ┌────────────┐
//! │ProjectExec    │      │impl                     │    ┌───▶│RecordBatch │
//! │  ...          │─────▶│SendableRecordBatchStream│────┤    └────────────┘
//! │    ParquetExec│      │                         │    │    ┌────────────┐
//! └───────────────┘      └─────────────────────────┘    ├───▶│RecordBatch │
//!               ▲                                       │    └────────────┘
//! ExecutionPlan │                                       │         ...
//!               │                                       │
//!               │                                       │    ┌────────────┐
//!             PhysicalOptimizerRules                    ├───▶│RecordBatch │
//!             request information                       │    └────────────┘
//!             such as partitioning                      │    ┌ ─ ─ ─ ─ ─ ─
//!                                                       └───▶ None        │
//!                                                            └ ─ ─ ─ ─ ─ ─
//! ```
//!
//! [`ExecutionPlan`]s process data using the [Apache Arrow] memory
//! format, largely with functions from the [arrow] crate. When
//! [`execute`] is called, a [`SendableRecordBatchStream`] is returned
//! that produces the desired output as a [`Stream`] of [`RecordBatch`]es.
//!
//! Values are
//! represented with [`ColumnarValue`], which are either single
//! constant values ([`ScalarValue`]) or Arrow Arrays ([`ArrayRef`]).
//!
//! [`execute`]: physical_plan::ExecutionPlan::execute
//! [`SendableRecordBatchStream`]: crate::physical_plan::SendableRecordBatchStream
//! [`ColumnarValue`]: datafusion_expr::ColumnarValue
//! [`ScalarValue`]: crate::scalar::ScalarValue
//! [`ArrayRef`]: arrow::array::ArrayRef
//! [`Stream`]: futures::stream::Stream
//!
//!
//! See the [implementors of `ExecutionPlan`] for a list of physical operators available.
//!
//! [implementors of `ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#implementors
//!
//! ## State Management and Configuration
//!
//! [`ConfigOptions`] contain options to control DataFusion's
//! execution.
//!
//! [`ConfigOptions`]: datafusion_common::config::ConfigOptions
//!
//! The state required to execute queries is managed by the following
//! structures:
//!
//! 1. [`SessionContext`]: State needed for create [`LogicalPlan`]s such
//! as the table definitions, and the function registries.
//!
//! 2. [`TaskContext`]: State needed for execution such as the
//! [`MemoryPool`], [`DiskManager`], and [`ObjectStoreRegistry`].
//!
//! 3. [`ExecutionProps`]: Per-execution properties and data (such as
//! starting timestamps, etc).
//!
//! [`SessionContext`]: crate::execution::context::SessionContext
//! [`TaskContext`]: crate::execution::context::TaskContext
//! [`ExecutionProps`]: crate::execution::context::ExecutionProps
//!
//! ### Resource Management
//!
//! The amount of memory and temporary local disk space used by
//! DataFusion when running a plan can be controlled using the
//! [`MemoryPool`] and [`DiskManager`].
//!
//! [`DiskManager`]: crate::execution::DiskManager
//! [`MemoryPool`]: crate::execution::memory_pool::MemoryPool
//! [`ObjectStoreRegistry`]: crate::datasource::object_store::ObjectStoreRegistry
//!
//! ## Crate Organization
//!
//! DataFusion is organized into multiple crates to enforce modularity
//! and improve compilation times. The crates are:
//!
//! * [datafusion_common]: Common traits and types
//! * [datafusion_expr]: [`LogicalPlan`],  [`Expr`] and related logical planning structure
//! * [datafusion_execution]: State and structures needed for execution
//! * [datafusion_optimizer]: [`OptimizerRule`]s and [`AnalyzerRule`]s
//! * [datafusion_physical_expr]: [`PhysicalExpr`] and related expressions
//! * [datafusion_row]: Row based representation
//! * [datafusion_sql]: SQL planner ([`SqlToRel`])
//!
//! [sqlparser]: https://docs.rs/sqlparser/latest/sqlparser
//! [`SqlToRel`]: sql::planner::SqlToRel
//! [`Expr`]: datafusion_expr::Expr
//! [`LogicalPlan`]: datafusion_expr::LogicalPlan
//! [`AnalyzerRule`]: datafusion_optimizer::analyzer::AnalyzerRule
//! [`OptimizerRule`]: optimizer::optimizer::OptimizerRule
//! [`ExecutionPlan`]: physical_plan::ExecutionPlan
//! [`PhysicalPlanner`]: physical_planner::PhysicalPlanner
//! [`PhysicalOptimizerRule`]: datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule
//! [`Schema`]: arrow::datatypes::Schema
//! [`PhysicalExpr`]: physical_plan::PhysicalExpr
//! [`AggregateExpr`]: physical_plan::AggregateExpr
//! [`RecordBatch`]: arrow::record_batch::RecordBatch
//! [`RecordBatchReader`]: arrow::record_batch::RecordBatchReader
//! [`Array`]: arrow::array::Array

/// DataFusion crate version
pub const DATAFUSION_VERSION: &str = env!("CARGO_PKG_VERSION");

extern crate core;
extern crate sqlparser;

pub mod catalog;
pub mod dataframe;
pub mod datasource;
pub mod error;
pub mod execution;
pub mod physical_optimizer;
pub mod physical_plan;
pub mod physical_planner;
pub mod prelude;
pub mod scalar;
pub mod variable;

// re-export dependencies from arrow-rs to minimize version maintenance for crate users
pub use arrow;
pub use parquet;

// re-export DataFusion sub-crates at the top level. Use `pub use *`
// so that the contents of the subcrates appears in rustdocs
// for details, see https://github.com/apache/arrow-datafusion/issues/6648

/// re-export of [`datafusion_common`] crate
pub mod common {
    pub use datafusion_common::*;
}

// Backwards compatibility
pub use common::config;

// NB datafusion execution is re-exported in the `execution` module

/// re-export of [`datafusion_expr`] crate
pub mod logical_expr {
    pub use datafusion_expr::*;
}

/// re-export of [`datafusion_optimizer`] crate
pub mod optimizer {
    pub use datafusion_optimizer::*;
}

/// re-export of [`datafusion_physical_expr`] crate
pub mod physical_expr {
    pub use datafusion_physical_expr::*;
}

/// re-export of [`datafusion_row`] crate
pub mod row {
    pub use datafusion_row::*;
}

/// re-export of [`datafusion_sql`] crate
pub mod sql {
    pub use datafusion_sql::*;
}

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
