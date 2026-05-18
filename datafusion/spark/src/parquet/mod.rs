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

//! Spark-compatible Parquet reading utilities.
//!
//! This module provides a [`PhysicalExprAdapterFactory`] implementation that,
//! when plugged into a `FileScanConfig`, makes DataFusion read Parquet files
//! with the same semantics as Apache Spark's vectorized reader.
//!
//! [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use datafusion_spark::parquet::{
//!     EvalMode, SparkParquetOptions, SparkPhysicalExprAdapterFactory,
//! };
//! use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;
//!
//! // Configure Spark options. Most users want to start from a defaults
//! // constructor and override the per-Spark-version flags.
//! let mut options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
//! // Spark 3.x rejects type promotion in the vectorized reader; flip this
//! // to `true` to emulate Spark 4.x.
//! options.allow_type_promotion = false;
//!
//! let factory: Arc<dyn PhysicalExprAdapterFactory> =
//!     Arc::new(SparkPhysicalExprAdapterFactory::new(options, None));
//!
//! // Plug `factory` into a `FileScanConfigBuilder` via `with_expr_adapter`.
//! ```
//!
//! # Per-Spark-version behavior
//!
//! Configure [`SparkParquetOptions`] flags to match the Spark version you are
//! emulating. Notable version-sensitive switches:
//!
//! * [`SparkParquetOptions::allow_type_promotion`] — Spark 3.x's vectorized
//!   reader rejects INT32→INT64, FLOAT→DOUBLE, INT32→DOUBLE; Spark 4.x
//!   allows them.
//! * [`SparkParquetOptions::return_null_struct_if_all_fields_missing`] —
//!   pre-4.1 Spark returned the struct as `null` when no requested fields
//!   matched; SPARK-53535 (Spark 4.1+) preserves the parent's nullness.
//! * [`SparkParquetOptions::eval_mode`] — Spark 4.0 made `Ansi` the default.

mod cast_column;
mod error;
mod options;
mod parquet_support;
mod schema_adapter;

pub use cast_column::SparkCastColumnExpr;
pub use error::ParquetSchemaError;
pub use options::{EvalMode, SparkParquetOptions};
pub use parquet_support::spark_parquet_convert;
pub use schema_adapter::SparkPhysicalExprAdapterFactory;
