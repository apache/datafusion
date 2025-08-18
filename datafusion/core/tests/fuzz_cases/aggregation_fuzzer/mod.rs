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

//! Fuzzer for aggregation functions
//!
//! The main idea behind aggregate fuzzing is: for aggregation, DataFusion has many
//! specialized implementations for performance. For example, when the group cardinality
//! is high, DataFusion will skip the first stage of two-stage hash aggregation; when
//! the input is ordered by the group key, there is a separate implementation to perform
//! streaming group by.
//! This fuzzer checks the results of different specialized implementations and
//! ensures their results are consistent. The execution path can be controlled by
//! changing the input ordering or by setting related configuration parameters in
//! `SessionContext`.
//!
//! # Architecture
//! - `aggregate_fuzz.rs` includes the entry point for fuzzer runs.
//! - `QueryBuilder` is used to generate candidate queries.
//! - `DatasetGenerator` is used to generate random datasets.
//! - `SessionContextGenerator` is used to generate `SessionContext` with
//!   different configuration parameters to control the execution path of aggregate
//!   queries.

use arrow::array::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::SessionContext;
use datafusion_common::error::Result;

mod context_generator;
mod data_generator;
mod fuzzer;
pub mod query_builder;

pub use crate::fuzz_cases::record_batch_generator::ColumnDescr;
pub use data_generator::DatasetGeneratorConfig;
pub use fuzzer::*;

#[derive(Debug)]
pub(crate) struct InconsistentResult {
    pub row_idx: usize,
    pub lhs_row: String,
    pub rhs_row: String,
}

pub(crate) fn check_equality_of_batches(
    lhs: &[RecordBatch],
    rhs: &[RecordBatch],
) -> std::result::Result<(), InconsistentResult> {
    let lhs_formatted_batches = pretty_format_batches(lhs).unwrap().to_string();
    let mut lhs_formatted_batches_sorted: Vec<&str> =
        lhs_formatted_batches.trim().lines().collect();
    lhs_formatted_batches_sorted.sort_unstable();
    let rhs_formatted_batches = pretty_format_batches(rhs).unwrap().to_string();
    let mut rhs_formatted_batches_sorted: Vec<&str> =
        rhs_formatted_batches.trim().lines().collect();
    rhs_formatted_batches_sorted.sort_unstable();

    for (row_idx, (lhs_row, rhs_row)) in lhs_formatted_batches_sorted
        .iter()
        .zip(&rhs_formatted_batches_sorted)
        .enumerate()
    {
        if lhs_row != rhs_row {
            return Err(InconsistentResult {
                row_idx,
                lhs_row: lhs_row.to_string(),
                rhs_row: rhs_row.to_string(),
            });
        }
    }

    Ok(())
}

pub(crate) async fn run_sql(sql: &str, ctx: &SessionContext) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}
