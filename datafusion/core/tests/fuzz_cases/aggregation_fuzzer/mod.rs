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

use arrow::util::pretty::pretty_format_batches;
use arrow_array::RecordBatch;
use datafusion::prelude::SessionContext;

mod context_generator;
mod data_generator;
mod fuzzer;

pub use data_generator::{DatasetGeneratorConfig, ColumnDescr};
pub use fuzzer::*;

pub(crate) fn check_equality_of_batches(rhs: &[RecordBatch], lhs: &[RecordBatch]) {
    let formatted_batches0 = pretty_format_batches(rhs).unwrap().to_string();
    let mut formatted_batches0_sorted: Vec<&str> =
        formatted_batches0.trim().lines().collect();
    formatted_batches0_sorted.sort_unstable();
    let formatted_batches1 = pretty_format_batches(lhs).unwrap().to_string();
    let mut formatted_batches1_sorted: Vec<&str> =
        formatted_batches1.trim().lines().collect();
    formatted_batches1_sorted.sort_unstable();
    assert_eq!(formatted_batches0_sorted, formatted_batches1_sorted);
}

pub(crate) async fn run_sql(sql: &str, ctx: &SessionContext) -> Vec<RecordBatch> {
    ctx.sql(sql).await.unwrap().collect().await.unwrap()
}
