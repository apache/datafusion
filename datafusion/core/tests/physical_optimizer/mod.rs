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

//! Physical Optimizer integration tests

mod aggregate_statistics;
mod combine_partial_final_agg;
mod enforce_distribution;
mod enforce_sorting;
mod limited_distinct_aggregation;
mod replace_with_order_preserving_variants;
mod sanity_checker;

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_optimizer::test_utils::schema;

/// Create a non sorted parquet exec
pub fn parquet_exec(schema: &SchemaRef) -> Arc<ParquetExec> {
    ParquetExec::builder(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema.clone())
            .with_file(PartitionedFile::new("x".to_string(), 100)),
    )
    .build_arc()
}

/// Create a single parquet file that is sorted
pub(crate) fn parquet_exec_with_sort(
    output_ordering: Vec<LexOrdering>,
) -> Arc<ParquetExec> {
    ParquetExec::builder(
        FileScanConfig::new(ObjectStoreUrl::parse("test:///").unwrap(), schema())
            .with_file(PartitionedFile::new("x".to_string(), 100))
            .with_output_ordering(output_ordering),
    )
    .build_arc()
}
