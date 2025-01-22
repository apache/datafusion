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

mod enforce_sorting;
mod sanity_checker;

use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use std::sync::Arc;

/// create a single parquet file that is sorted
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

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
        Field::new("d", DataType::Int32, true),
        Field::new("e", DataType::Boolean, true),
    ]))
}

/// Created a sorted Csv exec
pub fn csv_exec_sorted(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(
        CsvExec::builder(
            FileScanConfig::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                schema.clone(),
            )
            .with_file(PartitionedFile::new("x".to_string(), 100))
            .with_output_ordering(vec![sort_exprs]),
        )
        .with_has_header(false)
        .with_delimeter(0)
        .with_quote(0)
        .with_escape(None)
        .with_comment(None)
        .with_newlines_in_values(false)
        .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
        .build(),
    )
}

/// Create a csv exec for tests
pub fn csv_exec_ordered(
    schema: &SchemaRef,
    sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>,
) -> Arc<dyn ExecutionPlan> {
    let sort_exprs = sort_exprs.into_iter().collect();

    Arc::new(
        CsvExec::builder(
            FileScanConfig::new(
                ObjectStoreUrl::parse("test:///").unwrap(),
                schema.clone(),
            )
            .with_file(PartitionedFile::new("file_path".to_string(), 100))
            .with_output_ordering(vec![sort_exprs]),
        )
        .with_has_header(true)
        .with_delimeter(0)
        .with_quote(b'"')
        .with_escape(None)
        .with_comment(None)
        .with_newlines_in_values(false)
        .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
        .build(),
    )
}
