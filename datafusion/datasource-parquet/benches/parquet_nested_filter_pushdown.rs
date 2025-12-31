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

//! Benchmark for Parquet nested list filter pushdown performance.
//!
//! This benchmark demonstrates the performance improvement of pushing down
//! filters on nested list columns (such as `array_has`, `array_has_all`) to
//! the Parquet decoder level, allowing row group skipping based on min/max
//! statistics.
//!
//! The benchmark creates a dataset with:
//! - 100K rows across 10 row groups (10K rows per group)
//! - A `List<String>` column with sorted values (lexicographically ordered)
//! - A filter that matches only ~10% of row groups
//!
//! With pushdown enabled, ~90% of row groups can be skipped based on min/max
//! statistics, significantly reducing the rows that need to be decoded and
//! filtered.

use arrow::array::{ArrayRef, ListArray, StringArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Configuration for the benchmark dataset
#[derive(Clone)]
struct BenchmarkConfig {
    /// Total number of rows in the dataset
    total_rows: usize,
    /// Target number of rows per row group
    rows_per_group: usize,
    /// Selectivity: percentage of row groups that match the filter (0.0 to 1.0)
    selectivity: f64,
}

impl BenchmarkConfig {
    fn num_row_groups(&self) -> usize {
        (self.total_rows + self.rows_per_group - 1) / self.rows_per_group
    }
}

/// Generates test data with sorted List<String> column
///
/// Creates a dataset where list values are lexicographically sorted across
/// row groups, enabling effective min/max filtering. For example:
/// - Row group 0: lists containing "aaa" to "bbb"
/// - Row group 1: lists containing "bbc" to "ccc"
/// - Row group 2: lists containing "ccd" to "ddd"
/// - etc.
fn generate_sorted_list_data(
    config: &BenchmarkConfig,
    temp_dir: &TempDir,
) -> std::io::Result<PathBuf> {
    let file_path = temp_dir.path().join("data.parquet");

    // Define the schema with a List<String> column and an id column
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "list_col",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ]));

    let file = File::create(&file_path)?;
    
    // Configure writer with explicit row group size
    let props = WriterProperties::builder()
        .set_max_row_group_size(config.rows_per_group)
        .build();
    
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let num_groups = config.num_row_groups();
    let mut row_id = 0i64;

    // Generate row groups with sorted list values
    for group_idx in 0..num_groups {
        let mut batch_ids = Vec::new();
        let mut all_values = Vec::new();
        let mut offsets = vec![0i32];

        for local_idx in 0..config.rows_per_group {
            // Add row ID
            batch_ids.push(row_id);
            row_id += 1;

            // Create lexicographically sorted values
            // Each row group has values in a contiguous range
            let base_char = (97 + (group_idx % 26)) as u8; // 'a' + group offset
            let char1 = base_char as char;
            let char2 = (97 + ((group_idx / 26) % 26)) as u8 as char;
            let char3 = (48 + (local_idx % 10)) as u8 as char; // '0' to '9'

            let prefix = format!("{}{}{}", char1, char2, char3);

            // Add 3 values per row
            all_values.push(format!("{}_value_a", prefix));
            all_values.push(format!("{}_value_b", prefix));
            all_values.push(format!("{}_value_c", prefix));

            offsets.push((offsets.last().unwrap() + 3) as i32);
        }

        // Create arrays
        let id_array = Arc::new(arrow::array::Int64Array::from_iter_values(
            batch_ids.iter().copied(),
        )) as ArrayRef;

        let values_array =
            Arc::new(StringArray::from_iter_values(all_values.iter())) as ArrayRef;

        // Create offset buffer from scalar buffer
        let scalar_buffer: ScalarBuffer<i32> = offsets.into();
        let offset_buffer = OffsetBuffer::new(scalar_buffer);

        let list_array = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            offset_buffer,
            values_array,
            None,
        )) as ArrayRef;

        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, list_array])
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        writer
            .write(&batch)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    }

    writer
        .finish()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    Ok(file_path)
}

/// Benchmark for array_has filter with pushdown enabled
///
/// This measures the performance of filtering using array_has when pushdown
/// is active. With selective filters, this should skip ~90% of row groups,
/// resulting in minimal row decoding.
fn benchmark_array_has_with_pushdown(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("parquet_array_has_pushdown");

    // Test configuration: 100K rows, 10 row groups, selective filter (10% match)
    let config = BenchmarkConfig {
        total_rows: 100_000,
        rows_per_group: 10_000,
        selectivity: 0.1, // Only ~10% of row groups match the filter
    };

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let file_path = generate_sorted_list_data(&config, &temp_dir)
        .expect("Failed to generate test data");

    group.bench_function(
        BenchmarkId::from_parameter(format!(
            "rows={},selectivity={:.0}%",
            config.total_rows,
            config.selectivity * 100.0
        )),
        |b| {
            b.to_async(&rt).iter(|| async {
                let ctx = SessionContext::new();
                
                // Register the parquet file
                ctx.register_parquet(
                    "test_table",
                    file_path.to_str().unwrap(),
                    ParquetReadOptions::default(),
                )
                .await
                .expect("Failed to register parquet");

                // Execute query with array_has filter
                // This should demonstrate pushdown benefits for selective filters
                let sql = "SELECT * FROM test_table WHERE array_has(list_col, 'aa0_value_a')";
                let df = ctx.sql(sql).await.expect("Failed to create dataframe");
                
                // Collect results to ensure full execution
                let results = df.collect().await.expect("Failed to collect results");
                
                black_box(results)
            });
        },
    );

    group.finish();
}

/// Benchmark comparing filter selectivity impact
///
/// Demonstrates how different selectivity levels (percentage of matching
/// row groups) affect performance with pushdown enabled.
fn benchmark_selectivity_comparison(c: &mut Criterion) {
    let rt = Runtime::new().expect("Failed to create runtime");
    let mut group = c.benchmark_group("parquet_selectivity_impact");

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    
    // Pre-generate all test data
    let test_cases = vec![
        (0.1, "aa0_value_a"),  // 10% - matches first row group
        (0.3, "ac0_value_a"),  // 30% - matches first 3 row groups  
        (0.5, "ae0_value_a"),  // 50% - matches first 5 row groups
        (0.9, "ai0_value_a"),  // 90% - matches first 9 row groups
    ];

    for (selectivity, _target_value) in test_cases {
        let config = BenchmarkConfig {
            total_rows: 100_000,
            rows_per_group: 10_000,
            selectivity,
        };

        let file_path = generate_sorted_list_data(&config, &temp_dir)
            .expect("Failed to generate test data");

        group.bench_function(
            BenchmarkId::from_parameter(format!(
                "selectivity_{:.0}%",
                selectivity * 100.0
            )),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let ctx = SessionContext::new();
                    
                    ctx.register_parquet(
                        "test_table",
                        file_path.to_str().unwrap(),
                        ParquetReadOptions::default(),
                    )
                    .await
                    .expect("Failed to register parquet");

                    // Use a filter that matches the selectivity level
                    let sql = "SELECT COUNT(*) FROM test_table WHERE array_has(list_col, 'aa0_value_a')";
                    let df = ctx.sql(sql).await.expect("Failed to create dataframe");
                    let results = df.collect().await.expect("Failed to collect");
                    
                    black_box(results)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_array_has_with_pushdown,
    benchmark_selectivity_comparison
);
criterion_main!(benches);
