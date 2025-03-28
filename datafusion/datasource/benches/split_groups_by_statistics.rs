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

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use object_store::{path::Path, ObjectMeta};
use std::sync::Arc;
use std::time::Duration;

/// Generates test files with min-max statistics in different overlap patterns
fn generate_test_files(num_files: usize, overlap_factor: f64) -> Vec<FileGroup> {
    let mut files = Vec::with_capacity(num_files);
    let range_size = if overlap_factor == 0.0 {
        100 / num_files as i64
    } else {
        (100.0 / (overlap_factor * num_files as f64)).max(1.0) as i64
    };

    for i in 0..num_files {
        let base = (i as f64 * range_size as f64 * (1.0 - overlap_factor)) as i64;
        let min = base as f64;
        let max = (base + range_size) as f64;

        let file = PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::from(format!("file_{}.parquet", i)),
                last_modified: chrono::Utc::now(),
                size: 1000,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: Some(Statistics {
                num_rows: Precision::Exact(100),
                total_byte_size: Precision::Exact(1000),
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
                    min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
                    sum_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                }],
            }),
            extensions: None,
            metadata_size_hint: None,
        };
        files.push(file);
    }

    vec![FileGroup::new(files)]
}

pub fn compare_split_groups_by_statistics_algorithms(c: &mut Criterion) {
    let file_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));

    let sort_expr = PhysicalSortExpr {
        expr: Arc::new(datafusion_physical_expr::expressions::Column::new(
            "value", 0,
        )),
        options: arrow::compute::SortOptions::default(),
    };
    let sort_ordering = LexOrdering::from(vec![sort_expr]);

    let file_counts = [10, 100, 1000]; // Small, medium, large number of files
    let overlap_factors = [0.0, 0.2, 0.5, 0.8]; // Low, medium, high overlap

    let target_partitions: [usize; 4] = [4, 8, 16, 32];

    let mut group = c.benchmark_group("");
    group.measurement_time(Duration::from_secs(10));

    for &num_files in &file_counts {
        for &overlap in &overlap_factors {
            let file_groups = generate_test_files(num_files, overlap);
            // Benchmark original algorithm
            group.bench_with_input(
                BenchmarkId::new(
                    "original",
                    format!("files={},overlap={:.1}", num_files, overlap),
                ),
                &(
                    file_groups.clone(),
                    file_schema.clone(),
                    sort_ordering.clone(),
                ),
                |b, (fg, schema, order)| {
                    let mut result = Vec::new();
                    b.iter(|| {
                        result =
                            FileScanConfig::split_groups_by_statistics(schema, fg, order)
                                .unwrap();
                    });
                    assert!(verify_sort_integrity(&result));
                },
            );

            // Benchmark new algorithm with different target partitions
            for &tp in &target_partitions {
                group.bench_with_input(
                    BenchmarkId::new(
                        format!("v2_partitions={}", tp),
                        format!("files={},overlap={:.1}", num_files, overlap),
                    ),
                    &(
                        file_groups.clone(),
                        file_schema.clone(),
                        sort_ordering.clone(),
                        tp,
                    ),
                    |b, (fg, schema, order, target)| {
                        let mut result = Vec::new();
                        b.iter(|| {
                            result = FileScanConfig::split_groups_by_statistics_v2(
                                schema, fg, order, *target,
                            )
                            .unwrap();
                        });
                        assert!(verify_sort_integrity(&result));
                    },
                );
            }
        }
    }

    group.finish();
}

// Helper function to verify that files within each group maintain sort order
fn verify_sort_integrity(file_groups: &[FileGroup]) -> bool {
    for group in file_groups {
        let files = group.iter().collect::<Vec<_>>();
        for i in 1..files.len() {
            let prev_file = files[i - 1];
            let curr_file = files[i];

            // Check if the min value of current file is greater than max value of previous file
            if let (Some(prev_stats), Some(curr_stats)) =
                (&prev_file.statistics, &curr_file.statistics)
            {
                let prev_max = &prev_stats.column_statistics[0].max_value;
                let curr_min = &curr_stats.column_statistics[0].min_value;
                if curr_min.get_value().unwrap() <= prev_max.get_value().unwrap() {
                    return false;
                }
            }
        }
    }
    true
}

criterion_group!(benches, compare_split_groups_by_statistics_algorithms);
criterion_main!(benches);
