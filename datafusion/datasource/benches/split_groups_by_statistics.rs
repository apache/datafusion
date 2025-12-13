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

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::{generate_test_files, verify_sort_integrity};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

pub fn compare_split_groups_by_statistics_algorithms(c: &mut Criterion) {
    let file_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));

    let sort_expr = PhysicalSortExpr::new_default(Arc::new(Column::new("value", 0)));
    let sort_ordering = LexOrdering::from([sort_expr]);

    // Small, medium, large number of files
    let file_counts = [10, 100, 1000];
    let overlap_factors = [0.0, 0.2, 0.5, 0.8]; // No, low, medium, high overlap

    let target_partitions: [usize; 4] = [4, 8, 16, 32];

    let mut group = c.benchmark_group("split_groups");
    group.measurement_time(Duration::from_secs(10));

    for &num_files in &file_counts {
        for &overlap in &overlap_factors {
            let file_groups = generate_test_files(num_files, overlap);
            // Benchmark original algorithm
            group.bench_with_input(
                BenchmarkId::new(
                    "original",
                    format!("files={num_files},overlap={overlap:.1}"),
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
                        format!("v2_partitions={tp}"),
                        format!("files={num_files},overlap={overlap:.1}"),
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
                            result = FileScanConfig::split_groups_by_statistics_with_target_partitions(
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

criterion_group!(benches, compare_split_groups_by_statistics_algorithms);
criterion_main!(benches);
