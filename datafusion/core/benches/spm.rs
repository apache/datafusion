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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

use criterion::async_executor::FuturesExecutor;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_datasource::memory::MemorySourceConfig;

fn generate_spm_for_round_robin_tie_breaker(
    has_same_value: bool,
    enable_round_robin_repartition: bool,
    batch_count: usize,
    partition_count: usize,
) -> SortPreservingMergeExec {
    let row_size = 256;
    let rb = if has_same_value {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1; row_size]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("a"); row_size]));
        let c: ArrayRef = Arc::new(Int64Array::from_iter(vec![0; row_size]));
        RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap()
    } else {
        let v = (0i32..row_size as i32).collect::<Vec<_>>();
        let a: ArrayRef = Arc::new(Int32Array::from(v));

        // Use alphanumeric characters
        let charset: Vec<char> =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                .chars()
                .collect();

        let mut strings = Vec::new();
        for i in 0..256 {
            let mut s = String::new();
            s.push(charset[i % charset.len()]);
            s.push(charset[(i / charset.len()) % charset.len()]);
            strings.push(Some(s));
        }

        let b: ArrayRef = Arc::new(StringArray::from_iter(strings));

        let v = (0i64..row_size as i64).collect::<Vec<_>>();
        let c: ArrayRef = Arc::new(Int64Array::from_iter(v));
        RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap()
    };

    let schema = rb.schema();
    let rbs = std::iter::repeat_n(rb, batch_count).collect::<Vec<_>>();
    let partitions = vec![rbs.clone(); partition_count];
    let sort = [
        PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: Default::default(),
        },
        PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: Default::default(),
        },
    ]
    .into();

    let exec = MemorySourceConfig::try_new_exec(&partitions, schema, None).unwrap();
    SortPreservingMergeExec::new(sort, exec)
        .with_round_robin_repartition(enable_round_robin_repartition)
}

fn run_bench(
    c: &mut Criterion,
    has_same_value: bool,
    enable_round_robin_repartition: bool,
    batch_count: usize,
    partition_count: usize,
    description: &str,
) {
    let task_ctx = TaskContext::default();
    let task_ctx = Arc::new(task_ctx);

    let spm = Arc::new(generate_spm_for_round_robin_tie_breaker(
        has_same_value,
        enable_round_robin_repartition,
        batch_count,
        partition_count,
    )) as Arc<dyn ExecutionPlan>;

    c.bench_function(description, |b| {
        b.to_async(FuturesExecutor)
            .iter(|| black_box(collect(Arc::clone(&spm), Arc::clone(&task_ctx))))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let params = [
        (true, false, "low_card_without_tiebreaker"), // low cardinality, no tie breaker
        (true, true, "low_card_with_tiebreaker"),     // low cardinality, with tie breaker
        (false, false, "high_card_without_tiebreaker"), // high cardinality, no tie breaker
        (false, true, "high_card_with_tiebreaker"), // high cardinality, with tie breaker
    ];

    let batch_counts = [1, 25, 625];
    let partition_counts = [2, 8, 32];

    for &(has_same_value, enable_round_robin_repartition, cardinality_label) in &params {
        for &batch_count in &batch_counts {
            for &partition_count in &partition_counts {
                let description = format!(
                    "{cardinality_label}_batch_count_{batch_count}_partition_count_{partition_count}"
                );
                run_bench(
                    c,
                    has_same_value,
                    enable_round_robin_repartition,
                    batch_count,
                    partition_count,
                    &description,
                );
            }
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
