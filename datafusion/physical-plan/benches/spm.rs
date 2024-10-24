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

use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, Int32Array, Int64Array, StringArray};
use criterion::async_executor::FuturesExecutor;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_plan::memory::MemoryExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{collect, ExecutionPlan};
use std::sync::Arc;

fn generate_spm_for_round_robin_tie_breaker(
    has_same_value: bool,
    enable_round_robin_repartition: bool,
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
        // Create 256 unique strings
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

    let rbs = (0..16).map(|_| rb.clone()).collect::<Vec<_>>();
    // Ensure this to be larger than 2 so it goes to the SPM code path
    let partitiones = vec![rbs.clone(); 2];

    let schema = rb.schema();
    let sort = vec![
        PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: Default::default(),
        },
        PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: Default::default(),
        },
    ];

    let exec = MemoryExec::try_new(&partitiones, schema, None).unwrap();
    SortPreservingMergeExec::new(sort, Arc::new(exec))
        .with_round_robin_repartition(enable_round_robin_repartition)
}

fn criterion_benchmark(c: &mut Criterion) {
    // create input data
    let task_ctx = TaskContext::default();
    let task_ctx = Arc::new(task_ctx);

    let spm_wo_tb = Arc::new(generate_spm_for_round_robin_tie_breaker(true, false))
        as Arc<dyn ExecutionPlan>;
    c.bench_function("spm without tie breaker low card", |b| {
        b.to_async(FuturesExecutor).iter(|| {
            black_box(collect(Arc::clone(&spm_wo_tb), Arc::clone(&task_ctx)))
        })
    });

    let spm_w_tb = Arc::new(generate_spm_for_round_robin_tie_breaker(true, true))
        as Arc<dyn ExecutionPlan>;
    c.bench_function("spm with tie breaker low card", |b| {
        b.to_async(FuturesExecutor).iter(|| {
            black_box(collect(Arc::clone(&spm_w_tb), Arc::clone(&task_ctx)))
        })
    });

    let spm_wo_tb = Arc::new(generate_spm_for_round_robin_tie_breaker(false, false))
        as Arc<dyn ExecutionPlan>;
    c.bench_function("spm without tie breaker high card", |b| {
        b.to_async(FuturesExecutor).iter(|| {
            black_box(collect(Arc::clone(&spm_wo_tb), Arc::clone(&task_ctx)))
        })
    });

    let spm_w_tb = Arc::new(generate_spm_for_round_robin_tie_breaker(false, true))
        as Arc<dyn ExecutionPlan>;
    c.bench_function("spm with tie breaker high card", |b| {
        b.to_async(FuturesExecutor).iter(|| {
            black_box(collect(Arc::clone(&spm_w_tb), Arc::clone(&task_ctx)))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
