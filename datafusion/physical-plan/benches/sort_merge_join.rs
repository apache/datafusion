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

use arrow_schema::SortOptions;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::JoinType::Inner;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::DiskManagerConfig;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::joins::SortMergeJoinExec;
use datafusion_physical_plan::test::{build_table_i32, TestMemoryExec};
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_test_data() -> SortMergeJoinExec {
    let left_batch = build_table_i32(
        ("a1", &vec![0, 1, 2, 3, 4, 5]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let left_schema = left_batch.schema();
    let left =
        TestMemoryExec::try_new_exec(&[vec![left_batch]], left_schema, None).unwrap();
    let right_batch = build_table_i32(
        ("a2", &vec![0, 10, 20, 30, 40]),
        ("b2", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let right_schema = right_batch.schema();
    let right =
        TestMemoryExec::try_new_exec(&[vec![right_batch]], right_schema, None).unwrap();
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema()).unwrap()) as _,
        Arc::new(Column::new_with_schema("b2", &right.schema()).unwrap()) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    SortMergeJoinExec::try_new(left, right, on, None, Inner, sort_options, false)
        .unwrap()
}

// `cargo bench --bench sort_merge_join`
fn bench_spill(c: &mut Criterion) {
    let sort_merge_join_exec = create_test_data();

    let mut group = c.benchmark_group("sort_merge_join_spill");
    let rt = Runtime::new().unwrap();

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0) // Set memory limit to 100 bytes
        .with_disk_manager(DiskManagerConfig::NewOs) // Enable DiskManager to allow spilling
        .build_arc()
        .unwrap();
    let session_config = SessionConfig::default();
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config.clone())
            .with_runtime(Arc::clone(&runtime)),
    );

    group.bench_function("SortMergeJoinExec_spill", |b| {
        b.iter(|| {
            criterion::black_box(
                rt.block_on(async {
                    let stream = sort_merge_join_exec.execute(0, Arc::clone(&task_ctx)).unwrap();
                    collect(stream).await.unwrap()
                })
            )
        })
    });
    group.finish();

    assert!(sort_merge_join_exec.metrics().unwrap().spill_count().unwrap() > 0);
    assert!(sort_merge_join_exec.metrics().unwrap().spilled_bytes().unwrap() > 0);
    assert!(sort_merge_join_exec.metrics().unwrap().spilled_rows().unwrap() > 0);
}

criterion_group!(benches, bench_spill);
criterion_main!(benches);
