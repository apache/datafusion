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

use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::JoinType::Inner;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::disk_manager::DiskManagerConfig;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::joins::SortMergeJoinExec;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_smj_exec(array_len: usize, batch_size: usize) -> SortMergeJoinExec {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    // define data.
    let batches = (0..array_len / batch_size)
        .map(|i| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(vec![i as i32; batch_size])),
                    Arc::new(Int32Array::from(vec![i as i32; batch_size])),
                    Arc::new(Int32Array::from(vec![i as i32; batch_size])),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let datasource_exec =
        MemorySourceConfig::try_new_exec(&[batches], Arc::clone(&schema), None).unwrap();

    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &schema).unwrap()) as _,
        Arc::new(Column::new_with_schema("b1", &schema).unwrap()) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];
    SortMergeJoinExec::try_new(
        datasource_exec.clone(),
        datasource_exec.clone(),
        on,
        None,
        Inner,
        sort_options,
        false,
    )
    .unwrap()
}

// `cargo bench --bench sort_merge_join`
fn bench_spill(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    c.bench_function("SortMergeJoinExec_spill", |b| {
        let join_exec = create_smj_exec(1_048_576, 4096);

        // create a session context. enable spilling
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_limit(1024, 1.0) // Set memory limit to 1KB
            .with_disk_manager(DiskManagerConfig::NewOs) // Enable DiskManager to allow spilling
            .build_arc()
            .unwrap();
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new())
                .with_runtime(Arc::clone(&runtime_env)),
        );

        b.iter(|| {
            let stream = join_exec.execute(0, Arc::clone(&task_ctx)).unwrap();
            criterion::black_box(rt.block_on(collect(stream))).unwrap();
        });
        // check if spilling happened
        assert!(join_exec.metrics().unwrap().spilled_rows().unwrap() > 0);
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_spill
);
criterion_main!(benches);
