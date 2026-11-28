// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{
    Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::JoinType;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{
    HashJoinExecBuilder, PartitionMode, PreparedHashJoinBuild,
};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::test::TestMemoryExec;
use futures::TryStreamExt;
use tokio::runtime::Builder;

const BATCH_SIZE: usize = 8192;
const PROBE_BATCHES: usize = 32; // 262,144 probe rows

fn make_build_batch(keys: Vec<i32>) -> RecordBatch {
    let len = keys.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("b_key", DataType::Int32, false),
        Field::new("b_val", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int64Array::from_iter_values(0..len as i64)),
        ],
    )
    .unwrap()
}

fn make_probe_batches(keys_fn: impl Fn(usize) -> i32) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_key", DataType::Int32, false),
        Field::new("p_i32_1", DataType::Int32, false),
        Field::new("p_i32_2", DataType::Int32, false),
        Field::new("p_i64_1", DataType::Int64, false),
        Field::new("p_i64_2", DataType::Int64, false),
        Field::new("p_f64_1", DataType::Float64, false),
        Field::new("p_f64_2", DataType::Float64, false),
        Field::new("p_str", DataType::Utf8, false),
        Field::new("p_view", DataType::Utf8View, false),
    ]));

    (0..PROBE_BATCHES)
        .map(|b| {
            let base = b * BATCH_SIZE;
            let keys: Vec<i32> = (0..BATCH_SIZE).map(|i| keys_fn(base + i)).collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(keys)),
                    Arc::new(Int32Array::from_iter_values(
                        (0..BATCH_SIZE).map(|i| (base + i) as i32),
                    )),
                    Arc::new(Int32Array::from_iter_values(
                        (0..BATCH_SIZE).map(|i| ((base + i) * 3) as i32),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        (0..BATCH_SIZE).map(|i| (base + i) as i64 * 10),
                    )),
                    Arc::new(Int64Array::from_iter_values(
                        (0..BATCH_SIZE).map(|i| (base + i) as i64 * 17),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        (0..BATCH_SIZE).map(|i| (base + i) as f64 * 0.25),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        (0..BATCH_SIZE).map(|i| (base + i) as f64 * 1.5),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        (0..BATCH_SIZE).map(|i| format!("payload_str_{:08}", base + i)),
                    )),
                    Arc::new(StringViewArray::from_iter_values(
                        (0..BATCH_SIZE)
                            .map(|i| format!("payload_view_long_text_{:08}", base + i)),
                    )),
                ],
            )
            .unwrap()
        })
        .collect()
}

struct JoinCase {
    build: Vec<RecordBatch>,
    probe: Vec<RecordBatch>,
}

impl JoinCase {
    fn prepare(&self, rt: &tokio::runtime::Runtime) -> Arc<PreparedHashJoinBuild> {
        let ctx = Arc::new(TaskContext::default());
        let build_schema = self.build[0].schema();
        let probe_schema = self.probe[0].schema();
        let join = HashJoinExecBuilder::new(
            TestMemoryExec::try_new_exec(
                std::slice::from_ref(&self.build),
                Arc::clone(&build_schema),
                None,
            )
            .unwrap(),
            TestMemoryExec::try_new_exec(
                std::slice::from_ref(&self.probe),
                Arc::clone(&probe_schema),
                None,
            )
            .unwrap(),
            vec![(
                col("b_key", &build_schema).unwrap(),
                col("p_key", &probe_schema).unwrap(),
            )],
            JoinType::Inner,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .build()
        .unwrap();

        rt.block_on(join.prepare_build(
            Box::pin(
                MemoryStream::try_new(self.build.clone(), build_schema, None).unwrap(),
            ),
            Arc::clone(ctx.memory_pool()),
            Arc::clone(ctx.session_config().options()),
        ))
        .unwrap()
    }

    fn run(
        &self,
        rt: &tokio::runtime::Runtime,
        prepared: &Arc<PreparedHashJoinBuild>,
    ) -> usize {
        let ctx = Arc::new(TaskContext::default());
        let build_schema = self.build[0].schema();
        let probe_schema = self.probe[0].schema();
        let join = HashJoinExecBuilder::new(
            TestMemoryExec::try_new_exec(
                std::slice::from_ref(&self.build),
                Arc::clone(&build_schema),
                None,
            )
            .unwrap(),
            TestMemoryExec::try_new_exec(
                std::slice::from_ref(&self.probe),
                Arc::clone(&probe_schema),
                None,
            )
            .unwrap(),
            vec![(
                col("b_key", &build_schema).unwrap(),
                col("p_key", &probe_schema).unwrap(),
            )],
            JoinType::Inner,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_prepared_build(Arc::clone(prepared))
        .build()
        .unwrap();

        rt.block_on(async {
            let mut stream = join.execute(0, ctx).unwrap();
            let mut rows = 0usize;
            while let Some(batch) = stream.try_next().await.unwrap() {
                rows += batch.num_rows();
            }
            rows
        })
    }
}

fn benchmark(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("join_materialize");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));

    // 1. Well-predicted: 100%-hit PK-FK join with wide probe payload (identity fast path).
    let pk_fk = JoinCase {
        build: vec![make_build_batch((0..1024).collect())],
        probe: make_probe_batches(|i| ((i.wrapping_mul(2654435761)) & 1023) as i32),
    };
    let pk_fk_prep = pk_fk.prepare(&rt);
    assert_eq!(pk_fk.run(&rt, &pk_fk_prep), BATCH_SIZE * PROBE_BATCHES);
    group.bench_function("pk_fk_100pct_wide_probe", |b| {
        b.iter(|| black_box(pk_fk.run(&rt, &pk_fk_prep)));
    });

    // 2. Well-predicted slice: contiguous 75% suffix of each probe batch matches.
    let contiguous_75 = JoinCase {
        build: vec![make_build_batch((0..1024).collect())],
        probe: make_probe_batches(|i| {
            let row_in_batch = i % BATCH_SIZE;
            if row_in_batch < BATCH_SIZE / 4 {
                -1
            } else {
                (row_in_batch & 1023) as i32
            }
        }),
    };
    let contiguous_75_prep = contiguous_75.prepare(&rt);
    assert_eq!(
        contiguous_75.run(&rt, &contiguous_75_prep),
        (BATCH_SIZE * 3 / 4) * PROBE_BATCHES
    );
    group.bench_function("contiguous_slice_75pct", |b| {
        b.iter(|| black_box(contiguous_75.run(&rt, &contiguous_75_prep)));
    });

    // 3. Poorly-predicted: 50% random selectivity (fast-rejects contiguous check).
    let random_50 = JoinCase {
        build: vec![make_build_batch((0..1024).collect())],
        probe: make_probe_batches(|i| {
            let h = (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 32;
            if h & 1 == 0 { (h & 1023) as i32 } else { -1 }
        }),
    };
    let random_50_prep = random_50.prepare(&rt);
    group.bench_function("random_selective_50pct", |b| {
        b.iter(|| black_box(random_50.run(&rt, &random_50_prep)));
    });

    // 4. Untouched control: 2x fanout (duplicate probe indices, never contiguous).
    let fanout_2x = JoinCase {
        build: vec![make_build_batch((0..2048).map(|i| i % 1024).collect())],
        probe: make_probe_batches(|i| ((i.wrapping_mul(2654435761)) & 1023) as i32),
    };
    let fanout_2x_prep = fanout_2x.prepare(&rt);
    assert_eq!(
        fanout_2x.run(&rt, &fanout_2x_prep),
        BATCH_SIZE * PROBE_BATCHES * 2
    );
    group.bench_function("fanout_2x_untouched_control", |b| {
        b.iter(|| black_box(fanout_2x.run(&rt, &fanout_2x_prep)));
    });

    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
