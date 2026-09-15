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

//! Full in-memory HashJoin execution, including build, with fixed Int32 workloads.
//! All inputs carry an Int32 data column and a Utf8 payload column.
//! Session settings use the standard DataFusion environment configuration.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinType, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

const BATCH_SIZE: usize = 8_192;
const BUILD_ROWS: usize = 20_000;
const PROBE_ROWS: usize = 1_000_000;
const LARGE_BUILD_ROWS: usize = 100_000;
const SPARSE_KEY_STEP: i32 = 10;
const DENSE_KEY_STEP: i32 = 1;
const HIT_EVERY: usize = 10;

#[derive(Clone, Copy)]
enum Pattern {
    AllHit,
    InterleavedLowHit,
    ClusteredLowHit,
    HighToLow,
}

fn make_batches(
    rows: usize,
    build_rows: usize,
    key_step: i32,
    pattern: Pattern,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    let mut hits = 0;
    (0..rows)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = (start + BATCH_SIZE).min(rows);
            let keys = Int32Array::from_iter_values((start..end).map(|row| {
                let hit = match pattern {
                    Pattern::AllHit => true,
                    Pattern::InterleavedLowHit => row % HIT_EVERY == 0,
                    Pattern::ClusteredLowHit => row < rows / HIT_EVERY,
                    Pattern::HighToLow => row < rows / 2 || row % HIT_EVERY == 0,
                };
                if hit {
                    let key = (hits % build_rows) as i32 * key_step;
                    hits += 1;
                    key
                } else {
                    // Every hole stays strictly between the build min and max.
                    (row % (build_rows - 1)) as i32 * key_step + 1
                }
            }));
            let columns: Vec<ArrayRef> = vec![
                Arc::new(keys),
                Arc::new(Int32Array::from_iter_values(
                    (start..end).map(|row| row as i32),
                )),
                Arc::new(StringArray::from_iter_values(
                    (start..end).map(|row| format!("{row}")),
                )),
            ];
            RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
        })
        .collect()
}

fn make_join(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    join_type: JoinType,
) -> Arc<dyn ExecutionPlan> {
    let schema = build_batches[0].schema();
    let build = TestMemoryExec::try_new_exec(
        &[build_batches.to_vec()],
        Arc::clone(&schema),
        None,
    )
    .unwrap();
    let probe = TestMemoryExec::try_new_exec(
        &[probe_batches.to_vec()],
        Arc::clone(&schema),
        None,
    )
    .unwrap();
    Arc::new(
        HashJoinExec::try_new(
            build,
            probe,
            vec![(col("key", &schema).unwrap(), col("key", &schema).unwrap())],
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    )
}

fn bench_hash_join_probe(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = SessionConfig::from_env()
        .unwrap()
        .with_batch_size(BATCH_SIZE);
    let context = Arc::new(TaskContext::default().with_session_config(config));
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("data", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let mut group = c.benchmark_group("hash_join_probe");
    for (name, join_type, pattern, build_rows, probe_rows, key_step) in [
        (
            "right_semi_interleaved_10pct",
            JoinType::RightSemi,
            Pattern::InterleavedLowHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_semi_all_hit",
            JoinType::RightSemi,
            Pattern::AllHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_anti_interleaved_10pct",
            JoinType::RightAnti,
            Pattern::InterleavedLowHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_anti_all_hit",
            JoinType::RightAnti,
            Pattern::AllHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "inner_interleaved_10pct",
            JoinType::Inner,
            Pattern::InterleavedLowHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "inner_all_hit",
            JoinType::Inner,
            Pattern::AllHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_semi_clustered_10pct",
            JoinType::RightSemi,
            Pattern::ClusteredLowHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_anti_clustered_10pct",
            JoinType::RightAnti,
            Pattern::ClusteredLowHit,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_semi_high_to_low",
            JoinType::RightSemi,
            Pattern::HighToLow,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_anti_high_to_low",
            JoinType::RightAnti,
            Pattern::HighToLow,
            BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
        (
            "right_semi_single_batch_all_hit",
            JoinType::RightSemi,
            Pattern::AllHit,
            BUILD_ROWS,
            BATCH_SIZE,
            SPARSE_KEY_STEP,
        ),
        (
            "right_semi_dense_100k_all_hit",
            JoinType::RightSemi,
            Pattern::AllHit,
            LARGE_BUILD_ROWS,
            PROBE_ROWS,
            DENSE_KEY_STEP,
        ),
        (
            "right_semi_sparse_100k_all_hit",
            JoinType::RightSemi,
            Pattern::AllHit,
            LARGE_BUILD_ROWS,
            PROBE_ROWS,
            SPARSE_KEY_STEP,
        ),
    ] {
        let build =
            make_batches(build_rows, build_rows, key_step, Pattern::AllHit, &schema);
        let probe = make_batches(probe_rows, build_rows, key_step, pattern, &schema);
        let run = || {
            let join = make_join(&build, &probe, join_type);
            rt.block_on(collect(join, Arc::clone(&context))).unwrap()
        };

        let hits = match pattern {
            Pattern::AllHit => probe_rows,
            Pattern::InterleavedLowHit | Pattern::ClusteredLowHit => {
                probe_rows / HIT_EVERY
            }
            Pattern::HighToLow => probe_rows / 2 + probe_rows / 2 / HIT_EVERY,
        };
        let expected_rows = if join_type == JoinType::RightAnti {
            probe_rows - hits
        } else {
            hits
        };
        let output = run();
        assert_eq!(
            output.iter().map(RecordBatch::num_rows).sum::<usize>(),
            expected_rows,
            "{name}",
        );
        drop(output);

        group.bench_function(name, |b| b.iter(|| black_box(run())));
    }
    group.finish();
}

criterion_group!(benches, bench_hash_join_probe);
criterion_main!(benches);
