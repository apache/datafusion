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
//! Covers inner, semi, and anti joins with key-only and payload-bearing inputs.
//! Session settings use the standard DataFusion environment configuration.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_common::{JoinType, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::test::TestMemoryExec;
use futures::StreamExt;
use tokio::runtime::Runtime;

const BUILD_ROWS: usize = 8_192;
const PROBE_ROWS: usize = 1_048_576;
const BATCH_SIZE: usize = 8_192;
const SHORT_PROBE_ROWS: usize = 4_096;
const PHASE_ROWS: usize = PROBE_ROWS / 2;
const HIT_EVERY: usize = 16;
const SPARSE_KEY_STEP: i32 = 16;
const DENSE_KEY_STEP: i32 = 1;
const WIDE_KEY_STEP: i32 = 64;
const NON_NULL_EVERY: usize = 1_024;
const PAYLOAD_BYTES: usize = 64;

#[derive(Clone, Copy)]
enum Pattern {
    AllHit,
    InRange,
    OutOfRange,
    HighToLow,
    LowToHigh,
    ShortProbe,
    NullHeavyHits,
    NullHeavyMisses,
}

fn make_batches(
    rows: usize,
    key_step: i32,
    pattern: Pattern,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    (0..rows)
        .step_by(BATCH_SIZE)
        .map(|start| {
            let end = (start + BATCH_SIZE).min(rows);
            let keys = Int32Array::from_iter((start..end).map(|row| {
                if matches!(pattern, Pattern::NullHeavyHits | Pattern::NullHeavyMisses) {
                    return (row % NON_NULL_EVERY == 0).then(|| {
                        (row % (BUILD_ROWS - 1)) as i32 * key_step
                            + i32::from(matches!(pattern, Pattern::NullHeavyMisses))
                    });
                }
                let high_hit = match pattern {
                    Pattern::AllHit => true,
                    Pattern::HighToLow => row < PHASE_ROWS,
                    Pattern::LowToHigh => row >= PHASE_ROWS,
                    _ => false,
                };
                Some(if high_hit || row % HIT_EVERY == 0 {
                    (row % BUILD_ROWS) as i32 * key_step
                } else if matches!(pattern, Pattern::OutOfRange) {
                    (BUILD_ROWS + row % BUILD_ROWS) as i32 * key_step
                } else {
                    // Every hole stays strictly between the build min and max.
                    (row % (BUILD_ROWS - 1)) as i32 * key_step + 1
                })
            }));
            let mut columns: Vec<ArrayRef> = vec![Arc::new(keys)];
            if schema.fields().len() > 1 {
                columns.push(Arc::new(Int64Array::from_iter_values(
                    (start..end).map(|row| row as i64),
                )));
                columns.push(Arc::new(StringArray::from_iter_values(
                    (start..end).map(|row| format!("{row:0PAYLOAD_BYTES$}")),
                )));
            }
            RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
        })
        .collect()
}

fn make_join(
    build: &Arc<dyn ExecutionPlan>,
    probe: &Arc<dyn ExecutionPlan>,
    join_type: JoinType,
) -> HashJoinExec {
    HashJoinExec::try_new(
        Arc::clone(build),
        Arc::clone(probe),
        vec![(
            col("key", &build.schema()).unwrap(),
            col("key", &probe.schema()).unwrap(),
        )],
        None,
        &join_type,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
        false,
    )
    .unwrap()
}

fn bench_hash_join_probe(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let config = SessionConfig::from_env()
        .unwrap()
        .with_batch_size(BATCH_SIZE);
    let context = Arc::new(TaskContext::default().with_session_config(config));
    let mut group = c.benchmark_group("hash_join_probe");
    // Default PHJ settings select ArrayMap only for the dense case. Sparse keys
    // span 131,057 values; wide keys span 524,225.
    for (name, pattern, key_step, join_type, with_payload) in [
        (
            "low_hit_in_range",
            Pattern::InRange,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "low_hit_out_of_range",
            Pattern::OutOfRange,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "high_to_low",
            Pattern::HighToLow,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "low_to_high",
            Pattern::LowToHigh,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "all_hit",
            Pattern::AllHit,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "short_probe",
            Pattern::ShortProbe,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "dense",
            Pattern::OutOfRange,
            DENSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "wide_range",
            Pattern::InRange,
            WIDE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "right_semi_low_hit",
            Pattern::InRange,
            SPARSE_KEY_STEP,
            JoinType::RightSemi,
            false,
        ),
        (
            "right_anti_low_hit",
            Pattern::InRange,
            SPARSE_KEY_STEP,
            JoinType::RightAnti,
            false,
        ),
        (
            "inner_low_hit_payload",
            Pattern::InRange,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            true,
        ),
        (
            "right_semi_low_hit_payload",
            Pattern::InRange,
            SPARSE_KEY_STEP,
            JoinType::RightSemi,
            true,
        ),
        (
            "right_anti_low_hit_payload",
            Pattern::InRange,
            SPARSE_KEY_STEP,
            JoinType::RightAnti,
            true,
        ),
        (
            "null_heavy_hits",
            Pattern::NullHeavyHits,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
        (
            "null_heavy_misses",
            Pattern::NullHeavyMisses,
            SPARSE_KEY_STEP,
            JoinType::Inner,
            false,
        ),
    ] {
        let nullable =
            matches!(pattern, Pattern::NullHeavyHits | Pattern::NullHeavyMisses);
        let mut fields = vec![Field::new("key", DataType::Int32, nullable)];
        if with_payload {
            fields.push(Field::new("row_id", DataType::Int64, false));
            fields.push(Field::new("payload", DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));
        let rows = if matches!(pattern, Pattern::ShortProbe) {
            SHORT_PROBE_ROWS
        } else {
            PROBE_ROWS
        };
        let hits = match pattern {
            Pattern::AllHit => rows,
            Pattern::HighToLow | Pattern::LowToHigh => {
                PHASE_ROWS + PHASE_ROWS / HIT_EVERY
            }
            Pattern::NullHeavyHits => rows / NON_NULL_EVERY,
            Pattern::NullHeavyMisses => 0,
            _ => rows / HIT_EVERY,
        };
        let expected = if join_type == JoinType::RightAnti {
            rows - hits
        } else {
            hits
        };
        let expected_columns =
            schema.fields().len() * if join_type == JoinType::Inner { 2 } else { 1 };
        let build: Arc<dyn ExecutionPlan> = TestMemoryExec::try_new_exec(
            &[make_batches(BUILD_ROWS, key_step, Pattern::AllHit, &schema)],
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        let probe: Arc<dyn ExecutionPlan> = TestMemoryExec::try_new_exec(
            &[make_batches(rows, key_step, pattern, &schema)],
            Arc::clone(&schema),
            None,
        )
        .unwrap();

        let diagnostic = make_join(&build, &probe, join_type);
        let output_rows = rt.block_on(async {
            let mut stream = diagnostic.execute(0, Arc::clone(&context)).unwrap();
            let mut output_rows = 0;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                assert_eq!(batch.num_columns(), expected_columns, "{name}");
                output_rows += batch.num_rows();
            }
            output_rows
        });
        assert_eq!(output_rows, expected, "{name}");

        group.bench_function(name, |b| {
            b.iter_batched(
                || make_join(&build, &probe, join_type),
                |join| {
                    rt.block_on(async {
                        let mut stream = join.execute(0, Arc::clone(&context)).unwrap();
                        while let Some(batch) = stream.next().await {
                            black_box(batch.unwrap());
                        }
                    })
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_hash_join_probe);
criterion_main!(benches);
