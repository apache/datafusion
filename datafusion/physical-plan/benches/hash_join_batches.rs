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

//! Hash-join build storage across controlled input batch boundaries.
//!
//! SQL can prune payloads or change the input batch layout, so this benchmark
//! uses the public physical execution API. Input generation is excluded; fresh
//! plan construction, build, probe and output draining are included. Large cases
//! retain over 64 MiB of unique Arrow backing. Cases include tiny independent
//! batches, shared slices and oversized backing buffers. The small case measures
//! the compact-build path. Every case probes 4096 rows and checks the matched
//! build-row checksum before timing. Inner joins have 50% matches; the outer case
//! has 6.25%. Perfect-hash selection is enabled and disabled over identical inputs.
//! The separate correctness run reports peak reserved bytes, not process RSS;
//! timed runs use the normal memory pool without reservation instrumentation.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Int32Array, ListArray, RecordBatch, StringArray,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::utils::memory::RecordBatchMemoryCounter;
use datafusion_common::{JoinType, Result};
use datafusion_execution::memory_pool::{
    MemoryPool, PeakRecordingPool, UnboundedMemoryPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_execution::{TaskContext, config::SessionConfig};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, col, lit};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{HashJoinExec, HashJoinExecBuilder, PartitionMode};
use datafusion_physical_plan::test::TestMemoryExec;
use futures::TryStreamExt;
use tokio::runtime::Builder;

const PROBE_ROWS: usize = 4096;
const INPUT_BATCH_ROWS: usize = 4096;
const COMPACT_BUILD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy)]
enum Keys {
    Integer,
    Computed,
    Dictionary,
}

#[derive(Clone, Copy)]
enum Payload {
    Plain,
    Dictionary,
    List,
}

#[derive(Clone, Copy)]
enum Layout {
    Independent,
    Tiny,
    Sliced,
    Overallocated,
}

struct Workload {
    build: Vec<RecordBatch>,
    probe: RecordBatch,
    keys: Keys,
    join_type: JoinType,
    expected_rows: usize,
    expected_sum: i64,
}

fn key_array(keys: &[i32], kind: Keys) -> ArrayRef {
    if matches!(kind, Keys::Dictionary) {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(0..keys.len() as i32),
                Arc::new(StringArray::from_iter_values(
                    keys.iter().map(|key| format!("key-{key:08}")),
                )),
            )
            .unwrap(),
        )
    } else {
        Arc::new(Int32Array::from_iter_values(keys.iter().copied()))
    }
}

impl Workload {
    fn new(
        rows: usize,
        width: usize,
        keys: Keys,
        payload: Payload,
        layout: Layout,
        low_match_outer: bool,
    ) -> Self {
        let value = "x".repeat(width);
        let batch = |start, end| {
            let ids = (start..end).map(|id| id as i32).collect::<Vec<_>>();
            let key = key_array(&ids, keys);
            let values: ArrayRef = Arc::new(StringArray::from_iter_values(
                ids.iter().map(|_| value.as_str()),
            ));
            let values: ArrayRef = match payload {
                Payload::Plain => values,
                Payload::Dictionary => Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from_iter_values(0..ids.len() as i32),
                        values,
                    )
                    .unwrap(),
                ),
                Payload::List => Arc::new(ListArray::new(
                    Arc::new(Field::new_list_field(DataType::Utf8, false)),
                    OffsetBuffer::new(ScalarBuffer::from(
                        (0..=ids.len() as i32).collect::<Vec<_>>(),
                    )),
                    values,
                    None,
                )),
            };
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("key", key.data_type().clone(), false),
                    Field::new("payload", values.data_type().clone(), false),
                    Field::new("id", DataType::Int32, false),
                ])),
                vec![key, values, Arc::new(Int32Array::from(ids))],
            )
            .unwrap()
        };
        let build = if matches!(layout, Layout::Sliced) {
            let backing = batch(0, rows);
            (0..rows)
                .step_by(INPUT_BATCH_ROWS)
                .map(|start| backing.slice(start, (rows - start).min(INPUT_BATCH_ROWS)))
                .collect::<Vec<_>>()
        } else {
            let batch_rows = if matches!(layout, Layout::Tiny) {
                64
            } else {
                INPUT_BATCH_ROWS
            };
            (0..rows)
                .step_by(batch_rows)
                .map(|start| {
                    let count = (rows - start).min(batch_rows);
                    let allocated = if matches!(layout, Layout::Overallocated) {
                        count * 2
                    } else {
                        count
                    };
                    batch(start, start + allocated).slice(0, count)
                })
                .collect::<Vec<_>>()
        };
        let hit_every = if low_match_outer { 16 } else { 2 };
        let probe_ids = (0..PROBE_ROWS)
            .map(|index| {
                let candidate = (index * rows / PROBE_ROWS) % rows;
                (candidate + usize::from(index % hit_every != 0) * rows) as i32
            })
            .collect::<Vec<_>>();
        let probe_keys = key_array(&probe_ids, keys);
        let probe = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                probe_keys.data_type().clone(),
                false,
            )])),
            vec![probe_keys],
        )
        .unwrap();
        Self {
            build,
            probe,
            keys,
            join_type: if low_match_outer {
                JoinType::Right
            } else {
                JoinType::Inner
            },
            expected_rows: if low_match_outer {
                PROBE_ROWS
            } else {
                PROBE_ROWS / hit_every
            },
            expected_sum: probe_ids
                .iter()
                .step_by(hit_every)
                .map(|&id| i64::from(id))
                .sum(),
        }
    }

    fn join(&self) -> Result<HashJoinExec> {
        let left_schema = self.build[0].schema();
        let right_schema = self.probe.schema();
        let mut left_key = col("key", &left_schema)?;
        let mut right_key = col("key", &right_schema)?;
        if matches!(self.keys, Keys::Computed) {
            left_key = Arc::new(BinaryExpr::new(left_key, Operator::Plus, lit(7i32)));
            right_key = Arc::new(BinaryExpr::new(right_key, Operator::Plus, lit(7i32)));
        }
        HashJoinExecBuilder::new(
            TestMemoryExec::try_new_exec(
                std::slice::from_ref(&self.build),
                left_schema,
                None,
            )?,
            TestMemoryExec::try_new_exec(
                &[vec![self.probe.clone()]],
                right_schema,
                None,
            )?,
            vec![(left_key, right_key)],
            self.join_type,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_projection(Some(vec![2, 1]))
        .build()
    }

    async fn run(&self, context: Arc<TaskContext>) -> Result<(usize, i64)> {
        let mut stream = self.join()?.execute(0, context)?;
        let mut rows = 0;
        let mut sum = 0;
        while let Some(batch) = stream.try_next().await? {
            rows += batch.num_rows();
            sum += batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .flatten()
                .map(i64::from)
                .sum::<i64>();
            black_box(batch.column(1));
        }
        Ok((rows, sum))
    }
}

fn benchmark(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let mut group = c.benchmark_group("hash_join_batches");
    for (name, rows, width, keys, payload, layout, outer) in [
        (
            "small_plain",
            8192,
            16,
            Keys::Integer,
            Payload::Plain,
            Layout::Independent,
            false,
        ),
        (
            "large_plain",
            65536,
            1024,
            Keys::Integer,
            Payload::Plain,
            Layout::Independent,
            false,
        ),
        (
            "large_computed",
            65536,
            1024,
            Keys::Computed,
            Payload::Plain,
            Layout::Independent,
            false,
        ),
        (
            "large_dictionary_keys",
            65536,
            1024,
            Keys::Dictionary,
            Payload::Plain,
            Layout::Independent,
            false,
        ),
        (
            "large_dictionary_payload",
            65536,
            1024,
            Keys::Integer,
            Payload::Dictionary,
            Layout::Independent,
            false,
        ),
        (
            "large_list_payload",
            65536,
            1024,
            Keys::Integer,
            Payload::List,
            Layout::Independent,
            false,
        ),
        (
            "large_tiny_batches",
            65536,
            1024,
            Keys::Integer,
            Payload::Plain,
            Layout::Tiny,
            false,
        ),
        (
            "large_sliced",
            65536,
            1024,
            Keys::Integer,
            Payload::Plain,
            Layout::Sliced,
            false,
        ),
        (
            "large_overallocated",
            65536,
            1024,
            Keys::Integer,
            Payload::Plain,
            Layout::Overallocated,
            false,
        ),
        (
            "large_low_match_outer",
            65536,
            1024,
            Keys::Integer,
            Payload::Plain,
            Layout::Independent,
            true,
        ),
    ] {
        let workload = Workload::new(rows, width, keys, payload, layout, outer);
        let mut counter = RecordBatchMemoryCounter::new();
        for batch in &workload.build {
            counter.count_batch(batch);
        }
        assert_eq!(
            counter.memory_usage() > COMPACT_BUILD_BYTES,
            name.starts_with("large")
        );
        for perfect_hash in [false, true] {
            let mut config = SessionConfig::default();
            config
                .options_mut()
                .optimizer
                .enable_join_dynamic_filter_pushdown = false;
            config
                .options_mut()
                .execution
                .perfect_hash_join_small_build_threshold =
                if perfect_hash { usize::MAX } else { 0 };
            config
                .options_mut()
                .execution
                .perfect_hash_join_min_key_density =
                if perfect_hash { 0.0 } else { f64::INFINITY };
            let recording = Arc::new(PeakRecordingPool::new(Arc::new(
                UnboundedMemoryPool::default(),
            )));
            let recording_context = Arc::new(
                TaskContext::default()
                    .with_session_config(config.clone())
                    .with_runtime(
                        RuntimeEnvBuilder::new()
                            .with_memory_pool(
                                Arc::clone(&recording) as Arc<dyn MemoryPool>
                            )
                            .build_arc()
                            .unwrap(),
                    ),
            );
            assert_eq!(
                runtime
                    .block_on(workload.run(Arc::clone(&recording_context)))
                    .unwrap(),
                (workload.expected_rows, workload.expected_sum),
            );
            drop(recording_context);
            assert_eq!(recording.reserved(), 0);
            let mode = if perfect_hash {
                "perfect_hash_on"
            } else {
                "perfect_hash_off"
            };
            eprintln!(
                "HASH_JOIN_RESERVATION name={name} mode={mode} input_unique_bytes={} peak_reserved_bytes={}",
                counter.memory_usage(),
                recording.peak_reserved(),
            );
            let context = Arc::new(TaskContext::default().with_session_config(config));
            group.bench_function(BenchmarkId::new(name, mode), |b| {
                b.iter(|| {
                    black_box(
                        runtime
                            .block_on(workload.run(Arc::clone(&context)))
                            .unwrap(),
                    )
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
