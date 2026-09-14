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

//! Paired end-to-end physical-plan experiment. Set SAMPLES (default 21),
//! CASE (e.g. Utf8-8-10), or MODE (true/false) to restrict a run. CSV includes
//! plan construction, both repartitions, build, probe, output and destruction.
//! `peak_reserved` is engine-accounted memory, not process RSS.

use datafusion_common::instant::Instant;
use std::fmt;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Int64Array, StringArray, StringViewArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{JoinType, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::joins::{HashJoinExecBuilder, PartitionMode};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, Partitioning, collect};

#[derive(Debug, Default)]
struct PeakPool {
    current: AtomicUsize,
    peak: AtomicUsize,
}
impl fmt::Display for PeakPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeakPool")
    }
}
impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "PeakPool"
    }
    fn grow(&self, _: &MemoryReservation, bytes: usize) {
        let current = self.current.fetch_add(bytes, Ordering::SeqCst) + bytes;
        self.peak.fetch_max(current, Ordering::SeqCst);
    }
    fn shrink(&self, _: &MemoryReservation, bytes: usize) {
        self.current.fetch_sub(bytes, Ordering::SeqCst);
    }
    fn try_grow(&self, r: &MemoryReservation, bytes: usize) -> Result<()> {
        self.grow(r, bytes);
        Ok(())
    }
    fn reserved(&self) -> usize {
        self.current.load(Ordering::SeqCst)
    }
    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Infinite
    }
}

fn batch(
    rows: usize,
    kind: &str,
    coverage: usize,
    build: bool,
    skew: bool,
) -> RecordBatch {
    let keys: Vec<i64> = (0..rows)
        .filter(|i| !build || i % 100 < coverage)
        .map(|i| {
            if !build && skew && i % 10 != 0 {
                1
            } else {
                (i % 2048) as i64
            }
        })
        .collect();
    let n = keys.len();
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys))];
    match kind {
        "Wide" if !build => {
            for column in 0..24 {
                columns.push(Arc::new(Int64Array::from_iter_values(
                    (0..n).map(|i| (i * 31 + column) as i64),
                )));
            }
        }
        "Utf8" | "Utf8View" if !build => {
            let strings: Vec<_> = (0..n)
                .map(|i| format!("{i:08}-{}", "payload-".repeat(31)))
                .collect();
            for _ in 0..4 {
                columns.push(if kind == "Utf8" {
                    Arc::new(StringArray::from_iter_values(&strings)) as ArrayRef
                } else {
                    Arc::new(StringViewArray::from_iter_values(&strings)) as ArrayRef
                });
            }
        }
        _ => columns.push(Arc::new(Int64Array::from_iter_values(
            (0..n).map(|i| i as i64),
        ))),
    }
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, columns).unwrap()
}

fn source(batches: &[RecordBatch]) -> Arc<dyn ExecutionPlan> {
    let mut partitions = vec![vec![], vec![], vec![], vec![]];
    for (index, batch) in batches.iter().enumerate() {
        partitions[index % 4].push(batch.clone());
    }
    TestMemoryExec::try_new_exec(&partitions, batches[0].schema(), None).unwrap()
}

async fn query(
    left: &[RecordBatch],
    right: &[RecordBatch],
    partitions: usize,
    selected: bool,
) -> (usize, usize) {
    let pool = Arc::new(PeakPool::default());
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()
        .unwrap();
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .enable_hash_join_probe_selection = selected;
    let context = Arc::new(
        TaskContext::default()
            .with_runtime(runtime)
            .with_session_config(config),
    );
    let key: PhysicalExprRef = Arc::new(Column::new("c0", 0));
    let left = Arc::new(
        RepartitionExec::try_new(
            source(left),
            Partitioning::Hash(vec![Arc::clone(&key)], partitions),
        )
        .unwrap(),
    );
    let right = Arc::new(
        RepartitionExec::try_new(
            source(right),
            Partitioning::Hash(vec![Arc::clone(&key)], partitions),
        )
        .unwrap(),
    );
    let join = HashJoinExecBuilder::new(
        left,
        right,
        vec![(Arc::clone(&key), key)],
        JoinType::Inner,
    )
    .with_partition_mode(PartitionMode::Partitioned)
    .build()
    .unwrap();
    let join = Arc::new(join);
    let output = collect(join.clone(), context).await.unwrap();
    let used = join
        .metrics()
        .unwrap()
        .sum_by_name("probe_selection_partitions")
        .map_or(0, |m| m.as_usize());
    assert_eq!(used, if selected { partitions } else { 0 });
    drop(join);
    let rows = output.iter().map(RecordBatch::num_rows).sum();
    drop(output);
    // Ensure an asynchronous producer destructor is included in measurement.
    while pool.reserved() != 0 {
        tokio::task::yield_now().await;
    }
    (rows, pool.peak.load(Ordering::SeqCst))
}

fn main() {
    if std::env::args().any(|a| a == "--test") {
        return;
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();
    let samples: usize = std::env::var("SAMPLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(21);
    let case_filter = std::env::var("CASE").ok();
    let mode_filter = std::env::var("MODE")
        .ok()
        .map(|s| s.parse::<bool>().unwrap());
    println!(
        "kind,partitions,hit_percent,skew,sample,selection,total_us,peak_reserved,output_rows"
    );
    for kind in ["Narrow", "Wide", "Utf8", "Utf8View"] {
        for partitions in [8, 32] {
            for hit in [10, 100] {
                for skew in [false, true] {
                    if case_filter
                        .as_ref()
                        .is_some_and(|f| *f != format!("{kind}-{partitions}-{hit}"))
                    {
                        continue;
                    }
                    let left = vec![batch(2048, kind, hit, true, false)];
                    // Independent source buffers, not slices of one large buffer:
                    // otherwise every source batch's memory estimate counts that
                    // large allocation and obscures the exchange's retention cost.
                    let right: Vec<_> = (0..4)
                        .map(|_| batch(8192, kind, hit, false, skew))
                        .collect();
                    let build_keys: std::collections::HashSet<_> = left[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                        .collect();
                    let expected: usize = right
                        .iter()
                        .map(|batch| {
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .filter(|key| build_keys.contains(key))
                                .count()
                        })
                        .sum();
                    for sample in 0..=samples {
                        for selected in [sample % 2 == 0, sample % 2 != 0] {
                            if mode_filter.is_some_and(|m| m != selected) {
                                continue;
                            }
                            let start = Instant::now();
                            let (rows, peak) = black_box(
                                runtime
                                    .block_on(query(&left, &right, partitions, selected)),
                            );
                            let elapsed = start.elapsed().as_secs_f64() * 1e6;
                            assert_eq!(rows, expected);
                            if sample > 0 {
                                println!(
                                    "{kind},{partitions},{hit},{skew},{sample},{selected},{elapsed:.3},{peak},{rows}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
