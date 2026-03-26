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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::stream::SemiAntiSortMergeJoinStream;
use crate::ExecutionPlan;
use crate::RecordBatchStream;
use crate::common;
use crate::expressions::Column;
use crate::joins::SortMergeJoinExec;
use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::metrics::ExecutionPlanMetricsSet;
use crate::metrics::{MetricBuilder, SpillMetrics};
use crate::spill::spill_manager::SpillManager;
use crate::test::TestMemoryExec;

use arrow::array::{Int32Array, RecordBatch};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::JoinSide;
use datafusion_common::JoinType::*;
use datafusion_common::{NullEquality, Result};
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::BinaryExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExprRef;
use futures::Stream;

/// Create test memory/spill resources for stream-level tests.
fn test_stream_resources(
    inner_schema: SchemaRef,
    metrics: &ExecutionPlanMetricsSet,
) -> (
    datafusion_execution::memory_pool::MemoryReservation,
    crate::metrics::Gauge,
    SpillManager,
    Arc<datafusion_execution::runtime_env::RuntimeEnv>,
) {
    let ctx = TaskContext::default();
    let runtime_env = ctx.runtime_env();
    let reservation = MemoryConsumer::new("test").register(ctx.memory_pool());
    let peak_mem_used = MetricBuilder::new(metrics).gauge("peak_mem_used", 0);
    let spill_manager = SpillManager::new(
        Arc::clone(&runtime_env),
        SpillMetrics::new(metrics, 0),
        inner_schema,
    );
    (reservation, peak_mem_used, spill_manager, runtime_env)
}

fn build_table(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap();
    TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
}

// ── Async re-entry tests using PendingStream ──────────────────────────────
// reproduce bugs that only manifest when async input streams return
// Poll::Pending at specific points.

/// A RecordBatch stream that yields Poll::Pending once before delivering
/// each batch at a specified index. This simulates the behavior of
/// repartitioned tokio::sync::mpsc channels where data isn't immediately
/// available.
struct PendingStream {
    batches: Vec<RecordBatch>,
    index: usize,
    /// If pending_before[i] is true, yield Pending once before delivering
    /// the batch at index i.
    pending_before: Vec<bool>,
    /// True if we've already yielded Pending for the current index.
    yielded_pending: bool,
    schema: SchemaRef,
}

impl PendingStream {
    fn new(batches: Vec<RecordBatch>, pending_before: Vec<bool>) -> Self {
        assert_eq!(batches.len(), pending_before.len());
        let schema = batches[0].schema();
        Self {
            batches,
            index: 0,
            pending_before,
            yielded_pending: false,
            schema,
        }
    }
}

impl Stream for PendingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.index >= self.batches.len() {
            return Poll::Ready(None);
        }
        if self.pending_before[self.index] && !self.yielded_pending {
            self.yielded_pending = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.yielded_pending = false;
        let batch = self.batches[self.index].clone();
        self.index += 1;
        Poll::Ready(Some(Ok(batch)))
    }
}

impl RecordBatchStream for PendingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Helper: collect all output from a SemiAntiSortMergeJoinStream.
async fn collect_stream(stream: SemiAntiSortMergeJoinStream) -> Result<Vec<RecordBatch>> {
    common::collect(Box::pin(stream)).await
}

/// Reproduces the buffer_inner_key_group re-entry bug:
///
/// When buffer_inner_key_group buffers inner rows across batch boundaries
/// and poll_next_inner_batch returns Pending mid-way, the ready! macro
/// exits poll_join. On re-entry, the merge-scan reaches Equal again and
/// calls buffer_inner_key_group a second time — which starts with
/// clear(), destroying the partially collected inner rows. Previously
/// consumed batches are gone, so re-buffering misses them.
///
/// Setup:
/// - Inner: 3 single-row batches, all with key=1, filter values c2=[10, 20, 30]
/// - Outer: 1 row, key=1, filter value c1=10
/// - Filter: c1 == c2 (only first inner row c2=10 matches)
/// - Pending injected before 3rd inner batch
///
/// Without the bug: outer row emitted (match via c2=10)
/// With the bug: outer row missing (c2=10 batch lost on re-entry)
#[tokio::test]
async fn filter_buffer_pending_loses_inner_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Outer: 1 row, key=1, c1=10
    let outer_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])), // join key
            Arc::new(Int32Array::from(vec![10])), // filter value
        ],
    )?;

    // Inner: 3 single-row batches, key=1, c2=[10, 20, 30]
    let inner_batch1 = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Int32Array::from(vec![1])), // join key
            Arc::new(Int32Array::from(vec![10])), // matches filter
        ],
    )?;
    let inner_batch2 = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![200])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![20])), // doesn't match
        ],
    )?;
    let inner_batch3 = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![300])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![30])), // doesn't match
        ],
    )?;

    let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![outer_batch],
        vec![false], // outer delivers immediately
    ));
    let inner: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![inner_batch1, inner_batch2, inner_batch3],
        vec![false, false, true], // Pending before 3rd batch
    ));

    // Filter: c1 == c2
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Eq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    let metrics = ExecutionPlanMetricsSet::new();
    let inner_schema = inner.schema();
    let (reservation, peak_mem_used, spill_manager, runtime_env) =
        test_stream_resources(inner_schema, &metrics);
    let stream = SemiAntiSortMergeJoinStream::try_new(
        left_schema, // output schema = outer schema for semi
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        on_outer,
        on_inner,
        Some(filter),
        LeftSemi,
        8192,
        0,
        &metrics,
        reservation,
        peak_mem_used,
        spill_manager,
        runtime_env,
    )?;

    let batches = collect_stream(stream).await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 1,
        "LeftSemi with filter: outer row should be emitted because \
         inner row c2=10 matches filter c1==c2. Got {total} rows."
    );
    Ok(())
}

/// Reproduces the no-filter boundary Pending re-entry bug:
///
/// When an outer key group spans a batch boundary, the no-filter path
/// emits the current batch, then polls for the next outer batch. If
/// poll returns Pending, poll_join exits. On re-entry, without the
/// PendingBoundary fix, the new batch is processed fresh by the
/// merge-scan. Since inner already advanced past this key, the outer
/// rows with the matching key are skipped via Ordering::Less.
///
/// Setup:
/// - Outer: 2 single-row batches, both with key=1 (key group spans boundary)
/// - Inner: 1 row with key=1
/// - Pending injected on outer before 2nd batch
///
/// Without fix: only first outer row emitted (second lost on re-entry)
/// With fix: both outer rows emitted
#[tokio::test]
async fn no_filter_boundary_pending_loses_outer_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Outer: 2 single-row batches, both key=1
    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key
            Arc::new(Int32Array::from(vec![20])),
        ],
    )?;

    // Inner: 1 row, key=1
    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![50])),
        ],
    )?;

    let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![outer_batch1, outer_batch2],
        vec![false, true], // Pending before 2nd outer batch
    ));
    let inner: SendableRecordBatchStream =
        Box::pin(PendingStream::new(vec![inner_batch], vec![false]));

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    let metrics = ExecutionPlanMetricsSet::new();
    let inner_schema = inner.schema();
    let (reservation, peak_mem_used, spill_manager, runtime_env) =
        test_stream_resources(inner_schema, &metrics);
    let stream = SemiAntiSortMergeJoinStream::try_new(
        left_schema,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        on_outer,
        on_inner,
        None, // no filter
        LeftSemi,
        8192,
        0,
        &metrics,
        reservation,
        peak_mem_used,
        spill_manager,
        runtime_env,
    )?;

    let batches = collect_stream(stream).await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 2,
        "LeftSemi no filter: both outer rows (key=1) should be emitted \
         because inner has key=1. Got {total} rows."
    );
    Ok(())
}

/// Tests the filtered boundary Pending re-entry: outer key group spans
/// batches with a filter, and poll_next_outer_batch returns Pending.
///
/// Setup:
/// - Outer: 2 single-row batches, both key=1, c1=[10, 20]
/// - Inner: 1 row, key=1, c2=10
/// - Filter: c1 == c2 (first outer row matches, second doesn't)
/// - Pending before 2nd outer batch
///
/// Expected: 1 row (only the first outer row c1=10 passes the filter)
#[tokio::test]
async fn filtered_boundary_pending_outer_rows() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])), // matches filter
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key
            Arc::new(Int32Array::from(vec![20])), // doesn't match
        ],
    )?;

    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(vec![100])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![10])),
        ],
    )?;

    let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
        vec![outer_batch1, outer_batch2],
        vec![false, true], // Pending before 2nd outer batch
    ));
    let inner: SendableRecordBatchStream =
        Box::pin(PendingStream::new(vec![inner_batch], vec![false]));

    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Eq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    let metrics = ExecutionPlanMetricsSet::new();
    let inner_schema = inner.schema();
    let (reservation, peak_mem_used, spill_manager, runtime_env) =
        test_stream_resources(inner_schema, &metrics);
    let stream = SemiAntiSortMergeJoinStream::try_new(
        left_schema,
        vec![SortOptions::default()],
        NullEquality::NullEqualsNothing,
        outer,
        inner,
        on_outer,
        on_inner,
        Some(filter),
        LeftSemi,
        8192,
        0,
        &metrics,
        reservation,
        peak_mem_used,
        spill_manager,
        runtime_env,
    )?;

    let batches = collect_stream(stream).await?;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total, 1,
        "LeftSemi filtered boundary: only first outer row (c1=10) matches \
         filter c1==c2. Got {total} rows."
    );
    Ok(())
}

// ==================== SPILL TESTS ====================

/// Exercises inner key group spilling under memory pressure.
///
/// Uses a tiny memory limit (100 bytes) with disk spilling enabled. Since our
/// operator only buffers inner rows when a filter is present, this test includes
/// a filter (c1 < c2, always true). Verifies:
/// 1. Spill metrics are recorded (spill_count, spilled_bytes, spilled_rows > 0)
/// 2. Results match a non-spilled run
#[tokio::test]
async fn spill_with_filter() -> Result<()> {
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;

    let left = build_table(
        ("a1", &vec![1, 2, 3, 4, 5, 6]),
        ("b1", &vec![1, 2, 3, 4, 5, 6]),
        ("c1", &vec![4, 5, 6, 7, 8, 9]),
    );
    let right = build_table(
        ("a2", &vec![10, 20, 30, 40, 50]),
        ("b1", &vec![1, 3, 4, 6, 8]),
        ("c2", &vec![50, 60, 70, 80, 90]),
    );
    let on = vec![(
        Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
        Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
    )];
    let sort_options = vec![SortOptions::default(); on.len()];

    // c1 < c2 is always true for matching keys
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Lt,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    for batch_size in [1, 50] {
        let session_config = SessionConfig::default().with_batch_size(batch_size);

        for join_type in [LeftSemi, LeftAnti, RightSemi, RightAnti] {
            let task_ctx = Arc::new(
                TaskContext::default()
                    .with_session_config(session_config.clone())
                    .with_runtime(Arc::clone(&runtime)),
            );

            let join = SortMergeJoinExec::try_new(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                Some(filter.clone()),
                join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join.execute(0, task_ctx)?;
            let spilled_result = common::collect(stream).await.unwrap();

            assert!(
                join.metrics().is_some(),
                "metrics missing for {join_type:?}"
            );
            let metrics = join.metrics().unwrap();
            assert!(
                metrics.spill_count().unwrap() > 0,
                "expected spill_count > 0 for {join_type:?}, batch_size={batch_size}"
            );
            assert!(
                metrics.spilled_bytes().unwrap() > 0,
                "expected spilled_bytes > 0 for {join_type:?}, batch_size={batch_size}"
            );
            assert!(
                metrics.spilled_rows().unwrap() > 0,
                "expected spilled_rows > 0 for {join_type:?}, batch_size={batch_size}"
            );

            // Run without spilling and compare results
            let task_ctx_no_spill = Arc::new(
                TaskContext::default().with_session_config(session_config.clone()),
            );
            let join_no_spill = SortMergeJoinExec::try_new(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                Some(filter.clone()),
                join_type,
                sort_options.clone(),
                NullEquality::NullEqualsNothing,
            )?;
            let stream = join_no_spill.execute(0, task_ctx_no_spill)?;
            let no_spill_result = common::collect(stream).await.unwrap();

            let no_spill_metrics = join_no_spill.metrics().unwrap();
            assert_eq!(
                no_spill_metrics.spill_count(),
                Some(0),
                "unexpected spill for {join_type:?} without memory limit"
            );

            assert_eq!(
                spilled_result, no_spill_result,
                "spilled vs non-spilled results differ for {join_type:?}, batch_size={batch_size}"
            );
        }
    }

    Ok(())
}

/// Reproduces a bug where `resume_boundary` for the Filtered pending case
/// only checks `inner_key_buffer.is_empty()` but ignores `inner_key_spill`.
/// After spilling, the in-memory buffer is cleared while the spill file
/// holds the data. If the outer key group spans a batch boundary, the
/// second outer batch's rows are never evaluated against the inner group.
///
/// Setup:
/// - Outer: 2 single-row batches, both key=1, c1=[10, 10]
/// - Inner: 1 batch with many rows all key=1 (enough to trigger spill)
/// - Filter: c1 == c2 (matches when c2=10)
/// - Memory limit: tiny (100 bytes) to force spilling
/// - Pending before 2nd outer batch to trigger boundary re-entry
///
/// Expected: both outer rows match (semi=2 rows, anti=0 rows)
/// Bug: second outer row is skipped because resume_boundary sees empty
///      inner_key_buffer and skips re-evaluation.
#[tokio::test]
async fn spill_filtered_boundary_loses_outer_rows() -> Result<()> {
    use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("a1", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c1", DataType::Int32, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("a2", DataType::Int32, false),
        Field::new("b1", DataType::Int32, false),
        Field::new("c2", DataType::Int32, false),
    ]));

    // Two single-row outer batches with the same key — key group spans boundary
    let outer_batch1 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![1])), // key=1
            Arc::new(Int32Array::from(vec![10])), // matches filter
        ],
    )?;
    let outer_batch2 = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![1])), // same key=1
            Arc::new(Int32Array::from(vec![10])), // also matches filter
        ],
    )?;

    // Inner: many rows with key=1 to force spilling, followed by key=2.
    // c2=10 so the filter c1==c2 passes for both outer rows.
    // The key=2 row ensures the inner cursor advances past the key group
    // (buffer_inner_key_group returns Ok(false) instead of Ok(true)).
    let n_inner = 200;
    let mut inner_a = vec![100; n_inner];
    inner_a.push(101);
    let mut inner_b = vec![1; n_inner];
    inner_b.push(2); // different key — forces inner cursor past key=1
    let mut inner_c = vec![10; n_inner];
    inner_c.push(10);
    let inner_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int32Array::from(inner_a)),
            Arc::new(Int32Array::from(inner_b)),
            Arc::new(Int32Array::from(inner_c)),
        ],
    )?;

    // Filter: c1 == c2
    let filter = JoinFilter::new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("c1", 0)),
            Operator::Eq,
            Arc::new(Column::new("c2", 1)),
        )),
        vec![
            ColumnIndex {
                index: 2,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ])),
    );

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(100, 1.0)
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
        )
        .build_arc()?;

    let on_outer: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];
    let on_inner: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b1", 1))];

    for join_type in [LeftSemi, LeftAnti] {
        let outer: SendableRecordBatchStream = Box::pin(PendingStream::new(
            vec![outer_batch1.clone(), outer_batch2.clone()],
            vec![false, true], // Pending before 2nd outer batch
        ));
        let inner: SendableRecordBatchStream =
            Box::pin(PendingStream::new(vec![inner_batch.clone()], vec![false]));

        let metrics = ExecutionPlanMetricsSet::new();
        let reservation = MemoryConsumer::new("test").register(&runtime.memory_pool);
        let peak_mem_used = MetricBuilder::new(&metrics).gauge("peak_mem_used", 0);
        let spill_manager = SpillManager::new(
            Arc::clone(&runtime),
            SpillMetrics::new(&metrics, 0),
            Arc::clone(&right_schema),
        );

        let stream = SemiAntiSortMergeJoinStream::try_new(
            Arc::clone(&left_schema),
            vec![SortOptions::default()],
            NullEquality::NullEqualsNothing,
            outer,
            inner,
            on_outer.clone(),
            on_inner.clone(),
            Some(filter.clone()),
            join_type,
            8192,
            0,
            &metrics,
            reservation,
            peak_mem_used,
            spill_manager,
            Arc::clone(&runtime),
        )?;

        let batches = collect_stream(stream).await?;
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();

        match join_type {
            LeftSemi => {
                assert_eq!(
                    total, 2,
                    "LeftSemi spill+boundary: both outer rows match filter, \
                     expected 2 rows, got {total}"
                );
            }
            LeftAnti => {
                assert_eq!(
                    total, 0,
                    "LeftAnti spill+boundary: both outer rows match filter, \
                     expected 0 rows, got {total}"
                );
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}
