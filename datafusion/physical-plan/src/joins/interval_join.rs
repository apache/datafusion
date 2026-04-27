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

//! [`IntervalJoinExec`] for point-in-interval range joins.
//!
//! Handles queries with two range predicates forming a point-in-interval pattern:
//! ```sql
//! SELECT * FROM events e JOIN windows w
//!   ON e.event_time >= w.start_time AND e.event_time < w.end_time
//! ```
//!
//! # Algorithm
//!
//! Build phase:
//! 1. Collect all build-side (interval) batches into a single RecordBatch
//! 2. Evaluate the low-bound and high-bound expressions
//! 3. Sort by the low-bound column (ascending)
//!
//! Probe phase (per batch):
//! 1. Evaluate the probe expression and sort the batch by it (ascending)
//! 2. Iterate through sorted probe rows with a monotonic `boundary` pointer:
//!    - Advance `boundary` while `build_low[boundary]` satisfies the low-bound condition
//!    - Scan `[0..boundary)` checking the high-bound condition
//!    - Emit matched pairs via BatchCoalescer
//!
//! # Performance
//!
//! Complexity: O(N log N + M log M + scan_cost) where scan_cost is the total
//! number of candidate checks across all probe rows.
//! For non-overlapping intervals: O(N log N + M log M + N).
//! Worst case (all intervals overlap): O(N × M) but with much better per-comparison
//! constants than NestedLoopJoin (direct scalar comparison vs JoinFilter evaluation).

use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;
use std::task::Poll;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{
    BatchCoalescer, concat_batches, sort_to_indices, take, take_record_batch,
};
use arrow_ord::ord::make_comparator;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{JoinSide, Result, internal_err};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::equivalence::join_equivalence_properties;
use datafusion_physical_expr::{
    Distribution, LexOrdering, OrderingRequirements, PhysicalExpr, PhysicalExprRef,
    PhysicalSortExpr,
};
use datafusion_physical_expr_common::physical_expr::fmt_sql;
use futures::{Stream, StreamExt, TryStreamExt};

use crate::execution_plan::{EmissionType, boundedness_from_children};
use crate::joins::utils::{
    BuildProbeJoinMetrics, OnceAsync, OnceFut, asymmetric_join_output_partitioning,
    build_join_schema,
};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::spill::get_record_batch_memory_size;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

/// Execution plan for point-in-interval range joins.
///
/// Specialized for queries with two range predicates where a single probe column
/// is compared against an interval defined by two build-side columns:
/// `probe_col >= build_low AND probe_col < build_high`
///
/// The build side (left child) contains the intervals and is collected, sorted
/// by the low bound, and broadcast to all probe partitions. The probe side
/// (right child) is streamed, with each batch sorted internally before processing.
#[derive(Debug)]
pub struct IntervalJoinExec {
    /// Left child (preserves logical plan order)
    left: Arc<dyn ExecutionPlan>,
    /// Right child (preserves logical plan order)
    right: Arc<dyn ExecutionPlan>,
    /// Which side is the build (interval) side
    build_side: JoinSide,
    /// Probe column expression (evaluated on probe side)
    probe_expr: PhysicalExprRef,
    /// Interval low-bound expression (evaluated on build side)
    build_low_expr: PhysicalExprRef,
    /// Interval high-bound expression (evaluated on build side)
    build_high_expr: PhysicalExprRef,
    /// Operator for low bound: GtEq or Gt (probe >= build_low)
    low_op: Operator,
    /// Operator for high bound: Lt or LtEq (probe < build_high)
    high_op: Operator,
    /// Join type
    join_type: JoinType,
    /// Output schema (left columns ++ right columns)
    schema: SchemaRef,
    /// Async-once build side collection
    build_data: OnceAsync<IntervalBuildData>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Sort order required on build side (build_low ASC)
    build_sort_order: LexOrdering,
    /// Sort options for per-batch probe sorting (ASC NULLS LAST)
    probe_sort_options: SortOptions,
    /// Cached plan properties
    cache: Arc<PlanProperties>,
}

impl IntervalJoinExec {
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        build_side: JoinSide,
        probe_expr: PhysicalExprRef,
        build_low_expr: PhysicalExprRef,
        build_high_expr: PhysicalExprRef,
        low_op: Operator,
        high_op: Operator,
        join_type: JoinType,
    ) -> Result<Self> {
        if !matches!(join_type, JoinType::Inner) {
            return internal_err!(
                "IntervalJoinExec currently only supports Inner join, got {:?}",
                join_type
            );
        }

        if !matches!(low_op, Operator::GtEq | Operator::Gt) {
            return internal_err!(
                "IntervalJoinExec low_op must be >= or >, got {:?}",
                low_op
            );
        }

        if !matches!(high_op, Operator::Lt | Operator::LtEq) {
            return internal_err!(
                "IntervalJoinExec high_op must be < or <=, got {:?}",
                high_op
            );
        }

        let left_schema = left.schema();
        let right_schema = right.schema();
        let (schema, _column_indices) =
            build_join_schema(&left_schema, &right_schema, &join_type);
        let schema = Arc::new(schema);

        let build = match build_side {
            JoinSide::Left => &left,
            JoinSide::Right => &right,
            _ => unreachable!(),
        };
        let probe = match build_side {
            JoinSide::Left => &right,
            JoinSide::Right => &left,
            _ => unreachable!(),
        };

        // Build side sorted ascending by low bound (nulls last)
        let sort_options = SortOptions::new(false, false);
        let build_sort_expr =
            PhysicalSortExpr::new(Arc::clone(&build_low_expr), sort_options);
        let build_sort_order =
            LexOrdering::new(vec![build_sort_expr]).expect("non-empty sort expression");

        // Probe side sorted ascending per-batch (nulls last)
        let probe_sort_options = SortOptions::new(false, false);

        let cache =
            Self::compute_properties(build, probe, Arc::clone(&schema), join_type)?;

        Ok(Self {
            left,
            right,
            build_side,
            probe_expr,
            build_low_expr,
            build_high_expr,
            low_op,
            high_op,
            join_type,
            schema,
            build_data: Default::default(),
            metrics: ExecutionPlanMetricsSet::new(),
            build_sort_order,
            probe_sort_options,
            cache: Arc::new(cache),
        })
    }

    fn compute_properties(
        build: &Arc<dyn ExecutionPlan>,
        probe: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        join_type: JoinType,
    ) -> Result<PlanProperties> {
        let eq_properties = join_equivalence_properties(
            build.equivalence_properties().clone(),
            probe.equivalence_properties().clone(),
            &join_type,
            schema,
            &[false, false], // does not maintain input order (probe sorted per-batch)
            Some(JoinSide::Right), // probe side
            &[],             // no equi-join keys
        )?;

        let output_partitioning =
            asymmetric_join_output_partitioning(build, probe, &join_type)?;

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness_from_children([build, probe]),
        ))
    }
}

impl DisplayAs for IntervalJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let on_str = format!(
            "({} {} {} AND {} {} {})",
            fmt_sql(self.probe_expr.as_ref()),
            self.low_op,
            fmt_sql(self.build_low_expr.as_ref()),
            fmt_sql(self.probe_expr.as_ref()),
            self.high_op,
            fmt_sql(self.build_high_expr.as_ref()),
        );

        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "IntervalJoin: join_type={:?}, on={}",
                    self.join_type, on_str
                )
            }
            DisplayFormatType::TreeRender => {
                if self.join_type != JoinType::Inner {
                    writeln!(f, "join_type={:?}", self.join_type)?;
                }
                writeln!(f, "on={on_str}")
            }
        }
    }
}

impl ExecutionPlan for IntervalJoinExec {
    fn name(&self) -> &str {
        "IntervalJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        f(self.probe_expr.as_ref())?
            .visit_sibling(|| f(self.build_low_expr.as_ref()))?
            .visit_sibling(|| f(self.build_high_expr.as_ref()))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Build side needs SinglePartition, probe side any distribution
        let mut dists = vec![Distribution::UnspecifiedDistribution; 2];
        let build_idx = match self.build_side {
            JoinSide::Left => 0,
            JoinSide::Right => 1,
            _ => unreachable!(),
        };
        dists[build_idx] = Distribution::SinglePartition;
        dists
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // Build side needs sort order, probe side sorted per-batch internally
        let mut orders: Vec<Option<OrderingRequirements>> = vec![None, None];
        let build_idx = match self.build_side {
            JoinSide::Left => 0,
            JoinSide::Right => 1,
            _ => unreachable!(),
        };
        orders[build_idx] =
            Some(OrderingRequirements::from(self.build_sort_order.clone()));
        orders
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(IntervalJoinExec::try_new(
                Arc::clone(left),
                Arc::clone(right),
                self.build_side,
                Arc::clone(&self.probe_expr),
                Arc::clone(&self.build_low_expr),
                Arc::clone(&self.build_high_expr),
                self.low_op,
                self.high_op,
                self.join_type,
            )?)),
            _ => internal_err!(
                "IntervalJoinExec should have 2 children, found {}",
                children.len()
            ),
        }
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IntervalJoinExec::try_new(
            Arc::clone(&self.left),
            Arc::clone(&self.right),
            self.build_side,
            Arc::clone(&self.probe_expr),
            Arc::clone(&self.build_low_expr),
            Arc::clone(&self.build_high_expr),
            self.low_op,
            self.high_op,
            self.join_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let metrics = BuildProbeJoinMetrics::new(partition, &self.metrics);

        let build_low_expr = Arc::clone(&self.build_low_expr);
        let build_high_expr = Arc::clone(&self.build_high_expr);

        let (build_plan, probe_plan) = match self.build_side {
            JoinSide::Left => (&self.left, &self.right),
            JoinSide::Right => (&self.right, &self.left),
            _ => unreachable!(),
        };

        let build_fut = self.build_data.try_once(|| {
            let reservation =
                MemoryConsumer::new("IntervalJoinExec").register(context.memory_pool());

            let build_stream = build_plan.execute(0, Arc::clone(&context))?;
            Ok(collect_build_data(
                build_stream,
                build_low_expr,
                build_high_expr,
                metrics.clone(),
                reservation,
            ))
        })?;

        let probe_stream = probe_plan.execute(partition, Arc::clone(&context))?;
        let batch_size = context.session_config().batch_size();

        Ok(Box::pin(IntervalJoinStream {
            schema: Arc::clone(&self.schema),
            probe_expr: Arc::clone(&self.probe_expr),
            probe_stream,
            build_side: BuildSideState::Initial(build_fut),
            build_join_side: self.build_side,
            low_op: self.low_op,
            high_op: self.high_op,
            probe_sort_options: self.probe_sort_options,
            coalescer: BatchCoalescer::new(Arc::clone(&self.schema), batch_size),
            join_metrics: metrics,
            state: IntervalJoinState::WaitBuildSide,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

// ---------------------------------------------------------------------------
// Build-side data
// ---------------------------------------------------------------------------

/// Collected and sorted build-side data.
struct IntervalBuildData {
    /// All build-side rows, sorted by low-bound
    batch: RecordBatch,
    /// Pre-evaluated & sorted low-bound values
    low_values: ArrayRef,
    /// Pre-evaluated & sorted high-bound values (same row order)
    high_values: ArrayRef,
    /// Memory reservation (freed on drop)
    #[expect(dead_code)]
    reservation: MemoryReservation,
}

/// Collects the build-side stream into a single sorted batch.
async fn collect_build_data(
    stream: SendableRecordBatchStream,
    build_low_expr: PhysicalExprRef,
    build_high_expr: PhysicalExprRef,
    metrics: BuildProbeJoinMetrics,
    reservation: MemoryReservation,
) -> Result<IntervalBuildData> {
    let schema = stream.schema();

    let _build_timer = metrics.build_time.timer();

    // 1. Collect all batches with memory tracking
    let (batches, _metrics, reservation) = stream
        .try_fold(
            (Vec::new(), metrics.clone(), reservation),
            |(mut batches, metrics, reservation), batch| async move {
                let batch_size = get_record_batch_memory_size(&batch);
                reservation.try_grow(batch_size)?;
                metrics.build_mem_used.add(batch_size);
                metrics.build_input_batches.add(1);
                metrics.build_input_rows.add(batch.num_rows());
                batches.push(batch);
                Ok((batches, metrics, reservation))
            },
        )
        .await?;

    // 2. Concatenate into single batch
    let single_batch = concat_batches(&schema, batches.iter())?;

    if single_batch.num_rows() == 0 {
        return Ok(IntervalBuildData {
            batch: single_batch.clone(),
            low_values: build_low_expr.evaluate(&single_batch)?.into_array(0)?,
            high_values: build_high_expr.evaluate(&single_batch)?.into_array(0)?,
            reservation,
        });
    }

    // 3. Evaluate low and high expressions
    let low_values = build_low_expr
        .evaluate(&single_batch)?
        .into_array(single_batch.num_rows())?;
    let high_values = build_high_expr
        .evaluate(&single_batch)?
        .into_array(single_batch.num_rows())?;

    // 4. Sort by low values ascending (nulls last)
    let sort_options = SortOptions::new(false, false);
    let sort_indices = sort_to_indices(low_values.as_ref(), Some(sort_options), None)?;

    // 5. Reorder everything by sort indices
    let sorted_batch = take_record_batch(&single_batch, &sort_indices)?;
    let sorted_low = take(low_values.as_ref(), &sort_indices, None)?;
    let sorted_high = take(high_values.as_ref(), &sort_indices, None)?;

    // Track sorted data memory
    let sorted_size = get_record_batch_memory_size(&sorted_batch);
    reservation.try_grow(sorted_size)?;

    Ok(IntervalBuildData {
        batch: sorted_batch,
        low_values: sorted_low,
        high_values: sorted_high,
        reservation,
    })
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

/// State machine for the interval join stream.
enum IntervalJoinState {
    /// Waiting for build-side data to be collected
    WaitBuildSide,
    /// Fetching the next probe batch
    FetchProbeBatch,
    /// Processing a sorted probe batch
    ProcessProbeBatch {
        sorted_batch: RecordBatch,
        probe_values: ArrayRef,
    },
    /// Join completed
    Completed,
}

/// Holds the build-side data, transitioning from pending to ready.
enum BuildSideState {
    /// Build data is still being collected
    Initial(OnceFut<IntervalBuildData>),
    /// Build data is ready
    Ready(Arc<IntervalBuildData>),
}

/// Stream that executes the interval join.
struct IntervalJoinStream {
    /// Output schema
    schema: SchemaRef,
    /// Probe expression to evaluate per batch
    probe_expr: PhysicalExprRef,
    /// Probe side stream
    probe_stream: SendableRecordBatchStream,
    /// Build data (transitions from Initial to Ready)
    build_side: BuildSideState,
    /// Which logical child is the build side
    build_join_side: JoinSide,
    /// Operator for low bound condition
    low_op: Operator,
    /// Operator for high bound condition
    high_op: Operator,
    /// Sort options for per-batch probe sorting
    probe_sort_options: SortOptions,
    /// Output batch coalescer
    coalescer: BatchCoalescer,
    /// Metrics
    join_metrics: BuildProbeJoinMetrics,
    /// State machine
    state: IntervalJoinState,
}

impl Stream for IntervalJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for IntervalJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl IntervalJoinStream {
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            // First check if coalescer has a completed batch to emit
            if let Some(batch) = self.coalescer.next_completed_batch() {
                return Poll::Ready(Some(Ok(batch)));
            }

            match &self.state {
                IntervalJoinState::WaitBuildSide => {
                    let build_data = match &mut self.build_side {
                        BuildSideState::Initial(fut) => {
                            match std::task::ready!(fut.get_shared(cx)) {
                                Ok(data) => data,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            }
                        }
                        BuildSideState::Ready(_) => unreachable!(),
                    };
                    self.build_side = BuildSideState::Ready(build_data);
                    self.state = IntervalJoinState::FetchProbeBatch;
                }

                IntervalJoinState::FetchProbeBatch => {
                    match std::task::ready!(self.probe_stream.poll_next_unpin(cx)) {
                        None => {
                            // Probe stream exhausted — flush remaining coalescer buffer
                            self.state = IntervalJoinState::Completed;
                            if let Err(e) = self.coalescer.finish_buffered_batch() {
                                return Poll::Ready(Some(Err(
                                    datafusion_common::DataFusionError::ArrowError(
                                        Box::new(e),
                                        None,
                                    ),
                                )));
                            }
                            if let Some(batch) = self.coalescer.next_completed_batch() {
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            return Poll::Ready(None);
                        }
                        Some(Ok(batch)) => {
                            if batch.num_rows() == 0 {
                                continue;
                            }

                            self.join_metrics.input_batches.add(1);
                            self.join_metrics.input_rows.add(batch.num_rows());

                            // Evaluate probe expression
                            let probe_values = match self
                                .probe_expr
                                .evaluate(&batch)
                                .and_then(|v| v.into_array(batch.num_rows()))
                            {
                                Ok(v) => v,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };

                            // Sort probe batch by probe values ascending
                            let indices =
                                match sort_to_indices(
                                    probe_values.as_ref(),
                                    Some(self.probe_sort_options),
                                    None,
                                ) {
                                    Ok(i) => i,
                                    Err(e) => return Poll::Ready(Some(Err(
                                        datafusion_common::DataFusionError::ArrowError(
                                            Box::new(e),
                                            None,
                                        ),
                                    ))),
                                };
                            let sorted_batch = match take_record_batch(&batch, &indices) {
                                Ok(b) => b,
                                Err(e) => return Poll::Ready(Some(Err(e.into()))),
                            };
                            let sorted_probe_values =
                                match take(probe_values.as_ref(), &indices, None) {
                                    Ok(v) => v,
                                    Err(e) => return Poll::Ready(Some(Err(
                                        datafusion_common::DataFusionError::ArrowError(
                                            Box::new(e),
                                            None,
                                        ),
                                    ))),
                                };

                            self.state = IntervalJoinState::ProcessProbeBatch {
                                sorted_batch,
                                probe_values: sorted_probe_values,
                            };
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                    }
                }

                IntervalJoinState::ProcessProbeBatch { .. } => {
                    // Take ownership of state data
                    let (sorted_batch, probe_values) = match std::mem::replace(
                        &mut self.state,
                        IntervalJoinState::FetchProbeBatch,
                    ) {
                        IntervalJoinState::ProcessProbeBatch {
                            sorted_batch,
                            probe_values,
                        } => (sorted_batch, probe_values),
                        _ => unreachable!(),
                    };

                    // Get build data reference
                    let build_data = match &self.build_side {
                        BuildSideState::Ready(data) => Arc::clone(data),
                        BuildSideState::Initial(_) => unreachable!(),
                    };

                    let _join_timer = self.join_metrics.join_time.timer();

                    if let Err(e) = process_probe_batch(
                        &build_data,
                        &sorted_batch,
                        &probe_values,
                        self.low_op,
                        self.high_op,
                        self.build_join_side,
                        &self.schema,
                        &mut self.coalescer,
                        &self.join_metrics,
                    ) {
                        return Poll::Ready(Some(Err(e)));
                    }

                    // State already set to FetchProbeBatch above
                    // Loop back to check coalescer for completed batch
                }

                IntervalJoinState::Completed => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Core join logic
// ---------------------------------------------------------------------------

/// Process one sorted probe batch against the sorted build data.
#[expect(clippy::too_many_arguments)]
fn process_probe_batch(
    build: &IntervalBuildData,
    sorted_probe_batch: &RecordBatch,
    probe_values: &ArrayRef,
    low_op: Operator,
    high_op: Operator,
    build_join_side: JoinSide,
    schema: &SchemaRef,
    coalescer: &mut BatchCoalescer,
    _metrics: &BuildProbeJoinMetrics,
) -> Result<()> {
    let build_len = build.batch.num_rows();
    let probe_len = sorted_probe_batch.num_rows();

    if build_len == 0 || probe_len == 0 {
        return Ok(());
    }

    // Create comparators: both ascending, nulls last
    let cmp_options = SortOptions::new(false, false);
    let low_cmp = make_comparator(
        probe_values.as_ref(),
        build.low_values.as_ref(),
        cmp_options,
    )?;
    let high_cmp = make_comparator(
        probe_values.as_ref(),
        build.high_values.as_ref(),
        cmp_options,
    )?;

    let mut boundary: usize = 0;

    for probe_idx in 0..probe_len {
        // Skip null probe values
        if probe_values.is_null(probe_idx) {
            continue;
        }

        // Advance boundary monotonically: find last build index where
        // build_low satisfies the low-bound condition against probe_val.
        // Never resets — total advances across all probe rows is O(N+M).
        //
        // Example (build sorted by start_time):
        //   idx 0: [0,20)  idx 1: [10,30)  idx 2: [20,40)  idx 3: [40,50)
        //   probe (sorted): 15, 25, 45
        //
        //   probe=15: advance while build_low ≤ 15 → boundary=2
        //   probe=25: continue from 2, advance while ≤ 25 → boundary=3
        //   probe=45: continue from 3, advance while ≤ 45 → boundary=4
        while boundary < build_len {
            // Skip null low values
            if build.low_values.is_null(boundary) {
                boundary += 1;
                continue;
            }

            let cmp = low_cmp(probe_idx, boundary);
            // For low_op GtEq: probe >= build_low → cmp is Equal or Greater
            // For low_op Gt:   probe > build_low  → cmp is Greater
            let satisfies = match low_op {
                Operator::GtEq => cmp != Ordering::Less,
                Operator::Gt => cmp == Ordering::Greater,
                _ => unreachable!(),
            };
            if !satisfies {
                break;
            }
            boundary += 1;
        }

        // Scan [0..boundary) checking high condition
        let mut build_indices: Vec<u32> = Vec::new();
        for build_idx in 0..boundary {
            // Skip rows with null low or high values
            if build.low_values.is_null(build_idx) || build.high_values.is_null(build_idx)
            {
                continue;
            }

            let cmp = high_cmp(probe_idx, build_idx);
            // For high_op Lt:   probe < build_high  → cmp is Less
            // For high_op LtEq: probe <= build_high → cmp is Less or Equal
            let satisfies = match high_op {
                Operator::Lt => cmp == Ordering::Less,
                Operator::LtEq => cmp != Ordering::Greater,
                _ => unreachable!(),
            };
            if satisfies {
                build_indices.push(build_idx as u32);
            }
        }

        if build_indices.is_empty() {
            continue;
        }

        let num_matches = build_indices.len();

        // Build output batch for this probe row's matches
        let build_idx_arr = UInt32Array::from(build_indices);
        let matched_build = take_record_batch(&build.batch, &build_idx_arr)?;
        let probe_idx_arr = UInt32Array::from_value(probe_idx as u32, num_matches);
        let matched_probe = take_record_batch(sorted_probe_batch, &probe_idx_arr)?;

        // Combine in left ++ right order (not build ++ probe)
        // Combine columns in left ++ right order (schema always expects this).
        // e.g. build=Left (windows), probe=Right (events):
        //   output = [w.id, w.start, w.end] ++ [e.id, e.time]
        // e.g. build=Right, probe=Left:
        //   output = [e.id, e.time] ++ [w.id, w.start, w.end]
        let columns = match build_join_side {
            JoinSide::Left => {
                // build is left, probe is right
                let mut cols = matched_build.columns().to_vec();
                cols.extend(matched_probe.columns().to_vec());
                cols
            }
            JoinSide::Right => {
                // probe is left, build is right
                let mut cols = matched_probe.columns().to_vec();
                cols.extend(matched_build.columns().to_vec());
                cols
            }
            _ => unreachable!(),
        };
        let batch = RecordBatch::try_new(Arc::clone(schema), columns)?;
        coalescer.push_batch(batch)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common;
    use crate::test::{TestMemoryExec, build_table_i32};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::col;

    fn build_interval_table() -> Arc<dyn ExecutionPlan> {
        // windows: (wid, start_time, end_time)
        // (1, 0, 20), (2, 20, 40), (3, 40, 50)
        let batch = build_table_i32(
            ("wid", &vec![1, 2, 3]),
            ("start_time", &vec![0, 20, 40]),
            ("end_time", &vec![20, 40, 50]),
        );
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn build_probe_table() -> Arc<dyn ExecutionPlan> {
        // events: (id, event_time, dummy)
        // (1, 10, 0), (2, 15, 0), (3, 30, 0), (4, 45, 0)
        let batch = build_table_i32(
            ("id", &vec![1, 2, 3, 4]),
            ("event_time", &vec![10, 15, 30, 45]),
            ("dummy", &vec![0, 0, 0, 0]),
        );
        let schema = batch.schema();
        Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_interval_join_basic() -> Result<()> {
        let build = build_interval_table();
        let probe = build_probe_table();

        let probe_expr = col("event_time", &probe.schema())?;
        let build_low_expr = col("start_time", &build.schema())?;
        let build_high_expr = col("end_time", &build.schema())?;

        let join = IntervalJoinExec::try_new(
            build,
            probe,
            JoinSide::Left, // build is left (intervals)
            probe_expr,
            build_low_expr,
            build_high_expr,
            Operator::GtEq,
            Operator::Lt,
            JoinType::Inner,
        )?;

        let session_config =
            datafusion_execution::config::SessionConfig::new().with_batch_size(4096);
        let ctx = Arc::new(TaskContext::default().with_session_config(session_config));

        let stream = join.execute(0, ctx)?;
        let batches = common::collect(stream).await?;

        // Expected matches:
        // event 10 → window (0, 20) → wid 1
        // event 15 → window (0, 20) → wid 1
        // event 30 → window (20, 40) → wid 2
        // event 45 → window (40, 50) → wid 3
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);

        // Verify values — schema is: left(wid, start_time, end_time) ++ right(id, event_time, dummy)
        let mut results: Vec<(i32, i32)> = Vec::new();
        for batch in &batches {
            let wids = batch
                .column(0) // wid from build (left)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let event_times = batch
                .column(4) // event_time from probe (right)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                results.push((event_times.value(i), wids.value(i)));
            }
        }
        results.sort();
        assert_eq!(results, vec![(10, 1), (15, 1), (30, 2), (45, 3)]);

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_join_empty_build() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("wid", DataType::Int32, false),
            Field::new("start_time", DataType::Int32, false),
            Field::new("end_time", DataType::Int32, false),
        ]));
        let build: Arc<dyn ExecutionPlan> = Arc::new(
            TestMemoryExec::try_new(&[vec![]], Arc::clone(&schema), None).unwrap(),
        );
        let probe = build_probe_table();

        let probe_expr = col("event_time", &probe.schema())?;
        let build_low_expr = col("start_time", &build.schema())?;
        let build_high_expr = col("end_time", &build.schema())?;

        let join = IntervalJoinExec::try_new(
            build,
            probe,
            JoinSide::Left,
            probe_expr,
            build_low_expr,
            build_high_expr,
            Operator::GtEq,
            Operator::Lt,
            JoinType::Inner,
        )?;

        let ctx = Arc::new(TaskContext::default());
        let stream = join.execute(0, ctx)?;
        let batches = common::collect(stream).await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_interval_join_overlapping_intervals() -> Result<()> {
        // Overlapping: (0, 30), (10, 40), (20, 50)
        let batch = build_table_i32(
            ("wid", &vec![1, 2, 3]),
            ("start_time", &vec![0, 10, 20]),
            ("end_time", &vec![30, 40, 50]),
        );
        let schema = batch.schema();
        let build: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap());

        // Probe: event_time = 25 (should match all three intervals)
        let batch = build_table_i32(
            ("id", &vec![1]),
            ("event_time", &vec![25]),
            ("dummy", &vec![0]),
        );
        let schema = batch.schema();
        let probe: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None).unwrap());

        let probe_expr = col("event_time", &probe.schema())?;
        let build_low_expr = col("start_time", &build.schema())?;
        let build_high_expr = col("end_time", &build.schema())?;

        let join = IntervalJoinExec::try_new(
            build,
            probe,
            JoinSide::Left,
            probe_expr,
            build_low_expr,
            build_high_expr,
            Operator::GtEq,
            Operator::Lt,
            JoinType::Inner,
        )?;

        let ctx = Arc::new(TaskContext::default());
        let stream = join.execute(0, ctx)?;
        let batches = common::collect(stream).await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // Matches all three overlapping intervals

        Ok(())
    }
}
