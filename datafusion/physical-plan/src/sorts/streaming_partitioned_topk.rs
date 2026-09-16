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

//! [`StreamingPartitionedTopKExec`]: streaming per-partition top-K operator for
//! window queries whose input is **already sorted** by
//! `(partition_keys, order_keys)`.
//!
//! This is the streaming sibling of
//! [`PartitionedTopKExec`](crate::sorts::partitioned_topk::PartitionedTopKExec).
//! Both replace a `FilterExec(rank <= K) → BoundedWindowAggExec` limit, but they
//! make opposite trade-offs:
//!
//! - `PartitionedTopKExec` makes no assumption about the input order and
//!   maintains per-partition heaps — O(K × P) memory, blocking emission.
//! - `StreamingPartitionedTopKExec` assumes input **already sorted** by
//!   `(partition_keys, order_keys)` (the rule only builds it when the child's
//!   `output_ordering` already satisfies that) and computes the per-partition
//!   rank in a **single streaming pass with O(1) state**, emitting incrementally.
//!
//! Because it only ever drops rows, it preserves the input ordering that the
//! downstream `BoundedWindowAggExec` requires, and declares the ordering as a
//! `required_input_ordering` so `EnsureRequirements` restores it if some later
//! pass perturbs it.
//!
//! # Row encoding
//!
//! Partition keys are never row-encoded for a whole batch: run boundaries come
//! from arrow's `partition` kernel, and only the rows sitting at a batch boundary
//! are encoded, to carry partition identity across batches. Removing that
//! full-batch encode — not the skipping of dropped rows — is what made this
//! materially faster than the row-at-a-time implementation it replaced.
//!
//! ORDER BY keys are the exception: RANK and DENSE_RANK detect ties against the
//! preceding row, so those *are* encoded for the whole batch. ROW_NUMBER, whose
//! rank is positional, encodes nothing per batch — which is why it gained the
//! most from the rewrite.
//!
//! # Memory
//!
//! Unlike [`PartitionedTopKExec`](crate::sorts::partitioned_topk::PartitionedTopKExec),
//! this operator holds no [`MemoryReservation`](datafusion_execution::memory_pool::MemoryReservation)
//! and needs none: its state is O(1) and independent of partition cardinality.
//! Across batches it carries only two `OwnedRow`s (the trailing partition and
//! ORDER BY keys) plus three scalar counters. The `keep` index vector and the
//! order-key `Rows` are per-batch transients bounded by the batch size, and
//! nothing accumulates — there is no per-partition state to grow, which is the
//! whole point of requiring sorted input.

use std::fmt::{self, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::{SortColumn, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter, Rows};
use datafusion_common::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};
use futures::{Stream, StreamExt, ready};

use crate::execution_plan::EmissionType;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::sorts::partitioned_topk::{PartitionedTopKConfig, WindowFnKind};
use crate::topk::build_sort_fields;
use crate::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};

/// Streaming per-partition top-K operator for pre-sorted input.
///
/// See the [module docs](self) for how this relates to
/// [`PartitionedTopKExec`](crate::sorts::partitioned_topk::PartitionedTopKExec).
#[derive(Debug, Clone)]
pub struct StreamingPartitionedTopKExec {
    /// Shared top-K configuration (input, ordering, partition prefix, fetch, fn).
    ///
    /// `config.input()` MUST be sorted by `[partition_keys..., order_keys...]`.
    config: PartitionedTopKConfig,
    /// Execution metrics.
    metrics_set: ExecutionPlanMetricsSet,
    /// Cached plan properties.
    cache: Arc<PlanProperties>,
}

impl StreamingPartitionedTopKExec {
    /// Create a new `StreamingPartitionedTopKExec`.
    ///
    /// `input` must already be ordered by `expr`
    /// (`[partition_keys..., order_keys...]`); the rule only builds this
    /// operator when `input.output_ordering()` satisfies that ordering.
    ///
    /// All three [`WindowFnKind`]s are supported: each assigns a rank that is
    /// monotonic non-decreasing within a sorted partition, which is what makes
    /// the single-pass, O(1)-state form possible.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expr: LexOrdering,
        partition_prefix_len: usize,
        fetch: usize,
        fn_kind: WindowFnKind,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input)?;
        Ok(Self {
            config: PartitionedTopKConfig::new(
                input,
                expr,
                partition_prefix_len,
                fetch,
                fn_kind,
            ),
            metrics_set: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Returns the child execution plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        self.config.input()
    }

    /// Returns the full sort ordering `[partition_keys..., order_keys...]`.
    pub fn expr(&self) -> &LexOrdering {
        self.config.expr()
    }

    /// Returns the number of leading expressions in [`Self::expr`] that define
    /// the partition key.
    pub fn partition_prefix_len(&self) -> usize {
        self.config.partition_prefix_len()
    }

    /// Returns the maximum number of rows retained per partition.
    pub fn fetch(&self) -> usize {
        self.config.fetch()
    }

    /// Returns which window function this operator is optimizing.
    pub fn fn_kind(&self) -> WindowFnKind {
        self.config.fn_kind()
    }

    /// Compute [`PlanProperties`]. This operator only drops rows, so it
    /// preserves the input's ordering, partitioning, and boundedness, and emits
    /// incrementally.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> Result<PlanProperties> {
        Ok(PlanProperties::new(
            input.equivalence_properties().clone(),
            input.output_partitioning().clone(),
            EmissionType::Incremental,
            input.boundedness(),
        ))
    }
}

impl DisplayAs for StreamingPartitionedTopKExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.config.fmt_as("StreamingPartitionedTopKExec", t, f)
    }
}

impl ExecutionPlan for StreamingPartitionedTopKExec {
    fn name(&self) -> &'static str {
        "StreamingPartitionedTopKExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> crate::InputDistributionRequirements {
        crate::InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(
            self.config.partition_exprs(),
        )])
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.config.expr().clone()))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Only drops rows, so the input ordering is preserved.
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.config.input()]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(StreamingPartitionedTopKExec::try_new(
            Arc::clone(&children[0]),
            self.config.expr().clone(),
            self.config.partition_prefix_len(),
            self.config.fetch(),
            self.config.fn_kind(),
        )?))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        crate::apply_expression_roots(
            self.config.expr().iter().map(|sort_expr| &sort_expr.expr),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self
            .config
            .input()
            .execute(partition, Arc::clone(&context))?;
        let schema = input.schema();

        let expr = self.config.expr();
        let partition_prefix_len = self.config.partition_prefix_len();
        let partition_converter = RowConverter::new(build_sort_fields(
            &expr[..partition_prefix_len],
            &schema,
        )?)?;

        // RANK and DENSE_RANK need to detect ORDER BY ties against the previous
        // row; ROW_NUMBER only needs the positional count, so it never touches
        // the order keys.
        let order_converter = match self.config.fn_kind() {
            WindowFnKind::Rank | WindowFnKind::DenseRank => Some(RowConverter::new(
                build_sort_fields(&expr[partition_prefix_len..], &schema)?,
            )?),
            WindowFnKind::RowNumber => None,
        };

        Ok(Box::pin(StreamingPartitionedTopKStream {
            input,
            schema,
            partition_exprs: self.config.partition_exprs(),
            order_exprs: self.config.order_exprs(),
            partition_converter,
            order_converter,
            fetch: self.config.fetch(),
            fn_kind: self.config.fn_kind(),
            prev_partition: None,
            prev_order: None,
            rank: 0,
            count: 0,
            partition_satisfied: false,
            baseline: BaselineMetrics::new(&self.metrics_set, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }
}

/// Streaming state machine. Carries only scalar running state
/// (`prev_partition`, `prev_order`, `rank`, `count`) across batches — O(1)
/// memory regardless of partition cardinality.
struct StreamingPartitionedTopKStream {
    /// Sorted input stream.
    input: SendableRecordBatchStream,
    /// Output schema (identical to the input schema — rows are only dropped).
    schema: SchemaRef,
    /// Partition-key expressions.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Order-by expressions (used only for RANK / DENSE_RANK tie detection).
    order_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Encodes partition-key columns into comparable rows.
    partition_converter: RowConverter,
    /// Encodes order-by columns into comparable rows (RANK / DENSE_RANK only).
    order_converter: Option<RowConverter>,
    /// K in "top-K".
    fetch: usize,
    /// Which ranking function is being computed.
    fn_kind: WindowFnKind,
    /// Encoded partition key of the last row of the previous batch, to detect a
    /// partition change straddling a batch boundary.
    prev_partition: Option<OwnedRow>,
    /// Encoded ORDER BY key of the last row of the previous batch, to detect a
    /// tie straddling a batch boundary (RANK / DENSE_RANK only).
    prev_order: Option<OwnedRow>,
    /// Rank (0-indexed) assigned to the previous row in the current partition.
    rank: usize,
    /// Number of rows seen so far in the current partition (0-indexed position
    /// of the next row).
    count: usize,
    /// Whether the current partition has already yielded its top-K. The assigned
    /// rank value is monotonic non-decreasing within a sorted partition — for
    /// ROW_NUMBER (`count`), RANK, and DENSE_RANK alike — so once a row's rank
    /// reaches `fetch` no later row in the same partition can survive, and the
    /// rest of the partition (including whole subsequent batches) is skipped.
    /// Reset to `false` on every partition change.
    partition_satisfied: bool,
    /// Execution metrics.
    baseline: BaselineMetrics,
}

impl StreamingPartitionedTopKStream {
    /// Filter one batch to the rows whose per-partition rank is `< fetch`,
    /// advancing the running state. Returns `None` if no row survives.
    fn process_batch(&mut self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(None);
        }

        // Whole-batch fast path: if the current partition is already satisfied
        // and the batch's *last* row is still that same partition, then (input
        // being sorted) every row in the batch belongs to the satisfied
        // partition and is dropped. Only the last row is encoded — a new
        // partition, if any, could only start later in the run, so if the last
        // row hasn't crossed the boundary, none have. This keeps skipped
        // batches O(1) rather than paying a full-batch encode.
        if self.partition_satisfied {
            let last = batch.slice(num_rows - 1, 1);
            let last_partition = self
                .partition_converter
                .convert_columns(&eval_columns(&self.partition_exprs, &last)?)?;
            if self
                .prev_partition
                .as_ref()
                .is_some_and(|prev| prev.row() == last_partition.row(0))
            {
                return Ok(None);
            }
        }

        // Partition-run boundaries come from the same helper the sibling window
        // operators use (`BoundedWindowAggExec`, `PartialSortExec`), which
        // delegates to arrow's `partition` kernel: a vectorized compare of each
        // partition column against itself shifted by one row, yielding every run
        // in a single pass.
        let partition_arrays = eval_columns(&self.partition_exprs, batch)?;
        let sort_columns: Vec<SortColumn> = partition_arrays
            .iter()
            .map(|values| SortColumn {
                values: Arc::clone(values),
                options: None,
            })
            .collect();
        let ranges = evaluate_partition_ranges(num_rows, &sort_columns)?;

        let order_rows: Option<Rows> = match &self.order_converter {
            // RANK / DENSE_RANK compare ORDER BY keys against the preceding row,
            // so unlike the partition keys these are encoded for the *whole*
            // batch — the one full-width encode the operator still pays.
            // ROW_NUMBER's rank is positional, so it encodes nothing here.
            Some(converter) => Some(
                converter.convert_columns(&eval_columns(&self.order_exprs, batch)?)?,
            ),
            None => None,
        };

        let mut keep: Vec<u32> = Vec::new();

        for range in ranges {
            // Only the batch's first run can continue the previous batch's
            // partition; every later run begins at a key change by construction.
            // The previous batch's arrays are gone, so the test compares against a
            // carried `OwnedRow` — and `range.start == 0` short-circuits, so this
            // encodes at most one row per batch.
            let same_partition = range.start == 0
                && match &self.prev_partition {
                    Some(prev) => {
                        prev.row()
                            == encode_row(
                                &self.partition_converter,
                                &partition_arrays,
                                0,
                            )?
                            .row()
                    }
                    None => false,
                };

            if !same_partition {
                self.count = 0;
                self.rank = 0;
                self.partition_satisfied = false;
            } else if self.partition_satisfied {
                // Continuation of an already-satisfied partition: drop it whole.
                continue;
            }

            match self.fn_kind {
                // ROW_NUMBER: rank is the position within the partition, so the
                // survivors are the first `fetch - count` rows of the run — no
                // per-row inspection needed.
                WindowFnKind::RowNumber => {
                    let remaining = self.fetch.saturating_sub(self.count);
                    let run_len = range.end - range.start;
                    let take = remaining.min(run_len);
                    keep.extend((range.start..range.start + take).map(|i| i as u32));
                    self.count += take;
                    if take < run_len {
                        self.partition_satisfied = true;
                    }
                }
                // RANK / DENSE_RANK: ties must be detected against the preceding
                // row, so the run is walked — but only up to the point where the
                // rank crosses `fetch`.
                WindowFnKind::Rank | WindowFnKind::DenseRank => {
                    let dense = matches!(self.fn_kind, WindowFnKind::DenseRank);
                    let order_rows = order_rows
                        .as_ref()
                        .expect("RANK/DENSE_RANK requires an ORDER BY row encoding");
                    for i in range {
                        let count = self.count;
                        // First row of a partition → rank 0; a row tied on
                        // ORDER BY with its predecessor inherits its rank;
                        // otherwise RANK jumps to the 0-indexed position
                        // (leaving a gap) while DENSE_RANK advances by one
                        // (counting distinct ORDER BY values, no gaps).
                        let this_rank = if count == 0 {
                            0
                        } else {
                            let tie = if i == 0 {
                                self.prev_order
                                    .as_ref()
                                    .is_some_and(|prev| prev.row() == order_rows.row(0))
                            } else {
                                order_rows.row(i) == order_rows.row(i - 1)
                            };
                            if tie {
                                self.rank
                            } else if dense {
                                self.rank + 1
                            } else {
                                count
                            }
                        };

                        self.rank = this_rank;
                        self.count = count + 1;

                        if this_rank < self.fetch {
                            keep.push(i as u32);
                        } else {
                            // Rank is monotonic within a partition, so no later
                            // row of this run can survive either.
                            self.partition_satisfied = true;
                            break;
                        }
                    }
                }
            }
        }

        // Carry the boundary state of the last row into the next batch.
        self.prev_partition = Some(encode_row(
            &self.partition_converter,
            &partition_arrays,
            num_rows - 1,
        )?);
        if let Some(order_rows) = &order_rows {
            self.prev_order = Some(order_rows.row(num_rows - 1).owned());
        }

        if keep.is_empty() {
            // Skip fully-filtered batches so downstream never sees spurious
            // empty batches.
            return Ok(None);
        }
        if keep.len() == num_rows {
            return Ok(Some(batch.clone()));
        }
        Ok(Some(take_record_batch(batch, &UInt32Array::from(keep))?))
    }
}

impl Stream for StreamingPartitionedTopKStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline.elapsed_compute().clone();
        let poll = loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();
                    let result = self.process_batch(&batch);
                    timer.done();
                    match result {
                        Ok(Some(output)) => break Poll::Ready(Some(Ok(output))),
                        // Every row of this batch was dropped; pull the next one.
                        Ok(None) => continue,
                        Err(e) => break Poll::Ready(Some(Err(e))),
                    }
                }
                Some(Err(e)) => break Poll::Ready(Some(Err(e))),
                None => break Poll::Ready(None),
            }
        };
        self.baseline.record_poll(poll)
    }
}

impl RecordBatchStream for StreamingPartitionedTopKStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Evaluate `exprs` against `batch`, returning one array per expression.
fn eval_columns(
    exprs: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    exprs
        .iter()
        .map(|e| e.evaluate(batch)?.into_array(batch.num_rows()))
        .collect()
}

/// Row-encode a single row of already-evaluated columns, for carrying a
/// partition key across a batch boundary.
fn encode_row(
    converter: &RowConverter,
    arrays: &[ArrayRef],
    idx: usize,
) -> Result<OwnedRow> {
    let sliced: Vec<ArrayRef> = arrays.iter().map(|a| a.slice(idx, 1)).collect();
    Ok(converter.convert_columns(&sliced)?.row(0).owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Array;
    use arrow::array::Int32Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::Column;

    use crate::collect;
    use crate::test::TestMemoryExec;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]))
    }

    fn batch(pk: Vec<Option<i32>>, val: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(pk)),
                Arc::new(Int32Array::from(val)),
            ],
        )
        .unwrap()
    }

    /// Run the streaming operator over `batches` (a single, already-sorted
    /// partition of input) and return the flattened `(pk, val)` output rows.
    /// The ORDER BY key (`val`) uses `order_opts`; the input batches must
    /// already be sorted consistently with it.
    async fn run_with_order_opts(
        fn_kind: WindowFnKind,
        fetch: usize,
        batches: Vec<RecordBatch>,
        order_opts: SortOptions,
    ) -> Vec<(Option<i32>, Option<i32>)> {
        let expr = LexOrdering::new([
            PhysicalSortExpr::new(
                Arc::new(Column::new("pk", 0)),
                SortOptions::new(false, true),
            ),
            PhysicalSortExpr::new(Arc::new(Column::new("val", 1)), order_opts),
        ])
        .unwrap();
        let input = TestMemoryExec::try_new_exec(&[batches], schema(), None).unwrap();
        let exec = Arc::new(
            StreamingPartitionedTopKExec::try_new(input, expr, 1, fetch, fn_kind)
                .unwrap(),
        );
        let ctx = Arc::new(TaskContext::default());
        let out = collect(exec, ctx).await.unwrap();

        let mut rows = Vec::new();
        for b in out {
            let pk = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let val = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                let pk = if pk.is_null(i) {
                    None
                } else {
                    Some(pk.value(i))
                };
                let val = if val.is_null(i) {
                    None
                } else {
                    Some(val.value(i))
                };
                rows.push((pk, val));
            }
        }
        rows
    }

    /// Convenience wrapper for the common ASC (NULLS FIRST) order.
    async fn run(
        fn_kind: WindowFnKind,
        fetch: usize,
        batches: Vec<RecordBatch>,
    ) -> Vec<(Option<i32>, Option<i32>)> {
        run_with_order_opts(fn_kind, fetch, batches, SortOptions::new(false, true)).await
    }

    #[tokio::test]
    async fn row_number_keeps_first_k_per_partition() {
        // pk=1: 10,20,30 ; pk=2: 5,6 . fetch=2 → drop (1,30).
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(2), Some(2)],
            vec![Some(10), Some(20), Some(30), Some(5), Some(6)],
        );
        let rows = run(WindowFnKind::RowNumber, 2, vec![b]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(2), Some(5)),
                (Some(2), Some(6)),
            ]
        );
    }

    #[tokio::test]
    async fn row_number_counts_across_batch_and_partition_boundary() {
        // Batch 1: (1,1),(1,2). Batch 2: (1,3),(2,5). fetch=2.
        // pk=1 continues into batch 2 → (1,3) is rank 3, dropped. pk=2 resets.
        let b1 = batch(vec![Some(1), Some(1)], vec![Some(1), Some(2)]);
        let b2 = batch(vec![Some(1), Some(2)], vec![Some(3), Some(5)]);
        let rows = run(WindowFnKind::RowNumber, 2, vec![b1, b2]).await;
        assert_eq!(
            rows,
            vec![(Some(1), Some(1)), (Some(1), Some(2)), (Some(2), Some(5))]
        );
    }

    #[tokio::test]
    async fn rank_retains_boundary_ties() {
        // pk=1 vals 10,10,20,30. ranks (0-indexed) = 0,0,2,3. fetch=2 keeps
        // rank<2 → the two tied 10s only.
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(1)],
            vec![Some(10), Some(10), Some(20), Some(30)],
        );
        let rows = run(WindowFnKind::Rank, 2, vec![b]).await;
        assert_eq!(rows, vec![(Some(1), Some(10)), (Some(1), Some(10))]);
    }

    #[tokio::test]
    async fn rank_detects_ties_across_batch_boundary() {
        // Batch 1: (1,10),(1,10). Batch 2: (1,10),(1,20).
        // ranks: 0,0 then (tie with prev 10)→0, then 20→rank 3. fetch=2 keeps
        // all three 10s.
        let b1 = batch(vec![Some(1), Some(1)], vec![Some(10), Some(10)]);
        let b2 = batch(vec![Some(1), Some(1)], vec![Some(10), Some(20)]);
        let rows = run(WindowFnKind::Rank, 2, vec![b1, b2]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(1), Some(10)),
            ]
        );
    }

    #[tokio::test]
    async fn rank_treats_null_order_keys_as_ties() {
        // pk=1 vals NULL,NULL,5 (NULLS FIRST). ranks 0,0(tie),2. fetch=2 keeps
        // the two NULLs.
        let b = batch(vec![Some(1), Some(1), Some(1)], vec![None, None, Some(5)]);
        let rows = run(WindowFnKind::Rank, 2, vec![b]).await;
        assert_eq!(rows, vec![(Some(1), None), (Some(1), None)]);
    }

    #[tokio::test]
    async fn dense_rank_keeps_first_k_distinct_values() {
        // pk=1 vals 10,10,20,20,30. dense ranks (0-indexed) = 0,0,1,1,2.
        // fetch=2 keeps dense_rank<2 → both 10s and both 20s. RANK would keep
        // only the 10s here (its ranks are 0,0,2,2,4), so this pins the
        // gapless-advance semantics.
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(1), Some(1)],
            vec![Some(10), Some(10), Some(20), Some(20), Some(30)],
        );
        let rows = run(WindowFnKind::DenseRank, 2, vec![b]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(1), Some(20)),
            ]
        );
    }

    #[tokio::test]
    async fn dense_rank_detects_ties_across_batch_boundary() {
        // Batch 1: (1,10),(1,10). Batch 2: (1,10),(1,20),(1,30).
        // dense ranks: 0,0, then (tie with prev 10)→0, 20→1, 30→2. fetch=2 keeps
        // the three 10s and the 20. If the cross-batch carry were lost, the
        // batch-2 leading 10 would start a fresh partition at rank 0.
        let b1 = batch(vec![Some(1), Some(1)], vec![Some(10), Some(10)]);
        let b2 = batch(
            vec![Some(1), Some(1), Some(1)],
            vec![Some(10), Some(20), Some(30)],
        );
        let rows = run(WindowFnKind::DenseRank, 2, vec![b1, b2]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(1), Some(10)),
                (Some(1), Some(20)),
            ]
        );
    }

    #[tokio::test]
    async fn dense_rank_resets_per_partition() {
        // pk=1 vals 1,2,3 → dense ranks 0,1,2; pk=2 vals 5,6,7 → 0,1,2.
        // fetch=2 keeps the first two distinct values of each partition.
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(2), Some(2), Some(2)],
            vec![Some(1), Some(2), Some(3), Some(5), Some(6), Some(7)],
        );
        let rows = run(WindowFnKind::DenseRank, 2, vec![b]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(1)),
                (Some(1), Some(2)),
                (Some(2), Some(5)),
                (Some(2), Some(6)),
            ]
        );
    }

    #[tokio::test]
    async fn dense_rank_treats_null_order_keys_as_ties() {
        // pk=1 vals NULL,NULL,5 (NULLS FIRST). dense ranks 0,0(tie),1. fetch=2
        // keeps all three — unlike RANK, where the 5 sits at rank 2 and is
        // dropped.
        let b = batch(vec![Some(1), Some(1), Some(1)], vec![None, None, Some(5)]);
        let rows = run(WindowFnKind::DenseRank, 2, vec![b]).await;
        assert_eq!(
            rows,
            vec![(Some(1), None), (Some(1), None), (Some(1), Some(5))]
        );
    }

    #[tokio::test]
    async fn dense_rank_desc_order_retains_ties() {
        // ORDER BY val DESC. vals 30,30,20,10 → dense ranks 0,0,1,2. fetch=2
        // keeps the two 30s and the 20.
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(1)],
            vec![Some(30), Some(30), Some(20), Some(10)],
        );
        let rows = run_with_order_opts(
            WindowFnKind::DenseRank,
            2,
            vec![b],
            SortOptions::new(true, true),
        )
        .await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(30)),
                (Some(1), Some(30)),
                (Some(1), Some(20)),
            ]
        );
    }

    #[tokio::test]
    async fn dense_rank_skips_satisfied_partition_across_batches() {
        // fetch=1: pk=1's first distinct value (10) is the only survivor. The
        // dense rank crosses `fetch` at the 20 in batch 1, marking the partition
        // satisfied; batch 2 must then be dropped by the whole-batch fast path
        // even though its values keep increasing.
        let b1 = batch(
            vec![Some(1), Some(1), Some(1)],
            vec![Some(10), Some(10), Some(20)],
        );
        let b2 = batch(vec![Some(1), Some(1)], vec![Some(30), Some(40)]);
        let rows = run(WindowFnKind::DenseRank, 1, vec![b1, b2]).await;
        assert_eq!(rows, vec![(Some(1), Some(10)), (Some(1), Some(10))]);
    }

    #[tokio::test]
    async fn fully_filtered_batch_is_skipped() {
        // fetch=1: pk=1 first row kept; a batch that contributes only dropped
        // rows must not surface as an empty batch.
        let b1 = batch(vec![Some(1)], vec![Some(1)]);
        let b2 = batch(vec![Some(1), Some(1)], vec![Some(2), Some(3)]);
        let asc = SortOptions::new(false, true);
        let expr = LexOrdering::new([
            PhysicalSortExpr::new(Arc::new(Column::new("pk", 0)), asc),
            PhysicalSortExpr::new(Arc::new(Column::new("val", 1)), asc),
        ])
        .unwrap();
        let input =
            TestMemoryExec::try_new_exec(&[vec![b1, b2]], schema(), None).unwrap();
        let exec = Arc::new(
            StreamingPartitionedTopKExec::try_new(
                input,
                expr,
                1,
                1,
                WindowFnKind::RowNumber,
            )
            .unwrap(),
        );
        let ctx = Arc::new(TaskContext::default());
        let out = collect(exec, ctx).await.unwrap();
        // Exactly one non-empty batch (the second batch is fully dropped).
        assert_eq!(out.iter().filter(|b| b.num_rows() > 0).count(), 1);
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn row_number_desc_order() {
        // ORDER BY val DESC → input sorted descending within each partition.
        // fetch=2 keeps the two largest per partition (positional, so ties
        // don't matter for ROW_NUMBER).
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(2), Some(2)],
            vec![Some(30), Some(20), Some(10), Some(9), Some(8)],
        );
        let rows = run_with_order_opts(
            WindowFnKind::RowNumber,
            2,
            vec![b],
            SortOptions::new(true, true),
        )
        .await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(30)),
                (Some(1), Some(20)),
                (Some(2), Some(9)),
                (Some(2), Some(8)),
            ]
        );
    }

    #[tokio::test]
    async fn rank_desc_order_retains_ties() {
        // ORDER BY val DESC with ties. vals 30,30,20,10 → ranks 0,0,2,3.
        // Row equality is direction-independent, so the two tied 30s are kept
        // at fetch=2 exactly as in the ASC case.
        let b = batch(
            vec![Some(1), Some(1), Some(1), Some(1)],
            vec![Some(30), Some(30), Some(20), Some(10)],
        );
        let rows = run_with_order_opts(
            WindowFnKind::Rank,
            2,
            vec![b],
            SortOptions::new(true, true),
        )
        .await;
        assert_eq!(rows, vec![(Some(1), Some(30)), (Some(1), Some(30))]);
    }

    #[tokio::test]
    async fn rank_nulls_last_order() {
        // ORDER BY val ASC NULLS LAST → nulls sort to the end of the partition.
        // vals 5,5,NULL → ranks 0,0(tie),2. fetch=2 keeps the two 5s and drops
        // the trailing NULL (which is rank 2). Confirms tie detection works
        // when the null sentinel sits at the opposite end from NULLS FIRST.
        let b = batch(
            vec![Some(1), Some(1), Some(1)],
            vec![Some(5), Some(5), None],
        );
        let rows = run_with_order_opts(
            WindowFnKind::Rank,
            2,
            vec![b],
            SortOptions::new(false, false),
        )
        .await;
        assert_eq!(rows, vec![(Some(1), Some(5)), (Some(1), Some(5))]);
    }

    #[tokio::test]
    async fn skips_satisfied_partition_across_batches() {
        // One partition (pk=1) spread over three batches, fetch=2. The first two
        // rows satisfy the partition; every later row/batch must be dropped
        // (whole-batch fast path for batch 3, per-row skip for the tail of
        // batch 2).
        let b1 = batch(vec![Some(1), Some(1)], vec![Some(10), Some(20)]);
        let b2 = batch(vec![Some(1), Some(1)], vec![Some(30), Some(40)]);
        let b3 = batch(vec![Some(1), Some(1)], vec![Some(50), Some(60)]);
        let rows = run(WindowFnKind::RowNumber, 2, vec![b1, b2, b3]).await;
        assert_eq!(rows, vec![(Some(1), Some(10)), (Some(1), Some(20))]);
    }

    #[tokio::test]
    async fn satisfied_partition_resets_on_new_partition_mid_batch() {
        // pk=1 satisfied within batch 1 (fetch=2). Batch 2 starts still in pk=1
        // (those rows dropped) then switches to pk=2 mid-batch — pk=2 must reset
        // and emit its own top-2, i.e. the whole-batch skip must NOT fire when
        // the batch's last row has crossed into a new partition.
        let b1 = batch(
            vec![Some(1), Some(1), Some(1)],
            vec![Some(1), Some(2), Some(3)],
        );
        let b2 = batch(
            vec![Some(1), Some(2), Some(2)],
            vec![Some(4), Some(5), Some(6)],
        );
        let rows = run(WindowFnKind::RowNumber, 2, vec![b1, b2]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(1)),
                (Some(1), Some(2)),
                (Some(2), Some(5)),
                (Some(2), Some(6)),
            ]
        );
    }

    #[tokio::test]
    async fn satisfied_partition_boundary_exactly_at_batch_edge() {
        // pk=1 fills exactly batch 1 (fetch=2, so both kept). Batch 2 is entirely
        // a new partition pk=2 — the fast path must not skip it just because the
        // previous partition was satisfied.
        let b1 = batch(vec![Some(1), Some(1)], vec![Some(10), Some(20)]);
        let b2 = batch(
            vec![Some(2), Some(2), Some(2)],
            vec![Some(5), Some(6), Some(7)],
        );
        let rows = run(WindowFnKind::RowNumber, 2, vec![b1, b2]).await;
        assert_eq!(
            rows,
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(2), Some(5)),
                (Some(2), Some(6)),
            ]
        );
    }

    #[tokio::test]
    async fn rank_ties_not_lost_at_satisfied_point_across_batches() {
        // RANK, fetch=2. Batch 1: pk=1 vals 10,10,20 → ranks 0,0,2; the 20 marks
        // the partition satisfied. Batch 2 continues pk=1 (all dropped). A change
        // that skipped batch 1's tie rows, or failed to keep both 10s, would be
        // caught here.
        let b1 = batch(
            vec![Some(1), Some(1), Some(1)],
            vec![Some(10), Some(10), Some(20)],
        );
        let b2 = batch(vec![Some(1), Some(1)], vec![Some(30), Some(40)]);
        let rows = run(WindowFnKind::Rank, 2, vec![b1, b2]).await;
        assert_eq!(rows, vec![(Some(1), Some(10)), (Some(1), Some(10))]);
    }

    #[tokio::test]
    async fn skips_satisfied_partition_desc_across_batches() {
        // Same satisfied-skip as the ASC case, but ORDER BY val DESC — the input
        // is sorted descending, so the "best" rows arrive first and get the low
        // ranks. fetch=2 keeps the two largest (60, 50); the rest of the
        // partition, including whole later batches, must be skipped. Confirms
        // rank monotonicity (and thus the skip) is positional, not value-based,
        // and holds under DESC.
        let b1 = batch(vec![Some(1), Some(1)], vec![Some(60), Some(50)]);
        let b2 = batch(vec![Some(1), Some(1)], vec![Some(40), Some(30)]);
        let b3 = batch(vec![Some(1), Some(1)], vec![Some(20), Some(10)]);
        let rows = run_with_order_opts(
            WindowFnKind::RowNumber,
            2,
            vec![b1, b2, b3],
            SortOptions::new(true, true),
        )
        .await;
        assert_eq!(rows, vec![(Some(1), Some(60)), (Some(1), Some(50))]);
    }

    /// Reference implementation: assign the rank row-at-a-time over the whole
    /// concatenated input and keep every row whose rank is `< fetch`. Slow and
    /// obviously correct — no run detection, no skipping — so it pins down what
    /// the operator's run-based, search-skipping path must reproduce.
    fn reference(
        fn_kind: WindowFnKind,
        fetch: usize,
        rows: &[(Option<i32>, Option<i32>)],
    ) -> Vec<(Option<i32>, Option<i32>)> {
        let mut out = Vec::new();
        let mut count = 0usize;
        let mut rank = 0usize;
        for (i, row) in rows.iter().enumerate() {
            let same_partition = i > 0 && rows[i - 1].0 == row.0;
            if !same_partition {
                count = 0;
                rank = 0;
            }
            let this_rank = match fn_kind {
                WindowFnKind::RowNumber => count,
                WindowFnKind::Rank => {
                    if count == 0 {
                        0
                    } else if rows[i - 1].1 == row.1 {
                        rank
                    } else {
                        count
                    }
                }
                WindowFnKind::DenseRank => {
                    if count == 0 {
                        0
                    } else if rows[i - 1].1 == row.1 {
                        rank
                    } else {
                        rank + 1
                    }
                }
            };
            if this_rank < fetch {
                out.push(*row);
            }
            rank = this_rank;
            count += 1;
        }
        out
    }

    /// Split `rows` into batches of `batch_size` (the last one short).
    fn batches_of(
        rows: &[(Option<i32>, Option<i32>)],
        batch_size: usize,
    ) -> Vec<RecordBatch> {
        rows.chunks(batch_size)
            .map(|chunk| {
                batch(
                    chunk.iter().map(|(pk, _)| *pk).collect(),
                    chunk.iter().map(|(_, val)| *val).collect(),
                )
            })
            .collect()
    }

    /// Differential test over many layouts: the run-based path (kernel run
    /// detection, arithmetic ROW_NUMBER survivor count, tail skipping) must
    /// agree row-for-row with the naive reference.
    ///
    /// The layouts deliberately vary the three things the fast paths key on:
    /// rows-per-partition against the batch size (so runs fall inside a batch,
    /// span exactly one, and span several), the number of distinct ORDER BY
    /// values per partition (no ties → every row ties), and `fetch` relative to
    /// the partition size (nothing dropped → almost everything dropped).
    #[tokio::test]
    async fn matches_reference_across_layouts() {
        // (rows_per_partition, n_partitions, distinct_obs_per_partition)
        let layouts: [(usize, usize, usize); 8] = [
            (1, 20, 1),   // single-row partitions
            (3, 15, 3),   // short runs, no ties
            (3, 15, 1),   // short runs, all tied
            (8, 9, 4),    // runs with paired ties
            (17, 5, 17),  // odd run length, no ties
            (17, 5, 2),   // odd run length, heavy ties
            (64, 3, 8),   // runs longer than some batch sizes
            (100, 2, 25), // long runs
        ];

        for (rows_per_partition, n_partitions, distinct_obs) in layouts {
            // Sorted by (pk, val), val restarting per partition.
            let rows_per_ob = rows_per_partition.div_ceil(distinct_obs).max(1);
            let input: Vec<(Option<i32>, Option<i32>)> = (0..n_partitions)
                .flat_map(|p| {
                    (0..rows_per_partition)
                        .map(move |r| (Some(p as i32), Some((r / rows_per_ob) as i32)))
                })
                .collect();

            for batch_size in [1, 2, 7, 16, 64, 1024] {
                for fetch in [1, 2, 3, 5, 17, 64] {
                    for fn_kind in [
                        WindowFnKind::RowNumber,
                        WindowFnKind::Rank,
                        WindowFnKind::DenseRank,
                    ] {
                        let got =
                            run(fn_kind, fetch, batches_of(&input, batch_size)).await;
                        let want = reference(fn_kind, fetch, &input);
                        assert_eq!(
                            got, want,
                            "layout=(P={rows_per_partition}, n={n_partitions}, \
                             obs={distinct_obs}) batch_size={batch_size} \
                             fetch={fetch} fn={fn_kind:?}"
                        );
                    }
                }
            }
        }
    }

    /// NULL partition keys must group together rather than each forming its own
    /// partition. Run boundaries come from arrow's `partition` kernel (via
    /// `evaluate_partition_ranges`), which treats NULL as a value for the
    /// purposes of "did the key change", so a NULL run is one partition.
    #[tokio::test]
    async fn null_partition_keys_form_one_partition() {
        // pk = NULL,NULL,NULL then 1,1. fetch=2 → the NULL partition keeps its
        // first two rows (10, 20) and drops 30; pk=1 keeps both.
        let b = batch(
            vec![None, None, None, Some(1), Some(1)],
            vec![Some(10), Some(20), Some(30), Some(5), Some(6)],
        );
        let rows = run(WindowFnKind::RowNumber, 2, vec![b]).await;
        assert_eq!(
            rows,
            vec![
                (None, Some(10)),
                (None, Some(20)),
                (Some(1), Some(5)),
                (Some(1), Some(6)),
            ]
        );
    }

    /// A NULL partition run that straddles a batch boundary must still be
    /// treated as one partition — the cross-batch carry encodes the boundary row
    /// rather than comparing arrays, so NULL has to round-trip through the row
    /// encoding correctly.
    #[tokio::test]
    async fn null_partition_key_continues_across_batches() {
        // Batch 1: (NULL,10),(NULL,20). Batch 2: (NULL,30),(1,5). fetch=2 →
        // (NULL,30) is the third row of the NULL partition and must be dropped.
        let b1 = batch(vec![None, None], vec![Some(10), Some(20)]);
        let b2 = batch(vec![None, Some(1)], vec![Some(30), Some(5)]);
        let rows = run(WindowFnKind::RowNumber, 2, vec![b1, b2]).await;
        assert_eq!(
            rows,
            vec![(None, Some(10)), (None, Some(20)), (Some(1), Some(5))]
        );
    }

    /// Multi-column partition keys: the `partition` kernel ORs the per-column
    /// boundary masks, so a change in *either* key must start a new partition.
    /// `(1,1),(1,2)` are different partitions even though the first column is
    /// unchanged.
    #[tokio::test]
    async fn multi_column_partition_key_splits_on_either_column() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk1", DataType::Int32, true),
            Field::new("pk2", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
        ]));
        // (1,1): vals 10,20 ; (1,2): vals 5,6 ; (2,1): val 7.
        // fetch=1 keeps the first row of each of the three partitions.
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 1, 1, 2])),
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 1])),
                Arc::new(Int32Array::from(vec![10, 20, 5, 6, 7])),
            ],
        )
        .unwrap();

        let asc = SortOptions::new(false, true);
        let expr = LexOrdering::new([
            PhysicalSortExpr::new(Arc::new(Column::new("pk1", 0)), asc),
            PhysicalSortExpr::new(Arc::new(Column::new("pk2", 1)), asc),
            PhysicalSortExpr::new(Arc::new(Column::new("val", 2)), asc),
        ])
        .unwrap();
        let input =
            TestMemoryExec::try_new_exec(&[vec![b]], Arc::clone(&schema), None).unwrap();
        let exec = Arc::new(
            StreamingPartitionedTopKExec::try_new(
                input,
                expr,
                2,
                1,
                WindowFnKind::RowNumber,
            )
            .unwrap(),
        );
        let out = collect(exec, Arc::new(TaskContext::default()))
            .await
            .unwrap();

        let mut got = Vec::new();
        for b in out {
            let pk1 = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let pk2 = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            let val = b.column(2).as_any().downcast_ref::<Int32Array>().unwrap();
            for i in 0..b.num_rows() {
                got.push((pk1.value(i), pk2.value(i), val.value(i)));
            }
        }
        assert_eq!(got, vec![(1, 1, 10), (1, 2, 5), (2, 1, 7)]);
    }
}
