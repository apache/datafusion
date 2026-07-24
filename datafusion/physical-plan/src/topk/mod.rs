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

//! TopK: Combination of Sort / LIMIT

use arrow::{
    array::{Array, AsArray},
    compute::{
        BatchCoalescer, FilterBuilder, interleave_record_batch, prep_null_mask_filter,
        take_record_batch,
    },
    row::{OwnedRow, RowConverter, Rows, SortField},
};
use datafusion_expr::{ColumnarValue, Operator};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};

use super::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
    RecordOutput,
};
use crate::spill::get_record_batch_memory_size;
use crate::{SendableRecordBatchStream, stream::RecordBatchStreamAdapter};

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::datatypes::SchemaRef;
use datafusion_common::{
    HashMap, Result, ScalarValue, internal_datafusion_err, internal_err,
};
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    runtime_env::RuntimeEnv,
};
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{BinaryExpr, DynamicFilterPhysicalExpr, is_not_null, is_null, lit},
};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use parking_lot::RwLock;

/// TopK
///
/// # Background
///
/// "Top K" is a common query optimization used for queries such as
/// "find the top 3 customers by revenue". The (simplified) SQL for
/// such a query might be:
///
/// ```sql
/// SELECT customer_id, revenue FROM 'sales.csv' ORDER BY revenue DESC limit 3;
/// ```
///
/// The simple plan would be:
///
/// ```sql
/// > explain SELECT customer_id, revenue FROM sales ORDER BY revenue DESC limit 3;
/// +--------------+----------------------------------------+
/// | plan_type    | plan                                   |
/// +--------------+----------------------------------------+
/// | logical_plan | Limit: 3                               |
/// |              |   Sort: revenue DESC NULLS FIRST       |
/// |              |     Projection: customer_id, revenue   |
/// |              |       TableScan: sales                 |
/// +--------------+----------------------------------------+
/// ```
///
/// While this plan produces the correct answer, it will fully sorts the
/// input before discarding everything other than the top 3 elements.
///
/// The same answer can be produced by simply keeping track of the top
/// K=3 elements, reducing the total amount of required buffer memory.
///
/// # Partial Sort Optimization
///
/// This implementation additionally optimizes queries where the input is already
/// partially sorted by a common prefix of the requested ordering. If subsequent
/// rows are guaranteed to be strictly greater (in sort order) than a known TopK
/// boundary on this prefix, the operator safely terminates early.
///
/// For a local TopK, that boundary comes from the local heap once it has K rows.
/// For a partitioned `SortExec`, a shared dynamic-filter threshold can provide
/// the same prefix boundary before a lagging partition has filled its local heap.
///
/// ## Example
///
/// For input sorted by `(day DESC)`, but not by `timestamp`, a query such as:
///
/// ```sql
/// SELECT day, timestamp FROM sensor ORDER BY day DESC, timestamp DESC LIMIT 10;
/// ```
///
/// can terminate scanning early once sufficient rows from the latest days have been
/// collected, skipping older data.
///
/// # Structure
///
/// This operator tracks the top K items using a `TopKHeap`.
pub struct TopK {
    /// schema of the output (and the input)
    schema: SchemaRef,
    /// Runtime metrics
    metrics: TopKMetrics,
    /// Reservation
    reservation: MemoryReservation,
    /// The target number of rows for output batches
    batch_size: usize,
    /// sort expressions
    expr: LexOrdering,
    /// row converter, for sort keys
    row_converter: RowConverter,
    /// scratch space for converting rows
    scratch_rows: Rows,
    /// stores the top k values and their sort key values, in order
    heap: TopKHeap,
    /// row converter, for common keys between the sort keys and the input ordering
    common_sort_prefix_converter: Option<RowConverter>,
    /// Common sort prefix between the input and the sort expressions to allow early exit optimization
    common_sort_prefix: Arc<[PhysicalSortExpr]>,
    /// Filter matching the state of the `TopK` heap used for dynamic filter pushdown
    filter: Arc<RwLock<TopKDynamicFilters>>,
    /// If true, indicates that all rows of subsequent batches are guaranteed
    /// to be greater (by byte order, after row conversion) than the top K,
    /// which means the top K won't change and the computation can be finished early.
    pub(crate) finished: bool,
}

/// For more background, please also see the [Dynamic Filters: Passing Information Between Operators During Execution for 25x Faster Queries blog]
///
/// [Dynamic Filters: Passing Information Between Operators During Execution for 25x Faster Queries blog]: https://datafusion.apache.org/blog/2025/09/10/dynamic-filters
#[derive(Debug)]
pub struct TopKDynamicFilters {
    /// The current threshold shared by all TopK emitters that use this dynamic
    /// filter. Any emitter may tighten it.
    ///
    /// The full sort-key row and common-prefix row are stored together so they
    /// always describe the same heap row.
    shared_threshold: Option<TopKThreshold>,
    /// The expression used to evaluate the dynamic filter
    /// Only updated when lock held for the duration of the update
    expr: Arc<DynamicFilterPhysicalExpr>,
    /// Number of local TopK emitters that have not called `emit` yet.
    ///
    /// A partition-preserving `SortExec` creates one local TopK per output
    /// partition. The shared dynamic filter is complete only after every local
    /// TopK has emitted.
    ///
    /// `emit` only needs a read guard on the shared filter wrapper, so
    /// concurrent emitters use this atomic counter instead of taking an
    /// exclusive lock just to mark their partition done.
    remaining_topk_emitters: AtomicUsize,
}

#[derive(Debug, Clone)]
struct TopKThreshold {
    /// The full sort-key row bytes for efficient comparison.
    full_sort_key_row: Vec<u8>,
    /// The same heap row encoded with the common-prefix converter, when the
    /// input ordering shares a prefix with the TopK ordering.
    ///
    /// This lets each partition stop from a shared TopK threshold even if its
    /// local heap has not filled yet.
    common_prefix_row: Option<Vec<u8>>,
}

impl TopKThreshold {
    fn new(full_sort_key_row: Vec<u8>, common_prefix_row: Option<Vec<u8>>) -> Self {
        Self {
            full_sort_key_row,
            common_prefix_row,
        }
    }

    fn full_sort_key_row(&self) -> &[u8] {
        self.full_sort_key_row.as_slice()
    }

    fn common_prefix_row(&self) -> Option<&[u8]> {
        self.common_prefix_row.as_deref()
    }

    fn is_more_selective_than(&self, current: &Self) -> bool {
        self.full_sort_key_row() < current.full_sort_key_row()
    }
}

#[derive(Clone, Copy)]
struct TopKHeapBoundaryRow<'a> {
    row: &'a TopKRow,
}

impl<'a> TopKHeapBoundaryRow<'a> {
    fn new(row: &'a TopKRow) -> Self {
        Self { row }
    }

    fn full_sort_key_row(&self) -> &[u8] {
        self.row.row()
    }

    fn is_more_selective_than(&self, current: Option<&TopKThreshold>) -> bool {
        current
            .map(|current| self.full_sort_key_row() < current.full_sort_key_row())
            .unwrap_or(true)
    }
}

#[derive(Clone, Copy)]
struct TopKHeapBoundary<'a> {
    row: &'a TopKRow,
    batch: &'a RecordBatch,
}

impl<'a> TopKHeapBoundary<'a> {
    fn new(row: &'a TopKRow, batch: &'a RecordBatch) -> Self {
        Self { row, batch }
    }

    fn threshold_values(
        &self,
        sort_exprs: &[PhysicalSortExpr],
    ) -> Result<Vec<ScalarValue>> {
        let mut scalar_values = Vec::with_capacity(sort_exprs.len());
        for sort_expr in sort_exprs {
            let value = sort_expr
                .expr
                .evaluate(&self.batch.slice(self.row.index, 1))?;

            let scalar = match value {
                ColumnarValue::Scalar(scalar) => scalar,
                ColumnarValue::Array(array) if array.len() == 1 => {
                    ScalarValue::try_from_array(&array, 0)?
                }
                array => {
                    return internal_err!("Expected a scalar value, got {:?}", array);
                }
            };
            scalar_values.push(scalar);
        }

        Ok(scalar_values)
    }

    fn threshold(&self, common_prefix_row: Option<Vec<u8>>) -> TopKThreshold {
        TopKThreshold::new(self.row.row().to_vec(), common_prefix_row)
    }
}

impl TopKDynamicFilters {
    /// Create a new `TopKDynamicFilters` with the given expression
    pub fn new(expr: Arc<DynamicFilterPhysicalExpr>) -> Self {
        Self::new_with_topk_emitter_count(expr, 1)
    }

    /// Create a new `TopKDynamicFilters` with the expected number of local
    /// TopK emitters that share it.
    pub fn new_with_topk_emitter_count(
        expr: Arc<DynamicFilterPhysicalExpr>,
        topk_emitter_count: usize,
    ) -> Self {
        debug_assert!(topk_emitter_count > 0);
        Self {
            shared_threshold: None,
            expr,
            remaining_topk_emitters: AtomicUsize::new(topk_emitter_count),
        }
    }

    pub fn expr(&self) -> Arc<DynamicFilterPhysicalExpr> {
        Arc::clone(&self.expr)
    }

    fn mark_topk_emitted(&self) {
        let previous = self
            .remaining_topk_emitters
            .fetch_update(
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
                |remaining| remaining.checked_sub(1),
            )
            .unwrap_or(0);
        debug_assert!(
            previous > 0,
            "TopK dynamic filter emitter completed more times than expected"
        );

        if previous == 1 {
            self.expr.mark_complete();
        }
    }
}

// Guesstimate for memory allocation: estimated number of bytes used per row in the RowConverter
const ESTIMATED_BYTES_PER_ROW: usize = 20;

/// Owned data of a row that was just evicted from a [`TopKHeap`].
///
/// Returned by [`TopKHeap::add`] so that callers (e.g. rank-aware
/// wrappers that retain boundary ties) can decide whether to retain
/// the evicted row externally. The underlying batch is captured
/// before the heap's internal `RecordBatchStore` decrements the
/// batch's use count, so the data remains accessible even if the
/// heap drops its internal reference to the batch.
#[derive(Debug, Clone)]
pub(crate) struct EvictedRow {
    /// The record batch the evicted row came from.
    pub batch: RecordBatch,
    /// Row index within `batch`.
    pub index: usize,
    /// Encoded ORDER BY tuple for the evicted row, in [`arrow::row`] format.
    pub row_bytes: Vec<u8>,
}

pub(crate) fn build_sort_fields(
    ordering: &[PhysicalSortExpr],
    schema: &SchemaRef,
) -> Result<Vec<SortField>> {
    ordering
        .iter()
        .map(|e| {
            Ok(SortField::new_with_options(
                e.expr.data_type(schema)?,
                e.options,
            ))
        })
        .collect::<Result<_>>()
}

impl TopK {
    /// Create a new [`TopK`] that stores the top `k` values, as
    /// defined by the sort expressions in `expr`.
    // TODO: make a builder or some other nicer API
    #[expect(clippy::too_many_arguments)]
    #[expect(clippy::needless_pass_by_value)]
    pub fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        common_sort_prefix: Vec<PhysicalSortExpr>,
        expr: LexOrdering,
        k: usize,
        batch_size: usize,
        runtime: Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
        filter: Arc<RwLock<TopKDynamicFilters>>,
    ) -> Result<Self> {
        let reservation = MemoryConsumer::new(format!("TopK[{partition_id}]"))
            .register(&runtime.memory_pool);

        let sort_fields = build_sort_fields(&expr, &schema)?;

        // TODO there is potential to add special cases for single column sort fields
        // to improve performance
        let row_converter = RowConverter::new(sort_fields)?;
        let scratch_rows =
            row_converter.empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        let common_prefix_row_converter = if common_sort_prefix.is_empty() {
            None
        } else {
            let input_sort_fields = build_sort_fields(&common_sort_prefix, &schema)?;
            Some(RowConverter::new(input_sort_fields)?)
        };

        Ok(Self {
            schema: Arc::clone(&schema),
            metrics: TopKMetrics::new(metrics, partition_id),
            reservation,
            batch_size,
            expr,
            row_converter,
            scratch_rows,
            heap: TopKHeap::new(k),
            common_sort_prefix_converter: common_prefix_row_converter,
            common_sort_prefix: Arc::from(common_sort_prefix),
            finished: false,
            filter,
        })
    }

    /// Insert `batch`, remembering if any of its values are among
    /// the top k seen so far.
    #[expect(clippy::needless_pass_by_value)]
    pub fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Updates on drop
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let mut sort_keys: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|expr| {
                let value = expr.expr.evaluate(&batch)?;
                value.into_array(batch.num_rows())
            })
            .collect::<Result<Vec<_>>>()?;

        let mut selected_rows = None;

        // If a filter is provided, update it with the new rows
        let filter = self.filter.read().expr.current()?;
        let filtered = filter.evaluate(&batch)?;
        let num_rows = batch.num_rows();
        let array = filtered.into_array(num_rows)?;
        let mut filter = array.as_boolean().clone();
        if !filter.has_true() {
            // The heap is unchanged, but a fully rejected batch can still prove
            // that the shared sort prefix has passed the heap boundary.
            self.attempt_early_completion(&batch)?;
            return Ok(());
        }
        // only update the keys / rows if the filter does not match all rows
        if filter.null_count() > 0 || filter.has_false() {
            // Indices in `set_indices` should be correct if filter contains nulls
            // So we prepare the filter here. Note this is also done in the `FilterBuilder`
            // so there is no overhead to do this here.
            if filter.nulls().is_some() {
                filter = prep_null_mask_filter(&filter);
            }

            let filter_predicate = FilterBuilder::new(&filter);
            let filter_predicate = if sort_keys.len() > 1 {
                // Optimize filter when it has multiple sort keys
                filter_predicate.optimize().build()
            } else {
                filter_predicate.build()
            };
            selected_rows = Some(filter);
            sort_keys = sort_keys
                .iter()
                .map(|key| filter_predicate.filter(key).map_err(|x| x.into()))
                .collect::<Result<Vec<_>>>()?;
        }
        // reuse existing `Rows` to avoid reallocations
        let rows = &mut self.scratch_rows;
        rows.clear();
        self.row_converter.append(rows, &sort_keys)?;

        let mut batch_entry = self.heap.register_batch(batch.clone());

        let replacements = match selected_rows {
            Some(filter) => {
                self.find_new_topk_items(filter.values().set_indices(), &mut batch_entry)
            }
            None => self.find_new_topk_items(0..sort_keys[0].len(), &mut batch_entry),
        };

        if replacements > 0 {
            self.metrics.row_replacements.add(replacements);

            self.heap.insert_batch_entry(batch_entry);

            // conserve memory
            self.heap.maybe_compact()?;

            // update memory reservation
            self.reservation.try_resize(self.size())?;

            // flag the topK as finished if we know that all
            // subsequent batches are guaranteed to be greater (by byte order, after row conversion) than the top K,
            // which means the top K won't change and the computation can be finished early.
            self.attempt_early_completion(&batch)?;

            // update the filter representation of our TopK heap
            self.update_filter()?;
        } else {
            // The heap did not change, but this batch's prefix may still prove
            // that no later rows can enter the TopK.
            self.attempt_early_completion(&batch)?;
        }

        Ok(())
    }

    fn find_new_topk_items(
        &mut self,
        items: impl Iterator<Item = usize>,
        batch_entry: &mut RecordBatchEntry,
    ) -> usize {
        let mut replacements = 0;
        let rows = &mut self.scratch_rows;
        for (index, row) in items.zip(rows.iter()) {
            match self.heap.max() {
                // heap has k items, and the new row is greater than the
                // current max in the heap ==> it is not a new topk
                Some(max_row) if row.as_ref() >= max_row.row() => {}
                // don't yet have k items or new item is lower than the currently k low values
                None | Some(_) => {
                    self.heap.add(batch_entry, row, index);
                    replacements += 1;
                }
            }
        }
        replacements
    }

    fn current_heap_boundary_row(&self) -> Option<TopKHeapBoundaryRow<'_>> {
        self.heap.max().map(TopKHeapBoundaryRow::new)
    }

    fn current_heap_boundary(&self) -> Result<Option<TopKHeapBoundary<'_>>> {
        let Some(row) = self.heap.max() else {
            return Ok(None);
        };

        self.heap_boundary(row).map(Some)
    }

    fn heap_boundary<'a>(&'a self, row: &'a TopKRow) -> Result<TopKHeapBoundary<'a>> {
        let batch_entry = self
            .heap
            .store
            .get(row.batch_id)
            .ok_or_else(|| internal_datafusion_err!("Invalid batch ID in TopKRow"))?;

        Ok(TopKHeapBoundary::new(row, &batch_entry.batch))
    }

    /// Update the filter representation of our TopK heap.
    /// For example, given the sort expression `ORDER BY a DESC, b ASC LIMIT 3`,
    /// and the current heap values `[(1, 5), (1, 4), (2, 3)]`,
    /// the filter will be updated to:
    ///
    /// ```sql
    /// (a > 1 OR (a = 1 AND b < 5)) AND
    /// (a > 1 OR (a = 1 AND b < 4)) AND
    /// (a > 2 OR (a = 2 AND b < 3))
    /// ```
    fn update_filter(&mut self) -> Result<()> {
        // If the heap doesn't have k elements yet, we can't create thresholds
        let Some(boundary_row) = self.current_heap_boundary_row() else {
            return Ok(());
        };

        // Fast path: check if the current value in topk is better than what is
        // currently set in the filter with a read only lock
        let needs_update = {
            let filter = self.filter.read();
            boundary_row.is_more_selective_than(filter.shared_threshold.as_ref())
        };

        // exit early if the current values are better
        if !needs_update {
            return Ok(());
        }

        let boundary = self.heap_boundary(boundary_row.row)?;

        // Extract scalar values BEFORE acquiring lock to reduce critical section
        let thresholds = boundary.threshold_values(&self.expr)?;

        // Build the filter expression OUTSIDE any synchronization
        let predicate = Self::build_filter_expression(&self.expr, &thresholds)?;
        let new_threshold =
            boundary.threshold(self.encode_topk_common_prefix_row(boundary)?);

        // update the threshold. Since there was a lock gap, we must check if it is still the best
        // may have changed while we were building the expression without the lock
        let mut filter = self.filter.write();
        let still_needs_update = filter
            .shared_threshold
            .as_ref()
            .map(|current| new_threshold.is_more_selective_than(current))
            .unwrap_or(true);
        if !still_needs_update {
            // some other thread updated the threshold to a better one while we
            // were building so there is no need to update the filter
            return Ok(());
        }
        filter.shared_threshold = Some(new_threshold);

        // Update the filter expression
        if let Some(pred) = predicate
            && !pred.eq(&lit(true))
        {
            filter.expr.update(pred)?;
        }

        Ok(())
    }

    /// Build the filter expression with the given thresholds.
    /// This is now called outside of any locks to reduce critical section time.
    fn build_filter_expression(
        sort_exprs: &[PhysicalSortExpr],
        thresholds: &[ScalarValue],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // Create filter expressions for each threshold
        let mut filters: Vec<Arc<dyn PhysicalExpr>> =
            Vec::with_capacity(thresholds.len());

        let mut prev_sort_expr: Option<Arc<dyn PhysicalExpr>> = None;
        for (sort_expr, value) in sort_exprs.iter().zip(thresholds.iter()) {
            // Create the appropriate operator based on sort order
            let op = if sort_expr.options.descending {
                // For descending sort, we want col > threshold (exclude smaller values)
                Operator::Gt
            } else {
                // For ascending sort, we want col < threshold (exclude larger values)
                Operator::Lt
            };

            let value_null = value.is_null();

            let comparison = Arc::new(BinaryExpr::new(
                Arc::clone(&sort_expr.expr),
                op,
                lit(value.clone()),
            ));

            let comparison_with_null = match (sort_expr.options.nulls_first, value_null) {
                // For nulls first, transform to (threshold.value is not null) and (threshold.expr is null or comparison)
                (true, true) => lit(false),
                (true, false) => Arc::new(BinaryExpr::new(
                    is_null(Arc::clone(&sort_expr.expr))?,
                    Operator::Or,
                    comparison,
                )),
                // For nulls last, transform to (threshold.value is null and threshold.expr is not null)
                // or (threshold.value is not null and comparison)
                (false, true) => is_not_null(Arc::clone(&sort_expr.expr))?,
                (false, false) => comparison,
            };

            let mut eq_expr = Arc::new(BinaryExpr::new(
                Arc::clone(&sort_expr.expr),
                Operator::Eq,
                lit(value.clone()),
            ));

            if value_null {
                eq_expr = Arc::new(BinaryExpr::new(
                    is_null(Arc::clone(&sort_expr.expr))?,
                    Operator::Or,
                    eq_expr,
                ));
            }

            // For a query like order by a, b, the filter for column `b` is only applied if
            // the condition a = threshold.value (considering null equality) is met.
            // Therefore, we add equality predicates for all preceding fields to the filter logic of the current field,
            // and include the current field's equality predicate in `prev_sort_expr` for use with subsequent fields.
            match prev_sort_expr.take() {
                None => {
                    prev_sort_expr = Some(eq_expr);
                    filters.push(comparison_with_null);
                }
                Some(p) => {
                    filters.push(Arc::new(BinaryExpr::new(
                        Arc::clone(&p),
                        Operator::And,
                        comparison_with_null,
                    )));

                    prev_sort_expr =
                        Some(Arc::new(BinaryExpr::new(p, Operator::And, eq_expr)));
                }
            }
        }

        let dynamic_predicate = filters
            .into_iter()
            .reduce(|a, b| Arc::new(BinaryExpr::new(a, Operator::Or, b)));

        Ok(dynamic_predicate)
    }

    /// If input ordering shares a common sort prefix with the TopK,
    /// check if the computation can be finished early.
    ///
    /// This is the case if the last row of the current batch is strictly
    /// greater than either the shared dynamic-filter threshold prefix or the max
    /// row in the local heap, comparing only on the shared prefix columns.
    fn attempt_early_completion(&mut self, batch: &RecordBatch) -> Result<()> {
        // Early exit if the batch is empty as there is no last row to extract from it.
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // common_prefix_row_converter is only `Some` if the input ordering has a common prefix with the TopK,
        // so early exit if it is `None`.
        let Some(prefix_converter) = &self.common_sort_prefix_converter else {
            return Ok(());
        };

        // Evaluate the prefix for the last row of the current batch.
        let last_row_idx = batch.num_rows() - 1;
        let mut batch_prefix_scratch =
            prefix_converter.empty_rows(1, ESTIMATED_BYTES_PER_ROW); // 1 row with capacity ESTIMATED_BYTES_PER_ROW

        self.append_common_prefix_row(
            prefix_converter,
            batch,
            last_row_idx,
            &mut batch_prefix_scratch,
        )?;
        let batch_common_prefix_row = batch_prefix_scratch.row(0);
        let batch_common_prefix = batch_common_prefix_row.as_ref();

        let finished_by_shared_threshold = self
            .filter
            .read()
            .shared_threshold
            .as_ref()
            .and_then(TopKThreshold::common_prefix_row)
            .map(|common_prefix_row| batch_common_prefix > common_prefix_row)
            .unwrap_or(false);
        if finished_by_shared_threshold {
            self.finished = true;
            return Ok(());
        }

        // Early exit only from the local heap once it has a full boundary row.
        let Some(boundary) = self.current_heap_boundary()? else {
            return Ok(());
        };

        if self.batch_prefix_exceeds_heap_boundary(batch_common_prefix, boundary)? {
            self.finished = true;
        }

        Ok(())
    }

    fn batch_prefix_exceeds_heap_boundary(
        &self,
        batch_common_prefix: &[u8],
        boundary: TopKHeapBoundary<'_>,
    ) -> Result<bool> {
        let Some(heap_common_prefix_row) =
            self.encode_topk_common_prefix_row(boundary)?
        else {
            return Ok(false);
        };

        Ok(batch_common_prefix > heap_common_prefix_row.as_slice())
    }

    fn encode_topk_common_prefix_row(
        &self,
        boundary: TopKHeapBoundary<'_>,
    ) -> Result<Option<Vec<u8>>> {
        let Some(prefix_converter) = &self.common_sort_prefix_converter else {
            return Ok(None);
        };

        let mut scratch = prefix_converter.empty_rows(1, ESTIMATED_BYTES_PER_ROW);
        self.append_common_prefix_row(
            prefix_converter,
            boundary.batch,
            boundary.row.index,
            &mut scratch,
        )?;
        Ok(Some(scratch.row(0).as_ref().to_vec()))
    }

    fn append_common_prefix_row(
        &self,
        prefix_converter: &RowConverter,
        batch: &RecordBatch,
        row_idx: usize,
        scratch: &mut Rows,
    ) -> Result<()> {
        let row = batch.slice(row_idx, 1);
        let prefix_columns: Vec<ArrayRef> = self
            .common_sort_prefix
            .iter()
            .map(|expr| expr.expr.evaluate(&row)?.into_array(1))
            .collect::<Result<_>>()?;

        prefix_converter.append(scratch, &prefix_columns)?;
        Ok(())
    }

    /// Returns the top k results broken into `batch_size` [`RecordBatch`]es, consuming the heap
    pub fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation: _,
            batch_size,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            mut heap,
            common_sort_prefix_converter: _,
            common_sort_prefix: _,
            finished: _,
            filter,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer(); // time updated on drop

        // Mark this local TopK as emitted. For shared filters, the final
        // local emitter marks the dynamic filter complete.
        filter.read().mark_topk_emitted();

        // break into record batches as needed
        let mut batches = vec![];
        if let Some(mut batch) = heap.emit()? {
            (&batch).record_output(&metrics.baseline);

            loop {
                if batch.num_rows() <= batch_size {
                    batches.push(Ok(batch));
                    break;
                } else {
                    batches.push(Ok(batch.slice(0, batch_size)));
                    let remaining_length = batch.num_rows() - batch_size;
                    batch = batch.slice(batch_size, remaining_length);
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        )))
    }

    /// return the size of memory used by this operator, in bytes
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.row_converter.size()
            + self.scratch_rows.size()
            + self.heap.size()
    }
}

struct TopKMetrics {
    /// metrics
    pub baseline: BaselineMetrics,

    /// count of how many rows were replaced in the heap
    pub row_replacements: Count,
}

impl TopKMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            row_replacements: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("row_replacements", partition),
        }
    }
}

/// This structure keeps at most the *smallest* k items, using the
/// [arrow::row] format for sort keys. While it is called "topK" for
/// values like `1, 2, 3, 4, 5` the "top 3" really means the
/// *smallest* 3 , `1, 2, 3`, not the *largest* 3 `3, 4, 5`.
///
/// Using the `Row` format handles things such as ascending vs
/// descending and nulls first vs nulls last.
struct TopKHeap {
    /// The maximum number of elements to store in this heap.
    k: usize,
    /// Storage for up at most `k` items using a BinaryHeap. Reversed
    /// so that the smallest k so far is on the top
    inner: BinaryHeap<TopKRow>,
    /// Storage the original row values (TopKRow only has the sort key)
    store: RecordBatchStore,
    /// The size of all owned data held by this heap
    owned_bytes: usize,
}

impl TopKHeap {
    fn new(k: usize) -> Self {
        assert!(k > 0);
        Self {
            k,
            inner: BinaryHeap::new(),
            store: RecordBatchStore::new(),
            owned_bytes: 0,
        }
    }

    /// Register a [`RecordBatch`] with the heap, returning the
    /// appropriate entry
    pub fn register_batch(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        self.store.register(batch)
    }

    /// Insert a [`RecordBatchEntry`] created by a previous call to
    /// [`Self::register_batch`] into storage.
    pub fn insert_batch_entry(&mut self, entry: RecordBatchEntry) {
        self.store.insert(entry)
    }

    /// Returns the largest value stored by the heap if there are k
    /// items, otherwise returns None. Remember this structure is
    /// keeping the "smallest" k values
    fn max(&self) -> Option<&TopKRow> {
        if self.inner.len() < self.k {
            None
        } else {
            self.inner.peek()
        }
    }

    /// Adds `row` to this heap. If inserting this new item would
    /// increase the size past `k`, removes the previously smallest
    /// item.
    ///
    /// Returns `Some(EvictedRow)` if an existing row was evicted to
    /// make room for `row`, or `None` if the row was inserted into a
    /// non-full heap.
    fn add(
        &mut self,
        batch_entry: &mut RecordBatchEntry,
        row: impl AsRef<[u8]>,
        index: usize,
    ) -> Option<EvictedRow> {
        let batch_id = batch_entry.id;
        batch_entry.uses += 1;

        assert!(self.inner.len() <= self.k);
        let row = row.as_ref();

        // Reuse storage for evicted item if possible
        if self.inner.len() == self.k {
            let mut prev_min = self.inner.peek_mut().unwrap();

            // Capture evicted row data before `unuse` (which may GC the
            // batch from the store) and `replace_with` (which overwrites
            // `prev_min` in place). The batch comes from `self.store` for
            // cross-batch evictions, or directly from `batch_entry` when
            // a row evicts another row from the same in-flight batch
            // (entry not yet registered in the store).
            let evicted_batch = if prev_min.batch_id == batch_entry.id {
                batch_entry.batch.clone()
            } else {
                self.store
                    .get(prev_min.batch_id)
                    .map(|entry| entry.batch.clone())
                    .expect("evicted row's batch must be present in the store")
            };
            let evicted = EvictedRow {
                batch: evicted_batch,
                index: prev_min.index,
                row_bytes: prev_min.row.clone(),
            };

            // Update batch use
            if prev_min.batch_id == batch_entry.id {
                batch_entry.uses -= 1;
            } else {
                self.store.unuse(prev_min.batch_id);
            }

            // update memory accounting
            self.owned_bytes -= prev_min.owned_size();

            prev_min.replace_with(row, batch_id, index);

            self.owned_bytes += prev_min.owned_size();

            Some(evicted)
        } else {
            let new_row = TopKRow::new(row, batch_id, index);
            self.owned_bytes += new_row.owned_size();
            // put the new row into the heap
            self.inner.push(new_row);
            None
        }
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], resetting the inner heap
    pub fn emit(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.emit_with_state()?.0)
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], and a sorted vec of the
    /// current heap's contents
    fn emit_with_state(&mut self) -> Result<(Option<RecordBatch>, Vec<TopKRow>)> {
        // generate sorted rows
        let topk_rows = std::mem::take(&mut self.inner).into_sorted_vec();

        if self.store.is_empty() {
            return Ok((None, topk_rows));
        }

        // Collect the batches into a vec and store the "batch_id -> array_pos" mapping, to then
        // build the `indices` vec below. This is needed since the batch ids are not continuous.
        let mut record_batches = Vec::new();
        let mut batch_id_array_pos = HashMap::new();
        for (array_pos, (batch_id, batch)) in self.store.batches.iter().enumerate() {
            record_batches.push(&batch.batch);
            batch_id_array_pos.insert(*batch_id, array_pos);
        }

        let indices: Vec<_> = topk_rows
            .iter()
            .map(|k| (batch_id_array_pos[&k.batch_id], k.index))
            .collect();

        // At this point `indices` contains indexes within the
        // rows and `input_arrays` contains a reference to the
        // relevant RecordBatch for that index. `interleave_record_batch` pulls
        // them together into a single new batch
        let new_batch = interleave_record_batch(&record_batches, &indices)?;

        Ok((Some(new_batch), topk_rows))
    }

    /// Compact this heap, rewriting all stored batches into a single
    /// input batch
    pub fn maybe_compact(&mut self) -> Result<()> {
        // Don't compact if there's only one batch (compacting into itself is pointless)
        if self.store.len() <= 1 {
            return Ok(());
        }

        let total_rows = self.store.total_rows;
        let num_rows = self.inner.len();

        // Compact when current store memory exceeds 2x what the compacted
        // result would need. The multiplier avoids compacting when the
        // savings would be marginal.
        if total_rows <= num_rows * 2 {
            return Ok(());
        }

        // at first, compact the entire thing always into a new batch
        // (maybe we can get fancier in the future about ignoring
        // batches that have a high usage ratio already

        // Note: new batch is in the same order as inner
        let (new_batch, mut topk_rows) = self.emit_with_state()?;
        let Some(new_batch) = new_batch else {
            return Ok(());
        };

        // clear all old entries in store (this invalidates all
        // store_ids in `inner`)
        self.store.clear();

        let mut batch_entry = self.register_batch(new_batch);
        batch_entry.uses = num_rows;

        // rewrite all existing entries to use the new batch, and
        // remove old entries. The sortedness and their relative
        // position do not change
        for (i, topk_row) in topk_rows.iter_mut().enumerate() {
            topk_row.batch_id = batch_entry.id;
            topk_row.index = i;
        }
        self.insert_batch_entry(batch_entry);
        // restore the heap
        self.inner = BinaryHeap::from(topk_rows);

        Ok(())
    }

    /// return the size of memory used by this heap, in bytes
    fn size(&self) -> usize {
        size_of::<Self>()
            + (self.inner.capacity() * size_of::<TopKRow>())
            + self.store.size()
            + self.owned_bytes
    }
}

/// Represents one of the top K rows held in this heap. Orders
/// according to memcmp of row (e.g. the arrow Row format, but could
/// also be primitive values)
///
/// Reuses allocations to minimize runtime overhead of creating new Vecs
#[derive(Debug, PartialEq)]
struct TopKRow {
    /// the value of the sort key for this row. This contains the
    /// bytes that could be stored in `OwnedRow` but uses `Vec<u8>` to
    /// reuse allocations.
    row: Vec<u8>,
    /// the RecordBatch this row came from: an id into a [`RecordBatchStore`]
    batch_id: u32,
    /// the index in this record batch the row came from
    index: usize,
}

impl TopKRow {
    /// Create a new TopKRow with new allocation
    fn new(row: impl AsRef<[u8]>, batch_id: u32, index: usize) -> Self {
        Self {
            row: row.as_ref().to_vec(),
            batch_id,
            index,
        }
    }

    // Replace the existing row capacity with new values
    fn replace_with(&mut self, new_row: impl AsRef<[u8]>, batch_id: u32, index: usize) {
        self.row.clear();
        self.row.extend_from_slice(new_row.as_ref());

        self.batch_id = batch_id;
        self.index = index;
    }

    /// Returns the number of bytes owned by this row in the heap (not
    /// including itself)
    fn owned_size(&self) -> usize {
        self.row.capacity()
    }

    /// Returns a slice to the owned row value
    fn row(&self) -> &[u8] {
        self.row.as_slice()
    }
}

impl Eq for TopKRow {}

impl PartialOrd for TopKRow {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // TODO PartialOrd is not consistent with PartialEq; PartialOrd contract is violated
        Some(self.cmp(other))
    }
}

impl Ord for TopKRow {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

#[derive(Debug)]
struct RecordBatchEntry {
    id: u32,
    batch: RecordBatch,
    // for this batch, how many times has it been used
    uses: usize,
}

/// This structure tracks [`RecordBatch`] by an id so that:
///
/// 1. The baches can be tracked via an id that can be copied cheaply
/// 2. The total memory held by all batches is tracked
#[derive(Debug)]
struct RecordBatchStore {
    /// id generator
    next_id: u32,
    /// storage
    batches: HashMap<u32, RecordBatchEntry>,
    /// total size of all record batches tracked by this store
    batches_size: usize,
    /// row count of all the batches
    total_rows: usize,
}

impl RecordBatchStore {
    fn new() -> Self {
        Self {
            next_id: 0,
            batches: HashMap::new(),
            batches_size: 0,
            total_rows: 0,
        }
    }

    /// Register this batch with the store and assign an ID. No
    /// attempt is made to compare this batch to other batches
    pub fn register(&mut self, batch: RecordBatch) -> RecordBatchEntry {
        let id = self.next_id;
        self.next_id += 1;
        RecordBatchEntry { id, batch, uses: 0 }
    }

    /// Insert a record batch entry into this store, tracking its
    /// memory use, if it has any uses
    pub fn insert(&mut self, entry: RecordBatchEntry) {
        // uses of 0 means that none of the rows in the batch were stored in the topk
        if entry.uses > 0 {
            self.batches_size += get_record_batch_memory_size(&entry.batch);
            self.total_rows += entry.batch.num_rows();
            self.batches.insert(entry.id, entry);
        }
    }

    /// Clear all values in this store, invalidating all previous batch ids
    fn clear(&mut self) {
        self.batches.clear();
        self.batches_size = 0;
        self.total_rows = 0;
    }

    fn get(&self, id: u32) -> Option<&RecordBatchEntry> {
        self.batches.get(&id)
    }

    /// returns the total number of batches stored in this store
    fn len(&self) -> usize {
        self.batches.len()
    }

    /// returns true if the store has nothing stored
    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// remove a use from the specified batch id. If the use count
    /// reaches zero the batch entry is removed from the store
    ///
    /// panics if there were no remaining uses of id
    pub fn unuse(&mut self, id: u32) {
        let remove = if let Some(batch_entry) = self.batches.get_mut(&id) {
            batch_entry.uses = batch_entry.uses.checked_sub(1).expect("underflow");
            batch_entry.uses == 0
        } else {
            panic!("No entry for id {id}");
        };

        if remove {
            let old_entry = self.batches.remove(&id).unwrap();
            self.batches_size = self
                .batches_size
                .checked_sub(get_record_batch_memory_size(&old_entry.batch))
                .unwrap();

            self.total_rows = self
                .total_rows
                .checked_sub(old_entry.batch.num_rows())
                .unwrap();
        }
    }

    /// returns the size of memory used by this store, including all
    /// referenced `RecordBatch`es, in bytes
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + self.batches.capacity() * (size_of::<u32>() + size_of::<RecordBatchEntry>())
            + self.batches_size
    }
}

/// Top-K-per-partition operator state.
///
/// Sibling to [`TopK`]. Where `TopK` maintains a single global heap,
/// `PartitionedTopK` maintains one [`TopKHeap`] per distinct partition
/// key while sharing a single [`RowConverter`], [`MemoryReservation`],
/// scratch [`Rows`] buffer, and [`TopKMetrics`] across all partitions.
///
/// This sharing is the point of the type: with N distinct partition
/// keys, a naive `HashMap<_, TopK>` pays N × constant overhead for
/// `RowConverter::new`, `MemoryConsumer::register`, and metric
/// counter setup. `PartitionedTopK` pays it once.
pub(crate) struct PartitionedTopK {
    schema: SchemaRef,
    metrics: TopKMetrics,
    reservation: MemoryReservation,
    /// ORDER BY expressions (excludes PARTITION BY).
    expr: LexOrdering,
    /// Encoder for ORDER BY columns. Reused across partitions.
    row_converter: RowConverter,
    /// Scratch row buffer reused across `insert_batch` calls.
    scratch_rows: Rows,
    /// PARTITION BY expressions.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Encoder for the partition key.
    partition_converter: RowConverter,
    /// One heap per distinct partition key seen so far.
    heaps: HashMap<OwnedRow, TopKHeap>,
    k: usize,
    batch_size: usize,
}

impl PartitionedTopK {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
        partition_sort_fields: Vec<SortField>,
        order_expr: LexOrdering,
        k: usize,
        batch_size: usize,
        runtime: &Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        assert!(k > 0, "PartitionedTopK requires k > 0");
        let reservation = MemoryConsumer::new(format!("PartitionedTopK[{partition_id}]"))
            .register(&runtime.memory_pool);

        let order_sort_fields = build_sort_fields(&order_expr, &schema)?;
        let row_converter = RowConverter::new(order_sort_fields)?;
        let scratch_rows =
            row_converter.empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        let partition_converter = RowConverter::new(partition_sort_fields)?;

        Ok(Self {
            schema,
            metrics: TopKMetrics::new(metrics, partition_id),
            reservation,
            expr: order_expr,
            row_converter,
            scratch_rows,
            partition_exprs,
            partition_converter,
            heaps: HashMap::new(),
            k,
            batch_size,
        })
    }

    /// Demultiplex `batch` rows by partition key, encode the ORDER BY
    /// columns once for the whole batch, and feed each partition's
    /// rows into its dedicated [`TopKHeap`].
    pub(crate) fn insert_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // 1. Evaluate + encode partition columns.
        let pk_arrays: Vec<ArrayRef> = self
            .partition_exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        let pk_rows = self.partition_converter.convert_columns(&pk_arrays)?;

        // 2. Demultiplex row indices by partition key (per-batch).
        let mut groups: HashMap<OwnedRow, Vec<u32>> = HashMap::new();
        for i in 0..num_rows {
            groups
                .entry(pk_rows.row(i).owned())
                .or_default()
                .push(i as u32);
        }

        // 3. Evaluate ORDER BY columns on the full batch and encode ONCE.
        let ob_arrays: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|e| e.expr.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.scratch_rows.clear();
        self.row_converter
            .append(&mut self.scratch_rows, &ob_arrays)?;

        // 4. Per-partition: take the sub-batch, walk indices, dispatch
        //    qualifying rows into the partition's heap.
        let k = self.k;
        let mut replacements: usize = 0;
        for (pk, indices) in groups {
            let heap = self.heaps.entry(pk).or_insert_with(|| TopKHeap::new(k));

            // Once a heap is full, most rows at high partition cardinality
            // are rejected. Skip the gather + batch registration entirely
            // when nothing in this partition group can improve the heap.
            let any_qualify = indices.iter().any(|&orig_idx| {
                let bytes = self.scratch_rows.row(orig_idx as usize);
                match heap.max() {
                    Some(max_row) => bytes.as_ref() < max_row.row(),
                    None => true,
                }
            });
            if !any_qualify {
                continue;
            }

            let indices_arr = UInt32Array::from(indices);
            let sub_batch = take_record_batch(batch, &indices_arr)?;
            let mut entry = heap.register_batch(sub_batch);

            for (sub_idx, &orig_idx) in indices_arr.values().iter().enumerate() {
                let row = self.scratch_rows.row(orig_idx as usize);
                match heap.max() {
                    Some(max_row) if row.as_ref() >= max_row.row() => {}
                    None | Some(_) => {
                        heap.add(&mut entry, row, sub_idx);
                        replacements += 1;
                    }
                }
            }

            heap.insert_batch_entry(entry);
            heap.maybe_compact()?;
        }

        if replacements > 0 {
            self.metrics.row_replacements.add(replacements);
        }
        self.reservation.try_resize(self.size())?;
        Ok(())
    }

    /// Drain all heaps in partition-key order and return the rows as
    /// a stream of coalesced `RecordBatch`es ordered by
    /// `(partition_keys, order_keys)`.
    pub(crate) fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation: _,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            partition_exprs: _,
            partition_converter: _,
            mut heaps,
            k: _,
            batch_size,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer();

        let mut sorted_pks: Vec<OwnedRow> = heaps.keys().cloned().collect();
        sorted_pks.sort();

        let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);

        for pk in sorted_pks {
            let mut heap = heaps.remove(&pk).expect("key from heaps.keys()");
            if let Some(batch) = heap.emit()? {
                (&batch).record_output(&metrics.baseline);
                coalescer.push_batch(batch)?;
            }
        }
        coalescer.finish_buffered_batch()?;

        let mut out: Vec<Result<RecordBatch>> = Vec::new();
        while let Some(b) = coalescer.next_completed_batch() {
            out.push(Ok(b));
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(out),
        )))
    }

    /// Total memory currently held by this operator, including all
    /// per-partition heaps.
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.row_converter.size()
            + self.partition_converter.size()
            + self.scratch_rows.size()
            + self.heaps.values().map(|h| h.size()).sum::<usize>()
            + self.heaps.capacity() * (size_of::<OwnedRow>() + size_of::<TopKHeap>())
    }
}

/// A run of rows from a single source [`RecordBatch`] that tied at the
/// boundary when inserted. Stored as `(batch, indices)` and materialized
/// at emit time via [`take_record_batch`].
#[derive(Debug)]
struct TieEntry {
    batch: RecordBatch,
    /// Indices into `batch` of the rows tied at the (then-current)
    /// boundary. Always non-empty by construction.
    row_indices: Vec<u32>,
    /// `get_record_batch_memory_size(&batch)` captured at push time so
    /// `RankPartitionState::size()` doesn't recurse through `batch`'s
    /// columns on every `try_resize` call.
    batch_bytes: usize,
}

/// Per-partition state for `RANK()` semantics.
///
/// Composes [`TopKHeap`] as the K-bounded core plus a sibling
/// `Vec<TieEntry>` for boundary-tied rows. `RANK ≤ K` keeps the K
/// best rows by ORDER BY plus every row tied at the K-th-best
/// ORDER BY value — the boundary. So the total retained rows can
/// exceed K when ties straddle the boundary.
struct RankPartitionState {
    heap: TopKHeap,
    ties: Vec<TieEntry>,
}

impl RankPartitionState {
    fn size(&self) -> usize {
        let ties_buffer = self.ties.capacity() * size_of::<TieEntry>();
        let ties_contents: usize = self
            .ties
            .iter()
            .map(|t| t.row_indices.capacity() * size_of::<u32>() + t.batch_bytes)
            .sum();
        self.heap.size() + ties_buffer + ties_contents
    }
}

/// Sibling to [`PartitionedTopK`] implementing `RANK()` semantics.
///
/// Per partition, retains the K-best rows plus every row tied at the
/// K-th-best ORDER BY value (so `WHERE rk <= K` may keep more than K
/// rows when ties straddle the boundary). Like [`PartitionedTopK`],
/// the [`RowConverter`], [`MemoryReservation`], scratch [`Rows`]
/// buffer, and [`TopKMetrics`] are shared across all partitions for
/// this operator instance.
///
/// # Algorithm (per row)
///
/// For each incoming row, compare its encoded ORDER BY bytes against
/// `heap.max()` — the K-th-best row, which is by definition the
/// admission boundary. `heap.max()` is `None` until the heap fills
/// to K rows:
///
/// - heap not full (`max() == None`) → forward to the heap
/// - row's ob `==` max → push to ties (no heap call)
/// - row's ob `>` max → drop
/// - row's ob `<` max → forward to heap; on eviction, compare the
///   new `heap.max()` to the evicted row's bytes: if equal, push
///   evicted to ties (still tied at the new boundary's rank); else
///   clear ties (boundary moved up, old ties no longer satisfy
///   `rk ≤ K`)
pub(crate) struct PartitionedTopKRank {
    schema: SchemaRef,
    metrics: TopKMetrics,
    reservation: MemoryReservation,
    /// ORDER BY expressions (excludes PARTITION BY).
    expr: LexOrdering,
    /// Encoder for ORDER BY columns. Reused across partitions.
    row_converter: RowConverter,
    /// Scratch row buffer reused across `insert_batch` calls.
    scratch_rows: Rows,
    /// PARTITION BY expressions.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Encoder for the partition key.
    partition_converter: RowConverter,
    /// Scratch row buffer for partition-key encoding. Reused across
    /// `insert_batch` calls (cleared + appended each batch) so we
    /// avoid allocating a fresh `Rows` buffer every batch.
    partition_scratch_rows: Rows,
    /// One rank state per distinct partition key seen so far.
    states: HashMap<OwnedRow, RankPartitionState>,
    k: usize,
    batch_size: usize,
}

impl PartitionedTopKRank {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
        partition_sort_fields: Vec<SortField>,
        order_expr: LexOrdering,
        k: usize,
        batch_size: usize,
        runtime: &Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        assert!(k > 0, "PartitionedTopKRank requires k > 0");
        let reservation =
            MemoryConsumer::new(format!("PartitionedTopKRank[{partition_id}]"))
                .register(&runtime.memory_pool);

        let order_sort_fields = build_sort_fields(&order_expr, &schema)?;
        let row_converter = RowConverter::new(order_sort_fields)?;
        let scratch_rows =
            row_converter.empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        let partition_converter = RowConverter::new(partition_sort_fields)?;
        let partition_scratch_rows = partition_converter
            .empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        Ok(Self {
            schema,
            metrics: TopKMetrics::new(metrics, partition_id),
            reservation,
            expr: order_expr,
            row_converter,
            scratch_rows,
            partition_exprs,
            partition_converter,
            partition_scratch_rows,
            states: HashMap::new(),
            k,
            batch_size,
        })
    }

    /// Demultiplex `batch` rows by partition key, encode the ORDER BY
    /// columns once for the whole batch, and feed each partition's
    /// rows through the rank classifier into its dedicated heap and
    /// ties Vec.
    pub(crate) fn insert_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Captured once so the per-tie push from this batch can reuse
        // it (computing `get_record_batch_memory_size` is O(cols ×
        // buffer walk) and we'd otherwise pay it per push and again
        // per `try_resize` call).
        let input_batch_bytes = get_record_batch_memory_size(batch);

        // 1. Evaluate + encode partition columns into the reusable
        //    scratch (cleared then appended).
        let pk_arrays: Vec<ArrayRef> = self
            .partition_exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.partition_scratch_rows.clear();
        self.partition_converter
            .append(&mut self.partition_scratch_rows, &pk_arrays)?;
        let pk_rows = &self.partition_scratch_rows;

        // 2. Demultiplex row indices by partition key (per-batch).
        let mut groups: HashMap<OwnedRow, Vec<u32>> = HashMap::new();
        for i in 0..num_rows {
            groups
                .entry(pk_rows.row(i).owned())
                .or_default()
                .push(i as u32);
        }

        // 3. Evaluate ORDER BY columns on the full batch and encode ONCE.
        let ob_arrays: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|e| e.expr.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.scratch_rows.clear();
        self.row_converter
            .append(&mut self.scratch_rows, &ob_arrays)?;

        // 4. Per-partition: classify each row and dispatch.
        let k = self.k;
        let mut replacements: usize = 0;

        for (pk, indices) in groups {
            let state = self.states.entry(pk).or_insert_with(|| RankPartitionState {
                heap: TopKHeap::new(k),
                ties: Vec::new(),
            });

            // Equal indices for THIS batch only. Coalesced into a single
            // tie entry at the end of the partition's loop. Discarded if
            // the boundary moves up mid-loop (those rows were tied to the
            // old boundary, which is now strictly worse than the new K-th).
            let mut equal_indices: Vec<u32> = Vec::new();
            // Lazy-registered: only attached if at least one row reaches
            // the heap from this batch in this partition.
            let mut entry: Option<RecordBatchEntry> = None;

            for &orig_idx in &indices {
                let row = self.scratch_rows.row(orig_idx as usize);

                // Classify against the current K-th-best (the heap top).
                // `heap.max()` returns `None` while the heap is filling,
                // so unclassified rows fall through to the heap path.
                let classification = state
                    .heap
                    .max()
                    .map(|max_row| row.as_ref().cmp(max_row.row()));

                match classification {
                    Some(Ordering::Equal) => {
                        equal_indices.push(orig_idx);
                        continue;
                    }
                    Some(Ordering::Greater) => continue,
                    Some(Ordering::Less) | None => {
                        // Heap path: heap not yet full, or row strictly
                        // better than the current boundary.
                        let entry_ref = entry.get_or_insert_with(|| {
                            state.heap.register_batch(batch.clone())
                        });
                        if let Some(EvictedRow {
                            batch: evicted_batch,
                            index: evicted_index,
                            row_bytes: evicted_bytes,
                        }) = state.heap.add(entry_ref, row, orig_idx as usize)
                        {
                            // Compare the new boundary (post-eviction heap
                            // top) against the evicted row's bytes — both
                            // already in encoded form, no clones needed.
                            let boundary_changed = state
                                .heap
                                .max()
                                .expect("heap was full to evict; must still be full")
                                .row()
                                != evicted_bytes.as_slice();
                            if boundary_changed {
                                // Boundary moved up — prior ties (across
                                // all prior batches) and equal_indices
                                // accumulated earlier in THIS batch were
                                // tied to the old boundary, now strictly
                                // worse than the new K-th-best. Discard.
                                state.ties.clear();
                                equal_indices.clear();
                            } else {
                                // Boundary unchanged — evicted row is tied
                                // at the (unchanged) boundary; push as a
                                // single-row entry.
                                let batch_bytes =
                                    get_record_batch_memory_size(&evicted_batch);
                                state.ties.push(TieEntry {
                                    batch: evicted_batch,
                                    row_indices: vec![evicted_index as u32],
                                    batch_bytes,
                                });
                            }
                        }
                        replacements += 1;
                    }
                }
            }

            if let Some(e) = entry {
                state.heap.insert_batch_entry(e);
                state.heap.maybe_compact()?;
            }

            // Commit this batch's ties as a single entry.
            if !equal_indices.is_empty() {
                state.ties.push(TieEntry {
                    batch: batch.clone(),
                    row_indices: equal_indices,
                    batch_bytes: input_batch_bytes,
                });
            }
        }

        if replacements > 0 {
            self.metrics.row_replacements.add(replacements);
        }
        self.reservation.try_resize(self.size())?;
        Ok(())
    }

    /// Drain all heaps and ties in partition-key order and return the
    /// rows as a stream of coalesced [`RecordBatch`]es ordered by
    /// `(partition_keys, order_keys)`. Within a partition, heap rows
    /// come first (sorted by ob), then tie rows (all sharing the
    /// boundary ob).
    pub(crate) fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation: _,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            partition_exprs: _,
            partition_converter: _,
            partition_scratch_rows: _,
            mut states,
            k: _,
            batch_size,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer();

        let mut sorted_pks: Vec<OwnedRow> = states.keys().cloned().collect();
        sorted_pks.sort();

        let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);

        for pk in sorted_pks {
            let RankPartitionState { mut heap, ties, .. } =
                states.remove(&pk).expect("key from states.keys()");
            if let Some(batch) = heap.emit()? {
                (&batch).record_output(&metrics.baseline);
                coalescer.push_batch(batch)?;
            }
            for tie in ties {
                let indices = UInt32Array::from(tie.row_indices);
                let tie_batch = take_record_batch(&tie.batch, &indices)?;
                (&tie_batch).record_output(&metrics.baseline);
                coalescer.push_batch(tie_batch)?;
            }
        }
        coalescer.finish_buffered_batch()?;

        let mut out: Vec<Result<RecordBatch>> = Vec::new();
        while let Some(b) = coalescer.next_completed_batch() {
            out.push(Ok(b));
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(out),
        )))
    }

    /// Total memory currently held, including all per-partition states.
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.row_converter.size()
            + self.partition_converter.size()
            + self.scratch_rows.size()
            + self.partition_scratch_rows.size()
            + self.states.values().map(|s| s.size()).sum::<usize>()
            + self.states.capacity()
                * (size_of::<OwnedRow>() + size_of::<RankPartitionState>())
    }
}

/// Per-partition state for `DENSE_RANK()` semantics.
///
/// A `HashMap<Vec<u8>, Vec<TieEntry>>` keyed by the row-encoded ORDER BY
/// bytes, capped at `k` distinct keys. Each key's `Vec<TieEntry>` holds
/// every row seen at that ob value, one entry per contributing source
/// `RecordBatch`. `TieEntry` is reused verbatim from [`RankPartitionState`].
struct DenseRankPartitionState {
    groups: HashMap<Vec<u8>, Vec<TieEntry>>,
}

impl DenseRankPartitionState {
    fn size(&self) -> usize {
        let table_overhead =
            self.groups.capacity() * (size_of::<Vec<u8>>() + size_of::<Vec<TieEntry>>());
        let contents: usize = self
            .groups
            .iter()
            .map(|(key, entries)| {
                key.capacity()
                    + entries.capacity() * size_of::<TieEntry>()
                    + entries
                        .iter()
                        .map(|e| {
                            e.row_indices.capacity() * size_of::<u32>() + e.batch_bytes
                        })
                        .sum::<usize>()
            })
            .sum();
        table_overhead + contents
    }
}

/// Sibling to [`PartitionedTopK`] / [`PartitionedTopKRank`] implementing
/// `DENSE_RANK()` semantics.
///
/// Per partition, retains every row whose ORDER BY value is among the K
/// distinct-smallest ob values seen for that partition. The total row
/// count kept per partition is unbounded in `rows_per_distinct_value`
/// (unlike `RANK`, which is bounded above by K + boundary ties).
///
/// Like [`PartitionedTopK`], the [`RowConverter`], [`MemoryReservation`],
/// scratch [`Rows`] buffer, and [`TopKMetrics`] are shared across all
/// partitions for this operator instance.
///
/// # Algorithm (per batch)
///
/// Evaluate + encode partition-by and order-by columns once, then group
/// the batch's row indices by partition key. For each partition, bucket
/// that partition's rows by distinct ob value (a within-call
/// accumulation), then merge each bucket into the partition state. Every
/// bucket is built from the current batch's rows, so each `TieEntry` is
/// pinned to the batch its `row_indices` point into.
///
/// For each partition, for each distinct `ob_key` run in this batch:
/// - `ob_key` already in `state.groups` → push this batch's run as a
///   new `TieEntry` (one entry per contributing batch).
/// - `ob_key` new, `state.groups.len() < k` → insert the run as a new
///   group.
/// - `ob_key` new, `state.groups.len() == k` → find the current max via
///   an O(K) scan of `state.groups.keys()`:
///   - `ob_key < max` → remove the max key (evict the entire max-key
///     group — up to many rows) and insert the run. The evicted group's
///     row count is added to the `row_replacements` metric.
///   - `ob_key >= max` → drop the whole run; no map mutation.
pub(crate) struct PartitionedTopKDenseRank {
    schema: SchemaRef,
    metrics: TopKMetrics,
    reservation: MemoryReservation,
    /// ORDER BY expressions (excludes PARTITION BY).
    expr: LexOrdering,
    /// Encoder for ORDER BY columns. Reused across partitions.
    row_converter: RowConverter,
    /// Scratch row buffer reused across `insert_batch` calls.
    scratch_rows: Rows,
    /// PARTITION BY expressions.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Encoder for the partition key.
    partition_converter: RowConverter,
    /// Scratch row buffer for partition-key encoding. Reused across
    /// `insert_batch` calls (cleared + appended each batch).
    partition_scratch_rows: Rows,
    /// One state per distinct partition key seen so far. Keyed by the
    /// row-encoded PARTITION BY bytes (byte-comparable encoding, so the
    /// `Vec<u8>` hashes, compares, and sorts identically to an
    /// `OwnedRow`) which lets `insert_batch` look partitions up with
    /// `entry_ref` — allocating a key only on first sight of a partition
    /// rather than once per row.
    states: HashMap<Vec<u8>, DenseRankPartitionState>,
    /// Scratch map reused across `insert_batch` calls to group a batch's
    /// row indices by partition key. Drained (not reallocated) each batch
    /// so its backing table is allocated once, not per batch.
    partition_groups: HashMap<Vec<u8>, Vec<u32>>,
    /// Scratch map reused across partitions within a batch to bucket a
    /// partition's rows by distinct ORDER BY value. Drained (not
    /// reallocated) per partition so its backing table is allocated once,
    /// not once per distinct partition key.
    ob_runs: HashMap<Vec<u8>, Vec<u32>>,
    k: usize,
    batch_size: usize,
}

impl PartitionedTopKDenseRank {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
        partition_sort_fields: Vec<SortField>,
        order_expr: LexOrdering,
        k: usize,
        batch_size: usize,
        runtime: &Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        assert!(k > 0, "PartitionedTopKDenseRank requires k > 0");
        let reservation =
            MemoryConsumer::new(format!("PartitionedTopKDenseRank[{partition_id}]"))
                .register(&runtime.memory_pool);

        let order_sort_fields = build_sort_fields(&order_expr, &schema)?;
        let row_converter = RowConverter::new(order_sort_fields)?;
        let scratch_rows =
            row_converter.empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        let partition_converter = RowConverter::new(partition_sort_fields)?;
        let partition_scratch_rows = partition_converter
            .empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        Ok(Self {
            schema,
            metrics: TopKMetrics::new(metrics, partition_id),
            reservation,
            expr: order_expr,
            row_converter,
            scratch_rows,
            partition_exprs,
            partition_converter,
            partition_scratch_rows,
            states: HashMap::new(),
            partition_groups: HashMap::new(),
            ob_runs: HashMap::new(),
            k,
            batch_size,
        })
    }

    /// Encode PARTITION BY and ORDER BY columns once, demultiplex the
    /// batch's rows by partition key, then per partition bucket the rows
    /// by distinct ob value and merge each bucket into the partition
    /// state as one [`TieEntry`].
    pub(crate) fn insert_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Captured once so every `TieEntry` push from this batch can
        // reuse it (avoids `get_record_batch_memory_size` per push).
        let input_batch_bytes = get_record_batch_memory_size(batch);

        // 1. Encode partition columns.
        let pk_arrays: Vec<ArrayRef> = self
            .partition_exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.partition_scratch_rows.clear();
        self.partition_converter
            .append(&mut self.partition_scratch_rows, &pk_arrays)?;

        // 2. Group this batch's row indices by partition key.
        //    `partition_groups` is a reused scratch map: taken out here
        //    and drained below, so its backing table is allocated once
        //    for the operator, not once per batch. `entry_ref` owns the
        //    key only on Vacant, so it allocates one `Vec<u8>` per
        //    distinct partition rather than one per row.
        let mut groups = std::mem::take(&mut self.partition_groups);
        groups.clear();
        {
            let pk_rows = &self.partition_scratch_rows;
            for i in 0..num_rows {
                groups
                    .entry_ref(pk_rows.row(i).as_ref())
                    .or_default()
                    .push(i as u32);
            }
        }

        // 3. Evaluate ORDER BY columns and encode ONCE.
        let ob_arrays: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|e| e.expr.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.scratch_rows.clear();
        self.row_converter
            .append(&mut self.scratch_rows, &ob_arrays)?;

        let k = self.k;
        let mut replacements: usize = 0;

        // 4. Per-partition: bucket this batch's rows by distinct ob value
        //    (within-call accumulation), then merge each bucket into the
        //    partition state as a single `TieEntry`.
        for (pk, indices) in groups.drain() {
            let state =
                self.states
                    .entry(pk)
                    .or_insert_with(|| DenseRankPartitionState {
                        groups: HashMap::new(),
                    });

            // Bucket by ob key. `ob_runs` is a reused scratch map (taken
            // out and drained below) so its backing table is allocated
            // once, not once per distinct partition key. `entry_ref` owns
            // the key only on Vacant, so repeated rows of the same ob
            // value don't re-allocate.
            let mut runs = std::mem::take(&mut self.ob_runs);
            runs.clear();
            for &orig_idx in &indices {
                let ob_row = self.scratch_rows.row(orig_idx as usize);
                runs.entry_ref(ob_row.as_ref()).or_default().push(orig_idx);
            }

            for (ob_key, run_indices) in runs.drain() {
                // Case A: ob already tracked — push this batch's run as a
                // new `TieEntry` (one entry per contributing batch, exactly
                // like RANK pushing one tie entry per batch).
                if let Some(entries) = state.groups.get_mut(&ob_key) {
                    entries.push(TieEntry {
                        batch: batch.clone(),
                        row_indices: run_indices,
                        batch_bytes: input_batch_bytes,
                    });
                    continue;
                }

                // Case B: new ob, room available.
                if state.groups.len() < k {
                    state.groups.insert(
                        ob_key,
                        vec![TieEntry {
                            batch: batch.clone(),
                            row_indices: run_indices,
                            batch_bytes: input_batch_bytes,
                        }],
                    );
                    continue;
                }

                // Case C: new ob, at K distinct keys. Find the current
                // max (K-th distinct-best) via an O(K) scan — cold path.
                // Scoped so the immutable borrow ends before mutation.
                let evict_key: Option<Vec<u8>> = {
                    let max_key = state
                        .groups
                        .keys()
                        .map(|key| key.as_slice())
                        .max()
                        .expect("state.groups has k >= 1 keys");
                    (ob_key.as_slice() < max_key).then(|| max_key.to_vec())
                };
                if let Some(evicted_key) = evict_key {
                    let evicted =
                        state.groups.remove(&evicted_key).expect("max key present");
                    replacements +=
                        evicted.iter().map(|e| e.row_indices.len()).sum::<usize>();
                    state.groups.insert(
                        ob_key,
                        vec![TieEntry {
                            batch: batch.clone(),
                            row_indices: run_indices,
                            batch_bytes: input_batch_bytes,
                        }],
                    );
                }
                // else: ob >= max — drop the whole run.
            }

            // Return the drained scratch map (capacity retained) for the
            // next partition to reuse.
            self.ob_runs = runs;
        }

        // Return the drained scratch map (capacity retained) for the next
        // batch to reuse.
        self.partition_groups = groups;

        if replacements > 0 {
            self.metrics.row_replacements.add(replacements);
        }
        self.reservation.try_resize(self.size())?;
        Ok(())
    }

    /// Drain all per-partition maps in partition-key order and return
    /// the rows as a stream of coalesced [`RecordBatch`]es ordered by
    /// `(partition_keys, order_keys)`. Within a partition the distinct
    /// ob keys are sorted (byte-comparable encoding == sort order) so
    /// emitted rows are in ob-sorted order.
    pub(crate) fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation: _,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            partition_exprs: _,
            partition_converter: _,
            partition_scratch_rows: _,
            mut states,
            partition_groups: _,
            ob_runs: _,
            k: _,
            batch_size,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer();

        let mut sorted_pks: Vec<Vec<u8>> = states.keys().cloned().collect();
        sorted_pks.sort();

        let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);

        for pk in sorted_pks {
            let DenseRankPartitionState { groups } =
                states.remove(&pk).expect("key from states.keys()");
            // HashMap is unordered — sort the <= K distinct ob keys so
            // rows emit ascending (byte-comparable encoding == sort order).
            let mut sorted_obs: Vec<(Vec<u8>, Vec<TieEntry>)> =
                groups.into_iter().collect();
            sorted_obs.sort_by(|a, b| a.0.cmp(&b.0));
            for (_ob, entries) in sorted_obs {
                for entry in entries {
                    let indices = UInt32Array::from(entry.row_indices);
                    let sub = take_record_batch(&entry.batch, &indices)?;
                    (&sub).record_output(&metrics.baseline);
                    coalescer.push_batch(sub)?;
                }
            }
        }
        coalescer.finish_buffered_batch()?;

        let mut out: Vec<Result<RecordBatch>> = Vec::new();
        while let Some(b) = coalescer.next_completed_batch() {
            out.push(Ok(b));
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(out),
        )))
    }

    /// Total memory currently held, including all per-partition states.
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.row_converter.size()
            + self.partition_converter.size()
            + self.scratch_rows.size()
            + self.partition_scratch_rows.size()
            + self.states.values().map(|s| s.size()).sum::<usize>()
            + self.states.capacity()
                * (size_of::<Vec<u8>>() + size_of::<DenseRankPartitionState>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SortOptions;
    use datafusion_common::assert_batches_eq;
    use datafusion_physical_expr::{DynamicFilterTracking, expressions::col};
    use futures::TryStreamExt;

    /// This test ensures the size calculation is correct for RecordBatches with multiple columns.
    #[test]
    fn test_record_batch_store_size() {
        // given
        let schema = Arc::new(Schema::new(vec![
            Field::new("ints", DataType::Int32, true),
            Field::new("float64", DataType::Float64, false),
        ]));
        let mut record_batch_store = RecordBatchStore::new();
        let int_array =
            Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]); // 5 * 4 = 20
        let float64_array = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]); // 5 * 8 = 40

        let record_batch_entry = RecordBatchEntry {
            id: 0,
            batch: RecordBatch::try_new(
                schema,
                vec![Arc::new(int_array), Arc::new(float64_array)],
            )
            .unwrap(),
            uses: 1,
        };

        // when insert record batch entry
        record_batch_store.insert(record_batch_entry);
        assert_eq!(record_batch_store.batches_size, 60);

        // when unuse record batch entry
        record_batch_store.unuse(0);
        assert_eq!(record_batch_store.batches_size, 0);
    }

    fn make_ab_schema() -> SchemaRef {
        make_ab_schema_with_nullable_a(false)
    }

    fn make_ab_schema_with_nullable_a(a_nullable: bool) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, a_nullable),
            Field::new("b", DataType::Float64, false),
        ]))
    }

    // Local TopK tests use one emitter; shared-filter cases pass the partition count explicitly.
    fn make_topk_filter() -> Arc<RwLock<TopKDynamicFilters>> {
        make_shared_topk_filter(1)
    }

    fn make_shared_topk_filter(
        topk_emitter_count: usize,
    ) -> Arc<RwLock<TopKDynamicFilters>> {
        Arc::new(RwLock::new(
            TopKDynamicFilters::new_with_topk_emitter_count(
                Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true))),
                topk_emitter_count,
            ),
        ))
    }

    /// Builds the `(a, b)` fixture used by prefix-completion tests:
    /// full sort `(a, b)`, input prefix `[a]`, `k = 3`, and batch size 2.
    fn make_ab_topk(
        schema: SchemaRef,
        filter: Arc<RwLock<TopKDynamicFilters>>,
    ) -> Result<TopK> {
        make_ab_topk_with_options(0, schema, filter, SortOptions::default())
    }

    fn make_ab_topk_with_options(
        partition_id: usize,
        schema: SchemaRef,
        filter: Arc<RwLock<TopKDynamicFilters>>,
        a_options: SortOptions,
    ) -> Result<TopK> {
        let sort_expr_a = PhysicalSortExpr {
            expr: col("a", schema.as_ref())?,
            options: a_options,
        };
        let sort_expr_b = PhysicalSortExpr {
            expr: col("b", schema.as_ref())?,
            options: SortOptions::default(),
        };

        TopK::try_new(
            partition_id,
            schema,
            vec![sort_expr_a.clone()],
            LexOrdering::from([sort_expr_a, sort_expr_b]),
            3,
            2,
            Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
            filter,
        )
    }

    fn make_ab_batch(
        schema: SchemaRef,
        a: &[Option<i32>],
        b: &[f64],
    ) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a.to_vec())) as ArrayRef,
                Arc::new(Float64Array::from(b.to_vec())) as ArrayRef,
            ],
        )?)
    }

    type AbRow = (Option<i32>, f64);

    fn make_ab_rows_batch(schema: SchemaRef, rows: &[AbRow]) -> Result<RecordBatch> {
        let (a, b): (Vec<_>, Vec<_>) = rows.iter().copied().unzip();
        make_ab_batch(schema, &a, &b)
    }

    #[tokio::test]
    async fn test_early_completion_marks_finished_with_prefix() -> Result<()> {
        let schema = make_ab_schema();
        let mut topk = make_ab_topk(Arc::clone(&schema), make_topk_filter())?;

        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(1), Some(1), Some(2)],
            &[20.0, 15.0, 30.0],
        )?)?;
        assert!(!topk.finished);

        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(2), Some(3)],
            &[10.0, 20.0],
        )?)?;
        assert!(topk.finished);

        let results: Vec<_> = topk.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+---+------+",
                "| a | b    |",
                "+---+------+",
                "| 1 | 15.0 |",
                "| 1 | 20.0 |",
                "| 2 | 10.0 |",
                "+---+------+",
            ],
            &results
        );

        Ok(())
    }

    /// Regression test for #22849: a batch whose rows are entirely rejected by the
    /// heap's dynamic filter must still trigger `attempt_early_completion` when its
    /// last row's prefix is worse than the heap's worst.
    #[tokio::test]
    async fn test_early_completion_fires_when_filter_rejects_entire_batch() -> Result<()>
    {
        let schema = make_ab_schema();
        let mut topk = make_ab_topk(Arc::clone(&schema), make_topk_filter())?;

        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(1), Some(1), Some(2)],
            &[20.0, 15.0, 30.0],
        )?)?;
        assert!(!topk.finished);

        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(3), Some(3)],
            &[10.0, 20.0],
        )?)?;
        assert!(topk.finished);

        let results: Vec<_> = topk.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+---+------+",
                "| a | b    |",
                "+---+------+",
                "| 1 | 15.0 |",
                "| 1 | 20.0 |",
                "| 2 | 30.0 |",
                "+---+------+",
            ],
            &results
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_early_completion_fires_when_batch_makes_no_replacements() -> Result<()>
    {
        let schema = make_ab_schema();
        let filter = make_topk_filter();
        let mut topk = make_ab_topk(Arc::clone(&schema), Arc::clone(&filter))?;

        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(1), Some(1), Some(2)],
            &[20.0, 15.0, 30.0],
        )?)?;
        assert!(!topk.finished);

        let replacements_before = topk.metrics.row_replacements.value();

        // Keep the dynamic filter permissive so the second batch reaches
        // `find_new_topk_items`; all of its rows are worse than the heap max,
        // so this specifically exercises the `replacements == 0` path.
        filter.read().expr().update(lit(true))?;
        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(3), Some(3)],
            &[10.0, 20.0],
        )?)?;
        assert_eq!(topk.metrics.row_replacements.value(), replacements_before);
        assert!(topk.finished);

        let results: Vec<_> = topk.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+---+------+",
                "| a | b    |",
                "+---+------+",
                "| 1 | 15.0 |",
                "| 1 | 20.0 |",
                "| 2 | 30.0 |",
                "+---+------+",
            ],
            &results
        );

        Ok(())
    }

    struct SharedPrefixCase {
        name: &'static str,
        a_nullable: bool,
        a_options: SortOptions,
        threshold_source_rows: &'static [AbRow],
        lagging_partition_rows: &'static [AbRow],
        expected_finished: bool,
    }

    fn assert_shared_prefix_case(case: SharedPrefixCase) -> Result<()> {
        let schema = make_ab_schema_with_nullable_a(case.a_nullable);
        let filter = make_shared_topk_filter(2);

        let mut threshold_source = make_ab_topk_with_options(
            0,
            Arc::clone(&schema),
            Arc::clone(&filter),
            case.a_options,
        )?;
        threshold_source.insert_batch(make_ab_rows_batch(
            Arc::clone(&schema),
            case.threshold_source_rows,
        )?)?;
        assert!(
            filter
                .read()
                .shared_threshold
                .as_ref()
                .and_then(TopKThreshold::common_prefix_row)
                .is_some(),
            "{}: threshold-source partition should establish the shared prefix threshold",
            case.name
        );

        let mut lagging_partition = make_ab_topk_with_options(
            1,
            Arc::clone(&schema),
            Arc::clone(&filter),
            case.a_options,
        )?;
        lagging_partition
            .insert_batch(make_ab_rows_batch(schema, case.lagging_partition_rows)?)?;

        assert!(
            lagging_partition.heap.inner.is_empty(),
            "{}: lagging partition's local heap should remain empty",
            case.name
        );
        assert_eq!(
            lagging_partition.finished, case.expected_finished,
            "{}",
            case.name
        );

        Ok(())
    }

    #[test]
    fn test_shared_filter_can_finish_partition_before_local_heap_is_full() -> Result<()> {
        assert_shared_prefix_case(SharedPrefixCase {
            name: "shared threshold should finish lagging partition",
            a_nullable: false,
            a_options: SortOptions::default(),
            threshold_source_rows: &[(Some(1), 20.0), (Some(1), 15.0), (Some(2), 30.0)],
            lagging_partition_rows: &[(Some(3), 10.0), (Some(3), 20.0)],
            expected_finished: true,
        })
    }

    #[test]
    fn test_shared_prefix_threshold_boundary_cases() -> Result<()> {
        for case in [
            SharedPrefixCase {
                name: "equal prefix cannot prove completion",
                a_nullable: false,
                a_options: SortOptions::default(),
                threshold_source_rows: &[
                    (Some(1), 20.0),
                    (Some(1), 15.0),
                    (Some(2), 30.0),
                ],
                lagging_partition_rows: &[(Some(2), 40.0), (Some(2), 50.0)],
                expected_finished: false,
            },
            SharedPrefixCase {
                name: "descending prefix uses sort-order row encoding",
                a_nullable: false,
                a_options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
                threshold_source_rows: &[
                    (Some(10), 1.0),
                    (Some(10), 2.0),
                    (Some(9), 3.0),
                ],
                lagging_partition_rows: &[(Some(8), 1.0), (Some(8), 2.0)],
                expected_finished: true,
            },
            SharedPrefixCase {
                name: "NULLS LAST prefix uses sort-order row encoding",
                a_nullable: true,
                a_options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
                threshold_source_rows: &[
                    (Some(1), 20.0),
                    (Some(1), 15.0),
                    (Some(2), 30.0),
                ],
                lagging_partition_rows: &[(None, 10.0), (None, 20.0)],
                expected_finished: true,
            },
        ] {
            assert_shared_prefix_case(case)?;
        }
        Ok(())
    }

    fn make_single_column_topk(
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    ) -> Result<(SchemaRef, TopK)> {
        make_single_column_topk_with_filter(
            0,
            Arc::new(RwLock::new(TopKDynamicFilters::new(dynamic_filter))),
        )
    }

    fn make_single_column_topk_with_filter(
        partition_id: usize,
        filter: Arc<RwLock<TopKDynamicFilters>>,
    ) -> Result<(SchemaRef, TopK)> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let sort_expr = PhysicalSortExpr {
            expr: col("a", schema.as_ref())?,
            options: SortOptions::default(),
        };

        let topk = TopK::try_new(
            partition_id,
            Arc::clone(&schema),
            vec![sort_expr.clone()],
            LexOrdering::from([sort_expr]),
            2,
            10,
            Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
            filter,
        )?;

        Ok((schema, topk))
    }

    #[tokio::test]
    async fn test_topk_marks_filter_complete() -> Result<()> {
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true)));
        let dynamic_filter_clone = Arc::clone(&dynamic_filter);
        let (schema, mut topk) = make_single_column_topk(dynamic_filter)?;

        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(1), Some(2)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        topk.insert_batch(batch)?;

        let _results: Vec<_> = topk.emit()?.try_collect().await?;

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            dynamic_filter_clone.wait_complete(),
        )
        .await
        .expect("single-emitter TopK should mark the dynamic filter complete");

        Ok(())
    }

    #[tokio::test]
    async fn test_shared_topk_filter_completes_after_last_emitter() -> Result<()> {
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true)));
        let dynamic_filter_clone = Arc::clone(&dynamic_filter);
        let shared_filter = Arc::new(RwLock::new(
            TopKDynamicFilters::new_with_topk_emitter_count(dynamic_filter, 2),
        ));

        let (schema, mut topk_0) =
            make_single_column_topk_with_filter(0, Arc::clone(&shared_filter))?;
        let (_, mut topk_1) =
            make_single_column_topk_with_filter(1, Arc::clone(&shared_filter))?;

        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), Some(1), Some(2)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        topk_0.insert_batch(batch)?;
        let _results: Vec<_> = topk_0.emit()?.try_collect().await?;

        let dynamic_filter_expr: Arc<dyn PhysicalExpr> =
            Arc::<DynamicFilterPhysicalExpr>::clone(&dynamic_filter_clone);
        assert!(
            matches!(
                DynamicFilterTracking::classify(&dynamic_filter_expr),
                DynamicFilterTracking::Watching(_)
            ),
            "the shared filter should remain watchable until every TopK emits"
        );

        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(6), Some(4), Some(5)]));
        let batch = RecordBatch::try_new(schema, vec![array])?;
        topk_1.insert_batch(batch)?;
        let _results: Vec<_> = topk_1.emit()?.try_collect().await?;

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            dynamic_filter_clone.wait_complete(),
        )
        .await
        .expect("the final shared TopK emitter should mark the dynamic filter complete");

        Ok(())
    }

    /// Tests that memory-based compaction triggers when a large batch
    /// has very few rows referenced by the top-k heap.
    #[tokio::test]
    async fn test_topk_memory_compaction() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        let sort_expr = PhysicalSortExpr {
            expr: col("a", schema.as_ref())?,
            options: SortOptions::default(),
        };

        let full_expr = LexOrdering::from([sort_expr.clone()]);
        let prefix = vec![sort_expr];

        let runtime = Arc::new(RuntimeEnv::default());
        let metrics = ExecutionPlanMetricsSet::new();

        let k = 5;
        let mut topk = TopK::try_new(
            0,
            Arc::clone(&schema),
            prefix,
            full_expr,
            k,
            8192,
            runtime,
            &metrics,
            Arc::new(RwLock::new(TopKDynamicFilters::new(Arc::new(
                DynamicFilterPhysicalExpr::new(vec![], lit(true)),
            )))),
        )?;

        // Insert a large batch (100,000 rows) with values 1..=100_000.
        // Only the smallest 5 values (1..=5) will end up in the heap.
        let large_values: Vec<i32> = (1..=100_000).collect();
        let array1: ArrayRef = Arc::new(Int32Array::from(large_values));
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![array1])?;
        topk.insert_batch(batch1)?;

        // After the first batch, store has 1 batch — compaction should
        // not trigger (guard: store.len() <= 1).
        assert_eq!(
            topk.heap.store.len(),
            1,
            "should have 1 batch before second insert"
        );

        // Insert a second batch whose values displace entries in the heap.
        // -1 and 0 are smaller than the current top-5 (1..=5), so they
        // produce 2 replacements. With replacements > 0, `insert_batch`
        // calls `insert_batch_entry` (briefly making store.len() == 2)
        // and then `maybe_compact`, which should collapse it back to 1.
        let array2: ArrayRef = Arc::new(Int32Array::from(vec![-1, 0]));
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![array2])?;
        let replacements_before = topk.metrics.row_replacements.value();
        topk.insert_batch(batch2)?;

        // Sanity check: batch2 was actually integrated. Without
        // replacements, `maybe_compact` is never called and the
        // store-length assertion below would pass vacuously.
        assert!(
            topk.metrics.row_replacements.value() > replacements_before,
            "batch2 must produce replacements so compaction is exercised"
        );

        // The compacted-estimate guard is `total_rows <= num_rows * 2`,
        // i.e. 100_002 <= 10, which is false, so compaction fires and
        // collapses the two stored batches back into one.
        assert_eq!(
            topk.heap.store.len(),
            1,
            "store should be compacted to 1 batch"
        );

        // Verify the emitted results are correct (top 5 ascending).
        let results: Vec<_> = topk.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+", "| a  |", "+----+", "| -1 |", "| 0  |", "| 1  |", "| 2  |",
                "| 3  |", "+----+",
            ],
            &results
        );

        Ok(())
    }

    /// Negative path: when stored rows are close to the heap size,
    /// compaction must NOT fire even with multiple batches present,
    /// because the savings would be marginal
    /// (guard: `total_rows <= num_rows * 2`).
    ///
    /// Uses a bit-packed `BooleanArray` so that future changes to the
    /// compaction heuristic that reintroduce a per-byte estimate
    /// (where integer truncation could misbehave on sub-byte types)
    /// are caught here.
    #[tokio::test]
    async fn test_topk_memory_compaction_skipped_when_marginal() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)]));

        let sort_expr = PhysicalSortExpr {
            expr: col("a", schema.as_ref())?,
            options: SortOptions::default(),
        };
        let full_expr = LexOrdering::from([sort_expr.clone()]);
        let prefix = vec![sort_expr];

        let runtime = Arc::new(RuntimeEnv::default());
        let metrics = ExecutionPlanMetricsSet::new();

        let k = 10;
        let mut topk = TopK::try_new(
            0,
            Arc::clone(&schema),
            prefix,
            full_expr,
            k,
            8192,
            runtime,
            &metrics,
            Arc::new(RwLock::new(TopKDynamicFilters::new(Arc::new(
                DynamicFilterPhysicalExpr::new(vec![], lit(true)),
            )))),
        )?;

        // Two small batches; every row from both batches ends up referenced
        // by the heap, so total_rows == num_rows == 10.
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![false, false, true, true, true]))
                    as ArrayRef,
            ],
        )?;
        topk.insert_batch(batch1)?;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(BooleanArray::from(vec![false, false, false, true, true]))
                    as ArrayRef,
            ],
        )?;
        topk.insert_batch(batch2)?;

        // Guard `total_rows <= num_rows * 2` should hold (10 <= 20),
        // so compaction is skipped and BOTH batches remain in the store.
        assert_eq!(
            topk.heap.store.len(),
            2,
            "store must keep 2 batches when savings would be marginal"
        );
        assert_eq!(topk.heap.inner.len(), 10, "heap should hold all 10 rows");

        // Output is still correct (5 falses then 5 trues ascending).
        let results: Vec<_> = topk.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+-------+",
                "| a     |",
                "+-------+",
                "| false |",
                "| false |",
                "| false |",
                "| false |",
                "| false |",
                "| true  |",
                "| true  |",
                "| true  |",
                "| true  |",
                "| true  |",
                "+-------+",
            ],
            &results
        );

        Ok(())
    }

    /// Builds a `(pk Int32, val Int32)` schema and a `PartitionedTopK`
    /// partitioned by `pk` with order `val ASC`. Helper for the
    /// `PartitionedTopK` tests below.
    fn build_partitioned_topk(k: usize) -> Result<(Arc<Schema>, PartitionedTopK)> {
        build_partitioned_topk_with_opts(k, SortOptions::default(), false)
    }

    /// Variant of [`build_partitioned_topk`] that lets the test pick the
    /// `val` column's `SortOptions` (direction, null ordering) and
    /// nullability. Used by tests that exercise the shared encoder
    /// across `ASC`/`DESC` and `NULLS FIRST/LAST` paths.
    fn build_partitioned_topk_with_opts(
        k: usize,
        val_sort_options: SortOptions,
        val_nullable: bool,
    ) -> Result<(Arc<Schema>, PartitionedTopK)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("val", DataType::Int32, val_nullable),
        ]));

        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let pk_sort_expr = PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        };
        let val_sort_expr = PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: val_sort_options,
        };

        let partition_sort_fields = build_sort_fields(&[pk_sort_expr], &schema)?;
        let order_expr = LexOrdering::from([val_sort_expr]);

        let state = PartitionedTopK::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            partition_sort_fields,
            order_expr,
            k,
            8, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok((schema, state))
    }

    fn pk_val_batch(
        schema: &Arc<Schema>,
        pks: Vec<i32>,
        vals: Vec<i32>,
    ) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(pks)),
                Arc::new(Int32Array::from(vals)),
            ],
        )?)
    }

    /// Variant of [`pk_val_batch`] that accepts nullable `val`s. Used by
    /// tests that exercise null-ordering through the shared encoder.
    fn nullable_pk_val_batch(
        schema: &Arc<Schema>,
        pks: Vec<i32>,
        vals: Vec<Option<i32>>,
    ) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(pks)),
                Arc::new(Int32Array::from(vals)),
            ],
        )?)
    }

    /// Multiple distinct partition keys interleaved within a single
    /// input batch — grouping rows by partition key, per-partition heap eviction,
    /// and partition-key-ordered emit must all behave correctly.
    #[tokio::test]
    async fn test_partitioned_topk_multi_partition_within_batch() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk(2)?;

        // pk=1 vals: 10, 5, 8 → top-2 ASC = [5, 8]
        // pk=2 vals: 20, 15   → top-2 ASC = [15, 20]
        // pk=3 vals: 7        → top-2 ASC = [7]
        let batch =
            pk_val_batch(&schema, vec![1, 2, 1, 2, 1, 3], vec![10, 20, 5, 15, 8, 7])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 8   |",
                "| 2  | 15  |",
                "| 2  | 20  |",
                "| 3  | 7   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// State must accumulate across `insert_batch` calls: a partition
    /// key seen in batch 1 should still own its heap when batch 2
    /// arrives, and a row in batch 2 that beats the existing K-th
    /// best should evict the loser.
    #[tokio::test]
    async fn test_partitioned_topk_cross_batch_eviction() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk(2)?;

        // Batch 1: pk=1 fills the heap with [50, 40].
        state.insert_batch(&pk_val_batch(&schema, vec![1, 1], vec![50, 40])?)?;

        // Batch 2: pk=1 sees a smaller value (10) — it must evict 50.
        // pk=2 appears for the first time mid-stream.
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 2, 1],
            vec![10, 99, 60], // 60 > 40 stays on top, gets discarded
        )?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 10  |",
                "| 1  | 40  |",
                "| 2  | 99  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Empty input must produce an empty output stream, not panic.
    #[tokio::test]
    async fn test_partitioned_topk_empty_input() -> Result<()> {
        let (_schema, state) = build_partitioned_topk(3)?;
        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert!(results.is_empty(), "empty input → empty output");
        Ok(())
    }

    /// `fetch = 1` is a common case (rn = 1 filter). The heap should
    /// hold exactly one row per partition: the partition's minimum.
    #[tokio::test]
    async fn test_partitioned_topk_fetch_one() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk(1)?;
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 1, 2, 2, 3],
            vec![3, 1, 9, 4, 7],
        )?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 1   |",
                "| 2  | 4   |",
                "| 3  | 7   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `ORDER BY val DESC` exercises the shared encoder's sort-direction
    /// handling: the row converter must flip the sort sign for `val` so
    /// that larger values compare smaller in row-encoded form. Each
    /// partition should keep its top-K *largest* values.
    #[tokio::test]
    async fn test_partitioned_topk_desc_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_with_opts(
            2,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            false,
        )?;

        // pk=1 vals: 10, 5, 8, 12 → top-2 DESC = [12, 10]
        // pk=2 vals: 20, 15, 25   → top-2 DESC = [25, 20]
        let batch = pk_val_batch(
            &schema,
            vec![1, 2, 1, 2, 1, 1, 2],
            vec![10, 20, 5, 15, 8, 12, 25],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 12  |",
                "| 1  | 10  |",
                "| 2  | 25  |",
                "| 2  | 20  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// NULL sort values exercise the shared encoder's null-ordering
    /// handling. With `ASC NULLS LAST`, NULLs sort *after* every
    /// non-NULL value, so a partition whose only non-NULL value beats
    /// a NULL must evict the NULL when `K = 1`. A partition that holds
    /// only NULLs must still emit them.
    #[tokio::test]
    async fn test_partitioned_topk_nulls_last_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_with_opts(
            1,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            true,
        )?;

        // pk=1 vals: NULL, 7, NULL → top-1 ASC NULLS LAST = [7]
        // pk=2 vals: NULL          → top-1                 = [NULL]
        // pk=3 vals: NULL, 4, 2    → top-1                 = [2]
        let batch = nullable_pk_val_batch(
            &schema,
            vec![1, 2, 1, 1, 3, 3, 3],
            vec![None, None, Some(7), None, None, Some(4), Some(2)],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 7   |",
                "| 2  |     |",
                "| 3  | 2   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `ASC NULLS FIRST` (the `SortOptions::default()`) sorts NULLs
    /// *before* every non-NULL value, so under `fetch = K` a partition's
    /// NULLs are kept preferentially over larger non-NULL values.
    #[tokio::test]
    async fn test_partitioned_topk_nulls_first_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_with_opts(
            2,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            true,
        )?;

        // pk=1 vals: NULL, 5, NULL, 8 → top-2 ASC NULLS FIRST = [NULL, NULL]
        // pk=2 vals: 7, NULL          → top-2                  = [NULL, 7]
        // pk=3 vals: 3, 1             → top-2                  = [1, 3]
        let batch = nullable_pk_val_batch(
            &schema,
            vec![1, 2, 1, 3, 1, 2, 1, 3],
            vec![
                None,
                Some(7),
                Some(5),
                Some(3),
                None,
                None,
                Some(8),
                Some(1),
            ],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  |     |",
                "| 1  |     |",
                "| 2  |     |",
                "| 2  | 7   |",
                "| 3  | 1   |",
                "| 3  | 3   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    // ====================================================================
    // PartitionedTopKRank operator tests
    //
    // These mirror the PartitionedTopK tests above plus three RANK-specific
    // cases for the Equal / boundary-shift / boundary-unchanged-eviction
    // arms in `PartitionedTopKRank::insert_batch`.
    // ====================================================================

    /// Builds a `(pk Int32, val Int32)` schema and a `PartitionedTopKRank`
    /// keyed on `pk ASC` (partition) and `val ASC` (ORDER BY).
    fn build_partitioned_topk_rank(
        k: usize,
    ) -> Result<(Arc<Schema>, PartitionedTopKRank)> {
        build_partitioned_topk_rank_with_opts(k, SortOptions::default(), false)
    }

    /// Variant of [`build_partitioned_topk_rank`] that lets the test pick
    /// the `val` column's `SortOptions` (direction, null ordering) and
    /// nullability.
    fn build_partitioned_topk_rank_with_opts(
        k: usize,
        val_sort_options: SortOptions,
        val_nullable: bool,
    ) -> Result<(Arc<Schema>, PartitionedTopKRank)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("val", DataType::Int32, val_nullable),
        ]));

        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let pk_sort_expr = PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        };
        let val_sort_expr = PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: val_sort_options,
        };

        let partition_sort_fields = build_sort_fields(&[pk_sort_expr], &schema)?;
        let order_expr = LexOrdering::from([val_sort_expr]);

        let state = PartitionedTopKRank::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            partition_sort_fields,
            order_expr,
            k,
            8, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok((schema, state))
    }

    /// Multiple distinct partition keys interleaved within a single
    /// input batch — grouping rows by partition key, per-partition heap eviction,
    /// and partition-key-ordered emit must all behave correctly. No
    /// ties: result should match a `ROW_NUMBER` top-K under the same K.
    #[tokio::test]
    async fn test_partitioned_topk_rank_multi_partition_within_batch() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank(2)?;

        // pk=1 vals: 10, 5, 8 → top-2 ASC = [5, 8]
        // pk=2 vals: 20, 15   → top-2 ASC = [15, 20]
        // pk=3 vals: 7        → top-2 ASC = [7]
        let batch =
            pk_val_batch(&schema, vec![1, 2, 1, 2, 1, 3], vec![10, 20, 5, 15, 8, 7])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 8   |",
                "| 2  | 15  |",
                "| 2  | 20  |",
                "| 3  | 7   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// State must accumulate across `insert_batch` calls. A row in
    /// batch 2 that's strictly better than the existing K-th must
    /// evict it; an evicted row whose bytes match the new boundary
    /// becomes a `TieEntry` pinned to the prior batch.
    #[tokio::test]
    async fn test_partitioned_topk_rank_cross_batch_eviction() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank(2)?;

        // Batch 1: pk=1 fills the heap with [50, 40].
        state.insert_batch(&pk_val_batch(&schema, vec![1, 1], vec![50, 40])?)?;

        // Batch 2: pk=1 sees a smaller value (10) — it must evict 50;
        // 60 > 40 so it's dropped. pk=2 appears mid-stream.
        state.insert_batch(&pk_val_batch(&schema, vec![1, 2, 1], vec![10, 99, 60])?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 10  |",
                "| 1  | 40  |",
                "| 2  | 99  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Empty input must produce an empty output stream, not panic.
    #[tokio::test]
    async fn test_partitioned_topk_rank_empty_input() -> Result<()> {
        let (_schema, state) = build_partitioned_topk_rank(3)?;
        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert!(results.is_empty(), "empty input → empty output");
        Ok(())
    }

    /// `fetch = 1` is a common case (rk = 1 filter) and exercises the
    /// boundary-defined-immediately path: after the first admission per
    /// partition, `heap.max()` is `Some`, so every subsequent row goes
    /// through full Equal/Greater/Less classification.
    #[tokio::test]
    async fn test_partitioned_topk_rank_fetch_one() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank(1)?;
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 1, 2, 2, 3],
            vec![3, 1, 9, 4, 7],
        )?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 1   |",
                "| 2  | 4   |",
                "| 3  | 7   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `ORDER BY val DESC` exercises the shared encoder's sort-direction
    /// handling: the row converter flips the sort sign for `val` so
    /// larger values compare smaller in row-encoded form. Each
    /// partition keeps its top-K *largest* values.
    #[tokio::test]
    async fn test_partitioned_topk_rank_desc_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank_with_opts(
            2,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            false,
        )?;

        // pk=1 vals: 10, 5, 8, 12 → top-2 DESC = [12, 10]
        // pk=2 vals: 20, 15, 25   → top-2 DESC = [25, 20]
        let batch = pk_val_batch(
            &schema,
            vec![1, 2, 1, 2, 1, 1, 2],
            vec![10, 20, 5, 15, 8, 12, 25],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 12  |",
                "| 1  | 10  |",
                "| 2  | 25  |",
                "| 2  | 20  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// NULL sort values exercise the shared encoder's null-ordering
    /// handling. With `ASC NULLS LAST`, NULLs sort *after* every
    /// non-NULL value, so a partition whose only non-NULL value beats
    /// a NULL must evict the NULL when `K = 1`. A partition that holds
    /// only NULLs must still emit them.
    #[tokio::test]
    async fn test_partitioned_topk_rank_nulls_last_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank_with_opts(
            1,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            true,
        )?;

        // pk=1 vals: NULL, 7, NULL → top-1 ASC NULLS LAST = [7]
        // pk=2 vals: NULL          → top-1                 = [NULL]
        // pk=3 vals: NULL, 4, 2    → top-1                 = [2]
        let batch = nullable_pk_val_batch(
            &schema,
            vec![1, 2, 1, 1, 3, 3, 3],
            vec![None, None, Some(7), None, None, Some(4), Some(2)],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 7   |",
                "| 2  |     |",
                "| 3  | 2   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `ASC NULLS FIRST` (the `SortOptions::default()`) sorts NULLs
    /// *before* every non-NULL value, so under `fetch = K` a partition's
    /// NULLs are kept preferentially over larger non-NULL values.
    #[tokio::test]
    async fn test_partitioned_topk_rank_nulls_first_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank_with_opts(
            2,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            true,
        )?;

        // pk=1 vals: NULL, 5, NULL, 8 → top-2 ASC NULLS FIRST = [NULL, NULL]
        // pk=2 vals: 7, NULL          → top-2                  = [NULL, 7]
        // pk=3 vals: 3, 1             → top-2                  = [1, 3]
        let batch = nullable_pk_val_batch(
            &schema,
            vec![1, 2, 1, 3, 1, 2, 1, 3],
            vec![
                None,
                Some(7),
                Some(5),
                Some(3),
                None,
                None,
                Some(8),
                Some(1),
            ],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  |     |",
                "| 1  |     |",
                "| 2  |     |",
                "| 2  | 7   |",
                "| 3  | 1   |",
                "| 3  | 3   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// RANK-specific: heap fills with K rows tied at the same OB value,
    /// then more rows at that same value arrive. They take the Equal arm
    /// (heap is full, `heap.max() == row`) and accumulate as ties, while
    /// strictly-greater rows are dropped. All retained rows have rank 1.
    #[tokio::test]
    async fn test_partitioned_topk_rank_boundary_ties_retained() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank(2)?;

        // pk=1 vals: 5, 5, 10, 5
        //   - first two 5s fill the heap (max=None until heap reaches K=2)
        //   - third row 10 > 5 → drop (Greater)
        //   - fourth row 5 == 5 → push to ties (Equal)
        // Sorted RANKs: 5→1, 5→1, 5→1, 10→4. WHERE rk ≤ 2 keeps the three 5s.
        let batch = pk_val_batch(&schema, vec![1, 1, 1, 1], vec![5, 5, 10, 5])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 5   |",
                "| 1  | 5   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// RANK-specific: heap fills with K rows tied at value V, equal_indices
    /// accumulate at V, then a strictly-better row arrives whose admission
    /// shifts the boundary strictly below V. The boundary-changed branch
    /// must clear both `state.ties` and the in-flight `equal_indices` —
    /// otherwise the now-rank-> K rows at value V would leak into output.
    #[tokio::test]
    async fn test_partitioned_topk_rank_boundary_shifts_clears_ties() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank(2)?;

        // pk=1 vals: 10, 10, 10, 5, 3
        //   - first two 10s fill heap (max=10)
        //   - third 10 → Equal → equal_indices=[2]
        //   - 5 < 10 → admit, evict 10 → heap={5,10}, max=10 (unchanged).
        //       Push evicted to ties: ties=[10@curr_batch[ev_idx]].
        //   - 3 < 10 → admit, evict 10 → heap={3,5}, max=5 (CHANGED).
        //       Clear ties AND equal_indices.
        // Sorted RANKs: 3→1, 5→2, 10→3, 10→3, 10→3. WHERE rk ≤ 2 → [3, 5].
        let batch = pk_val_batch(&schema, vec![1, 1, 1, 1, 1], vec![10, 10, 10, 5, 3])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 3   |",
                "| 1  | 5   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// RANK-specific: heap has multiple rows at boundary value V, then a
    /// strictly-better row arrives. The heap evicts one V (popping
    /// `prev_min`), but `heap.max()` is still V — boundary unchanged.
    /// The evicted V row must be pushed as a `TieEntry`; without that
    /// branch a `rk <= K` query would silently lose a tied row.
    #[tokio::test]
    async fn test_partitioned_topk_rank_eviction_at_unchanged_boundary() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_rank(2)?;

        // pk=1 vals: 10, 10, 5
        //   - first two 10s fill the heap (max=10)
        //   - 5 < 10 → admit, evict 10. New heap={5,10}, max=10 (unchanged).
        //       Push the evicted 10 to ties.
        // Sorted RANKs: 5→1, 10→2, 10→2. WHERE rk ≤ 2 → all 3 rows.
        let batch = pk_val_batch(&schema, vec![1, 1, 1], vec![10, 10, 5])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 10  |",
                "| 1  | 10  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    // ====================================================================
    // PartitionedTopKDenseRank operator tests
    //
    // These mirror the RANK tests plus DENSE_RANK-specific cases: rows
    // sharing an ob key coalesce into one `TieEntry`, unbounded
    // rows-per-distinct-key, and eviction removes the entire max group
    // when a strictly-smaller distinct ob arrives.
    // ====================================================================

    /// Builds a `(pk Int32, val Int32)` schema and a
    /// `PartitionedTopKDenseRank` keyed on `pk ASC` (partition) and
    /// `val ASC` (ORDER BY).
    fn build_partitioned_topk_dense_rank(
        k: usize,
    ) -> Result<(Arc<Schema>, PartitionedTopKDenseRank)> {
        build_partitioned_topk_dense_rank_with_opts(k, SortOptions::default(), false)
    }

    fn build_partitioned_topk_dense_rank_with_opts(
        k: usize,
        val_sort_options: SortOptions,
        val_nullable: bool,
    ) -> Result<(Arc<Schema>, PartitionedTopKDenseRank)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("val", DataType::Int32, val_nullable),
        ]));

        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let pk_sort_expr = PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        };
        let val_sort_expr = PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: val_sort_options,
        };

        let partition_sort_fields = build_sort_fields(&[pk_sort_expr], &schema)?;
        let order_expr = LexOrdering::from([val_sort_expr]);

        let state = PartitionedTopKDenseRank::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            partition_sort_fields,
            order_expr,
            k,
            8, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok((schema, state))
    }

    /// Single-batch DENSE_RANK top-2 across multiple partitions with
    /// distinct ob values only — should behave identically to a
    /// ROW_NUMBER top-2. Exercises per-partition grouping + emit order.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_multi_partition_within_batch() -> Result<()>
    {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        // pk=1 vals: 10, 5, 8 → distinct-top-2 ASC = {5, 8}
        // pk=2 vals: 20, 15   → distinct-top-2 ASC = {15, 20}
        // pk=3 vals: 7        → distinct-top-2 ASC = {7}
        let batch =
            pk_val_batch(&schema, vec![1, 2, 1, 2, 1, 3], vec![10, 20, 5, 15, 8, 7])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 8   |",
                "| 2  | 15  |",
                "| 2  | 20  |",
                "| 3  | 7   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// DENSE_RANK-specific: heavy ties within a batch. All rows at each
    /// distinct ob value must be kept — within-call bucketing groups them
    /// into one `TieEntry` per distinct ob.
    ///
    /// vals per partition (sorted logically):
    ///   pk=1: 1, 1, 1, 2, 2, 3, 3, 3, 4
    ///   distinct-top-2 = {1, 2} → all 5 rows at those values retained.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_heavy_ties_coalesced() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        let batch = pk_val_batch(
            &schema,
            vec![1, 1, 1, 1, 1, 1, 1, 1, 1],
            vec![1, 3, 1, 2, 3, 1, 2, 3, 4],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 1   |",
                "| 1  | 1   |",
                "| 1  | 1   |",
                "| 1  | 2   |",
                "| 1  | 2   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Rows tied at the same ob across two source batches must both
    /// land under the same map key as separate `TieEntry`s — one per
    /// source batch — but emit as a single contiguous run.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_cross_batch_same_key() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        // Batch 1: pk=1 with ob values {5, 5, 8}. groups after: {5→[..], 8→[..]}.
        state.insert_batch(&pk_val_batch(&schema, vec![1, 1, 1], vec![5, 5, 8])?)?;

        // Batch 2: pk=1 with more 5s and an 8, plus a 20 that's dropped.
        state.insert_batch(&pk_val_batch(&schema, vec![1, 1, 1], vec![5, 8, 20])?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 5   |",
                "| 1  | 5   |",
                "| 1  | 8   |",
                "| 1  | 8   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Refactor guard: the full RANK-style path in one run — multi-partition
    /// per-batch grouping, within-batch bucketing of scattered same-ob rows,
    /// cross-batch append to an existing group, cross-batch new-key insert,
    /// and cross-batch eviction of a whole max group. Every `TieEntry` is
    /// built from its own source batch (no cross-batch coalescing), so the
    /// retained rows must be exactly the K=2 smallest distinct ob values
    /// per partition with all their rows, regardless of arrival order.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_multi_batch_multi_partition() -> Result<()>
    {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        // Batch 1 interleaves pk=1 and pk=2, with same-ob rows scattered:
        //   pk=1 vals: 10, 20, 10, 20, 10  → {10:[×3], 20:[×2]}
        //   pk=2 vals: 100, 100            → {100:[×2]}
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 2, 1, 1, 2, 1, 1],
            vec![10, 100, 20, 20, 100, 10, 10],
        )?)?;

        // Batch 2:
        //   pk=1 vals: 20, 5, 10 → append a 20, insert 5 (evicts the whole
        //              20 group), append a 10 → retained distinct {5, 10}.
        //   pk=2 vals: 50        → insert 5th... new key, room → {50, 100}.
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 2, 1, 1],
            vec![20, 50, 5, 10],
        )?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        // pk=1: val=5 (×1 from batch 2), val=10 (×3 batch 1 + ×1 batch 2 = ×4).
        //       All 20s dropped (evicted). pk=2: val=50 (×1), val=100 (×2).
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 10  |",
                "| 1  | 10  |",
                "| 1  | 10  |",
                "| 1  | 10  |",
                "| 2  | 50  |",
                "| 2  | 100 |",
                "| 2  | 100 |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// DENSE_RANK-specific: eviction removes the entire max group when
    /// a strictly-smaller distinct ob arrives. Multiple rows at the
    /// evicted key all disappear.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_max_group_eviction() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        // Batch 1: pk=1 with {10, 10, 20, 20}. groups={10→[..], 20→[..]}, at K.
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 1, 1, 1],
            vec![10, 10, 20, 20],
        )?)?;

        // Batch 2: pk=1 with 5 — strictly smaller than max=20, evict entire
        // 20 group; now groups={10, 5}. Then a 30 comes in and is dropped.
        state.insert_batch(&pk_val_batch(&schema, vec![1, 1], vec![5, 30])?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 10  |",
                "| 1  | 10  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Empty input must produce an empty output stream, not panic.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_empty_input() -> Result<()> {
        let (_schema, state) = build_partitioned_topk_dense_rank(3)?;
        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert!(results.is_empty(), "empty input → empty output");
        Ok(())
    }

    /// `fetch = 1` retains only the smallest distinct ob per partition,
    /// with all rows at that value kept.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_fetch_one() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(1)?;

        // pk=1 vals: 5, 3, 5, 3, 7 → distinct-top-1 = {3} → both 3s kept.
        // pk=2 vals: 9, 4          → distinct-top-1 = {4} → single 4.
        let batch = pk_val_batch(
            &schema,
            vec![1, 1, 1, 2, 1, 2, 1],
            vec![5, 3, 5, 9, 3, 4, 7],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 3   |",
                "| 1  | 3   |",
                "| 2  | 4   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `K > distinct_ob_count` — nothing should be dropped.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_k_exceeds_distinct() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(10)?;

        // Only 3 distinct ob values under pk=1; all rows must be retained.
        let batch = pk_val_batch(&schema, vec![1, 1, 1, 1], vec![5, 3, 3, 7])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 3   |",
                "| 1  | 3   |",
                "| 1  | 5   |",
                "| 1  | 7   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `ORDER BY val DESC` — the row-encoded key ordering must reflect
    /// the direction so the "distinct-K best" set is the K *largest*
    /// distinct ob values.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_desc_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank_with_opts(
            2,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            false,
        )?;

        // pk=1 vals: 10, 5, 8, 12, 10 → distinct-top-2 DESC = {12, 10}
        //   → keep both 10s and 12.
        let batch = pk_val_batch(&schema, vec![1, 1, 1, 1, 1], vec![10, 5, 8, 12, 10])?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 12  |",
                "| 1  | 10  |",
                "| 1  | 10  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Cross-partition eviction independence — Case-C eviction in one
    /// partition must not affect another partition's state.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_partition_independence() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        // Batch 1: pk=1 fills {10, 20}; pk=2 fills {30, 40}.
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 1, 2, 2],
            vec![10, 20, 30, 40],
        )?)?;

        // Batch 2: pk=1 sees 5 (evicts 20). pk=2 sees 25 (evicts 40).
        // Each partition's Case-C branch is independent.
        state.insert_batch(&pk_val_batch(&schema, vec![1, 2], vec![5, 25])?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 10  |",
                "| 2  | 25  |",
                "| 2  | 30  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// NULL sort values exercise the shared encoder's null-ordering
    /// through the row-encoded key byte order. With `ASC NULLS
    /// LAST`, a NULL is the *largest* distinct ob, so a partition with
    /// >= K non-NULL distinct values evicts its NULLs, while a partition
    /// whose only distinct value is NULL still emits it.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_nulls_last_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank_with_opts(
            2,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            true,
        )?;

        // pk=1 vals: NULL, 10, 20, NULL → distinct-top-2 NULLS LAST = {10, 20}
        // pk=2 vals: NULL               → distinct-top-2            = {NULL}
        // pk=3 vals: 3, 3               → distinct-top-2            = {3}
        let batch = nullable_pk_val_batch(
            &schema,
            vec![1, 1, 1, 1, 2, 3, 3],
            vec![None, Some(10), Some(20), None, None, Some(3), Some(3)],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 10  |",
                "| 1  | 20  |",
                "| 2  |     |",
                "| 3  | 3   |",
                "| 3  | 3   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `ASC NULLS FIRST` sorts NULLs *before* every non-NULL value, so a
    /// NULL is the smallest distinct ob and is kept preferentially. Every
    /// row at a retained distinct ob — including all tied NULLs — emits.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_nulls_first_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank_with_opts(
            2,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            true,
        )?;

        // pk=1 vals: NULL, 5, NULL, 8 → distinct-top-2 NULLS FIRST = {NULL, 5}
        // pk=2 vals: 7, NULL          → distinct-top-2             = {NULL, 7}
        // pk=3 vals: 3, 1             → distinct-top-2             = {1, 3}
        let batch = nullable_pk_val_batch(
            &schema,
            vec![1, 2, 1, 3, 1, 2, 1, 3],
            vec![
                None,
                Some(7),
                Some(5),
                Some(3),
                None,
                None,
                Some(8),
                Some(1),
            ],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  |     |",
                "| 1  |     |",
                "| 1  | 5   |",
                "| 2  |     |",
                "| 2  | 7   |",
                "| 3  | 1   |",
                "| 3  | 3   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }
}
