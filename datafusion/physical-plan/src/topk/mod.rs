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
    row::{RowConverter, Rows, SortField},
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
    memory_pool::{MemoryConsumer, MemoryReservation, proxy::VecAllocExt},
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
            loop {
                if batch.num_rows() <= batch_size {
                    (&batch).record_output(&metrics.baseline);
                    batches.push(Ok(batch));
                    break;
                } else {
                    let head = batch.slice(0, batch_size);
                    (&head).record_output(&metrics.baseline);
                    batches.push(Ok(head));
                    let remaining_length = batch.num_rows() - batch_size;
                    batch = batch.slice(batch_size, remaining_length);
                }
            }
        }
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

/// Rows a [`RecordBatchStore`] may hold per row still referenced by a heap,
/// before the referencing operator rewrites it.
///
/// A store entry is freed only when the last row referencing it is evicted, so
/// what it holds tracks the *input* rather than the rows retained. Both top-K
/// paths bound that the same way — compact once the store holds this multiple
/// of what is still referenced — and only the denominator differs: one heap's
/// length in [`TopKHeap::maybe_compact`], every partition's slots in
/// [`PartitionedTopK::compact_store`]. The multiplier avoids compacting when
/// the savings would be marginal.
const STORE_COMPACTION_RATIO: usize = 2;

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

        // Compact when the store holds more than STORE_COMPACTION_RATIO x what
        // the compacted result would need. The multiplier avoids compacting when the
        // savings would be marginal.
        if total_rows <= num_rows * STORE_COMPACTION_RATIO {
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

    /// The id the next [`Self::register`] call will assign.
    ///
    /// For callers that must reference a batch before they can build it — see
    /// `PartitionedTopK::insert_batch`, which does not know which rows to gather
    /// until it has finished deciding. Valid until the next `register`, and only
    /// if nothing is left pointing at the id when that `register` is skipped.
    fn next_batch_id(&self) -> u32 {
        self.next_id
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

    /// The stored batches in iteration order, paired with the
    /// `batch_id -> position` map that indexes them.
    ///
    /// `interleave_record_batch` addresses its inputs positionally while this
    /// store keys them by a non-contiguous id, so every caller that interleaves
    /// out of the store needs both. The batches are cloned — an `Arc` bump per
    /// column — so they can outlive the store: `emit` needs that, since the
    /// store is dropped as it returns. `compact_store` does not, and only
    /// shares the helper.
    fn positional(&self) -> (Vec<RecordBatch>, HashMap<u32, usize>) {
        let mut batches = Vec::with_capacity(self.batches.len());
        let mut positions = HashMap::with_capacity(self.batches.len());
        for (pos, (batch_id, entry)) in self.batches.iter().enumerate() {
            batches.push(entry.batch.clone());
            positions.insert(*batch_id, pos);
        }
        (batches, positions)
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

/// One retained row: its sort key by value, its output columns by reference.
///
/// `key` is the row-encoded ORDER BY tuple, owned inline. It is
/// byte-comparable, so `Ord` on the bytes is the sort order — the heap compares
/// rows without dereferencing anything, and eviction needs no access to the
/// payload at all.
///
/// `(batch_id, row)` locates the output columns in the operator's shared
/// [`RecordBatchStore`], which refcounts each batch by the slots pointing into
/// it. A slot's existence is what keeps its batch alive.
#[derive(Debug)]
struct PartitionSlot {
    key: Vec<u8>,
    /// Id in the shared store of the batch holding this row's output columns.
    batch_id: u32,
    /// Row of that batch.
    row: u32,
}

impl PartialEq for PartitionSlot {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl Eq for PartitionSlot {}
impl PartialOrd for PartitionSlot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PartitionSlot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// A single partition's top-K.
///
/// A max-heap on `key`, so the root is the *worst* retained row and a new row
/// only has to beat that one. Replacement mutates the root in place (reusing its
/// `Vec` capacity) and lets `PeekMut` sift down on drop — so a steady-state
/// eviction performs no allocation and no teardown.
///
/// There is one of these per *distinct partition key*, so every field is
/// multiplied by the partition count. Hence it is nothing but the heap, 24
/// bytes: the batches live in the operator's one shared [`RecordBatchStore`],
/// and `k` is a single constant for the whole operator, so it is passed to the
/// methods that need it rather than stored per partition.
#[derive(Debug, Default)]
struct PartitionHeap {
    inner: BinaryHeap<PartitionSlot>,
}

impl PartitionHeap {
    /// True if `key` belongs in the top-`k`.
    ///
    /// The worst retained row is the root, so this is one comparison — and
    /// while the heap is not yet full every row qualifies.
    fn qualifies(&self, k: usize, key: &[u8]) -> bool {
        if self.inner.len() < k {
            return true;
        }
        match self.inner.peek() {
            Some(worst) => key < worst.key.as_slice(),
            None => true,
        }
    }

    /// Retain `key`, pointing at row `row` of store batch `batch_id`, evicting
    /// the worst retained row if the heap is already full.
    ///
    /// Returns the evicted row's `batch_id`, which the caller owes the store an
    /// `unuse` for — this type does not touch the store itself, because the
    /// in-flight batch is not registered yet and only the caller knows that —
    /// along with the bytes this call newly allocated, which the operator folds
    /// into its running total. Returning the delta rather than keeping a running
    /// sum per heap is what lets this type hold nothing but the heap.
    fn add(
        &mut self,
        k: usize,
        key: &[u8],
        batch_id: u32,
        row: u32,
    ) -> (Option<u32>, usize) {
        debug_assert!(self.inner.len() <= k);
        if self.inner.len() == k {
            let mut worst = self.inner.peek_mut().expect("heap is full");
            let before = worst.key.capacity();
            worst.key.clear();
            worst.key.extend_from_slice(key);
            let grown = worst.key.capacity() - before;
            let evicted = worst.batch_id;
            worst.batch_id = batch_id;
            worst.row = row;
            drop(worst);
            (Some(evicted), grown)
        } else {
            let slot = PartitionSlot {
                key: key.to_vec(),
                batch_id,
                row,
            };
            let key_bytes = slot.key.capacity();
            let inner_before = self.inner.capacity();
            self.inner.push(slot);
            let grown = key_bytes
                + (self.inner.capacity() - inner_before) * size_of::<PartitionSlot>();
            (None, grown)
        }
    }

    /// The retained slots, in unspecified order.
    fn slots(&self) -> impl Iterator<Item = &PartitionSlot> + '_ {
        self.inner.iter()
    }

    /// Retained rows in ascending ORDER BY order, leaving this heap empty.
    fn drain_sorted(&mut self) -> Vec<PartitionSlot> {
        std::mem::take(&mut self.inner).into_sorted_vec()
    }

    /// Rewrite every slot's store coordinates, leaving the keys — and so the
    /// heap order — untouched.
    ///
    /// For [`PartitionedTopK::compact_store`], which moves every live row into
    /// a fresh set of store batches and has to repoint the slots at it.
    /// `into_vec` hands back the heap's own allocation and `BinaryHeap::from`
    /// takes it again, so this allocates nothing.
    fn repoint(&mut self, mut f: impl FnMut(&mut PartitionSlot)) {
        let mut slots = std::mem::take(&mut self.inner).into_vec();
        for slot in &mut slots {
            f(slot);
        }
        self.inner = BinaryHeap::from(slots);
    }
}

/// Top-K-per-partition operator state.
///
/// Sibling to [`TopK`]. Where `TopK` maintains a single global heap,
/// `PartitionedTopK` maintains one [`PartitionHeap`] per distinct partition
/// key while sharing its two [`RowConverter`]s — one for the partition key,
/// one for the ORDER BY key — along with the [`MemoryReservation`], the
/// scratch [`Rows`] buffers, the [`RecordBatchStore`] holding retained rows,
/// and [`TopKMetrics`] across all partitions.
///
/// This sharing is the point of the type: with N distinct partition
/// keys, a naive `HashMap<_, TopK>` pays N × constant overhead for
/// `RowConverter::new`, `MemoryConsumer::register`, `RecordBatchStore::new`,
/// and metric counter setup. `PartitionedTopK` pays it once.
pub(crate) struct PartitionedTopK {
    schema: SchemaRef,
    metrics: TopKMetrics,
    reservation: MemoryReservation,
    /// ORDER BY expressions (excludes PARTITION BY).
    expr: LexOrdering,
    /// Encoder for the ORDER BY columns, whose encoding is the heap key.
    row_converter: RowConverter,
    /// Scratch row buffer for the ORDER BY encoding, reused across
    /// `insert_batch` calls.
    scratch_rows: Rows,
    /// PARTITION BY expressions.
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// Encoder for the partition key. The encoding is byte-comparable with
    /// ASC/DESC and NULLS FIRST/LAST folded in, so the encoded bytes sort
    /// identically to the partition ordering — which is what lets `emit`
    /// recover partition-key order by sorting the keys directly.
    partition_converter: RowConverter,
    /// Scratch row buffer for partition-key encoding. Reused across
    /// `insert_batch` calls (cleared + appended each batch).
    partition_scratch_rows: Rows,
    /// One heap per distinct partition key seen so far, keyed by the
    /// row-encoded PARTITION BY key.
    ///
    /// `entry_ref` owns the key only on Vacant, so a key is allocated once per
    /// partition for the lifetime of the operator — not once per row, and not
    /// once per partition per batch (which is what draining a per-batch map
    /// would cost). Map order is arbitrary, so `emit` sorts the keys itself.
    heaps: HashMap<Vec<u8>, PartitionHeap>,
    /// The batches holding every retained row's output columns, refcounted by
    /// the slots pointing into them.
    ///
    /// One store for the whole operator rather than one per partition, which is
    /// what [`TopKHeap`] would give. Each entry holds only the rows that were
    /// admitted from one input batch — see `insert_batch` phase 3 — and is
    /// dropped as soon as the last slot referencing it is evicted. When that
    /// alone leaves it holding far more than the heaps point at,
    /// `compact_store` rewrites it.
    store: RecordBatchStore,
    /// Rows of the batch currently being inserted that were admitted, in
    /// ascending row order. Reused across `insert_batch` calls.
    admitted_rows: Vec<u32>,
    /// Rows the heaps currently hold, i.e. slots pointing into `store`.
    ///
    /// Tracked incrementally for the same reason as `heaps_bytes`: it is the
    /// denominator of `compact_store`'s ratio, read on every batch, and summing
    /// `inner.len()` over the heaps would be O(partitions seen so far). Slots
    /// are replaced rather than removed, so this only grows — it settles at
    /// `partitions × K` once every partition's heap has filled.
    live_slots: usize,
    /// Running sum of the bytes every [`PartitionHeap`] has allocated: each
    /// slot's key and the `BinaryHeap`'s own buffer.
    ///
    /// Maintained incrementally because `size()` runs on every batch: summing
    /// over the heaps would make it O(partitions seen so far) per batch, which
    /// is quadratic in the input whenever partition count grows with it. Every
    /// allocation it tracks only grows, so a running total stays exact without a
    /// decrement path. `PartitionHeap::add` returns each call's delta rather
    /// than keeping its own running sum, so the heaps stay free of a field that
    /// would be multiplied by the partition count.
    heaps_bytes: usize,
    /// Running sum of the partition keys `heaps` has interned, one per distinct
    /// key for the operator's lifetime.
    ///
    /// Counted by length rather than capacity: `entry_ref` builds the owned key
    /// with `to_vec`, so the two are equal. Separate from [`Self::heaps_bytes`]
    /// because it is the map's allocation, not any one heap's.
    index_bytes: usize,
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

        // Both encoders are shared by every partition, and each scratch buffer
        // is sized to hold a whole batch so an `insert_batch` pass encodes once
        // per column set with no regrowth.
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
            heaps: HashMap::new(),
            store: RecordBatchStore::new(),
            admitted_rows: Vec::new(),
            live_slots: 0,
            heaps_bytes: 0,
            index_bytes: 0,
            k,
            batch_size,
        })
    }

    /// Encode the partition and ORDER BY columns once each for the whole batch,
    /// admit every qualifying row into the [`PartitionHeap`] of the partition it
    /// belongs to, then register the admitted rows in the shared store.
    pub(crate) fn insert_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // 1. Evaluate the partition and ORDER BY columns and encode each once
        //    for the whole batch. Both encodes are whole-batch kernels that do
        //    not care how the rows group, which is what lets the admission pass
        //    below be a single row loop.
        let pk_arrays: Vec<ArrayRef> = self
            .partition_exprs
            .iter()
            .map(|e| e.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.partition_scratch_rows.clear();
        self.partition_converter
            .append(&mut self.partition_scratch_rows, &pk_arrays)?;

        let ob_arrays: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|e| e.expr.evaluate(batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<_>>()?;
        self.scratch_rows.clear();
        self.row_converter
            .append(&mut self.scratch_rows, &ob_arrays)?;

        // 2. One pass over the rows: find the row's partition and admit it to
        //    that partition's heap if it qualifies. Only the key is copied
        //    here; an admission records which row it kept and phase 3 gathers
        //    them all in one `take`, so a batch is materialized once no matter
        //    how many partitions it touched.
        //
        //    Rows are visited in batch order rather than grouped by partition.
        //    Interleaving groups cannot change a decision — a heap is only
        //    read and written by rows of its own partition, which still reach
        //    it in ascending row order — and it costs no pass over the
        //    partitions seen so far, which would be quadratic in the input
        //    whenever partition count grows with it.
        let k = self.k;
        let mut replacements: usize = 0;
        self.admitted_rows.clear();
        // The gathered batch does not exist yet, but its id does, so a slot can
        // point at it during the pass with no back-patching afterwards. Its
        // `uses` is counted locally for the same reason.
        let batch_id = self.store.next_batch_id();
        let mut uses = 0usize;
        {
            let pk_rows = &self.partition_scratch_rows;
            let ob_rows = &self.scratch_rows;
            let heaps = &mut self.heaps;
            let admitted_rows = &mut self.admitted_rows;
            let store = &mut self.store;
            // Accumulated locally and folded in once, so the running totals are
            // not touched per row.
            let mut interned_bytes = 0usize;
            let mut admitted_bytes = 0usize;
            let mut new_slots = 0usize;

            for row in 0..num_rows {
                let pk = pk_rows.row(row);
                // One probe per row, and `entry_ref` owns the key only on
                // Vacant, so a key is allocated once per partition for the
                // operator's lifetime — not once per row.
                let mut interned = false;
                let heap = heaps.entry_ref(pk.as_ref()).or_insert_with(|| {
                    interned = true;
                    PartitionHeap::default()
                });
                if interned {
                    // The map has just taken the only copy of this key.
                    interned_bytes += pk.as_ref().len();
                }
                let key = ob_rows.row(row);
                if !heap.qualifies(k, key.as_ref()) {
                    continue;
                }
                // An admission's row in the gathered batch is its position in
                // `admitted_rows`, because the gather preserves that order.
                let gather_pos = admitted_rows.len() as u32;
                uses += 1;
                let (evicted, grown) = heap.add(k, key.as_ref(), batch_id, gather_pos);
                admitted_bytes += grown;
                // Mirrors `TopKHeap::add`: a row evicted from the batch being
                // inserted is not in the store yet, so its use comes off the
                // local count rather than through `unuse`, which would panic on
                // an unregistered id.
                match evicted {
                    Some(evicted_id) if evicted_id == batch_id => uses -= 1,
                    Some(evicted_id) => store.unuse(evicted_id),
                    // The heap was not yet full, so this slot is a new
                    // reference into the store rather than a replaced one.
                    None => new_slots += 1,
                }
                admitted_rows.push(row as u32);
                replacements += 1;
            }

            self.index_bytes += interned_bytes;
            self.heaps_bytes += admitted_bytes;
            self.live_slots += new_slots;
        }

        // 3. Gather the rows this batch contributed into a single batch and hand
        //    it to the store. Only rows admitted at some point during the pass
        //    are kept, so what stays pinned is bounded by admissions rather than
        //    by input size — and the entry is freed as soon as the last slot
        //    referencing it is evicted.
        //
        //    `uses == 0` means every admission from this batch was evicted again
        //    before the pass ended, so there is nothing to keep and the id goes
        //    back to the next batch.
        if uses > 0 {
            let gather_idx =
                UInt32Array::from_iter_values(self.admitted_rows.iter().copied());
            let mut entry = self.store.register(take_record_batch(batch, &gather_idx)?);
            debug_assert_eq!(
                entry.id, batch_id,
                "the id handed to the slots must be the id the gather got"
            );
            entry.uses = uses;
            self.store.insert(entry);
        }

        if replacements > 0 {
            self.metrics.row_replacements.add(replacements);
        }
        self.compact_store()?;
        self.reservation.try_resize(self.size())?;
        Ok(())
    }

    /// Rewrite the store to hold only the rows a heap still points at, once it
    /// holds [`STORE_COMPACTION_RATIO`]× more than that.
    ///
    /// An entry holds every row *admitted* from its input batch and is freed
    /// only when the last of them is evicted, so when survivors spread thinly
    /// one live row keeps a whole entry resident and residency tracks the
    /// *input*, not `partitions × K`: 512 partitions of `K = 1` fed 512 batches
    /// pin 131 K rows to retain 512. Nothing else bounds that. A single entry is
    /// no exception — rows admitted then superseded within their own batch stay
    /// in the gather unreferenced — so this does not skip a one-entry store.
    ///
    /// Amortized O(1) per admitted row: one pass over the live slots, and it
    /// cannot recur until the store has taken on another `live_slots` rows.
    ///
    /// Rows are rewritten into `batch_size` chunks, not one batch: an entry is
    /// released only when its last slot is evicted, so a single batch of every
    /// live row would free nothing until every partition has churned.
    ///
    /// Peak residency is the old store plus the new one — chunks are built
    /// before the old entries drop, and the reservation is not resized until
    /// `insert_batch` returns — so a pool sized at the steady-state bound can be
    /// exceeded transiently without erroring.
    ///
    /// All-or-nothing: plan the move, interleave, then rewrite the slots and the
    /// store. A failing interleave leaves the operator as it was rather than
    /// holding slots pointing at ids the store never got.
    fn compact_store(&mut self) -> Result<()> {
        if self.store.total_rows <= self.live_slots * STORE_COMPACTION_RATIO {
            return Ok(());
        }

        // These clones keep the old batches alive while the compacted ones are
        // built.
        let (old, array_pos) = self.store.positional();

        // Plan the move without touching anything: where each live row is now,
        // and where it is going. Keyed by the row's current `(batch_id, row)`
        // rather than by its position in this walk, so the rewrite below is free
        // to visit the heaps in any order — no two slots share a store row, so
        // the key identifies exactly one slot.
        let first_id = self.store.next_batch_id();
        let batch_size = self.batch_size;
        let mut coords: Vec<(usize, usize)> = Vec::with_capacity(self.live_slots);
        let mut moved: HashMap<(u32, u32), (u32, u32)> =
            HashMap::with_capacity(self.live_slots);
        for heap in self.heaps.values() {
            for slot in heap.slots() {
                let pos = *array_pos
                    .get(&slot.batch_id)
                    .expect("a live slot's batch_id is present in the store");
                let moved_to = coords.len();
                coords.push((pos, slot.row as usize));
                moved.insert(
                    (slot.batch_id, slot.row),
                    (
                        first_id + (moved_to / batch_size) as u32,
                        (moved_to % batch_size) as u32,
                    ),
                );
            }
        }
        debug_assert_eq!(
            coords.len(),
            self.live_slots,
            "live_slots must count exactly the slots the heaps hold"
        );
        debug_assert_eq!(
            moved.len(),
            coords.len(),
            "two slots must not share a store row"
        );

        // The only fallible step, and nothing has been mutated yet: an error
        // here leaves both the heaps and the store exactly as they were.
        let refs: Vec<&RecordBatch> = old.iter().collect();
        let mut compacted: Vec<(RecordBatch, usize)> =
            Vec::with_capacity(coords.len().div_ceil(batch_size));
        for chunk in coords.chunks(batch_size) {
            compacted.push((interleave_record_batch(&refs, chunk)?, chunk.len()));
        }
        drop(refs);
        drop(old);

        // Infallible from here, so the store's ids and the slots agree again.
        for heap in self.heaps.values_mut() {
            heap.repoint(|slot| {
                let (batch_id, row) = moved[&(slot.batch_id, slot.row)];
                slot.batch_id = batch_id;
                slot.row = row;
            });
        }
        self.store.clear();
        for (chunk_idx, (batch, uses)) in compacted.into_iter().enumerate() {
            let mut entry = self.store.register(batch);
            debug_assert_eq!(
                entry.id,
                first_id + chunk_idx as u32,
                "the ids handed to the slots must be the ids the chunks got"
            );
            entry.uses = uses;
            self.store.insert(entry);
        }
        Ok(())
    }

    /// Drain all heaps in partition-key order and return the rows as
    /// a stream of `RecordBatch`es ordered by
    /// `(partition_keys, order_keys)`.
    ///
    /// Retained rows live in the shared store as `(batch_id, row)` references,
    /// so this resolves them to `(array_pos, row)` pairs and interleaves them
    /// out in `batch_size` chunks. Because the chunking happens here the output
    /// batches are already the right size and no coalescing pass is needed.
    ///
    /// Only the ordering is done eagerly; the interleave happens one chunk per
    /// poll, which keeps `P × K` rows' worth of *output* arrays from existing at
    /// once. The store's batches stay pinned for the whole stream either way, so
    /// the reservation is carried into [`EmitState`] and released when the stream
    /// is dropped rather than when this returns.
    pub(crate) fn emit(self) -> Result<SendableRecordBatchStream> {
        let Self {
            schema,
            metrics,
            reservation,
            expr: _,
            row_converter: _,
            scratch_rows: _,
            partition_exprs: _,
            partition_converter: _,
            partition_scratch_rows: _,
            heaps,
            store,
            admitted_rows: _,
            live_slots: _,
            heaps_bytes: _,
            index_bytes: _,
            k: _,
            batch_size,
        } = self;
        let timer = metrics.baseline.elapsed_compute().timer();

        // Map order is arbitrary, so partition-key order has to be recovered
        // explicitly here.
        let mut sorted_groups: Vec<(Vec<u8>, PartitionHeap)> =
            heaps.into_iter().collect();
        sorted_groups.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        // The batches outlive the store, which is dropped as this returns.
        let (batches, batch_id_array_pos) = store.positional();

        // Flattened in output order, so the emit itself is a slice walk.
        let mut ordered: Vec<(usize, usize)> = Vec::new();
        for (_key, mut heap) in sorted_groups {
            for slot in heap.drain_sorted() {
                let array_pos = *batch_id_array_pos
                    .get(&slot.batch_id)
                    .expect("a retained slot's batch_id is present in the store");
                ordered.push((array_pos, slot.row as usize));
            }
        }
        drop(timer);

        // What survives this function is the store's batches — pinned until the
        // returned stream is dropped — plus `ordered`, one 16-byte pair per
        // retained row. Everything else the operator held (both scratch
        // buffers, every heap and its interned key) is freed above, so the
        // resize below is normally a shrink. The reservation moves into the
        // stream state rather than being dropped here: releasing it while the
        // store is still pinned would stop accounting for bytes that are still
        // held, which is the one direction that matters.
        let pinned_bytes = store.batches_size;
        reservation.try_resize(
            size_of::<EmitState>()
                + pinned_bytes
                + batches.capacity() * size_of::<RecordBatch>()
                + ordered.capacity() * size_of::<(usize, usize)>(),
        )?;

        let state = EmitState {
            metrics,
            _reservation: reservation,
            batch_size,
            batches,
            ordered,
            pos: 0,
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::try_unfold(state, |mut state| async move {
                Ok(state.next_batch()?.map(|batch| (batch, state)))
            }),
        )))
    }

    /// Total memory currently held by this operator, including all
    /// per-partition heaps and every batch the store still pins.
    ///
    /// Every term is O(1): this runs on every batch, so the per-partition
    /// contributions are the running totals `heaps_bytes` and `index_bytes`
    /// rather than a sum over partitions.
    fn size(&self) -> usize {
        size_of::<Self>()
            + self.row_converter.size()
            + self.partition_converter.size()
            + self.scratch_rows.size()
            + self.partition_scratch_rows.size()
            + self.admitted_rows.allocated_size()
            + self.heaps.capacity() * (size_of::<Vec<u8>>() + size_of::<PartitionHeap>())
            + self.heaps_bytes
            + self.index_bytes
            + self.store.size()
    }
}

/// Hands out `batch_size` rows at a time, interleaved out of the store batches
/// the heaps referenced.
struct EmitState {
    metrics: TopKMetrics,
    /// Covers the pinned batches and `ordered` for as long as they are held.
    ///
    /// Carried here rather than dropped at the end of `emit` so the bytes stay
    /// accounted for until the stream is, and released by this struct's drop.
    /// Never resized: the store's batches are pinned until the last chunk, so
    /// there is nothing to hand back as chunks are emitted. Underscored because
    /// it is held only for that drop, as in `HashJoinStream` and `AsofJoinStream`.
    _reservation: MemoryReservation,
    batch_size: usize,
    /// The store's batches, positionally indexed by `ordered`.
    batches: Vec<RecordBatch>,
    /// `(array_pos, row)` pairs in `(partition_keys, order_keys)` order.
    ordered: Vec<(usize, usize)>,
    pos: usize,
}

impl EmitState {
    /// Build the next chunk, or `None` once every retained row has been
    /// emitted.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.pos == self.ordered.len() {
            return Ok(None);
        }
        let _timer = self.metrics.baseline.elapsed_compute().timer();

        let end = (self.pos + self.batch_size).min(self.ordered.len());
        let chunk = &self.ordered[self.pos..end];
        self.pos = end;

        let refs: Vec<&RecordBatch> = self.batches.iter().collect();
        let batch = interleave_record_batch(&refs, chunk)?;
        (&batch).record_output(&self.metrics.baseline);
        Ok(Some(batch))
    }
}

/// Rows that tied at the boundary when inserted, materialized into a
/// batch holding *only* those rows.
///
/// The rows are gathered eagerly rather than kept as `(source_batch,
/// indices)`: a tie entry lives until the boundary moves, so holding the
/// source batch would pin an entire input batch — and charge for it —
/// for as long as a single row of it stays tied. With ties spread across
/// many input batches that makes retained memory grow with the *input*
/// size instead of with `K + ties`.
#[derive(Debug)]
struct TieEntry {
    /// The tied rows, and nothing else. Always non-empty by construction.
    batch: RecordBatch,
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
        let ties_contents: usize = self.ties.iter().map(|t| t.batch_bytes).sum();
        self.heap.size() + ties_buffer + ties_contents
    }

    /// Push `batch`'s rows onto the tie list, charging exactly their bytes.
    fn push_ties(&mut self, batch: RecordBatch) {
        let batch_bytes = get_record_batch_memory_size(&batch);
        self.ties.push(TieEntry { batch, batch_bytes });
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
    /// One rank state per distinct partition key seen so far. Keyed by
    /// the row-encoded PARTITION BY bytes (a byte-comparable encoding, so
    /// the `Vec<u8>` hashes, compares, and sorts identically to an
    /// `OwnedRow`) which lets `insert_batch` look partitions up with
    /// `entry_ref` — allocating a key only on first sight of a partition
    /// rather than once per row.
    states: HashMap<Vec<u8>, RankPartitionState>,
    /// Scratch map reused across `insert_batch` calls to group a batch's
    /// row indices by partition key. Drained (not reallocated) each batch
    /// so its backing table is allocated once, not per batch.
    partition_groups: HashMap<Vec<u8>, Vec<u32>>,
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
            partition_groups: HashMap::new(),
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

        // 2. Demultiplex row indices by partition key (per-batch).
        //    `partition_groups` is a reused scratch map: taken out here and
        //    drained below, so its backing table is allocated once for the
        //    operator, not once per batch. `entry_ref` owns the key only on
        //    Vacant, so it allocates one `Vec<u8>` per distinct partition
        //    rather than one per row.
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

        for (pk, indices) in groups.drain() {
            let state = self.states.entry(pk).or_insert_with(|| RankPartitionState {
                heap: TopKHeap::new(k),
                ties: Vec::new(),
            });

            // Once the heap is full, a group whose rows are *all* strictly
            // worse than the boundary changes neither the heap nor the
            // ties. Bail before the gather below — at high partition
            // cardinality this is the common case.
            if let Some(max_row) = state.heap.max() {
                let boundary = max_row.row();
                if indices
                    .iter()
                    .all(|&i| self.scratch_rows.row(i as usize).as_ref() > boundary)
                {
                    continue;
                }
            }

            // Gather this partition's rows into their own batch, as
            // `PartitionedTopK` does. Registering the whole input batch
            // instead would pin it — and charge for it — once per
            // partition key present in the batch, so a batch spanning P
            // partitions would be counted P times over.
            let indices_arr = UInt32Array::from(indices);
            let sub_batch = take_record_batch(batch, &indices_arr)?;

            // Indices *into `sub_batch`* of rows from this batch that tied
            // at the boundary. Coalesced into a single tie entry at the end
            // of the partition's loop. Discarded if the boundary moves up
            // mid-loop (those rows were tied to the old boundary, which is
            // now strictly worse than the new K-th).
            let mut equal_indices: Vec<u32> = Vec::new();
            // Lazy-registered: only attached if at least one row reaches
            // the heap from this batch in this partition.
            let mut heap_entry: Option<RecordBatchEntry> = None;

            for (sub_idx, &orig_idx) in indices_arr.values().iter().enumerate() {
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
                        equal_indices.push(sub_idx as u32);
                    }
                    // Strictly worse than the current boundary: drop the row.
                    Some(Ordering::Greater) => {}
                    Some(Ordering::Less) | None => {
                        // Heap path: heap not yet full, or row strictly
                        // better than the current boundary.
                        let entry_ref = heap_entry.get_or_insert_with(|| {
                            state.heap.register_batch(sub_batch.clone())
                        });
                        if let Some(EvictedRow {
                            batch: evicted_batch,
                            index: evicted_index,
                            row_bytes: evicted_bytes,
                        }) = state.heap.add(entry_ref, row, sub_idx)
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
                                // Boundary unchanged — the evicted row is
                                // still tied at the boundary. Gather just
                                // that row: holding `evicted_batch` would
                                // keep a whole heap batch alive for one row,
                                // and one such entry per input batch would
                                // again make memory grow with the input.
                                let one = UInt32Array::from(vec![evicted_index as u32]);
                                state.push_ties(take_record_batch(&evicted_batch, &one)?);
                            }
                        }
                        replacements += 1;
                    }
                }
            }

            let registered_with_heap = heap_entry.is_some();
            if let Some(e) = heap_entry {
                state.heap.insert_batch_entry(e);
                state.heap.maybe_compact()?;
            }

            // Commit this batch's ties as a single entry.
            if !equal_indices.is_empty() {
                // No row of this group reached the heap, so `sub_batch` is
                // not registered there and reusing it here cannot
                // double-charge it. Combined with every row having tied,
                // `sub_batch` already *is* exactly the tie rows — the
                // gather below would just copy it.
                let tie_batch = if !registered_with_heap
                    && equal_indices.len() == sub_batch.num_rows()
                {
                    sub_batch
                } else {
                    take_record_batch(&sub_batch, &UInt32Array::from(equal_indices))?
                };
                state.push_ties(tie_batch);
            }
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
            partition_groups: _,
            k: _,
            batch_size,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer();

        let mut sorted_pks: Vec<Vec<u8>> = states.keys().cloned().collect();
        sorted_pks.sort();

        let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);

        for pk in sorted_pks {
            let RankPartitionState { mut heap, ties } =
                states.remove(&pk).expect("key from states.keys()");
            if let Some(batch) = heap.emit()? {
                coalescer.push_batch(batch)?;
            }
            for tie in ties {
                coalescer.push_batch(tie.batch)?;
            }
        }
        coalescer.finish_buffered_batch()?;

        let mut out: Vec<Result<RecordBatch>> = Vec::new();
        while let Some(b) = coalescer.next_completed_batch() {
            (&b).record_output(&metrics.baseline);
            out.push(Ok(b));
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(out),
        )))
    }

    /// Total memory currently held, including all per-partition states.
    fn size(&self) -> usize {
        // Per partition: the state plus the encoded partition key owned by
        // the map. The key bytes are a heap allocation the table slot
        // doesn't cover.
        let states_contents: usize = self
            .states
            .iter()
            .map(|(pk, state)| pk.capacity() + state.size())
            .sum();
        size_of::<Self>()
            + self.row_converter.size()
            + self.partition_converter.size()
            + self.scratch_rows.size()
            + self.partition_scratch_rows.size()
            + states_contents
            + self.states.capacity()
                * (size_of::<Vec<u8>>() + size_of::<RankPartitionState>())
            + self.partition_groups.capacity()
                * (size_of::<Vec<u8>>() + size_of::<Vec<u32>>())
    }
}

/// A run of rows from a single source [`RecordBatch`] sharing one
/// distinct ORDER BY value. Materialized at emit time via
/// [`take_record_batch`].
///
/// The batch is referenced by `batch_id` rather than held directly, so the
/// operator-scoped [`RecordBatchStore`] can charge each distinct source
/// batch once however many entries reference it. Holding a batch per entry
/// and charging its bytes per entry would inflate the reservation by a
/// factor of (partitions × K), since a single batch contributes an entry to
/// every partition and ob group it touches.
#[derive(Debug)]
struct GroupEntry {
    /// Indices into the batch identified by `batch_id`. Always non-empty
    /// by construction.
    row_indices: Vec<u32>,
    /// Key into `PartitionedTopKDenseRank::store`.
    batch_id: u32,
}

/// Per-partition state for `DENSE_RANK()` semantics.
///
/// A `HashMap<Vec<u8>, Vec<GroupEntry>>` keyed by the row-encoded ORDER
/// BY bytes, capped at `k` distinct keys. Each key's `Vec<GroupEntry>`
/// holds every row seen at that ob value, one entry per contributing
/// source `RecordBatch`.
#[derive(Default)]
struct DenseRankPartitionState {
    groups: HashMap<Vec<u8>, Vec<GroupEntry>>,
    /// The same keys as `groups`, in a max-heap: the admission boundary
    /// (the largest tracked ob value) is an O(1) `peek()` check, and
    /// admission / removal are O(log K).
    ///
    /// INVARIANT: `keys` and `groups.keys()` hold the same set. Every
    /// insertion into / removal from `groups` must mirror into `keys`.
    keys: BinaryHeap<Vec<u8>>,
}

impl DenseRankPartitionState {
    fn size(&self) -> usize {
        let table_overhead = self.groups.capacity()
            * (size_of::<Vec<u8>>() + size_of::<Vec<GroupEntry>>());
        let contents: usize = self
            .groups
            .iter()
            .map(|(key, entries)| {
                key.capacity()
                    + entries.capacity() * size_of::<GroupEntry>()
                    + entries
                        .iter()
                        .map(|e| e.row_indices.capacity() * size_of::<u32>())
                        .sum::<usize>()
            })
            .sum();
        // `keys` duplicates every key's bytes; charge for them plus the
        // heap's backing Vec (one `Vec<u8>` slot per reserved element).
        let keys_overhead: usize = self.keys.capacity() * size_of::<Vec<u8>>()
            + self.keys.iter().map(|k| k.capacity()).sum::<usize>();
        table_overhead + contents + keys_overhead
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
/// partitions for this operator instance. So is the
/// [`RecordBatchStore`]: retained rows are held as `(batch_id, indices)`
/// so each source batch is charged once for the whole operator, however
/// many partitions and ob groups reference it.
///
/// # Algorithm (per batch)
///
/// Evaluate + encode partition-by and order-by columns once, then group
/// the batch's row indices by partition key. For each partition, bucket
/// that partition's rows by distinct ob value (a within-call
/// accumulation), then merge each bucket into the partition state. Every
/// bucket is built from the current batch's rows, so each `GroupEntry` is
/// pinned to the batch its `row_indices` point into.
///
/// For each partition, for each distinct `ob_key` run in this batch:
/// - `ob_key` already in `state.groups` → push this batch's run as a
///   new `GroupEntry` (one entry per contributing batch).
/// - `ob_key` new, `state.groups.len() < k` → insert the run as a new
///   group.
/// - `ob_key` new, `state.groups.len() == k` → the largest tracked ob
///   value is the admission boundary, read from the `state.keys` max-heap
///   in O(1):
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
    /// Source batches referenced by the retained `GroupEntry`s, held once
    /// for the whole operator with a use count per batch. This is what
    /// keeps the reservation proportional to the batches actually pinned
    /// rather than to the number of entries pointing at them.
    store: RecordBatchStore,
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
            store: RecordBatchStore::new(),
            k,
            batch_size,
        })
    }

    /// Encode PARTITION BY and ORDER BY columns once, demultiplex the
    /// batch's rows by partition key, then per partition bucket the rows
    /// by distinct ob value and merge each bucket into the partition
    /// state as one [`GroupEntry`].
    pub(crate) fn insert_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Register this batch with the store up front. `uses` is bumped
        // once per `GroupEntry` created below but lives on this local
        // entry, so the store sees a single insert per batch rather than
        // one per group — and `insert` drops batches that retained
        // nothing without ever charging for them.
        let mut batch_entry = self.store.register(batch.clone());
        let batch_id = batch_entry.id;

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
        //    partition state as a single `GroupEntry`.
        for (pk, indices) in groups.drain() {
            let state = self
                .states
                .entry(pk)
                .or_insert_with(DenseRankPartitionState::default);

            // Bucket by ob key. `ob_runs` is a reused scratch map (taken
            // out and drained below) so its backing table is allocated
            // once, not once per distinct partition key. `entry_ref` owns
            // the key only on Vacant, so repeated rows of the same ob
            // value don't re-allocate.
            let mut runs = std::mem::take(&mut self.ob_runs);
            runs.clear();
            // Once the partition holds its full K distinct ob values, the
            // largest of them is the bar a new value must beat, and that bar
            // only ever gets stricter: with K values held there is no free
            // slot left for a new (possibly larger) one, and the only way in
            // from here is to evict that largest and put something strictly
            // smaller in its place. So a row worse than today's bar is worse
            // than every later bar too — it is not held now and can never be
            // admitted — and can be dropped before it costs a bucket. Rows
            // *equal* to the bar must still go through: that value is one of
            // the K being held, so its rows belong to a live group.
            let boundary: Option<&[u8]> = if state.groups.len() == k {
                state.keys.peek().map(Vec::as_slice)
            } else {
                None
            };
            for &orig_idx in &indices {
                let ob_row = self.scratch_rows.row(orig_idx as usize);
                if boundary.is_some_and(|b| ob_row.as_ref() > b) {
                    continue;
                }
                runs.entry_ref(ob_row.as_ref()).or_default().push(orig_idx);
            }

            for (ob_key, run_indices) in runs.drain() {
                // Case A: ob already tracked — push this batch's run as a
                // new `GroupEntry` (one entry per contributing batch).
                if let Some(entries) = state.groups.get_mut(&ob_key) {
                    batch_entry.uses += 1;
                    entries.push(GroupEntry {
                        row_indices: run_indices,
                        batch_id,
                    });
                    continue;
                }

                // Case B: new ob, room available.
                if state.groups.len() < k {
                    batch_entry.uses += 1;
                    state.keys.push(ob_key.clone());
                    state.groups.insert(
                        ob_key,
                        vec![GroupEntry {
                            row_indices: run_indices,
                            batch_id,
                        }],
                    );
                    continue;
                }

                // Case C: new ob, at K distinct keys. The largest tracked
                // ob value is the admission boundary.
                let max_key = state.keys.peek().expect("state.groups has k >= 1 keys");
                if ob_key.as_slice() < max_key.as_slice() {
                    // Evict the entire max-key group, from both the map
                    // and its ordered mirror.
                    let evicted_key = state.keys.pop().expect("max key present");
                    let evicted = state
                        .groups
                        .remove(&evicted_key)
                        .expect("keys mirrors groups");
                    for e in &evicted {
                        replacements += e.row_indices.len();
                        if e.batch_id == batch_id {
                            // Admitted earlier in this same call, so the
                            // store has not seen `batch_entry` yet — drop
                            // the pending use rather than calling `unuse`,
                            // which panics on an unregistered id.
                            batch_entry.uses -= 1;
                        } else {
                            self.store.unuse(e.batch_id);
                        }
                    }
                    batch_entry.uses += 1;
                    state.keys.push(ob_key.clone());
                    state.groups.insert(
                        ob_key,
                        vec![GroupEntry {
                            row_indices: run_indices,
                            batch_id,
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

        // Charges `batch` once if any group retained rows from it.
        self.store.insert(batch_entry);

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
            store,
            k: _,
            batch_size,
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer();

        let mut sorted_pks: Vec<Vec<u8>> = states.keys().cloned().collect();
        sorted_pks.sort();

        let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);

        for pk in sorted_pks {
            let DenseRankPartitionState { groups, keys: _ } =
                states.remove(&pk).expect("key from states.keys()");
            // Sort the <= K distinct ob keys so rows emit ascending
            // (byte-comparable encoding == sort order).
            let mut sorted_obs: Vec<(Vec<u8>, Vec<GroupEntry>)> =
                groups.into_iter().collect();
            sorted_obs.sort_by(|a, b| a.0.cmp(&b.0));
            for (_ob, entries) in sorted_obs {
                for entry in entries {
                    let batch = &store
                        .get(entry.batch_id)
                        .expect("retained batch_id present in store")
                        .batch;
                    let indices = UInt32Array::from(entry.row_indices);
                    let sub = take_record_batch(batch, &indices)?;
                    coalescer.push_batch(sub)?;
                }
            }
        }
        coalescer.finish_buffered_batch()?;

        let mut out: Vec<Result<RecordBatch>> = Vec::new();
        while let Some(b) = coalescer.next_completed_batch() {
            (&b).record_output(&metrics.baseline);
            out.push(Ok(b));
        }

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(out),
        )))
    }

    /// Total memory currently held, including all per-partition states.
    fn size(&self) -> usize {
        // Per partition: the state itself plus the encoded partition key
        // owned by the map. The key bytes are a heap allocation the table
        // slot doesn't cover, and with wide or numerous partition keys
        // they dominate the fixed-size slots.
        let states_contents: usize = self
            .states
            .iter()
            .map(|(pk, state)| pk.capacity() + state.size())
            .sum();
        // `partition_groups` and `ob_runs` are drained, not dropped, so
        // their backing tables outlive every `insert_batch` call. Both are
        // empty by the time `size()` runs (drained above), so only the
        // retained capacity is charged.
        let scratch_tables = self.partition_groups.capacity()
            * (size_of::<Vec<u8>>() + size_of::<Vec<u32>>())
            + self.ob_runs.capacity() * (size_of::<Vec<u8>>() + size_of::<Vec<u32>>());
        size_of::<Self>()
            + self.row_converter.size()
            + self.partition_converter.size()
            + self.scratch_rows.size()
            + self.partition_scratch_rows.size()
            + states_contents
            + self.states.capacity()
                * (size_of::<Vec<u8>>() + size_of::<DenseRankPartitionState>())
            + scratch_tables
            + self.store.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::MetricValue;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SortOptions;
    use datafusion_common::{assert_batches_eq, exec_datafusion_err};
    use datafusion_execution::memory_pool::GreedyMemoryPool;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
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

    /// Reads the `output_batches` and `output_rows` metrics recorded by an emit.
    fn output_batches_and_rows(metrics: &ExecutionPlanMetricsSet) -> (usize, usize) {
        let metrics = metrics.clone_inner();
        let batches = metrics
            .sum(|m| matches!(m.value(), MetricValue::OutputBatches(_)))
            .expect("output_batches metric")
            .as_usize();
        (batches, metrics.output_rows().expect("output_rows metric"))
    }

    /// Regression test for #24468: `emit` splits the heap's single batch into
    /// `batch_size` chunks, so `output_batches` must count each emitted chunk
    /// rather than the one pre-split batch.
    #[tokio::test]
    async fn test_topk_output_batches_metric_counts_emitted_batches() -> Result<()> {
        let schema = make_ab_schema();
        let metrics = ExecutionPlanMetricsSet::new();
        let sort_expr = PhysicalSortExpr {
            expr: col("a", schema.as_ref())?,
            options: SortOptions::default(),
        };
        // k = 5 with batch_size = 2 => emitted batches of [2, 2, 1]
        let mut topk = TopK::try_new(
            0,
            Arc::clone(&schema),
            vec![],
            LexOrdering::from([sort_expr]),
            5,
            2,
            Arc::new(RuntimeEnv::default()),
            &metrics,
            make_topk_filter(),
        )?;

        topk.insert_batch(make_ab_batch(
            Arc::clone(&schema),
            &[Some(5), Some(4), Some(3), Some(2), Some(1), Some(0)],
            &[0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
        )?)?;

        let results: Vec<_> = topk.emit()?.try_collect().await?;
        let row_counts: Vec<usize> = results.iter().map(|b| b.num_rows()).collect();
        assert_eq!(row_counts, vec![2, 2, 1]);
        assert_eq!(output_batches_and_rows(&metrics), (3, 5));

        Ok(())
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
        build_partitioned_topk_inner(
            k,
            val_sort_options,
            val_nullable,
            &Arc::new(RuntimeEnv::default()),
        )
    }

    /// Variant of [`build_partitioned_topk`] that runs against a caller-supplied
    /// [`RuntimeEnv`], so a test can bound the memory pool.
    fn build_partitioned_topk_with_runtime(
        k: usize,
        runtime: &Arc<RuntimeEnv>,
    ) -> Result<(Arc<Schema>, PartitionedTopK)> {
        build_partitioned_topk_inner(k, SortOptions::default(), false, runtime)
    }

    fn build_partitioned_topk_inner(
        k: usize,
        val_sort_options: SortOptions,
        val_nullable: bool,
        runtime: &Arc<RuntimeEnv>,
    ) -> Result<(Arc<Schema>, PartitionedTopK)> {
        let schema = pk_val_schema(val_nullable);

        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let pk_sort_expr = PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        };
        let val_sort_expr = PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: val_sort_options,
        };

        let partition_ordering = vec![pk_sort_expr];
        let order_expr = LexOrdering::from([val_sort_expr]);

        let state = PartitionedTopK::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            build_sort_fields(&partition_ordering, &schema)?,
            order_expr,
            k,
            8, // batch_size
            runtime,
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok((schema, state))
    }

    /// Partition-key nullability is a distinct concern from ORDER BY
    /// nullability: the key layer decides whether NULL keys collapse
    /// into one partition, and emit-time ordering decides where that
    /// partition lands relative to the non-NULL ones.
    fn build_partitioned_topk_nullable_pk(
        k: usize,
        pk_sort_options: SortOptions,
    ) -> Result<(Arc<Schema>, PartitionedTopK)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, true),
            Field::new("val", DataType::Int32, false),
        ]));

        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let partition_ordering = [PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: pk_sort_options,
        }];
        let order_expr = LexOrdering::from([PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: SortOptions::default(),
        }]);

        let state = PartitionedTopK::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            build_sort_fields(&partition_ordering, &schema)?,
            order_expr,
            k,
            8, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok((schema, state))
    }

    fn nullable_pk_batch(
        schema: &Arc<Schema>,
        pks: Vec<Option<i32>>,
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

    /// A NULL partition key is a partition like any other: every NULL-keyed
    /// row belongs to the *same* partition, and that partition gets its own
    /// K-row heap. With `NULLS LAST` on the partition key it emits after
    /// every non-NULL partition.
    #[tokio::test]
    async fn test_partitioned_topk_null_partition_key_nulls_last() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_nullable_pk(
            2,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )?;

        // pk=NULL vals: 9, 3, 5 → top-2 ASC = [3, 5]   (one partition, not three)
        // pk=1    vals: 4, 2    → top-2      = [2, 4]
        // pk=2    vals: 8       → top-2      = [8]
        let batch = nullable_pk_batch(
            &schema,
            vec![None, Some(1), None, Some(2), None, Some(1)],
            vec![9, 4, 3, 8, 5, 2],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 2   |",
                "| 1  | 4   |",
                "| 2  | 8   |",
                "|    | 3   |",
                "|    | 5   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// Companion to [`test_partitioned_topk_null_partition_key_nulls_last`]
    /// with `NULLS FIRST` on the partition key: the same NULL partition must
    /// now emit *before* every non-NULL one. Also splits the NULL key across
    /// two batches to prove it interns to a stable group.
    #[tokio::test]
    async fn test_partitioned_topk_null_partition_key_nulls_first() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_nullable_pk(
            2,
            SortOptions {
                descending: false,
                nulls_first: true,
            },
        )?;

        // First batch sees pk=1 before any NULL, so the NULL partition is
        // interned second — emit must still place it first.
        state.insert_batch(&nullable_pk_batch(
            &schema,
            vec![Some(1), None, Some(1)],
            vec![4, 9, 2],
        )?)?;
        // Second batch must land in the *same* NULL partition and evict 9.
        state.insert_batch(&nullable_pk_batch(
            &schema,
            vec![None, None, Some(2)],
            vec![3, 5, 8],
        )?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "|    | 3   |",
                "|    | 5   |",
                "| 1  | 2   |",
                "| 1  | 4   |",
                "| 2  | 8   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// A `DESC` partition key must emit partitions in descending key order.
    /// Group indices are assigned in first-seen order, so this pins the
    /// partition key's `SortOptions` really being folded into the encoding
    /// the emit-time sort reads, rather than defaulting to ascending.
    #[tokio::test]
    async fn test_partitioned_topk_desc_partition_key_ordering() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_nullable_pk(
            1,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )?;

        let batch = nullable_pk_batch(
            &schema,
            vec![Some(1), Some(3), Some(2), Some(3)],
            vec![10, 30, 20, 31],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 3  | 30  |",
                "| 2  | 20  |",
                "| 1  | 10  |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// A multi-column partition key is encoded as one concatenated byte
    /// string, so emit ordering becomes a lexicographic comparison of those
    /// bytes across both columns rather than of a single column's.
    #[tokio::test]
    async fn test_partitioned_topk_multi_column_partition_key() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk_a", DataType::Int32, false),
            Field::new("pk_b", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]));

        let pk_a: Arc<dyn PhysicalExpr> = col("pk_a", schema.as_ref())?;
        let pk_b: Arc<dyn PhysicalExpr> = col("pk_b", schema.as_ref())?;
        let partition_ordering = [
            PhysicalSortExpr {
                expr: Arc::clone(&pk_a),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::clone(&pk_b),
                options: SortOptions::default(),
            },
        ];
        let order_expr = LexOrdering::from([PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: SortOptions::default(),
        }]);

        let mut state = PartitionedTopK::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_a, pk_b],
            build_sort_fields(&partition_ordering, &schema)?,
            order_expr,
            1,
            8,
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![2, 1, 2, 1, 1])),
                Arc::new(StringArray::from(vec!["b", "b", "a", "a", "a"])),
                Arc::new(Int32Array::from(vec![20, 15, 25, 11, 10])),
            ],
        )?;
        state.insert_batch(&batch)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+------+------+-----+",
                "| pk_a | pk_b | val |",
                "+------+------+-----+",
                "| 1    | a    | 10  |",
                "| 1    | b    | 15  |",
                "| 2    | a    | 25  |",
                "| 2    | b    | 20  |",
                "+------+------+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// `PartitionedTopK` must run under a memory pool sized from `K` and the
    /// partition count, not from the input.
    ///
    /// Retained rows reference batches in the shared store, so what stays
    /// pinned is data-dependent in a way that `K` alone does not bound — this
    /// is the test that holds it down. A store entry keeps only the rows that
    /// batch contributed and is freed when its last row is evicted, so pinning
    /// converges instead of tracking the input; feeding 16 fully-admitted
    /// batches and asserting the total stays flat is what pins that.
    ///
    /// Sibling of `test_partitioned_topk_rank_runs_under_bounded_memory_pool`
    /// for the `ROW_NUMBER` path, which had no bounded-pool coverage.
    #[tokio::test]
    async fn test_partitioned_topk_runs_under_bounded_memory_pool() -> Result<()> {
        const P: i32 = 256;
        const ROWS_PER_PARTITION: i32 = 32;
        const ROWS: i32 = P * ROWS_PER_PARTITION;
        const BATCHES: i32 = 16;
        const K: usize = 2;

        let schema = pk_val_schema(false);
        let pks: Vec<i32> = (0..ROWS).map(|i| i % P).collect();
        // Batch b's values all beat batch b-1's, so every row of every batch is
        // admitted and every admission evicts. That is the maximum churn this
        // design can be put under -- a gather of the whole batch 16 times over,
        // every one of which must be handed back to the allocator as the next
        // batch supersedes it -- and it is the case where pinning that tracked
        // the input rather than converging would be obvious.
        let vals = |b: i32| -> Vec<i32> {
            (0..ROWS)
                .map(|i| (BATCHES - b) * ROWS_PER_PARTITION + i / P)
                .collect()
        };

        // Derived from the data so no byte constant here goes stale. The
        // operator needs a little over 4x the batch, most of it the two
        // row-encoding scratch buffers (partition key, ORDER BY key) sized to
        // hold a whole batch plus the one gathered batch the store pins; 4x
        // fails, so this bound is tight rather than slack. Charging a whole
        // batch per partition, as a per-partition store would, needs ~P = 256x.
        let batch_bytes =
            get_record_batch_memory_size(&pk_val_batch(&schema, pks.clone(), vals(0))?);
        let limit = 5 * batch_bytes;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(limit)))
            .build_arc()?;

        let (schema, mut state) = build_partitioned_topk_with_runtime(K, &runtime)?;

        let mut after_first = 0;
        for b in 0..BATCHES {
            let batch = pk_val_batch(&schema, pks.clone(), vals(b))?;
            state.insert_batch(&batch).map_err(|e| {
                exec_datafusion_err!(
                    "batch {b} of {BATCHES} failed under a {limit}-byte pool: {e}"
                )
            })?;
            if b == 0 {
                after_first = state.size();
            }
        }

        // Flat, not merely under the limit. Every batch was fully admitted, so
        // a store that failed to release a superseded entry would grow linearly
        // in the batch count here; the 2x slack absorbs hash-map and buffer
        // capacity growth but not a batch's worth of pinned rows.
        assert!(
            state.size() <= after_first * 2,
            "retention grew from {after_first} to {} bytes across {BATCHES} \
             batches; it tracks the input rather than K per partition",
            state.size()
        );

        // A pool that was never pressed proves nothing, so pin down the output:
        // K rows per partition, drawn from the last batch, whose values are the
        // smallest seen.
        let expected: Vec<(i32, i32)> = (0..P)
            .flat_map(|pk| [(pk, ROWS_PER_PARTITION), (pk, ROWS_PER_PARTITION + 1)])
            .collect();
        assert_eq!(pk_val_rows(state.emit()?).await?, expected);
        Ok(())
    }

    /// The bytes `emit` leaves pinned must stay accounted for until the stream
    /// is dropped.
    ///
    /// Retained rows reference batches in the shared store, so those batches are
    /// still held after `emit` returns, for as long as the stream lives. An
    /// `emit` that dropped its reservation instead would report zero while
    /// holding all of it — the one direction of accounting error that lets a
    /// pool overcommit. Asserting both sides catches the opposite mistake too:
    /// carrying the reservation but never releasing it.
    #[tokio::test]
    async fn test_partitioned_topk_emit_keeps_pinned_bytes_reserved() -> Result<()> {
        use datafusion_execution::memory_pool::MemoryPool;

        let pool = Arc::new(GreedyMemoryPool::new(16 * 1024 * 1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build_arc()?;

        let (schema, mut state) = build_partitioned_topk_with_runtime(2, &runtime)?;
        let pks: Vec<i32> = (0..64).map(|i| i % 8).collect();
        let vals: Vec<i32> = (0..64).collect();
        state.insert_batch(&pk_val_batch(&schema, pks, vals)?)?;
        assert!(pool.reserved() > 0, "insert_batch must reserve");

        let stream = state.emit()?;
        assert!(
            pool.reserved() > 0,
            "emit released its reservation while the store's batches are still \
             pinned by the stream it returned"
        );

        // Consumes the stream, so it is dropped by the time this returns.
        let results: Vec<RecordBatch> = stream.try_collect().await?;
        assert_eq!(
            pool.reserved(),
            0,
            "the reservation must be released once the stream is dropped"
        );

        let rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 16, "K = 2 for each of 8 partitions");
        Ok(())
    }

    /// `PartitionedTopKRank` must not charge (or pin) a whole input batch
    /// once per partition key that batch touches.
    ///
    /// Regression: the heap used to be handed `batch.clone()` — the full
    /// input batch — rather than a gather of just that partition's rows,
    /// so a batch spanning P partitions was counted P times over. With 500
    /// partitions in one batch that reported ~516x the batch size.
    ///
    /// Measured against the input batch's own size rather than against the
    /// `ROW_NUMBER` state on the same input: the two operators hold their
    /// retained rows differently, so a ratio between them tracks changes to
    /// either one and this test is about `RANK` alone.
    #[tokio::test]
    async fn test_partitioned_topk_rank_size_is_not_per_partition_batch() -> Result<()> {
        // The over-count factor was exactly the number of partitions sharing
        // a batch, so P is what makes the bug visible at all.
        const P: i32 = 500;
        // 8 rows per partition: enough for every partition's k=2 heap to fill
        // and then evict, so the heaps hold real state rather than sitting
        // half-empty in the fill phase.
        const ROWS: i32 = 4000;

        // Distinct values throughout: no ties, so RANK retains exactly K per
        // partition and nothing is held by the tie list. k=2 is the smallest k
        // with a distinct fill phase before eviction begins.
        let pks: Vec<i32> = (0..ROWS).map(|i| i % P).collect();
        let vals: Vec<i32> = (0..ROWS).map(|i| i / P).collect();

        let (schema, mut rk) = build_partitioned_topk_rank(2)?;
        let batch = pk_val_batch(&schema, pks, vals)?;
        let batch_size = get_record_batch_memory_size(&batch);
        rk.insert_batch(&batch)?;
        let rk_size = rk.size();

        // Just under 19x the input batch measured here, against ~516x for the bug:
        // the per-partition scratch buffers and heap headers at P=500 dominate
        // what is retained, which is why the correct figure is a double-digit
        // multiple at all. A 100x bar sits ~5x clear of both, so it neither
        // flakes when the size calculation is legitimately adjusted nor misses a
        // reintroduced per-partition batch charge.
        assert!(
            rk_size <= batch_size * 100,
            "RANK reported {rk_size} bytes holding K=2 rows from each of {P} \
             partitions of a {batch_size}-byte batch; a per-partition batch \
             charge has crept back in"
        );
        Ok(())
    }

    /// A boundary tie must retain only the tied rows, not the batch they
    /// arrived in.
    ///
    /// Regression: `TieEntry` used to hold the source `RecordBatch` plus
    /// indices, so one tied row pinned — and was charged for — a whole
    /// input batch, for as long as the boundary held. Across a stream of
    /// batches that made retained memory grow with the *input* size rather
    /// than with `K + ties`.
    #[tokio::test]
    async fn test_partitioned_topk_rank_ties_do_not_pin_input_batches() -> Result<()> {
        // k=1 makes every retained row after the very first one a boundary
        // tie, which is the state under test.
        let (schema, mut rk) = build_partitioned_topk_rank(1)?;

        // Each batch carries exactly one row tied at the boundary and 999
        // rows that are strictly worse and must be dropped. The 1000:1 ratio
        // is the point: holding the tied row costs a few bytes, holding the
        // batch it arrived in costs ~8 KB, so the two outcomes cannot be
        // confused.
        let mut vals = vec![100; 1000];
        vals[0] = 7;
        let batch = pk_val_batch(&schema, vec![1; 1000], vals)?;

        rk.insert_batch(&batch)?;
        let after_first = rk.size();
        for _ in 0..7 {
            rk.insert_batch(&batch)?;
        }
        let after_eight = rk.size();

        // Seven more batches retain seven more single rows. Correct growth is
        // ~8 bytes per batch (~56 total, just the tie-list slots); the bug grew
        // by a whole ~8 KB batch each time (~56 KB total). A 2000-byte bar sits
        // between the two with more than an order of magnitude of clearance on
        // each side.
        let growth = after_eight - after_first;
        assert!(
            growth < 2000,
            "tie list grew {growth} bytes over 7 batches that contributed \
             1 row each; it is holding the source batches"
        );
        Ok(())
    }

    /// The reservation must clear a *bounded* memory pool, not merely
    /// report a plausible `size()`.
    ///
    /// The two tests above check what `size()` computes; this one checks
    /// how that number is actually spent — `try_resize` against a real
    /// pool, which is what returned `ResourcesExhausted` to the user.
    ///
    /// No byte constant is asserted. The limit is derived from the input
    /// at run time, because the property under test is a ratio: retention
    /// must be a small multiple of *one* input batch no matter how many
    /// batches stream through, where whole-batch retention was
    /// (partitions x batch) and needed ~P times as much. Only the ratio
    /// has to hold as the size accounting is legitimately adjusted.
    ///
    /// Scope: the insert path only. `emit` drops the reservation before
    /// it materializes ties, so emit-time growth is out of reach here.
    #[tokio::test]
    async fn test_partitioned_topk_rank_runs_under_bounded_memory_pool() -> Result<()> {
        // P is the whole point: the old code pinned and charged one copy of
        // each batch per partition that batch touched, so it needed ~P x the
        // batch where the fix needs a constant multiple of it.
        const P: i32 = 256;
        const ROWS_PER_PARTITION: i32 = 32;
        const ROWS: i32 = P * ROWS_PER_PARTITION;
        const BATCHES: i32 = 16;
        const K: usize = 2;

        let schema = pk_val_schema(false);
        let pks: Vec<i32> = (0..ROWS).map(|i| i % P).collect();
        // Batch b's values all beat batch b-1's, so every batch is admitted
        // by every partition and the batch it displaces must be released —
        // the case that separates "charged once per batch" from "charged
        // once per partition per batch". Within a batch a partition's
        // values are distinct, so nothing ties at the boundary and each
        // partition keeps exactly K rows throughout.
        let vals = |b: i32| -> Vec<i32> {
            (0..ROWS)
                .map(|i| (BATCHES - b) * ROWS_PER_PARTITION + i / P)
                .collect()
        };

        // A budget of 32 batches, derived from the data so no byte constant
        // here goes stale. The operator legitimately holds ~8x the batch
        // (row-encoding scratch for one batch, plus per-partition heap and
        // map overhead), so this clears it ~4x over; the old whole-batch
        // retention needed ~P = 256x the batch, which is 8x past the limit.
        let batch_bytes =
            get_record_batch_memory_size(&pk_val_batch(&schema, pks.clone(), vals(0))?);
        let limit = 32 * batch_bytes;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(limit)))
            .build_arc()?;

        let (schema, mut rk) = build_partitioned_topk_rank_with_runtime(K, &runtime)?;

        let mut after_first = 0;
        for b in 0..BATCHES {
            let batch = pk_val_batch(&schema, pks.clone(), vals(b))?;
            rk.insert_batch(&batch).map_err(|e| {
                exec_datafusion_err!(
                    "batch {b} of {BATCHES} failed under a {limit}-byte pool: {e}"
                )
            })?;
            if b == 0 {
                after_first = rk.size();
            }
        }

        // Flat, not merely under the limit: the last 15 batches must not
        // have added retention (in practice this is exactly equal). The 2x
        // slack absorbs map and tie-list capacity growth, but not a batch's
        // worth of pinned rows.
        assert!(
            rk.size() <= after_first * 2,
            "retention grew from {after_first} to {} bytes across {BATCHES} \
             batches; it tracks the input rather than a bounded window of it",
            rk.size()
        );

        // A pool that was never pressed proves nothing, so pin down what the
        // operator produced: K rows per partition, drawn from the last batch,
        // whose values are the smallest seen.
        let expected: Vec<(i32, i32)> = (0..P)
            .flat_map(|pk| [(pk, ROWS_PER_PARTITION), (pk, ROWS_PER_PARTITION + 1)])
            .collect();
        assert_eq!(sorted_pk_val(rk.emit()?).await?, expected);
        Ok(())
    }

    /// One deterministic pseudo-random input shape for the differential
    /// tests below: a `k`, and the `(pks, vals)` batches to feed in.
    ///
    /// Shapes are deliberately tiny. These bugs live in how ties, partitions
    /// and batch boundaries interleave, not in volume, so many small shapes
    /// cover far more of that space than a few large ones — and they keep the
    /// O(n^2) brute-force reference cheap.
    struct DiffShape {
        seed: u64,
        k: usize,
        n_partitions: i32,
        n_values: i32,
        batches: Vec<(Vec<i32>, Vec<i32>)>,
    }

    impl std::fmt::Display for DiffShape {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "seed={} k={} partitions={} values={} batches={}",
                self.seed,
                self.k,
                self.n_partitions,
                self.n_values,
                self.batches.len()
            )
        }
    }

    impl DiffShape {
        /// `max_values` caps the ORDER BY value domain. Keep it near `k` so
        /// rows tie above, at, and below the boundary frequently rather than
        /// by chance; raise it for an operator that bounds *distinct* values
        /// and so needs more than `k` of them before it will evict anything.
        fn new(seed: u64, max_values: i32) -> Self {
            use rand::rngs::StdRng;
            use rand::{Rng, SeedableRng};

            let mut rng = StdRng::seed_from_u64(seed);
            let k = rng.random_range(1..5usize);
            let n_partitions = rng.random_range(1..4i32);
            let n_values = rng.random_range(1..max_values);
            let n_batches = rng.random_range(1..5usize);

            let batches = (0..n_batches)
                .map(|_| {
                    let rows = rng.random_range(1..12usize);
                    let pks = (0..rows)
                        .map(|_| rng.random_range(0..n_partitions))
                        .collect();
                    let vals = (0..rows).map(|_| rng.random_range(0..n_values)).collect();
                    (pks, vals)
                })
                .collect();

            Self {
                seed,
                k,
                n_partitions,
                n_values,
                batches,
            }
        }

        /// Every `(pk, val)` fed in, across all batches.
        fn all_rows(&self) -> Vec<(i32, i32)> {
            self.batches
                .iter()
                .flat_map(|(pks, vals)| pks.iter().copied().zip(vals.iter().copied()))
                .collect()
        }

        /// Brute-force reference for `ROW_NUMBER`: per partition, the `k`
        /// smallest values by the ORDER BY key.
        ///
        /// Deliberately not expressed through [`Self::expected`]. That ranks a
        /// row by how many rows precede it, so with `k = 2` and values
        /// `[5, 5, 5]` every row ranks 1 and a `<= k` filter keeps all three —
        /// `RANK` semantics, not `ROW_NUMBER`, which keeps exactly two.
        ///
        /// Comparing `(pk, val)` pairs makes *which* of a tied group is kept
        /// unobservable, so this reference stays deterministic despite
        /// `DiffShape` generating ties on purpose.
        ///
        /// Built in emit order rather than sorted at the end: the `BTreeMap`
        /// walks partitions ascending and each partition's values are sorted
        /// ascending, which is exactly `(partition_key, order_key)` for the
        /// ASC/ASC sort options `build_partitioned_topk` uses. That makes this
        /// usable with [`pk_val_rows`], so the differential test checks the
        /// emitted order and not just the set.
        fn expected_row_number(&self) -> Vec<(i32, i32)> {
            let mut by_pk: std::collections::BTreeMap<i32, Vec<i32>> =
                std::collections::BTreeMap::new();
            for (pk, val) in self.all_rows() {
                by_pk.entry(pk).or_default().push(val);
            }
            let mut kept: Vec<(i32, i32)> = Vec::new();
            for (pk, mut vals) in by_pk {
                vals.sort_unstable();
                kept.extend(vals.into_iter().take(self.k).map(|v| (pk, v)));
            }
            kept
        }

        /// Brute-force reference: the sorted rows a `<= k` filter keeps, given
        /// `rank_of(all_rows, pk, val)` for the ranking function under test.
        fn expected(
            &self,
            rank_of: impl Fn(&[(i32, i32)], i32, i32) -> usize,
        ) -> Vec<(i32, i32)> {
            let all = self.all_rows();
            let mut kept: Vec<(i32, i32)> = all
                .iter()
                .copied()
                .filter(|&(pk, val)| rank_of(&all, pk, val) <= self.k)
                .collect();
            kept.sort_unstable();
            kept
        }
    }

    /// Drain an operator's output into `(pk, val)` pairs **in emit order**.
    ///
    /// Preferred over [`sorted_pk_val`] wherever the expected rows can be
    /// written in `(partition_key, order_key)` order, because emitting them in
    /// the wrong order is a real failure this then catches. Sorting here would
    /// hide it: a reversed `emit` passes a sorted comparison on every shape.
    async fn pk_val_rows(stream: SendableRecordBatchStream) -> Result<Vec<(i32, i32)>> {
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let mut rows: Vec<(i32, i32)> = Vec::new();
        for b in &batches {
            let pk = b.column(0).as_primitive::<arrow::datatypes::Int32Type>();
            let val = b.column(1).as_primitive::<arrow::datatypes::Int32Type>();
            for i in 0..b.num_rows() {
                rows.push((pk.value(i), val.value(i)));
            }
        }
        Ok(rows)
    }

    /// [`pk_val_rows`] sorted, for the `RANK` and `DENSE_RANK` paths, whose
    /// emit order is not `(pk, val)` ascending: boundary ties are appended
    /// after a partition's heap rows, so their relative order is not the ORDER
    /// BY key's. Compare against [`DiffShape::expected`].
    async fn sorted_pk_val(stream: SendableRecordBatchStream) -> Result<Vec<(i32, i32)>> {
        let mut rows = pk_val_rows(stream).await?;
        rows.sort_unstable();
        Ok(rows)
    }

    /// Randomized differential test for the plain `ROW_NUMBER` path.
    ///
    /// `PartitionedTopKRank` and `PartitionedTopKDenseRank` each had one of
    /// these; `ROW_NUMBER` did not, which left its retained-row storage
    /// without a randomized net.
    ///
    /// Compares in emit order, via [`pk_val_rows`] rather than
    /// [`sorted_pk_val`]: `(partition_key, order_key)` ordering is the
    /// operator's output contract, and a sorted comparison cannot see it —
    /// reversing both the partition sort and the within-partition order in
    /// `emit` leaves a sorted version of this test green on all 64 seeds.
    #[tokio::test]
    async fn test_partitioned_topk_matches_bruteforce() -> Result<()> {
        for seed in 0..64u64 {
            let shape = DiffShape::new(seed, 6);
            let (schema, mut state) = build_partitioned_topk(shape.k)?;
            for (pks, vals) in &shape.batches {
                state.insert_batch(&pk_val_batch(&schema, pks.clone(), vals.clone())?)?;
            }

            assert_eq!(
                pk_val_rows(state.emit()?).await?,
                shape.expected_row_number(),
                "{shape}"
            );
        }
        Ok(())
    }

    /// A row can be admitted and then superseded by a better row from the
    /// *same* batch, before that batch has been registered in the store.
    ///
    /// This is the case the use count has to get right: the superseded row's
    /// use is dropped on the in-flight count rather than through `unuse`, which
    /// would panic on an id the store has never seen. Every such row still ends
    /// up in the gather, so the surviving slot's row index has to account for
    /// admissions that no longer survive.
    ///
    /// Interleaving two partitions is what makes a slot pointing at the wrong
    /// gathered row visible: crossing them yields `(0, 2)` / `(1, 1)` instead
    /// of `(0, 1)` / `(1, 2)`, which a single-partition case could not tell
    /// apart from correct behaviour.
    #[tokio::test]
    async fn test_partitioned_topk_payload_survives_supersession() -> Result<()> {
        // pk 0 admits 9, then 3 supersedes it, then 1 supersedes that.
        // pk 1 admits 8, then 2 supersedes it.
        let (schema, mut state) = build_partitioned_topk(1)?;
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![0, 1, 0, 1, 0],
            vec![9, 8, 3, 2, 1],
        )?)?;

        assert_eq!(pk_val_rows(state.emit()?).await?, vec![(0, 1), (1, 2)]);

        // With K = 2 a supersession replaces only the worst slot, so the
        // other retained row must keep pointing at its own gathered row.
        let (schema, mut state) = build_partitioned_topk(2)?;
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0, 0], vec![4, 7, 5])?)?;

        assert_eq!(pk_val_rows(state.emit()?).await?, vec![(0, 4), (0, 5)]);
        Ok(())
    }

    /// The store must release a gathered batch the moment its last referencing
    /// slot is evicted, and must never register a batch nothing references.
    ///
    /// This is the property that makes pinning converge rather than track the
    /// input. The bounded-pool test above asserts the *consequence* (total size
    /// stays flat); this asserts the mechanism directly, because a leak that
    /// happened to be offset by a shrinking buffer elsewhere would satisfy a
    /// total-size bound while still holding batches forever.
    #[tokio::test]
    async fn test_partitioned_topk_store_releases_superseded_batches() -> Result<()> {
        const K: usize = 2;
        let (schema, mut state) = build_partitioned_topk(K)?;

        // Batch 0 fills the single partition's heap, so it is referenced and
        // must be pinned.
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0], vec![50, 60])?)?;
        assert_eq!(state.store.len(), 1, "the batch every slot points at");
        let first_id = *state.store.batches.keys().next().expect("one entry");

        // Batch 1 is entirely worse, so nothing is admitted and nothing may be
        // registered — a store that registered unconditionally would grow here.
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0], vec![70, 80])?)?;
        assert_eq!(
            state.store.len(),
            1,
            "a fully-rejected batch must not be registered"
        );

        // Batch 2 supersedes both retained rows, so batch 0 loses its last
        // reference and must be dropped as batch 2 takes its place.
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0], vec![10, 20])?)?;
        assert_eq!(
            state.store.len(),
            1,
            "superseded batch 0 must be released, not accumulated"
        );
        assert!(
            !state.store.batches.contains_key(&first_id),
            "the released entry must be the superseded one"
        );

        // Partial supersession: one of the two retained rows is replaced, so
        // both the old and the new batch are legitimately referenced.
        state.insert_batch(&pk_val_batch(&schema, vec![0], vec![5])?)?;
        assert_eq!(
            state.store.len(),
            2,
            "a batch still holding one live row must stay pinned"
        );

        assert_eq!(pk_val_rows(state.emit()?).await?, vec![(0, 5), (0, 10)]);
        Ok(())
    }

    /// Pinned rows must stay proportional to the rows the heaps hold, even when
    /// releasing superseded batches alone cannot manage it.
    ///
    /// The sibling test above covers the easy shape: every batch supersedes the
    /// last one whole, so every entry's use count reaches zero and the store
    /// converges to one entry on its own. This is the shape where it does not.
    /// `val = |batch - partition|` makes batch `j` the owner of partition `j`'s
    /// winner, so all `B` entries keep exactly one live row and each stays
    /// resident holding every row it admitted. Releasing is powerless here —
    /// nothing is ever fully superseded — and without `compact_store` the store
    /// pins `B(B+1)/2` rows (131,328 at `B = 512`, half the whole input) to
    /// retain `B`. That is the unbounded case: over-retention grows with `B`.
    #[tokio::test]
    async fn test_partitioned_topk_store_compacts_when_survivors_spread() -> Result<()> {
        const B: i32 = 512;

        let (schema, mut state) = build_partitioned_topk(1)?;
        for j in 0..B {
            let pks: Vec<i32> = (0..B).collect();
            let vals: Vec<i32> = (0..B).map(|p| (j - p).abs()).collect();
            state.insert_batch(&pk_val_batch(&schema, pks, vals)?)?;
        }

        // Every partition holds its K = 1 row, so the ratio's denominator is B.
        assert_eq!(state.live_slots, B as usize);
        let pinned: usize = state
            .store
            .batches
            .values()
            .map(|e| e.batch.num_rows())
            .sum();
        assert_eq!(pinned, state.store.total_rows, "store row count is exact");
        // An invariant, not a measurement: `compact_store` runs at the end of
        // every `insert_batch`, so on return the store either never tripped the
        // guard or was just rewritten down to `live_slots`. Expressed through
        // the constant so tuning it cannot leave this stale. Measured 977 here
        // against 131,328 unfixed, so the bar bites by two orders of magnitude
        // at this B and the gap widens with it.
        let bound = B as usize * STORE_COMPACTION_RATIO;
        assert!(
            pinned <= bound,
            "{pinned} rows pinned to retain {} — above the {bound}-row bound, so \
             retention tracks the input rather than the rows held",
            state.live_slots
        );

        // Compaction rewrote every slot's coordinates, so the rows it resolves
        // at emit are the check that it repointed them correctly: partition p's
        // single retained row is the one whose value is 0, contributed by batch
        // p — a slot left pointing at a pre-compaction coordinate would surface
        // some other partition's row here. Compared in emit order, so this also
        // covers the ordering of a compacted, multi-chunk emit; no other test
        // reaches that path at all.
        let expected: Vec<(i32, i32)> = (0..B).map(|pk| (pk, 0)).collect();
        assert_eq!(pk_val_rows(state.emit()?).await?, expected);
        Ok(())
    }

    /// A store holding a *single* entry must still be compacted when that entry
    /// is mostly dead rows.
    ///
    /// Regression: `compact_store` used to bail on `store.len() <= 1`, on the
    /// reasoning that one entry cannot compact to anything smaller. It can. An
    /// entry holds every row *admitted* from its batch, and a row admitted and
    /// then superseded within that same pass stays in the gather with nothing
    /// pointing at it — so a single entry is bounded by the input batch, not by
    /// `live_slots`.
    ///
    /// Descending values in one batch is the worst case and not an exotic one:
    /// every row beats the current worst, so all of them are admitted and `K`
    /// survive. `store.len() == 1` is also the state right after every
    /// compaction and on the first batch, so the old guard was live for the
    /// whole of a single-batch query.
    #[tokio::test]
    async fn test_partitioned_topk_store_compacts_a_single_oversized_entry() -> Result<()>
    {
        const ROWS: i32 = 200;

        let (schema, mut state) = build_partitioned_topk(1)?;
        state.insert_batch(&pk_val_batch(
            &schema,
            vec![0; ROWS as usize],
            (0..ROWS).map(|i| ROWS - i).collect(),
        )?)?;

        assert_eq!(state.live_slots, 1, "K = 1 in a single partition");
        assert_eq!(state.store.len(), 1, "one input batch, one gathered entry");
        // The bound the ratio promises, which the old guard exempted this shape
        // from entirely: 200 rows stayed pinned to retain 1.
        assert!(
            state.store.total_rows <= state.live_slots * STORE_COMPACTION_RATIO,
            "{} rows pinned to retain {}; a single entry escaped compaction",
            state.store.total_rows,
            state.live_slots
        );

        // Compaction repointed the surviving slot, so emit still resolves it.
        assert_eq!(pk_val_rows(state.emit()?).await?, vec![(0, 1)]);
        Ok(())
    }

    /// Every invariant the shared store's bookkeeping rests on, checked against
    /// a full recount of the heaps.
    ///
    /// The running totals (`live_slots`, `store.total_rows`,
    /// `store.batches_size`, each entry's `uses`) are maintained incrementally
    /// precisely so that neither `size()` nor `compact_store`'s guard has to
    /// walk the partitions. Nothing else re-derives them, so a drift in any one
    /// is invisible: an over-counted `uses` leaks an entry forever, an
    /// under-counted one drops a batch rows still point at, and a wrong
    /// `live_slots` silently disables the pinning bound.
    fn assert_store_invariants(state: &PartitionedTopK, label: &str) {
        // 1. `live_slots` is the number of slots the heaps actually hold.
        let counted: usize = state.heaps.values().map(|h| h.inner.len()).sum();
        assert_eq!(
            counted, state.live_slots,
            "{label}: live_slots disagrees with the heaps"
        );

        // 2. Every slot points at a live entry, and each entry's `uses` is
        //    exactly the number of slots pointing into it.
        let mut refs: HashMap<u32, usize> = HashMap::new();
        for heap in state.heaps.values() {
            for slot in heap.slots() {
                assert!(
                    state.store.get(slot.batch_id).is_some(),
                    "{label}: slot points at batch {} which the store does not hold",
                    slot.batch_id
                );
                *refs.entry(slot.batch_id).or_default() += 1;
            }
        }
        assert_eq!(
            refs.len(),
            state.store.len(),
            "{label}: the store holds entries nothing points at"
        );
        for (id, entry) in &state.store.batches {
            assert_eq!(
                entry.uses,
                refs.get(id).copied().unwrap_or(0),
                "{label}: entry {id} has uses={} but {} slots point at it",
                entry.uses,
                refs.get(id).copied().unwrap_or(0)
            );
            // A slot's row must be in range, or emit's interleave would read
            // out of bounds (or silently pick a different row).
            for heap in state.heaps.values() {
                for slot in heap.slots().filter(|s| s.batch_id == *id) {
                    assert!(
                        (slot.row as usize) < entry.batch.num_rows(),
                        "{label}: slot row {} is outside entry {id} ({} rows)",
                        slot.row,
                        entry.batch.num_rows()
                    );
                }
            }
        }

        // 3. The store's two running totals are exact, not estimates.
        let rows: usize = state
            .store
            .batches
            .values()
            .map(|e| e.batch.num_rows())
            .sum();
        assert_eq!(
            rows, state.store.total_rows,
            "{label}: store.total_rows drifted"
        );
        let bytes: usize = state
            .store
            .batches
            .values()
            .map(|e| get_record_batch_memory_size(&e.batch))
            .sum();
        assert_eq!(
            bytes, state.store.batches_size,
            "{label}: store.batches_size drifted"
        );

        // 4. The bound `compact_store` exists to enforce. Without it, what stays
        //    pinned tracks the *input* rather than `partitions × K`.
        assert!(
            state.store.total_rows <= state.live_slots * STORE_COMPACTION_RATIO,
            "{label}: {} rows pinned to retain {}",
            state.store.total_rows,
            state.live_slots
        );

        // 5. What the operator reported to the pool is what it computes now.
        assert_eq!(
            state.reservation.size(),
            state.size(),
            "{label}: the reservation does not match size()"
        );
    }

    /// Feed shapes designed to break the store's bookkeeping, checking every
    /// invariant after every batch and the output against brute force at the
    /// end.
    ///
    /// The shapes matter more than the count. `K = 1` with one partition per
    /// batch is the sparse-survivor case that makes a live row pin a whole
    /// gathered entry; a descending run at `K = 1` admits every row and keeps
    /// one, so the gather is nearly all dead on arrival and the pre-assigned
    /// batch id churns down to `uses == 1`; an all-ties batch admits nothing
    /// after the first `k` rows, so `uses == 0` and the id must go back to the
    /// next batch unconsumed.
    #[tokio::test]
    async fn test_partitioned_topk_store_bookkeeping_holds_under_stress() -> Result<()> {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        struct Case {
            name: &'static str,
            k: usize,
            partitions: i32,
            values: i32,
            batches: usize,
            rows: usize,
            /// Sort each batch's values descending, so every row is an
            /// admission that the next row supersedes.
            descending: bool,
            /// Give each batch a single partition, so survivors spread one per
            /// gathered entry.
            one_partition_per_batch: bool,
        }

        let cases = [
            Case {
                name: "k1_one_partition_per_batch",
                k: 1,
                partitions: 64,
                values: 1_000,
                batches: 64,
                rows: 16,
                descending: false,
                one_partition_per_batch: true,
            },
            Case {
                name: "k1_descending_runs",
                k: 1,
                partitions: 3,
                values: 1_000,
                batches: 20,
                rows: 50,
                descending: true,
                one_partition_per_batch: false,
            },
            Case {
                name: "all_ties_uses_zero",
                k: 2,
                partitions: 1,
                values: 1,
                batches: 20,
                rows: 30,
                descending: false,
                one_partition_per_batch: false,
            },
            Case {
                name: "k_exceeds_partition_size",
                k: 40,
                partitions: 50,
                values: 1_000,
                batches: 30,
                rows: 10,
                descending: false,
                one_partition_per_batch: false,
            },
            Case {
                name: "wide_churn",
                k: 3,
                partitions: 12,
                values: 20,
                batches: 40,
                rows: 64,
                descending: false,
                one_partition_per_batch: false,
            },
        ];

        for case in cases {
            for seed in 0..4u64 {
                let mut rng = StdRng::seed_from_u64(seed);
                let (schema, mut state) = build_partitioned_topk(case.k)?;
                let mut all_rows: Vec<(i32, i32)> = Vec::new();

                for b in 0..case.batches {
                    let pks: Vec<i32> = if case.one_partition_per_batch {
                        vec![(b as i32) % case.partitions; case.rows]
                    } else {
                        (0..case.rows)
                            .map(|_| rng.random_range(0..case.partitions))
                            .collect()
                    };
                    let mut vals: Vec<i32> = (0..case.rows)
                        .map(|_| rng.random_range(0..case.values))
                        .collect();
                    if case.descending {
                        vals.sort_unstable_by(|a, b| b.cmp(a));
                    }
                    all_rows.extend(pks.iter().copied().zip(vals.iter().copied()));

                    state.insert_batch(&pk_val_batch(&schema, pks, vals)?)?;
                    assert_store_invariants(
                        &state,
                        &format!("{} seed {seed} batch {b}", case.name),
                    );
                }

                // Brute force: per partition, the k smallest values, emitted in
                // (pk, val) ascending order.
                let mut expected: Vec<(i32, i32)> = Vec::new();
                let mut pks: Vec<i32> = all_rows.iter().map(|&(p, _)| p).collect();
                pks.sort_unstable();
                pks.dedup();
                for pk in pks {
                    let mut vals: Vec<i32> = all_rows
                        .iter()
                        .filter(|&&(p, _)| p == pk)
                        .map(|&(_, v)| v)
                        .collect();
                    vals.sort_unstable();
                    vals.truncate(case.k);
                    expected.extend(vals.into_iter().map(|v| (pk, v)));
                }

                assert_eq!(
                    pk_val_rows(state.emit()?).await?,
                    expected,
                    "{} seed {seed}",
                    case.name
                );
            }
        }
        Ok(())
    }

    /// A batch whose every admission is superseded before the pass ends must
    /// not consume the batch id `next_batch_id` handed the slots.
    ///
    /// `insert_batch` points slots at an id before the gather exists, and skips
    /// `register` entirely when `uses` falls back to 0. If the id were consumed
    /// anyway the sequence would gap harmlessly, but if `register` ran with
    /// `uses == 0` the store would keep an entry nothing references — and
    /// `compact_store`'s `first_id` would then collide with it.
    #[tokio::test]
    async fn test_partitioned_topk_unused_gather_id_is_reused() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk(1)?;

        // Batch 0: one partition, one row. Consumes id 0.
        state.insert_batch(&pk_val_batch(&schema, vec![0], vec![5])?)?;
        assert_eq!(state.store.next_batch_id(), 1, "batch 0 consumed its id");

        // Batch 1: every value is worse than the retained 5, so nothing is
        // admitted at all — `uses` never rises and the id is untouched. No
        // compaction either: neither total_rows nor live_slots moved, and the
        // guard already held when the previous call returned.
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0, 0], vec![7, 8, 9])?)?;
        assert_eq!(state.store.next_batch_id(), 1, "no admission, no id");
        assert_eq!(state.store.len(), 1);
        assert_store_invariants(&state, "after a batch with no admissions");

        // Batch 2: rows are admitted but each is superseded by a later row of
        // the *same* batch, and the first evicts the row from batch 0. The
        // gather therefore holds 3 rows for 1 live slot, which trips
        // `compact_store` — so this consumes two ids, the gather's and the
        // compacted chunk's.
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0, 0], vec![4, 3, 2])?)?;
        assert_store_invariants(&state, "after a self-superseding batch");
        assert_eq!(state.store.len(), 1, "batch 0's entry was released");
        assert_eq!(
            state.store.total_rows, 1,
            "compaction dropped the dead rows"
        );
        let after_compaction = state.store.next_batch_id();

        // Batch 3: admitted then superseded within the batch, ending worse than
        // the retained 2 — so `uses` returns to 0 and the id is handed back.
        state.insert_batch(&pk_val_batch(&schema, vec![0, 0], vec![3, 6])?)?;
        assert_eq!(
            state.store.next_batch_id(),
            after_compaction,
            "every admission was superseded, so the id must be reusable"
        );
        assert_store_invariants(&state, "after a fully superseded batch");

        assert_eq!(pk_val_rows(state.emit()?).await?, vec![(0, 2)]);
        Ok(())
    }

    /// `size()` must account for the ORDER BY keys the heaps hold by value.
    ///
    /// Every other term of `size()` is either a fixed struct, a `RowConverter`,
    /// or the store — all of which exist whatever the key width. `heaps_bytes`
    /// is the only term that grows with `partitions × K × key_width`, and with
    /// an `Int32` key it is a rounding error, so a test on the narrow schema
    /// cannot see it go missing. With a wide string key it is the dominant
    /// per-partition term: dropping the `heaps_bytes` fold leaves the operator
    /// under-reporting megabytes to the pool.
    #[tokio::test]
    async fn test_partitioned_topk_size_accounts_for_retained_keys() -> Result<()> {
        const PARTITIONS: usize = 200;
        const K: usize = 4;
        const KEY_WIDTH: usize = 512;

        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let partition_ordering = [PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        }];
        let order_expr = LexOrdering::from([PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: SortOptions::default(),
        }]);
        let mut state = PartitionedTopK::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            build_sort_fields(&partition_ordering, &schema)?,
            order_expr,
            K,
            8,
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;

        let empty = state.size();

        // Feed K rows for each partition, all retained, each key KEY_WIDTH wide.
        let mut pks: Vec<i32> = Vec::new();
        let mut vals: Vec<String> = Vec::new();
        for p in 0..PARTITIONS {
            for i in 0..K {
                pks.push(p as i32);
                vals.push(format!("{p:0>width$}{i}", width = KEY_WIDTH - 1));
            }
        }
        state.insert_batch(&RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(pks)),
                Arc::new(StringArray::from(vals)),
            ],
        )?)?;

        assert_eq!(
            state.live_slots,
            PARTITIONS * K,
            "every row should have been retained"
        );

        // The keys alone are this many bytes; `size()` has to have grown by at
        // least that much, on top of whatever the store and the map cost.
        let key_bytes = PARTITIONS * K * KEY_WIDTH;
        assert!(
            state.heaps_bytes >= key_bytes,
            "heaps_bytes is {} but the retained keys are at least {key_bytes} bytes",
            state.heaps_bytes
        );
        assert!(
            state.size() >= empty + key_bytes,
            "size() grew by {} for {key_bytes} bytes of retained keys",
            state.size() - empty
        );
        // And the interned partition keys are counted separately.
        assert!(
            state.index_bytes >= PARTITIONS,
            "index_bytes is {} for {PARTITIONS} interned keys",
            state.index_bytes
        );
        Ok(())
    }

    /// Randomized differential test for `PartitionedTopKRank`.
    ///
    /// The retention rule is subtle — a K-bounded heap plus a boundary-tie
    /// list that must be discarded the moment the K-th-best ORDER BY value
    /// improves — and it turns on how partitions, ties and batch boundaries
    /// interleave. 64 seeds runs in ~10 ms; deleting the tie-clear on a
    /// boundary shift is caught by seed 0.
    #[tokio::test]
    async fn test_partitioned_topk_rank_matches_bruteforce() -> Result<()> {
        for seed in 0..64u64 {
            let shape = DiffShape::new(seed, 6);
            let (schema, mut state) = build_partitioned_topk_rank(shape.k)?;
            for (pks, vals) in &shape.batches {
                state.insert_batch(&pk_val_batch(&schema, pks.clone(), vals.clone())?)?;
            }

            // RANK: 1 + the number of strictly smaller rows.
            let expected = shape.expected(|rows, pk, val| {
                1 + rows.iter().filter(|&&(p, v)| p == pk && v < val).count()
            });

            assert_eq!(sorted_pk_val(state.emit()?).await?, expected, "{shape}");
        }
        Ok(())
    }

    /// Randomized differential test for `PartitionedTopKDenseRank`.
    ///
    /// Same harness as [`test_partitioned_topk_rank_matches_bruteforce`],
    /// differing only in the ranking formula and a wider value domain:
    /// DENSE_RANK bounds *distinct* values, so a partition needs more than
    /// `k` of them before it evicts anything, and eviction is what the
    /// admission pre-filter guards. A pre-filter that wrongly rejects
    /// boundary-equal rows is caught by seed 6.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_matches_bruteforce() -> Result<()> {
        for seed in 0..64u64 {
            let shape = DiffShape::new(seed, 8);
            let (schema, mut state) = build_partitioned_topk_dense_rank(shape.k)?;
            for (pks, vals) in &shape.batches {
                state.insert_batch(&pk_val_batch(&schema, pks.clone(), vals.clone())?)?;
            }

            // DENSE_RANK: 1 + the number of *distinct* strictly smaller values.
            let expected = shape.expected(|rows, pk, val| {
                1 + rows
                    .iter()
                    .filter(|&&(p, v)| p == pk && v < val)
                    .map(|&(_, v)| v)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
            });

            assert_eq!(sorted_pk_val(state.emit()?).await?, expected, "{shape}");
        }
        Ok(())
    }

    /// The `(pk Int32, val Int32)` schema every `PartitionedTopK*` test
    /// builds against. `val_nullable` is what the null-ordering tests vary.
    fn pk_val_schema(val_nullable: bool) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("val", DataType::Int32, val_nullable),
        ]))
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

    /// Companion to [`test_topk_output_batches_metric_counts_emitted_batches`] for
    /// the partitioned emit path: rows from several per-partition heaps are
    /// interleaved into `batch_size` chunks, so the metric must count the
    /// emitted chunks rather than the per-partition heaps they came from.
    #[tokio::test]
    async fn test_partitioned_topk_output_batches_metric_counts_emitted_batches()
    -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let partition_ordering = vec![PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        }];
        let metrics = ExecutionPlanMetricsSet::new();
        // 3 per-partition heaps of 2 rows each, emitted as 2 batches of 3
        let order_ordering = [PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: SortOptions::default(),
        }];
        let mut state = PartitionedTopK::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            build_sort_fields(&partition_ordering, &schema)?,
            LexOrdering::from(order_ordering.clone()),
            2,
            3, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &metrics,
        )?;

        state.insert_batch(&pk_val_batch(
            &schema,
            vec![1, 1, 2, 2, 3, 3],
            vec![10, 5, 20, 15, 30, 25],
        )?)?;

        let results: Vec<_> = state.emit()?.try_collect().await?;
        let row_counts: Vec<usize> = results.iter().map(|b| b.num_rows()).collect();
        assert_eq!(row_counts, vec![3, 3]);
        assert_eq!(output_batches_and_rows(&metrics), (2, 6));

        Ok(())
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
        build_partitioned_topk_rank_inner(
            k,
            val_sort_options,
            val_nullable,
            &Arc::new(RuntimeEnv::default()),
        )
    }

    /// Variant of [`build_partitioned_topk_rank`] that takes the
    /// [`RuntimeEnv`] to register the reservation against, so a test can
    /// bound the memory pool the operator draws from.
    fn build_partitioned_topk_rank_with_runtime(
        k: usize,
        runtime: &Arc<RuntimeEnv>,
    ) -> Result<(Arc<Schema>, PartitionedTopKRank)> {
        build_partitioned_topk_rank_inner(k, SortOptions::default(), false, runtime)
    }

    fn build_partitioned_topk_rank_inner(
        k: usize,
        val_sort_options: SortOptions,
        val_nullable: bool,
        runtime: &Arc<RuntimeEnv>,
    ) -> Result<(Arc<Schema>, PartitionedTopKRank)> {
        let schema = pk_val_schema(val_nullable);

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
            runtime,
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

    /// Tie rows are emitted through the same coalescer as heap rows, so they
    /// must be counted in `output_rows` once, not once as a tie batch and
    /// again as part of the coalesced output batch.
    #[tokio::test]
    async fn test_partitioned_topk_rank_output_rows_counts_ties_once() -> Result<()> {
        let schema = pk_val_schema(false);
        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let pk_sort_expr = PhysicalSortExpr {
            expr: Arc::clone(&pk_expr),
            options: SortOptions::default(),
        };
        let val_sort_expr = PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: SortOptions::default(),
        };
        let metrics = ExecutionPlanMetricsSet::new();
        let mut state = PartitionedTopKRank::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            build_sort_fields(&[pk_sort_expr], &schema)?,
            LexOrdering::from([val_sort_expr]),
            2,
            8, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &metrics,
        )?;

        // Two 5s fill the heap, the third 5 is retained as a tie.
        let batch = pk_val_batch(&schema, vec![1, 1, 1, 1], vec![5, 5, 10, 5])?;
        state.insert_batch(&batch)?;

        let results: Vec<RecordBatch> = state.emit()?.try_collect().await?;
        let emitted_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(emitted_rows, 3);
        assert_eq!(
            output_batches_and_rows(&metrics),
            (results.len(), emitted_rows)
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
    // sharing an ob key coalesce into one `GroupEntry`, unbounded
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
        let schema = pk_val_schema(val_nullable);

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
    /// into one `GroupEntry` per distinct ob.
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
    /// land under the same map key as separate `GroupEntry`s — one per
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
    /// and cross-batch eviction of a whole max group. Every `GroupEntry` is
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

    /// Total `GroupEntry` count across all partitions.
    fn dense_rank_entry_count(state: &PartitionedTopKDenseRank) -> usize {
        state
            .states
            .values()
            .flat_map(|s| s.groups.values())
            .map(|entries| entries.len())
            .sum()
    }

    /// One source batch feeding many retained groups must be charged
    /// once, not once per group.
    ///
    /// Dense-rank retains up to K distinct-ob groups per partition and
    /// each can draw rows from the same batch, so charging per entry
    /// inflates the reservation by (partitions × K) — here 6× — and can
    /// trip a spurious `ResourcesExhausted`.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_charges_batch_once() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(3)?;

        // pk=1 retains {1,2,3}, pk=2 retains {10,20,30}: 6 groups, all
        // from this one batch.
        let batch = pk_val_batch(
            &schema,
            vec![1, 1, 1, 1, 2, 2, 2, 2],
            vec![1, 2, 3, 4, 10, 20, 30, 40],
        )?;
        let batch_bytes = get_record_batch_memory_size(&batch);
        state.insert_batch(&batch)?;

        assert_eq!(dense_rank_entry_count(&state), 6);
        assert_eq!(state.store.len(), 1);
        assert_eq!(state.store.batches_size, batch_bytes);
        Ok(())
    }

    /// Evicting the last group referencing a batch must release the
    /// batch's bytes, or the reservation only ever grows.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_releases_evicted_batch() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        let first = pk_val_batch(&schema, vec![1, 1], vec![50, 60])?;
        state.insert_batch(&first)?;
        assert_eq!(state.store.len(), 1);

        // Both values beat {50, 60}, so every group from `first` is
        // evicted and only `second` remains charged.
        let second = pk_val_batch(&schema, vec![1, 1], vec![5, 6])?;
        let second_bytes = get_record_batch_memory_size(&second);
        state.insert_batch(&second)?;

        assert_eq!(state.store.len(), 1);
        assert_eq!(state.store.batches_size, second_bytes);

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 5   |",
                "| 1  | 6   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }

    /// The mirror of the above: a batch whose every run is rejected must
    /// not be charged at all.
    ///
    /// Nothing references it, so nothing would ever release it — charging
    /// it would pin both the bytes and the batch for the operator's
    /// lifetime.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_ignores_fully_rejected_batch() -> Result<()>
    {
        let (schema, mut state) = build_partitioned_topk_dense_rank(2)?;

        let first = pk_val_batch(&schema, vec![1, 1], vec![5, 6])?;
        let first_bytes = get_record_batch_memory_size(&first);
        state.insert_batch(&first)?;
        assert_eq!(state.store.len(), 1);

        // At K=2 with {5, 6} tracked, both values lose to the boundary, so
        // no `GroupEntry` points at `second`.
        state.insert_batch(&pk_val_batch(&schema, vec![1, 1], vec![50, 60])?)?;

        assert_eq!(dense_rank_entry_count(&state), 2);
        assert_eq!(state.store.len(), 1);
        assert_eq!(state.store.batches_size, first_bytes);
        Ok(())
    }

    /// `keys` must grow on demand rather than reserve K slots when a
    /// partition is first seen. `size()` charges `keys.capacity()`, so
    /// eager sizing reserves O(partitions * K) for slots that never hold
    /// a key — enough to fail a memory limit on a high-cardinality input
    /// whose partitions each keep a handful of distinct values.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_heap_grows_on_demand() -> Result<()> {
        const K: usize = 512;
        const PARTITIONS: usize = 64;
        let (schema, mut state) = build_partitioned_topk_dense_rank(K)?;

        // One row per partition: every partition holds exactly one
        // distinct ob value, K - 1 slots short of capacity.
        let pks: Vec<i32> = (0..PARTITIONS as i32).collect();
        let vals = pks.clone();
        state.insert_batch(&pk_val_batch(&schema, pks, vals)?)?;

        assert_eq!(state.states.len(), PARTITIONS);
        let heap_slots: usize = state.states.values().map(|s| s.keys.capacity()).sum();
        // Eager `with_capacity(K)` would reserve PARTITIONS * K = 32768.
        assert!(
            heap_slots <= PARTITIONS * 8,
            "reserved {heap_slots} heap slots to hold {PARTITIONS} keys"
        );
        Ok(())
    }

    /// The encoded partition keys owned by `states`, and the backing
    /// tables the drained scratch maps keep, are long-lived heap
    /// allocations `size()` must charge: they persist for the operator's
    /// life yet belong to no `GroupEntry`, so per-entry accounting can't
    /// see them. With numerous or wide partition keys the key bytes are
    /// the larger term.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_size_covers_keys_and_scratch() -> Result<()>
    {
        const PARTITIONS: usize = 64;
        const KEY_WIDTH: usize = 1024;

        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Utf8, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let pk_expr: Arc<dyn PhysicalExpr> = col("pk", schema.as_ref())?;
        let partition_sort_fields = build_sort_fields(
            &[PhysicalSortExpr {
                expr: Arc::clone(&pk_expr),
                options: SortOptions::default(),
            }],
            &schema,
        )?;
        let order_expr = LexOrdering::from([PhysicalSortExpr {
            expr: col("val", schema.as_ref())?,
            options: SortOptions::default(),
        }]);
        let mut state = PartitionedTopKDenseRank::try_new(
            0,
            Arc::clone(&schema),
            vec![pk_expr],
            partition_sort_fields,
            order_expr,
            4,
            8, // batch_size
            &Arc::new(RuntimeEnv::default()),
            &ExecutionPlanMetricsSet::new(),
        )?;

        // One row per partition, each with a wide key.
        let pks: Vec<String> = (0..PARTITIONS)
            .map(|i| format!("{}{i:04}", "p".repeat(KEY_WIDTH - 4)))
            .collect();
        let vals: Vec<i32> = (0..PARTITIONS as i32).collect();
        state.insert_batch(&RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(pks)),
                Arc::new(Int32Array::from(vals)),
            ],
        )?)?;
        assert_eq!(state.states.len(), PARTITIONS);

        let key_bytes: usize = state.states.keys().map(|pk| pk.capacity()).sum();
        let scratch_bytes = state.partition_groups.capacity()
            * (size_of::<Vec<u8>>() + size_of::<Vec<u32>>())
            + state.ob_runs.capacity() * (size_of::<Vec<u8>>() + size_of::<Vec<u32>>());
        assert!(key_bytes >= PARTITIONS * KEY_WIDTH, "key bytes {key_bytes}");
        assert!(scratch_bytes > 0, "scratch tables never allocated");

        // Reconstruct the total from its parts. Both terms above have to
        // appear for this to balance, so dropping either from `size()`
        // fails here rather than being absorbed by the slack in some
        // other term.
        let expected = size_of::<PartitionedTopKDenseRank>()
            + state.row_converter.size()
            + state.partition_converter.size()
            + state.scratch_rows.size()
            + state.partition_scratch_rows.size()
            + key_bytes
            + state.states.values().map(|s| s.size()).sum::<usize>()
            + state.states.capacity()
                * (size_of::<Vec<u8>>() + size_of::<DenseRankPartitionState>())
            + scratch_bytes
            + state.store.size();
        assert_eq!(state.size(), expected);
        Ok(())
    }

    /// A group admitted and then evicted within the *same*
    /// `insert_batch` call: the batch is still pending (not yet handed to
    /// the store), so releasing it must decrement the in-flight use count
    /// rather than call `unuse` on an unregistered id.
    ///
    /// `ob_runs` drains in hash order, so with K=1 and many distinct
    /// values the minimum is almost never seen first and the run
    /// admit-then-evict path is taken repeatedly.
    #[tokio::test]
    async fn test_partitioned_topk_dense_rank_evicts_same_call_group() -> Result<()> {
        let (schema, mut state) = build_partitioned_topk_dense_rank(1)?;

        let pks = vec![1; 32];
        let vals: Vec<i32> = (0..32).rev().collect();
        let batch = pk_val_batch(&schema, pks, vals)?;
        let batch_bytes = get_record_batch_memory_size(&batch);
        state.insert_batch(&batch)?;

        assert_eq!(dense_rank_entry_count(&state), 1);
        assert_eq!(state.store.len(), 1);
        assert_eq!(state.store.batches_size, batch_bytes);

        let results: Vec<_> = state.emit()?.try_collect().await?;
        assert_batches_eq!(
            &[
                "+----+-----+",
                "| pk | val |",
                "+----+-----+",
                "| 1  | 0   |",
                "+----+-----+",
            ],
            &results
        );
        Ok(())
    }
}
