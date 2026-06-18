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
    compute::{FilterBuilder, interleave_record_batch, prep_null_mask_filter},
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

use arrow::array::{ArrayRef, RecordBatch};
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
        let Some(max_row) = self.heap.max() else {
            return Ok(());
        };

        let new_threshold_row = max_row.row();

        // Fast path: check if the current value in topk is better than what is
        // currently set in the filter with a read only lock
        let needs_update = self
            .filter
            .read()
            .shared_threshold
            .as_ref()
            .map(|current_threshold| {
                // new < current means new threshold is more selective
                new_threshold_row < current_threshold.full_sort_key_row()
            })
            .unwrap_or(true); // No current threshold, so we need to set one

        // exit early if the current values are better
        if !needs_update {
            return Ok(());
        }

        // Extract scalar values BEFORE acquiring lock to reduce critical section
        let thresholds = match self.heap.get_threshold_values(&self.expr)? {
            Some(t) => t,
            None => return Ok(()),
        };

        // Build the filter expression OUTSIDE any synchronization
        let predicate = Self::build_filter_expression(&self.expr, &thresholds)?;
        let new_threshold = TopKThreshold::new(
            new_threshold_row.to_vec(),
            self.encode_topk_common_prefix_row(max_row)?,
        );

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

        // Early exit if the heap is not full (`heap.max()` only returns `Some` if the heap is full).
        let Some(max_topk_row) = self.heap.max() else {
            return Ok(());
        };

        // Encode the local heap max row's common-prefix projection.
        let Some(heap_common_prefix_row) =
            self.encode_topk_common_prefix_row(max_topk_row)?
        else {
            return Ok(());
        };

        // If the last row's prefix is strictly greater than the max prefix, mark as finished.
        if batch_common_prefix > heap_common_prefix_row.as_slice() {
            self.finished = true;
        }

        Ok(())
    }

    fn encode_topk_common_prefix_row(
        &self,
        topk_row: &TopKRow,
    ) -> Result<Option<Vec<u8>>> {
        let Some(prefix_converter) = &self.common_sort_prefix_converter else {
            return Ok(None);
        };

        let store_entry = self
            .heap
            .store
            .get(topk_row.batch_id)
            .ok_or(internal_datafusion_err!("Invalid batch id in topK heap"))?;
        let mut scratch = prefix_converter.empty_rows(1, ESTIMATED_BYTES_PER_ROW);
        self.append_common_prefix_row(
            prefix_converter,
            &store_entry.batch,
            topk_row.index,
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
    fn add(
        &mut self,
        batch_entry: &mut RecordBatchEntry,
        row: impl AsRef<[u8]>,
        index: usize,
    ) {
        let batch_id = batch_entry.id;
        batch_entry.uses += 1;

        assert!(self.inner.len() <= self.k);
        let row = row.as_ref();

        // Reuse storage for evicted item if possible
        if self.inner.len() == self.k {
            let mut prev_min = self.inner.peek_mut().unwrap();

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
        } else {
            let new_row = TopKRow::new(row, batch_id, index);
            self.owned_bytes += new_row.owned_size();
            // put the new row into the heap
            self.inner.push(new_row);
        };
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], resetting the inner heap
    pub fn emit(&mut self) -> Result<Option<RecordBatch>> {
        Ok(self.emit_with_state()?.0)
    }

    /// Returns the values stored in this heap, from values low to
    /// high, as a single [`RecordBatch`], and a sorted vec of the
    /// current heap's contents
    pub fn emit_with_state(&mut self) -> Result<(Option<RecordBatch>, Vec<TopKRow>)> {
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

    fn get_threshold_values(
        &self,
        sort_exprs: &[PhysicalSortExpr],
    ) -> Result<Option<Vec<ScalarValue>>> {
        // If the heap doesn't have k elements yet, we can't create thresholds
        let max_row = match self.max() {
            Some(row) => row,
            None => return Ok(None),
        };

        // Get the batch that contains the max row
        let batch_entry = match self.store.get(max_row.batch_id) {
            Some(entry) => entry,
            None => return internal_err!("Invalid batch ID in TopKRow"),
        };

        // Extract threshold values for each sort expression
        let mut scalar_values = Vec::with_capacity(sort_exprs.len());
        for sort_expr in sort_exprs {
            // Extract the value for this column from the max row
            let expr = Arc::clone(&sort_expr.expr);
            let value = expr.evaluate(&batch_entry.batch.slice(max_row.index, 1))?;

            // Convert to scalar value - should be a single value since we're evaluating on a single row batch
            let scalar = match value {
                ColumnarValue::Scalar(scalar) => scalar,
                ColumnarValue::Array(array) if array.len() == 1 => {
                    // Extract the first (and only) value from the array
                    ScalarValue::try_from_array(&array, 0)?
                }
                array => {
                    return internal_err!("Expected a scalar value, got {:?}", array);
                }
            };

            scalar_values.push(scalar);
        }

        Ok(Some(scalar_values))
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
}
