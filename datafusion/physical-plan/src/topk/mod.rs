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
    compute::interleave_record_batch,
    row::{RowConverter, Rows, SortField},
};
use std::mem::size_of;
use std::{cmp::Ordering, collections::BinaryHeap, sync::Arc};

use super::metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder};
use crate::spill::get_record_batch_memory_size;
use crate::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::SchemaRef;
use datafusion_common::Result;
use datafusion_common::{internal_datafusion_err, HashMap};
use datafusion_execution::{
    memory_pool::{MemoryConsumer, MemoryReservation},
    runtime_env::RuntimeEnv,
};
use datafusion_physical_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

/// Global TopK
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
/// partially sorted by a common prefix of the requested ordering. Once the top K
/// heap is full, if subsequent rows are guaranteed to be strictly greater (in sort
/// order) on this prefix than the largest row currently stored, the operator
/// safely terminates early.
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
    expr: Arc<[PhysicalSortExpr]>,
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
    /// If true, indicates that all rows of subsequent batches are guaranteed
    /// to be greater (by byte order, after row conversion) than the top K,
    /// which means the top K won't change and the computation can be finished early.
    pub(crate) finished: bool,
}

// Guesstimate for memory allocation: estimated number of bytes used per row in the RowConverter
const ESTIMATED_BYTES_PER_ROW: usize = 20;

fn build_sort_fields(
    ordering: &LexOrdering,
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
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_id: usize,
        schema: SchemaRef,
        common_sort_prefix: LexOrdering,
        expr: LexOrdering,
        k: usize,
        batch_size: usize,
        runtime: Arc<RuntimeEnv>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let reservation = MemoryConsumer::new(format!("TopK[{partition_id}]"))
            .register(&runtime.memory_pool);

        let sort_fields: Vec<_> = build_sort_fields(&expr, &schema)?;

        // TODO there is potential to add special cases for single column sort fields
        // to improve performance
        let row_converter = RowConverter::new(sort_fields)?;
        let scratch_rows =
            row_converter.empty_rows(batch_size, ESTIMATED_BYTES_PER_ROW * batch_size);

        let prefix_row_converter = if common_sort_prefix.is_empty() {
            None
        } else {
            let input_sort_fields: Vec<_> =
                build_sort_fields(&common_sort_prefix, &schema)?;
            Some(RowConverter::new(input_sort_fields)?)
        };

        Ok(Self {
            schema: Arc::clone(&schema),
            metrics: TopKMetrics::new(metrics, partition_id),
            reservation,
            batch_size,
            expr: Arc::from(expr),
            row_converter,
            scratch_rows,
            heap: TopKHeap::new(k, batch_size),
            common_sort_prefix_converter: prefix_row_converter,
            common_sort_prefix: Arc::from(common_sort_prefix),
            finished: false,
        })
    }

    /// Insert `batch`, remembering if any of its values are among
    /// the top k seen so far.
    pub fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Updates on drop
        let baseline = self.metrics.baseline.clone();
        let _timer = baseline.elapsed_compute().timer();

        let sort_keys: Vec<ArrayRef> = self
            .expr
            .iter()
            .map(|expr| {
                let value = expr.expr.evaluate(&batch)?;
                value.into_array(batch.num_rows())
            })
            .collect::<Result<Vec<_>>>()?;

        // reuse existing `Rows` to avoid reallocations
        let rows = &mut self.scratch_rows;
        rows.clear();
        self.row_converter.append(rows, &sort_keys)?;

        // TODO make this algorithmically better?:
        // Idea: filter out rows >= self.heap.max() early (before passing to `RowConverter`)
        //       this avoids some work and also might be better vectorizable.
        let mut batch_entry = self.heap.register_batch(batch.clone());
        for (index, row) in rows.iter().enumerate() {
            match self.heap.max() {
                // heap has k items, and the new row is greater than the
                // current max in the heap ==> it is not a new topk
                Some(max_row) if row.as_ref() >= max_row.row() => {}
                // don't yet have k items or new item is lower than the currently k low values
                None | Some(_) => {
                    self.heap.add(&mut batch_entry, row, index);
                    self.metrics.row_replacements.add(1);
                }
            }
        }
        self.heap.insert_batch_entry(batch_entry);

        // conserve memory
        self.heap.maybe_compact()?;

        // update memory reservation
        self.reservation.try_resize(self.size())?;

        // flag the topK as finished if we know that all
        // subsequent batches are guaranteed to be greater (by byte order, after row conversion) than the top K,
        // which means the top K won't change and the computation can be finished early.
        self.attempt_early_completion(&batch)?;

        Ok(())
    }

    /// If input ordering shares a common sort prefix with the TopK, and if the TopK's heap is full,
    /// check if the computation can be finished early.
    /// This is the case if the last row of the current batch is strictly greater than the max row in the heap,
    /// comparing only on the shared prefix columns.
    fn attempt_early_completion(&mut self, batch: &RecordBatch) -> Result<()> {
        // Early exit if the batch is empty as there is no last row to extract from it.
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // prefix_row_converter is only `Some` if the input ordering has a common prefix with the TopK,
        // so early exit if it is `None`.
        let Some(prefix_converter) = &self.common_sort_prefix_converter else {
            return Ok(());
        };

        // Early exit if the heap is not full (`heap.max()` only returns `Some` if the heap is full).
        let Some(max_topk_row) = self.heap.max() else {
            return Ok(());
        };

        // Evaluate the prefix for the last row of the current batch.
        let last_row_idx = batch.num_rows() - 1;
        let mut batch_prefix_scratch =
            prefix_converter.empty_rows(1, ESTIMATED_BYTES_PER_ROW); // 1 row with capacity ESTIMATED_BYTES_PER_ROW

        self.compute_common_sort_prefix(batch, last_row_idx, &mut batch_prefix_scratch)?;

        // Retrieve the max row from the heap.
        let store_entry = self
            .heap
            .store
            .get(max_topk_row.batch_id)
            .ok_or(internal_datafusion_err!("Invalid batch id in topK heap"))?;
        let max_batch = &store_entry.batch;
        let mut heap_prefix_scratch =
            prefix_converter.empty_rows(1, ESTIMATED_BYTES_PER_ROW); // 1 row with capacity ESTIMATED_BYTES_PER_ROW
        self.compute_common_sort_prefix(
            max_batch,
            max_topk_row.index,
            &mut heap_prefix_scratch,
        )?;

        // If the last row's prefix is strictly greater than the max prefix, mark as finished.
        if batch_prefix_scratch.row(0).as_ref() > heap_prefix_scratch.row(0).as_ref() {
            self.finished = true;
        }

        Ok(())
    }

    // Helper function to compute the prefix for a given batch and row index, storing the result in scratch.
    fn compute_common_sort_prefix(
        &self,
        batch: &RecordBatch,
        last_row_idx: usize,
        scratch: &mut Rows,
    ) -> Result<()> {
        let last_row: Vec<ArrayRef> = self
            .common_sort_prefix
            .iter()
            .map(|expr| {
                expr.expr
                    .evaluate(&batch.slice(last_row_idx, 1))?
                    .into_array(1)
            })
            .collect::<Result<_>>()?;

        self.common_sort_prefix_converter
            .as_ref()
            .unwrap()
            .append(scratch, &last_row)?;
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
        } = self;
        let _timer = metrics.baseline.elapsed_compute().timer(); // time updated on drop

        // break into record batches as needed
        let mut batches = vec![];
        if let Some(mut batch) = heap.emit()? {
            metrics.baseline.output_rows().add(batch.num_rows());

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
    /// The target number of rows for output batches
    batch_size: usize,
    /// Storage for up at most `k` items using a BinaryHeap. Reversed
    /// so that the smallest k so far is on the top
    inner: BinaryHeap<TopKRow>,
    /// Storage the original row values (TopKRow only has the sort key)
    store: RecordBatchStore,
    /// The size of all owned data held by this heap
    owned_bytes: usize,
}

impl TopKHeap {
    fn new(k: usize, batch_size: usize) -> Self {
        assert!(k > 0);
        Self {
            k,
            batch_size,
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
        let new_top_k = if self.inner.len() == self.k {
            let prev_min = self.inner.pop().unwrap();

            // Update batch use
            if prev_min.batch_id == batch_entry.id {
                batch_entry.uses -= 1;
            } else {
                self.store.unuse(prev_min.batch_id);
            }

            // update memory accounting
            self.owned_bytes -= prev_min.owned_size();
            prev_min.with_new_row(row, batch_id, index)
        } else {
            TopKRow::new(row, batch_id, index)
        };

        self.owned_bytes += new_top_k.owned_size();

        // put the new row into the heap
        self.inner.push(new_top_k)
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

        // Indices for each row within its respective RecordBatch
        let indices: Vec<_> = topk_rows
            .iter()
            .enumerate()
            .map(|(i, k)| (i, k.index))
            .collect();

        let record_batches: Vec<_> = topk_rows
            .iter()
            .map(|k| {
                let entry = self.store.get(k.batch_id).expect("invalid stored batch id");
                &entry.batch
            })
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
        // we compact if the number of "unused" rows in the store is
        // past some pre-defined threshold. Target holding up to
        // around 20 batches, but handle cases of large k where some
        // batches might be partially full
        let max_unused_rows = (20 * self.batch_size) + self.k;
        let unused_rows = self.store.unused_rows();

        // don't compact if the store has one extra batch or
        // unused rows is under the threshold
        if self.store.len() <= 2 || unused_rows < max_unused_rows {
            return Ok(());
        }
        // at first, compact the entire thing always into a new batch
        // (maybe we can get fancier in the future about ignoring
        // batches that have a high usage ratio already

        // Note: new batch is in the same order as inner
        let num_rows = self.inner.len();
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

    /// Create a new  TopKRow reusing the existing allocation
    fn with_new_row(
        self,
        new_row: impl AsRef<[u8]>,
        batch_id: u32,
        index: usize,
    ) -> Self {
        let Self {
            mut row,
            batch_id: _,
            index: _,
        } = self;
        row.clear();
        row.extend_from_slice(new_row.as_ref());

        Self {
            row,
            batch_id,
            index,
        }
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
}

impl RecordBatchStore {
    fn new() -> Self {
        Self {
            next_id: 0,
            batches: HashMap::new(),
            batches_size: 0,
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
            self.batches.insert(entry.id, entry);
        }
    }

    /// Clear all values in this store, invalidating all previous batch ids
    fn clear(&mut self) {
        self.batches.clear();
        self.batches_size = 0;
    }

    fn get(&self, id: u32) -> Option<&RecordBatchEntry> {
        self.batches.get(&id)
    }

    /// returns the total number of batches stored in this store
    fn len(&self) -> usize {
        self.batches.len()
    }

    /// Returns the total number of rows in batches minus the number
    /// which are in use
    fn unused_rows(&self) -> usize {
        self.batches
            .values()
            .map(|batch_entry| batch_entry.batch.num_rows() - batch_entry.uses)
            .sum()
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
    use arrow::array::{Float64Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SortOptions;
    use datafusion_common::assert_batches_eq;
    use datafusion_physical_expr::expressions::col;
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

    /// This test validates that the `try_finish` method marks the TopK operator as finished
    /// when the prefix (on column "a") of the last row in the current batch is strictly greater
    /// than the max top‑k row.
    /// The full sort expression is defined on both columns ("a", "b"), but the input ordering is only on "a".
    #[tokio::test]
    async fn test_try_finish_marks_finished_with_prefix() -> Result<()> {
        // Create a schema with two columns.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float64, false),
        ]));

        // Create sort expressions.
        // Full sort: first by "a", then by "b".
        let sort_expr_a = PhysicalSortExpr {
            expr: col("a", schema.as_ref())?,
            options: SortOptions::default(),
        };
        let sort_expr_b = PhysicalSortExpr {
            expr: col("b", schema.as_ref())?,
            options: SortOptions::default(),
        };

        // Input ordering uses only column "a" (a prefix of the full sort).
        let input_ordering = LexOrdering::from(vec![sort_expr_a.clone()]);
        let full_expr = LexOrdering::from(vec![sort_expr_a, sort_expr_b]);

        // Create a dummy runtime environment and metrics.
        let runtime = Arc::new(RuntimeEnv::default());
        let metrics = ExecutionPlanMetricsSet::new();

        // Create a TopK instance with k = 3 and batch_size = 2.
        let mut topk = TopK::try_new(
            0,
            Arc::clone(&schema),
            input_ordering,
            full_expr,
            3,
            2,
            runtime,
            &metrics,
        )?;

        // Create the first batch with two columns:
        // Column "a": [1, 1, 2], Column "b": [20.0, 15.0, 30.0].
        let array_a1: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(2)]));
        let array_b1: ArrayRef = Arc::new(Float64Array::from(vec![20.0, 15.0, 30.0]));
        let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![array_a1, array_b1])?;

        // Insert the first batch.
        // At this point the heap is not yet “finished” because the prefix of the last row of the batch
        // is not strictly greater than the prefix of the max top‑k row (both being `2`).
        topk.insert_batch(batch1)?;
        assert!(
            !topk.finished,
            "Expected 'finished' to be false after the first batch."
        );

        // Create the second batch with two columns:
        // Column "a": [2, 3], Column "b": [10.0, 20.0].
        let array_a2: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(3)]));
        let array_b2: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0]));
        let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![array_a2, array_b2])?;

        // Insert the second batch.
        // The last row in this batch has a prefix value of `3`,
        // which is strictly greater than the max top‑k row (with value `2`),
        // so try_finish should mark the TopK as finished.
        topk.insert_batch(batch2)?;
        assert!(
            topk.finished,
            "Expected 'finished' to be true after the second batch."
        );

        // Verify the TopK correctly emits the top k rows from both batches
        // (the value 10.0 for b is from the second batch).
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
}
