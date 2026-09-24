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

//! [`LogicalBatch`]: a logically contiguous batch stored as a sequence of
//! [`RecordBatch`]es.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array, new_empty_array};
use arrow::compute::{TakeOptions, concat, concat_batches, take};
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, exec_datafusion_err, exec_err};

/// A logically contiguous batch backed by a sequence of [`RecordBatch`]es
/// that share one schema. Methods on this struct accept global row indices.
///
/// # Example
/// ```text
///
///   segment 0 (3 rows)  segment 1 (2 rows)  segment 2 (4 rows)
///   ┌───┬───┬───┐       ┌───┬───┐           ┌───┬───┬───┬───┐
///   │ a │ b │ c │       │ d │ e │           │ f │ g │ h │ i │
///   └───┴───┴───┘       └───┴───┘           └───┴───┴───┴───┘
///     0   1   2           3   4               5   6   7   8   ◀── global row index
/// ```
///
/// # Motivation
///
/// Joins (e.g. Nested Loop Join) usually buffer all build-side input, and next concatenating
/// them into a contiguous batch, before the next step. It will 2X the memory usage
/// since fragmented batches and final contiguous batch exist at the same time. This
/// struct avoids concatenation step, and helps reduce memory usage by 2X.
///
/// Avoiding memory concatenating overhead is not the motivation, since it's usually
/// fast and not a bottleneck in real workloads; at the same time single-batch abstraction
/// help simplify join logic.
///
/// See issue for details:
/// - <https://github.com/apache/datafusion/issues/23076>
///
/// # Potential Improvements
///
/// `LogicalBatch` exposes a logical view of the rows, independent of their
/// physical layout. This allows alternative layouts without changing the
/// interface. For example, fixed-size segments could enable O(1) lookup of
/// the segment containing a given row.
#[derive(Debug, Clone)]
pub(crate) struct LogicalBatch {
    schema: SchemaRef,
    /// The underlying batches, in row order. Empty batches are dropped on
    /// construction, so every segment holds at least one row.
    segments: Vec<RecordBatch>,
    /// `offsets[i]` is the global index of the first row of `segments[i]`;
    /// `offsets[segments.len()]` is the total number of rows.
    offsets: Vec<usize>,
}

impl LogicalBatch {
    /// Creates a logical batch from `batches`, which must all have `schema`.
    ///
    /// # Errors
    ///
    /// Returns an execution error if a batch has a different schema or the
    /// total row count overflows.
    pub(crate) fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        if batches.iter().any(|batch| batch.schema() != schema) {
            return exec_err!("LogicalBatch input batches must have the same schema");
        }
        let segments: Vec<RecordBatch> = batches
            .into_iter()
            .filter(|batch| batch.num_rows() > 0)
            .collect();
        let mut offsets = Vec::with_capacity(segments.len() + 1);
        let mut num_rows: usize = 0;
        offsets.push(num_rows);
        for segment in &segments {
            num_rows = num_rows.checked_add(segment.num_rows()).ok_or_else(|| {
                exec_datafusion_err!("LogicalBatch total row count exceeds usize::MAX")
            })?;
            offsets.push(num_rows);
        }
        Ok(Self {
            schema,
            segments,
            offsets,
        })
    }

    /// Creates a logical batch with no rows.
    pub(crate) fn new_empty(schema: SchemaRef) -> Self {
        Self {
            schema,
            segments: vec![],
            offsets: vec![0],
        }
    }

    pub(crate) fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Total number of rows across all segments.
    pub(crate) fn num_rows(&self) -> usize {
        // `offsets` always holds at least the leading 0
        self.offsets[self.offsets.len() - 1]
    }

    /// Returns the underlying batches, in row order.
    pub(crate) fn into_batches(self) -> Vec<RecordBatch> {
        self.segments
    }

    /// The row at global index `index`.
    ///
    /// # Errors
    ///
    /// Returns an execution error if `index >= num_rows()`.
    pub(crate) fn row(&self, index: usize) -> Result<BatchRow<'_>> {
        if index >= self.num_rows() {
            return exec_err!(
                "row index {index} out of bounds for a batch of {} rows",
                self.num_rows()
            );
        }
        let segment = self.segment_of(index, 0);
        Ok(BatchRow {
            batch: &self.segments[segment],
            index: index - self.offsets[segment],
        })
    }

    /// Resolves global row indices for [`Self::take_column`].
    ///
    /// This plays the role of the index array given to [`take`] on a plain
    /// batch: resolve the indices once, then gather as many columns as
    /// needed with them. Rows may repeat or appear in any order; gathers
    /// preserve that order.
    ///
    /// # Errors
    ///
    /// Returns an execution error if any index is `>= num_rows()` or its
    /// index within the segment cannot be represented as a `u32`.
    pub(crate) fn row_indices(
        &self,
        rows: impl IntoIterator<Item = usize>,
    ) -> Result<RowIndices> {
        let rows = rows.into_iter();
        let mut groups = Vec::new();
        let mut indices = Vec::with_capacity(rows.size_hint().0);
        // Consecutive rows usually sit in the same segment, so retry the last
        // segment before searching
        let mut segment = 0;
        for row in rows {
            if row >= self.num_rows() {
                return exec_err!(
                    "row index {row} out of bounds for a batch of {} rows",
                    self.num_rows()
                );
            }
            let next_segment = self.segment_of(row, segment);
            // Start a new group whenever the segment changes. Keeping separate
            // groups for repeated visits to a segment preserves output order.
            if next_segment != segment && !indices.is_empty() {
                groups.push(Single {
                    segment,
                    indices: UInt32Array::from(std::mem::take(&mut indices)),
                });
            }
            segment = next_segment;
            let index = u32::try_from(row - self.offsets[segment]).map_err(|_| {
                exec_datafusion_err!(
                    "row index {row} within segment {segment} exceeds u32::MAX"
                )
            })?;
            indices.push(index);
        }

        if !indices.is_empty() {
            groups.push(Single {
                segment,
                indices: UInt32Array::from(indices),
            });
        }

        Ok(RowIndices(match groups.len() {
            0 => IndicesVariant::Empty,
            1 => IndicesVariant::Single(groups.pop().unwrap()),
            _ => IndicesVariant::Multi(groups),
        }))
    }

    /// Gathers the rows selected by `indices` from column `column`, like
    /// [`take`] on the column of a plain batch.
    ///
    /// # Errors
    ///
    /// Returns an execution error if the column or resolved row indices
    /// are out of bounds for this batch.
    pub(crate) fn take_column(
        &self,
        column: usize,
        indices: &RowIndices,
    ) -> Result<ArrayRef> {
        let field = self.schema.fields().get(column).ok_or_else(|| {
            exec_datafusion_err!(
                "column index {column} out of bounds for a batch of {} columns",
                self.schema.fields().len()
            )
        })?;
        let take_single = |single: &Single| -> Result<ArrayRef> {
            let batch = self.segments.get(single.segment).ok_or_else(|| {
                exec_datafusion_err!(
                    "segment index {} out of bounds for a batch of {} segments",
                    single.segment,
                    self.segments.len()
                )
            })?;
            let values = batch.columns().get(column).ok_or_else(|| {
                exec_datafusion_err!(
                    "column index {column} out of bounds for a batch of {} columns",
                    batch.num_columns()
                )
            })?;
            take(
                values.as_ref(),
                &single.indices,
                Some(TakeOptions { check_bounds: true }),
            )
            .map_err(|error| exec_datafusion_err!("Failed to gather rows: {error}"))
        };

        Ok(match &indices.0 {
            IndicesVariant::Empty => new_empty_array(field.data_type()),
            IndicesVariant::Single(single) => take_single(single)?,
            IndicesVariant::Multi(groups) => {
                let arrays = groups
                    .iter()
                    .map(take_single)
                    .collect::<Result<Vec<_>, _>>()?;
                let arrays: Vec<&dyn Array> =
                    arrays.iter().map(|array| array.as_ref()).collect();
                concat(&arrays)?
            }
        })
    }

    /// Returns rows `offset..offset + length` as one batch.
    ///
    /// This is zero-copy when the range lies within a single segment, and
    /// copies the rows into a new batch otherwise.
    ///
    /// # Errors
    ///
    /// Returns an execution error if the range is out of bounds or
    /// `offset + length` overflows.
    pub(crate) fn slice(&self, offset: usize, length: usize) -> Result<RecordBatch> {
        let end = offset
            .checked_add(length)
            .filter(|&end| end <= self.num_rows())
            .ok_or_else(|| {
                exec_datafusion_err!(
                    "slice with offset {offset} and length {length} out of bounds for a batch of {} rows",
                    self.num_rows()
                )
            })?;
        if length == 0 {
            return Ok(RecordBatch::new_empty(self.schema()));
        }

        let first = self.segment_of(offset, 0);
        let last = self.segment_of(end - 1, first);
        let head = &self.segments[first];
        let head_offset = offset - self.offsets[first];
        if first == last {
            return Ok(head.slice(head_offset, length));
        }

        // The range spans several segments: only the first and the last one
        // need trimming, the ones in between are taken whole.
        let head = head.slice(head_offset, head.num_rows() - head_offset);
        let tail = self.segments[last].slice(0, end - self.offsets[last]);
        let pieces = std::iter::once(&head)
            .chain(&self.segments[first + 1..last])
            .chain(std::iter::once(&tail));
        Ok(concat_batches(&self.schema, pieces)?)
    }

    /// Index of the segment holding global row `row`, which must be in
    /// bounds. `hint` is checked before falling back to a binary search.
    fn segment_of(&self, row: usize, hint: usize) -> usize {
        debug_assert!(row < self.num_rows());
        if hint + 1 < self.offsets.len()
            && (self.offsets[hint]..self.offsets[hint + 1]).contains(&row)
        {
            return hint;
        }
        // `offsets[0] == 0 <= row`, so at least one offset precedes the row
        self.offsets.partition_point(|&start| start <= row) - 1
    }
}

impl From<RecordBatch> for LogicalBatch {
    fn from(batch: RecordBatch) -> Self {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Self::new_empty(batch.schema());
        }
        Self {
            schema: batch.schema(),
            segments: vec![batch],
            offsets: vec![0, num_rows],
        }
    }
}

/// One row of a [`LogicalBatch`], addressed by the [`RecordBatch`] holding
/// it and the row's index within that batch. Obtained from
/// [`LogicalBatch::row`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct BatchRow<'a> {
    batch: &'a RecordBatch,
    index: usize,
}

impl<'a> BatchRow<'a> {
    /// Row `index` of `batch`.
    ///
    /// # Errors
    ///
    /// Returns an execution error if `index >= batch.num_rows()`.
    pub(crate) fn new(batch: &'a RecordBatch, index: usize) -> Result<Self> {
        if index >= batch.num_rows() {
            return exec_err!(
                "row index {index} out of bounds for a batch of {} rows",
                batch.num_rows()
            );
        }
        Ok(Self { batch, index })
    }

    /// The array holding the row's value for `column`; the value is at
    /// [`Self::index`].
    ///
    /// # Errors
    ///
    /// Returns an execution error if the column index is out of bounds.
    pub(crate) fn column(&self, column: usize) -> Result<&'a ArrayRef> {
        self.batch.columns().get(column).ok_or_else(|| {
            exec_datafusion_err!(
                "column index {column} out of bounds for a batch of {} columns",
                self.batch.num_columns()
            )
        })
    }

    /// The row's index within the arrays returned by [`Self::column`].
    pub(crate) fn index(&self) -> usize {
        self.index
    }
}

/// Global row indices of a [`LogicalBatch`], resolved to the segments that
/// hold them. Built by [`LogicalBatch::row_indices`] and consumed by
/// [`LogicalBatch::take_column`].
#[derive(Debug)]
pub(crate) struct RowIndices(IndicesVariant);

impl RowIndices {
    /// Number of rows selected.
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            IndicesVariant::Empty => 0,
            IndicesVariant::Single(single) => single.indices.len(),
            IndicesVariant::Multi(groups) => {
                groups.iter().map(|single| single.indices.len()).sum()
            }
        }
    }
}

/// Row indices grouped by the segments from which they will be gathered.
#[derive(Debug)]
enum IndicesVariant {
    /// No rows selected
    Empty,
    /// Every row lives in the same segment, so a gather is a plain [`take`].
    ///
    /// ```text
    /// Segment 0:     [10, 20, 30]
    /// Global rows:   [ 0,  1,  2]
    /// Selected rows: [1, 2]
    ///
    /// Single { segment: 0, indices: [1, 2] }
    /// Output: [20, 30]
    /// ```
    Single(Single),
    /// Rows are gathered with [`take`] for each group, then [`concat()`] combines
    /// the results in group order. A segment may appear in several groups.
    ///
    /// ```text
    /// Segment 0: [10, 20]  (global rows 0, 1)
    /// Segment 1: [30, 40]  (global rows 2, 3)
    /// Selected rows: [1, 2]
    ///
    /// [
    ///     Single { segment: 0, indices: [1] },  // take [20]
    ///     Single { segment: 1, indices: [0] },  // take [30]
    /// ]
    /// Output: [20, 30]
    /// ```
    Multi(Vec<Single>),
}

/// One consecutive group of output rows gathered from the same segment.
#[derive(Debug)]
struct Single {
    /// Index into [`LogicalBatch::segments`].
    segment: usize,
    /// Row indices within that segment, in output order.
    indices: UInt32Array,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;

    use arrow::array::{AsArray, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion_common::DataFusionError;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]))
    }

    /// One batch holding `rows` with `a = i` and `b = "s{i}"` (`b` is NULL for
    /// odd `i`).
    fn batch(rows: Range<i32>) -> RecordBatch {
        let a = Int32Array::from_iter_values(rows.clone());
        let b =
            StringArray::from_iter(rows.map(|i| (i % 2 == 0).then(|| format!("s{i}"))));
        RecordBatch::try_new(schema(), vec![Arc::new(a), Arc::new(b)]).unwrap()
    }

    /// Segments of 3, 2 and 4 rows, plus empty ones that must be ignored,
    /// alongside the batch they are logically equal to.
    fn logical_batch() -> (LogicalBatch, RecordBatch) {
        let segments = vec![
            RecordBatch::new_empty(schema()),
            batch(0..3),
            batch(3..5),
            RecordBatch::new_empty(schema()),
            batch(5..9),
            RecordBatch::new_empty(schema()),
        ];
        let expected = concat_batches(&schema(), &segments).unwrap();
        (LogicalBatch::new(schema(), segments).unwrap(), expected)
    }

    #[test]
    fn construction() {
        let (logical_batch, expected) = logical_batch();
        assert_eq!(logical_batch.num_rows(), 9);
        assert_eq!(logical_batch.offsets, vec![0, 3, 5, 9]);
        assert_eq!(logical_batch.schema(), schema());
        let segments = logical_batch.into_batches();
        assert_eq!(segments.len(), 3);
        assert_eq!(concat_batches(&schema(), &segments).unwrap(), expected);

        let empty = LogicalBatch::new_empty(schema());
        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.slice(0, 0).unwrap(), RecordBatch::new_empty(schema()));
        assert_eq!(
            empty
                .take_column(0, &empty.row_indices([]).unwrap())
                .unwrap()
                .len(),
            0
        );

        let single = LogicalBatch::from(batch(0..4));
        assert_eq!(single.num_rows(), 4);
        assert_eq!(single.slice(0, 4).unwrap(), batch(0..4));
    }

    #[test]
    fn row_lookup() {
        let (logical_batch, _) = logical_batch();
        for i in 0..9 {
            let row = logical_batch.row(i).unwrap();
            let a = row.column(0).unwrap().as_primitive::<Int32Type>();
            assert_eq!(a.value(row.index()), i as i32, "row {i}");
        }
        // Rows are addressed within their own segment
        assert_eq!(logical_batch.row(0).unwrap().index(), 0);
        assert_eq!(logical_batch.row(2).unwrap().index(), 2);
        assert_eq!(logical_batch.row(3).unwrap().index(), 0);
        assert_eq!(logical_batch.row(4).unwrap().index(), 1);
        assert_eq!(logical_batch.row(5).unwrap().index(), 0);
        assert_eq!(logical_batch.row(8).unwrap().index(), 3);
    }

    #[test]
    fn row_lookup_out_of_bounds() {
        let (logical_batch, _) = logical_batch();
        for index in [9, usize::MAX] {
            assert!(matches!(
                logical_batch.row(index),
                Err(DataFusionError::Execution(_))
            ));
        }
        assert!(matches!(
            LogicalBatch::new_empty(schema()).row(0),
            Err(DataFusionError::Execution(_))
        ));
    }

    #[test]
    fn row_indices_out_of_bounds() {
        let (logical_batch, _) = logical_batch();
        for rows in [[0, 9], [3, usize::MAX]] {
            assert!(matches!(
                logical_batch.row_indices(rows),
                Err(DataFusionError::Execution(_))
            ));
        }
        assert!(matches!(
            LogicalBatch::new_empty(schema()).row_indices([0]),
            Err(DataFusionError::Execution(_))
        ));
    }

    #[test]
    fn batch_row_out_of_bounds() {
        let batch = batch(0..3);
        for index in [3, usize::MAX] {
            assert!(matches!(
                BatchRow::new(&batch, index),
                Err(DataFusionError::Execution(_))
            ));
        }
        let row = BatchRow::new(&batch, 2).unwrap();
        assert_eq!(row.index(), 2);
        assert_eq!(row.column(0).unwrap(), batch.column(0));
        assert!(matches!(row.column(2), Err(DataFusionError::Execution(_))));
        assert!(matches!(
            BatchRow::new(&RecordBatch::new_empty(schema()), 0),
            Err(DataFusionError::Execution(_))
        ));
    }

    /// Gathers `rows` from both columns through the logical batch and
    /// through `take` on the equivalent plain batch, and asserts they agree.
    fn assert_take(logical_batch: &LogicalBatch, expected: &RecordBatch, rows: &[usize]) {
        let indices = logical_batch.row_indices(rows.iter().copied()).unwrap();
        assert_eq!(indices.len(), rows.len());
        let plain_indices = UInt32Array::from_iter_values(rows.iter().map(|i| *i as u32));
        for column in 0..2 {
            let actual = logical_batch.take_column(column, &indices).unwrap();
            let expected = take(expected.column(column), &plain_indices, None).unwrap();
            assert_eq!(&actual, &expected, "column {column}, rows {rows:?}");
        }
    }

    #[test]
    fn take_within_one_segment() {
        let (logical_batch, expected) = logical_batch();
        let indices = logical_batch.row_indices([3, 4, 4, 3]).unwrap();
        assert!(matches!(
            indices.0,
            IndicesVariant::Single(Single { segment: 1, .. })
        ));
        assert_take(&logical_batch, &expected, &[3, 4, 4, 3]);
        assert_take(&logical_batch, &expected, &[8, 5, 6, 7]);
        assert_take(&logical_batch, &expected, &[0, 0, 0]);
    }

    #[test]
    fn take_across_segments() {
        let (logical_batch, expected) = logical_batch();
        let indices = logical_batch.row_indices([2, 3, 2, 3]).unwrap();
        assert!(matches!(indices.0, IndicesVariant::Multi(_)));
        assert_take(&logical_batch, &expected, &[2, 3, 2, 3]);
        assert_take(&logical_batch, &expected, &[8, 0, 4, 5]);
        assert_take(&logical_batch, &expected, &(0..9).collect::<Vec<_>>());
        // Repeated visits to a segment must stay in selection order.
        assert_take(&logical_batch, &expected, &[3, 4, 4, 7, 5, 3]);
        // Selections may skip segments.
        assert_take(&logical_batch, &expected, &[0, 8]);
    }

    #[test]
    fn take_nothing() {
        let (logical_batch, _) = logical_batch();
        let indices = logical_batch.row_indices([]).unwrap();
        assert_eq!(indices.len(), 0);
        let actual = logical_batch.take_column(1, &indices).unwrap();
        assert_eq!(actual.len(), 0);
        assert_eq!(actual.data_type(), &DataType::Utf8);
    }

    #[test]
    fn take_out_of_bounds() {
        let (logical_batch, _) = logical_batch();
        // Check empty, single-segment, and multi-segment selections.
        for rows in [vec![], vec![0], vec![0, 3]] {
            let indices = logical_batch.row_indices(rows).unwrap();
            assert!(matches!(
                logical_batch.take_column(2, &indices),
                Err(DataFusionError::Execution(_))
            ));
        }

        // Indices resolved against a different batch may refer to missing
        // segments or rows. Neither case should panic inside Arrow's take.
        let smaller = LogicalBatch::from(batch(0..1));
        for row in [2, 8] {
            let indices = logical_batch.row_indices([row]).unwrap();
            assert!(matches!(
                smaller.take_column(0, &indices),
                Err(DataFusionError::Execution(_))
            ));
        }
    }

    #[test]
    fn slice_within_one_segment() {
        let (logical_batch, expected) = logical_batch();
        for (offset, length) in [
            (0, 3),
            (1, 2),
            (3, 2),
            (5, 4),
            (6, 1),
            (8, 1),
            (4, 0),
            (9, 0),
        ] {
            let actual = logical_batch.slice(offset, length).unwrap();
            assert_eq!(
                actual,
                expected.slice(offset, length),
                "{offset}..{}",
                offset + length
            );
        }
    }

    #[test]
    fn slice_across_segments() {
        let (logical_batch, expected) = logical_batch();
        for (offset, length) in [(0, 9), (2, 2), (2, 4), (1, 7), (4, 5), (3, 6)] {
            let actual = logical_batch.slice(offset, length).unwrap();
            assert_eq!(
                actual,
                expected.slice(offset, length),
                "{offset}..{}",
                offset + length
            );
        }
    }

    #[test]
    fn slice_out_of_bounds() {
        let (logical_batch, _) = logical_batch();
        for (offset, length) in [
            (7, 3),
            (10, 0),
            (usize::MAX, 1),
            (1, usize::MAX),
            (usize::MAX, usize::MAX),
        ] {
            assert!(matches!(
                logical_batch.slice(offset, length),
                Err(DataFusionError::Execution(_))
            ));
        }
    }

    #[test]
    fn construction_errors() {
        let empty_schema = Arc::new(Schema::empty());
        assert!(matches!(
            LogicalBatch::new(Arc::clone(&empty_schema), vec![batch(0..1)]),
            Err(DataFusionError::Execution(_))
        ));

        // Empty-schema batches can represent these row counts without
        // allocating a correspondingly large array.
        let batch = |rows| {
            RecordBatch::try_new_with_options(
                Arc::clone(&empty_schema),
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap()
        };
        assert!(matches!(
            LogicalBatch::new(
                Arc::clone(&empty_schema),
                vec![batch(usize::MAX), batch(1)]
            ),
            Err(DataFusionError::Execution(_))
        ));
        if let Some(index) = (u32::MAX as usize).checked_add(1) {
            let large = LogicalBatch::from(batch(usize::MAX));
            assert!(matches!(
                large.row_indices([index]),
                Err(DataFusionError::Execution(_))
            ));
        }
    }

    #[test]
    fn empty_schema_counts_rows() {
        let schema = Arc::new(Schema::empty());
        let batch = |rows| {
            RecordBatch::try_new_with_options(
                Arc::clone(&schema),
                vec![],
                &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .unwrap()
        };
        let logical_batch =
            LogicalBatch::new(Arc::clone(&schema), vec![batch(2), batch(3)]).unwrap();
        assert_eq!(logical_batch.num_rows(), 5);
        assert_eq!(logical_batch.row(4).unwrap().index(), 2);
        assert_eq!(logical_batch.slice(1, 3).unwrap().num_rows(), 3);
        assert_eq!(logical_batch.slice(3, 2).unwrap().num_rows(), 2);
    }
}
