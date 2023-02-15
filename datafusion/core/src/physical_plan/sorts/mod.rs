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

//! Sort functionalities

use crate::{error::Result, physical_plan::SendableRecordBatchStream};
use std::{
    fmt::{Debug, Formatter},
    pin::Pin,
    sync::Arc,
};

mod cursor;
mod index;
pub mod sort;
pub mod sort_preserving_merge;

use arrow::{
    record_batch::RecordBatch,
    row::{Row, RowParser, Rows},
};
pub use cursor::SortKeyCursor;
use futures::{Stream, StreamExt, TryStreamExt};
pub use index::RowIndex;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;

use super::common::AbortOnDropSingle;

pub(crate) type RowStream = Pin<Box<dyn Stream<Item = Option<RowBatch>> + Send>>;
pub(crate) type SortStreamItem = Result<(RecordBatch, Option<RowBatch>)>;
pub(crate) type SendableSortStream = Pin<Box<dyn Stream<Item = SortStreamItem> + Send>>;
pub(crate) struct SortedStream {
    stream: SendableSortStream,
    mem_used: usize,
    // flag is only true if this was intialized wiith `new_no_row_encoding`
    row_encoding_ignored: bool,
}
impl SortedStream {
    pub(crate) fn new(stream: SendableSortStream, mem_used: usize) -> Self {
        Self {
            stream,
            mem_used,
            row_encoding_ignored: false,
        }
    }

    pub(crate) fn new_from_streams(
        stream: SendableRecordBatchStream,
        mem_used: usize,
        row_stream: RowStream,
    ) -> Self {
        let stream = Box::pin(stream.zip(row_stream).map(|item| {
            let batch: Result<RecordBatch> = item.0;
            let rows: Option<RowBatch> = item.1;
            match batch {
                Ok(batch) => Ok((batch, rows)),
                Err(err) => Err(err),
            }
        }));
        Self {
            stream,
            mem_used,
            row_encoding_ignored: false,
        }
        // if let Some(row_stream) = row_stream {
        // } else {
        //     Self::new_no_row_encoding(stream, mem_used)
        // }
    }
    /// create stream where the row encoding for each batch is always None
    pub(crate) fn new_no_row_encoding(
        stream: SendableRecordBatchStream,
        mem_used: usize,
    ) -> Self {
        let stream = Box::pin(stream.map_ok(|batch| (batch, None)));
        Self {
            stream,
            mem_used,
            row_encoding_ignored: true,
        }
    }
}
impl Debug for SortedStream {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InMemSorterStream")
    }
}

/// Cloneable batch of rows taken from multiple [RowSelection]s
#[derive(Debug, Clone)]
pub struct RowBatch {
    // refs to the rows referenced by `indices`
    rows: Vec<Arc<RowSelection>>,
    // first item = index of the ref in `rows`, second item=index within that `RowSelection`
    indices: Arc<Vec<(usize, usize)>>,
}

impl RowBatch {
    /// Create new batch of rows selected from `rows`.
    ///
    /// `indices` defines where each row comes from: first element of the tuple is the index
    /// of the ref in `rows`, second is the index within that `RowSelection`.
    pub fn new(rows: Vec<Arc<RowSelection>>, indices: Vec<(usize, usize)>) -> Self {
        Self {
            rows,
            indices: Arc::new(indices),
        }
    }

    /// Returns the nth row in the batch.
    pub fn row(&self, n: usize) -> Row {
        let (rows_ref_idx, row_idx) = self.indices[n];
        self.rows[rows_ref_idx].row(row_idx)
    }

    /// Number of rows selected
    pub fn num_rows(&self) -> usize {
        self.indices.len()
    }
    /// Iterate over rows in their selected order
    pub fn iter(&self) -> RowBatchIter {
        RowBatchIter {
            row_selection: self,
            cur_idx: 0,
        }
    }
    /// Amount of bytes
    pub fn memory_size(&self) -> usize {
        let indices_size = self.indices.len() * 2 * std::mem::size_of::<usize>();
        // rows are refs so dont count the size inside the refs, just the refs itself?
        let rows_size = 0;
        rows_size + indices_size + std::mem::size_of::<Self>()
    }
}
impl From<RowSelection> for RowBatch {
    fn from(value: RowSelection) -> Self {
        Self {
            indices: Arc::new((0..value.num_rows()).map(|i| (0, i)).collect()),
            rows: vec![Arc::new(value)],
        }
    }
}
impl From<Rows> for RowBatch {
    fn from(value: Rows) -> Self {
        Into::<RowSelection>::into(value).into()
    }
}

/// Iterate over each row in a [`RowBatch`]
pub struct RowBatchIter<'a> {
    row_selection: &'a RowBatch,
    cur_idx: usize,
}
impl<'a> Iterator for RowBatchIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_idx < self.row_selection.num_rows() {
            let row = self.row_selection.row(self.cur_idx);
            self.cur_idx += 1;
            Some(row)
        } else {
            None
        }
    }
}

/// A selection of rows from the same [`RowData`].
#[derive(Debug)]
pub struct RowSelection {
    rows: RowData,
    // None when this `RowSelection` is equivalent to its `Rows`
    indices: Option<Vec<usize>>,
}
#[derive(Debug)]
enum RowData {
    Rows(Rows),
    Spilled {
        parser: RowParser,
        bytes: Vec<bytes::Bytes>,
    },
}
impl RowData {
    fn row(&self, n: usize) -> Row {
        match self {
            RowData::Rows(rows) => rows.row(n),
            RowData::Spilled { parser, bytes } => parser.parse(&bytes[n]),
        }
    }
    fn size(&self) -> usize {
        match self {
            RowData::Rows(rows) => rows.size(),
            RowData::Spilled { bytes, .. } => bytes.len() + std::mem::size_of::<Self>(),
        }
    }
    fn num_rows(&self) -> usize {
        match self {
            RowData::Rows(rows) => rows.num_rows(),
            RowData::Spilled { bytes, .. } => bytes.len(),
        }
    }
}
impl RowSelection {
    /// New
    pub fn new(rows: Rows, indices: Vec<usize>) -> Self {
        Self {
            rows: RowData::Rows(rows),
            indices: Some(indices),
        }
    }
    fn from_spilled(parser: RowParser, bytes: Vec<bytes::Bytes>) -> Self {
        Self {
            rows: RowData::Spilled { parser, bytes },
            indices: None,
        }
    }
    /// Get the nth row of the selection.
    pub fn row(&self, n: usize) -> Row {
        if let Some(ref indices) = self.indices {
            let idx = indices[n];
            self.rows.row(idx)
        } else {
            self.rows.row(n)
        }
    }

    /// Iterate over the rows in the selected order.
    pub fn iter(&self) -> RowSelectionIter {
        RowSelectionIter {
            row_selection: self,
            cur_n: 0,
        }
    }
    /// Number of bytes held in rows and indices.
    pub fn size(&self) -> usize {
        let indices_size = self
            .indices
            .as_ref()
            .map(|i| i.len() * std::mem::size_of::<usize>())
            .unwrap_or(0);
        self.rows.size() + indices_size + std::mem::size_of::<Self>()
    }

    fn num_rows(&self) -> usize {
        if let Some(ref indices) = self.indices {
            indices.len()
        } else {
            self.rows.num_rows()
        }
    }
}
impl From<Rows> for RowSelection {
    fn from(value: Rows) -> Self {
        Self {
            indices: None,
            rows: RowData::Rows(value),
        }
    }
}
impl From<RowData> for RowSelection {
    fn from(value: RowData) -> Self {
        Self {
            indices: None,
            rows: value,
        }
    }
}
/// Iterator for [`RowSelection`]
pub struct RowSelectionIter<'a> {
    row_selection: &'a RowSelection,
    cur_n: usize,
}
impl<'a> Iterator for RowSelectionIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_n < self.row_selection.num_rows() {
            let row = self.row_selection.row(self.cur_n);
            self.cur_n += 1;
            Some(row)
        } else {
            None
        }
    }
}
pub(crate) struct SortReceiverStream {
    inner: ReceiverStream<SortStreamItem>,
    #[allow(dead_code)]
    drop_helper: AbortOnDropSingle<()>,
}
impl SortReceiverStream {
    fn new(rx: mpsc::Receiver<SortStreamItem>, handle: JoinHandle<()>) -> Self {
        let stream = ReceiverStream::new(rx);
        Self {
            inner: stream,
            drop_helper: AbortOnDropSingle::new(handle),
        }
    }
}
impl Stream for SortReceiverStream {
    type Item = SortStreamItem;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int64Array,
        datatypes::DataType,
        record_batch::RecordBatch,
        row::{RowConverter, SortField},
    };

    use crate::assert_batches_eq;

    use super::*;

    fn int64_rows(
        conv: &mut RowConverter,
        values: impl IntoIterator<Item = i64>,
    ) -> Rows {
        let array: Int64Array = values.into_iter().map(Some).collect();
        let batch =
            RecordBatch::try_from_iter(vec![("c1", Arc::new(array) as _)]).unwrap();
        conv.convert_columns(batch.columns()).unwrap()
    }

    #[test]
    fn test_row_batch_and_sorted_rows() {
        let mut conv = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let s1 = RowSelection::new(int64_rows(&mut conv, 0..3), vec![2, 2, 1]);
        let s2 = RowSelection::new(int64_rows(&mut conv, 5..8), vec![1, 2, 0]);
        let s3: RowSelection = int64_rows(&mut conv, 2..4).into(); // null indices case
        let selection = RowBatch::new(
            vec![s1, s2, s3].into_iter().map(Arc::new).collect(),
            vec![
                (2, 0), // 2
                (0, 2), // 1
                (0, 0), // 2
                (1, 1), // 7
            ],
        );
        let rows: Vec<Row> = selection.iter().collect();
        assert_eq!(rows.len(), 4);
        let parsed = conv.convert_rows(rows).unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("c1", parsed.get(0).unwrap().clone())])
                .unwrap();
        let expected = vec![
            "+----+", //
            "| c1 |", //
            "+----+", //
            "| 2  |", //
            "| 1  |", //
            "| 2  |", //
            "| 7  |", //
            "+----+",
        ];
        assert_batches_eq!(expected, &[batch]);
    }
}
