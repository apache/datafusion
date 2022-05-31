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

//! Contains tests for SortKeyCursor

use std::{cmp::Ordering, sync::Arc};

use arrow::{array::Int64Array, compute::SortOptions, record_batch::RecordBatch};
use datafusion::physical_plan::sorts::{RowIndex, SortKeyCursor};
use datafusion_physical_expr::expressions::col;

#[test]
fn test_single_column() {
    let batch1 = int64_batch(vec![Some(1), Some(2), Some(5), Some(6)]);
    let batch2 = int64_batch(vec![Some(3), Some(4), Some(8), Some(9)]);

    let mut cursor1 = CursorBuilder::new(batch1)
        .with_stream_idx(11)
        .with_batch_id(0)
        .build();

    let mut cursor2 = CursorBuilder::new(batch2)
        .with_stream_idx(22)
        .with_batch_id(0)
        .build();

    let expected = vec![
        "11: (0, 0)",
        "11: (0, 1)",
        "22: (0, 0)",
        "22: (0, 1)",
        "11: (0, 2)",
        "11: (0, 3)",
        "22: (0, 2)",
        "22: (0, 3)",
    ];

    assert_indexes(expected, run(&mut cursor1, &mut cursor2));
}

#[test]
fn test_stable_compare() {
    // Validate ties are broken by the lower stream idx to ensure stable sort
    let batch1 = int64_batch(vec![Some(3), Some(4)]);
    let batch2 = int64_batch(vec![Some(3)]);

    let cursor1 = CursorBuilder::new(batch1)
        // higher stream index
        .with_stream_idx(33)
        .with_batch_id(0);

    let cursor2 = CursorBuilder::new(batch2)
        // Lower stream index -- should always be first
        .with_stream_idx(22)
        .with_batch_id(0);

    let expected = vec!["22: (0, 0)", "33: (0, 0)", "33: (0, 1)"];

    // Output should be the same, regardless of order
    assert_indexes(
        &expected,
        run(&mut cursor1.clone().build(), &mut cursor2.clone().build()),
    );
    assert_indexes(&expected, run(&mut cursor2.build(), &mut cursor1.build()));
}

/// Runs the two cursors to completion, sorting them, and
/// returning the sorted order of rows that would have produced
fn run(cursor1: &mut SortKeyCursor, cursor2: &mut SortKeyCursor) -> Vec<RowIndex> {
    let mut indexes = vec![];
    loop {
        println!(
            "(cursor1.is_finished(), cursor2.is_finished()): ({}, {})",
            cursor1.is_finished(),
            cursor2.is_finished()
        );

        match (cursor1.is_finished(), cursor2.is_finished()) {
            (true, true) => return indexes,
            (true, false) => return drain(cursor2, indexes),
            (false, true) => return drain(cursor1, indexes),
            // both cursors have more rows
            (false, false) => match cursor1.compare(cursor2).unwrap() {
                Ordering::Less => {
                    indexes.push(advance(cursor1));
                }
                Ordering::Equal => {
                    indexes.push(advance(cursor1));
                    indexes.push(advance(cursor2));
                }
                Ordering::Greater => {
                    indexes.push(advance(cursor2));
                }
            },
        }
    }
}

// Advance the cursor and return the RowIndex created
fn advance(cursor: &mut SortKeyCursor) -> RowIndex {
    let row_idx = cursor.advance();
    RowIndex {
        stream_idx: cursor.stream_idx(),
        batch_idx: cursor.batch_id(),
        row_idx,
    }
}

// Drain remaining items in the cursor, appending result to indexes
fn drain(cursor: &mut SortKeyCursor, mut indexes: Vec<RowIndex>) -> Vec<RowIndex> {
    while !cursor.is_finished() {
        indexes.push(advance(cursor))
    }
    indexes
}

/// Return the values as an [`Int64Array`] single record batch, with
/// column "c1"
fn int64_batch(values: impl IntoIterator<Item = Option<i64>>) -> RecordBatch {
    let array: Int64Array = values.into_iter().collect();
    RecordBatch::try_from_iter(vec![("c1", Arc::new(array) as _)]).unwrap()
}

/// helper for creating cursors to test
#[derive(Debug, Clone)]
struct CursorBuilder {
    batch: RecordBatch,
    stream_idx: Option<usize>,
    batch_id: Option<usize>,
}

impl CursorBuilder {
    fn new(batch: RecordBatch) -> Self {
        Self {
            batch,
            stream_idx: None,
            batch_id: None,
        }
    }

    /// Set the stream index
    fn with_stream_idx(mut self, stream_idx: usize) -> Self {
        self.stream_idx = Some(stream_idx);
        self
    }

    /// Set the stream index
    fn with_batch_id(mut self, batch_id: usize) -> Self {
        self.batch_id = Some(batch_id);
        self
    }

    fn build(self) -> SortKeyCursor {
        let Self {
            batch,
            stream_idx,
            batch_id,
        } = self;
        let c1 = col("c1", &batch.schema()).unwrap();
        let sort_key = vec![c1];

        let sort_options = Arc::new(vec![SortOptions::default()]);

        SortKeyCursor::new(
            stream_idx.expect("stream idx not set"),
            batch_id.expect("batch id not set"),
            &batch,
            &sort_key,
            sort_options,
        )
        .unwrap()
    }
}

/// Compares [`RowIndex`]es with a vector of strings, the result of
/// pretty formatting the [`RowIndex`]es.
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
fn assert_indexes(
    expected_indexes: impl IntoIterator<Item = impl AsRef<str>>,
    indexes: impl IntoIterator<Item = RowIndex>,
) {
    let expected_lines: Vec<_> = expected_indexes
        .into_iter()
        .map(|s| s.as_ref().to_string())
        .collect();

    let actual_lines = format_as_strings(indexes);

    assert_eq!(
        expected_lines, actual_lines,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_lines, actual_lines
    );
}

/// Formats an terator of RowIndexes into strings for comparisons
///
/// ```text
/// stream: (batch, index)
/// ```
///
/// for example,
/// ```text
/// 1: (0, 2)
/// ```
/// means "Stream 1, batch id 0, row index 2"
fn format_as_strings(indexes: impl IntoIterator<Item = RowIndex>) -> Vec<String> {
    indexes
        .into_iter()
        .map(|row_index| {
            format!(
                "{}: ({}, {})",
                row_index.stream_idx, row_index.batch_idx, row_index.row_idx
            )
        })
        .collect()
}
