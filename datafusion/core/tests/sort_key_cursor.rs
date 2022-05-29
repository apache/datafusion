//! Contains tests for SortKeyCursor

use std::{cmp::Ordering, sync::Arc};

use arrow::{array::Int64Array, compute::SortOptions, record_batch::RecordBatch};
use datafusion::physical_plan::sorts::{RowIndex, SortKeyCursor};
use datafusion_physical_expr::expressions::col;

/// Compares [`RowIndex`]es with a vector of strings, the result of
/// pretty formatting the [`RowIndex`]es. This is a macro so errors
/// appear on the correct line.
///
/// Designed so that failure output can be directly copy/pasted
/// into the test code as expected results.
///
/// Expects to be called about like this:
///
/// `assert_indexes!(expected_indexes: &[&str], indexes: &[RowIndex])`
#[macro_export]
macro_rules! assert_indexes {
    ($EXPECTED_LINES: expr, $INDEXES: expr) => {
        let expected_lines: Vec<String> =
            $EXPECTED_LINES.iter().map(|&s| s.into()).collect();

        let actual_lines = format_as_strings($INDEXES);

        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    };
}

#[test]
fn test_single_column() {
    let array1 = Int64Array::from(vec![Some(1), Some(2), Some(5), Some(6)]);
    let batch1 = RecordBatch::try_from_iter(vec![("c1", Arc::new(array1) as _)]).unwrap();

    let array2 = Int64Array::from(vec![Some(3), Some(4), Some(8), Some(9)]);
    let batch2 = RecordBatch::try_from_iter(vec![("c1", Arc::new(array2) as _)]).unwrap();

    let c1 = col("c1", &batch1.schema()).unwrap();
    let sort_key = vec![c1];

    let sort_options = Arc::new(vec![SortOptions::default()]);

    let mut cursor1 =
        SortKeyCursor::new(1, 0, &batch1, &sort_key, Arc::clone(&sort_options)).unwrap();
    let mut cursor2 =
        SortKeyCursor::new(2, 0, &batch2, &sort_key, Arc::clone(&sort_options)).unwrap();

    let expected = vec![
        "1: (0, 0)",
        "1: (0, 1)",
        "2: (0, 0)",
        "2: (0, 1)",
        "1: (0, 2)",
        "1: (0, 3)",
        "2: (0, 2)",
        "2: (0, 3)",
    ];

    assert_indexes!(expected, run(&mut cursor1, &mut cursor2));
}

/// Runs the two cursors to completion, sorting them, and
/// returning the sorted order of rows that would have produced
fn run(cursor1: &mut SortKeyCursor, cursor2: &mut SortKeyCursor) -> Vec<RowIndex> {
    let mut indexes = vec![];

    // advance through the two cursors
    // TODO verify the order is correct
    loop {
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
