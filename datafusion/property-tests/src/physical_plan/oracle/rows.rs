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

//! Comparisons of the rows of two sets of batches: as multisets, in order,
//! and as a prefix of a sorted sequence.
//!
//! Values are compared exactly, including floating point values. The values
//! `SourceSpec` generates for floating point columns are small multiples of
//! 0.5, so sums of them are exact whatever order an operator adds them in.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion_common::Result;
use datafusion_physical_expr::LexOrdering;

use super::ordering::sort_keys;

/// Rows in the arrow row format, so that equal rows have equal bytes
type EncodedRows = Vec<Vec<u8>>;

/// Every row of `left` and `right`, encoded. Returns an error if the batches
/// do not all have the same column types.
fn encode_pair(
    left: &[RecordBatch],
    right: &[RecordBatch],
) -> Result<(EncodedRows, EncodedRows)> {
    let Some(first) = left.iter().chain(right).next() else {
        return Ok((vec![], vec![]));
    };
    let fields = first
        .columns()
        .iter()
        .map(|column| SortField::new(column.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields)?;
    let encode = |batches: &[RecordBatch]| -> Result<EncodedRows> {
        let mut rows = vec![];
        for batch in batches {
            // The converter cannot count the rows of a batch without columns
            if batch.num_columns() == 0 && first.num_columns() == 0 {
                rows.extend(std::iter::repeat_n(vec![], batch.num_rows()));
                continue;
            }
            let encoded = converter.convert_columns(batch.columns())?;
            rows.extend(encoded.iter().map(|row| row.as_ref().to_vec()));
        }
        Ok(rows)
    };
    Ok((encode(left)?, encode(right)?))
}

/// The number of rows of `candidate` without an equal row in `reference`,
/// counting rows as a multiset
fn count_unmatched(candidate: &[Vec<u8>], reference: &[Vec<u8>]) -> usize {
    let mut available: HashMap<&[u8], usize> = HashMap::new();
    for row in reference {
        *available.entry(row).or_default() += 1;
    }
    candidate
        .iter()
        .filter(|row| match available.get_mut(row.as_slice()) {
            Some(count) if *count > 0 => {
                *count -= 1;
                false
            }
            _ => true,
        })
        .count()
}

/// Returns the number of rows of `candidate` that do not appear in
/// `reference`, counting rows as a multiset: a row that appears twice in
/// `candidate` must appear twice in `reference`. Zero means `candidate` is
/// contained in `reference`.
///
/// Returns an error if the batches do not all have the same column types.
pub fn unmatched_rows(
    candidate: &[RecordBatch],
    reference: &[RecordBatch],
) -> Result<usize> {
    let (candidate, reference) = encode_pair(candidate, reference)?;
    Ok(count_unmatched(&candidate, &reference))
}

/// Returns true if `left` and `right` have the same rows, as multisets, in
/// any order and split into batches in any way.
///
/// Returns an error if the batches do not all have the same column types.
pub fn same_rows(left: &[RecordBatch], right: &[RecordBatch]) -> Result<bool> {
    let (mut left, mut right) = encode_pair(left, right)?;
    left.sort_unstable();
    right.sort_unstable();
    Ok(left == right)
}

/// Returns true if `left` and `right` have the same rows in the same order,
/// split into batches in any way.
///
/// Returns an error if the batches do not all have the same column types.
pub fn same_rows_in_order(left: &[RecordBatch], right: &[RecordBatch]) -> Result<bool> {
    let (left, right) = encode_pair(left, right)?;
    Ok(left == right)
}

/// Checks that `candidate` is a prefix of `reference` when rows that tie on
/// `ordering` may appear in any order and may be exchanged for each other.
/// This is what limiting a sorted sequence to its first rows may produce when
/// ties are broken differently. Returns the position of the first row of
/// `candidate` where this fails, or `None` if it holds.
///
/// Precisely, for every position `i` of `candidate`:
///
/// - `reference` has a row at `i`, and it has the same sort key as the row of
///   `candidate` at `i`.
/// - The rows of `candidate` in each run of equal sort keys appear, as a
///   multiset, among the rows of `reference` with that sort key in the same
///   run. The run in `reference` can continue past the end of `candidate`,
///   so which of the tied rows at the end are kept does not matter.
///
/// `candidate` can be shorter than `reference`, including empty. Neither side
/// has to be sorted: if `reference` is not sorted, `candidate` must be a
/// prefix of it with the same exceptions.
///
/// Returns an error if the ordering cannot be evaluated on the batches, or
/// the batches do not all have the same column types.
pub fn first_non_prefix_row(
    reference: &[RecordBatch],
    candidate: &[RecordBatch],
    ordering: &LexOrdering,
) -> Result<Option<usize>> {
    let [reference_keys, candidate_keys] = sort_keys([reference, candidate], ordering)?;
    let (reference_rows, candidate_rows) = encode_pair(reference, candidate)?;

    let mut start = 0;
    while start < candidate_keys.len() {
        let key = &candidate_keys[start];
        // The run of rows with this key in `candidate`, whose keys must match
        // `reference` position by position
        let mut end = start;
        while end < candidate_keys.len() && candidate_keys[end] == *key {
            if reference_keys.get(end) != Some(key) {
                return Ok(Some(end));
            }
            end += 1;
        }
        // The same run in `reference`, which can be longer
        let mut reference_end = end;
        while reference_keys.get(reference_end) == Some(key) {
            reference_end += 1;
        }
        let unmatched = count_unmatched(
            &candidate_rows[start..end],
            &reference_rows[start..reference_end],
        );
        if unmatched > 0 {
            return Ok(Some(start));
        }
        start = end;
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr::expressions::col;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("x", DataType::Float64, true),
        ]))
    }

    /// A batch with one row per entry of `rows`
    fn batch(rows: &[(Option<i32>, &str, Option<f64>)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(rows.iter().map(|r| r.0).collect::<Int32Array>()),
                Arc::new(rows.iter().map(|r| Some(r.1)).collect::<StringArray>()),
                Arc::new(rows.iter().map(|r| r.2).collect::<Float64Array>()),
            ],
        )
        .unwrap()
    }

    fn by_k() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("k", &schema()).unwrap(),
        )])
        .unwrap()
    }

    #[test]
    fn same_rows_ignores_order_and_batch_boundaries() {
        let a = [
            batch(&[(Some(1), "a", Some(1.0)), (Some(2), "b", None)]),
            batch(&[(None, "c", Some(3.0))]),
        ];
        let b = [
            batch(&[]),
            batch(&[(None, "c", Some(3.0))]),
            batch(&[(Some(2), "b", None), (Some(1), "a", Some(1.0))]),
        ];
        assert!(same_rows(&a, &b).unwrap());
        assert!(!same_rows_in_order(&a, &b).unwrap());
        assert!(same_rows_in_order(&a, &a).unwrap());
    }

    #[test]
    fn same_rows_counts_duplicates() {
        let once = [batch(&[(Some(1), "a", None), (Some(2), "b", None)])];
        let twice = [batch(&[
            (Some(1), "a", None),
            (Some(1), "a", None),
            (Some(2), "b", None),
        ])];
        assert!(!same_rows(&once, &twice).unwrap());
        assert_eq!(unmatched_rows(&once, &twice).unwrap(), 0);
        assert_eq!(unmatched_rows(&twice, &once).unwrap(), 1);
    }

    #[test]
    fn nulls_differ_from_values() {
        let null = [batch(&[(None, "a", None)])];
        let value = [batch(&[(Some(0), "a", Some(0.0))])];
        assert!(!same_rows(&null, &value).unwrap());
        assert_eq!(unmatched_rows(&null, &value).unwrap(), 1);
    }

    #[test]
    fn floats_are_compared_exactly() {
        let a = [batch(&[(Some(1), "a", Some(0.1 + 0.2))])];
        let b = [batch(&[(Some(1), "a", Some(0.3))])];
        assert!(!same_rows(&a, &b).unwrap());
    }

    #[test]
    fn nan_equals_nan() {
        let a = [batch(&[(Some(1), "a", Some(f64::NAN))])];
        assert!(same_rows(&a, &a).unwrap());
        assert!(same_rows_in_order(&a, &a).unwrap());
    }

    #[test]
    fn empty_inputs() {
        let rows = [batch(&[(Some(1), "a", None)])];
        assert!(same_rows(&[], &[]).unwrap());
        assert!(same_rows(&[], &[batch(&[])]).unwrap());
        assert!(!same_rows(&rows, &[]).unwrap());
        assert_eq!(unmatched_rows(&[], &rows).unwrap(), 0);
        assert_eq!(unmatched_rows(&rows, &[]).unwrap(), 1);
        assert_eq!(first_non_prefix_row(&rows, &[], &by_k()).unwrap(), None);
        assert_eq!(first_non_prefix_row(&[], &rows, &by_k()).unwrap(), Some(0));
    }

    #[test]
    fn batches_without_columns() {
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(3));
        let empty_schema = Arc::new(Schema::empty());
        let three =
            RecordBatch::try_new_with_options(empty_schema, vec![], &options).unwrap();
        let three = std::slice::from_ref(&three);
        assert!(same_rows(three, three).unwrap());
        assert!(!same_rows(three, &[three[0].slice(0, 2)]).unwrap());
    }

    #[test]
    fn different_column_types_are_an_error() {
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)])),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let rows = [batch(&[(Some(1), "a", None)])];
        assert!(same_rows(&rows, &[other]).is_err());
    }

    #[test]
    fn prefix_modulo_ties() {
        // Sorted by k; rows with k = 2 tie
        let reference = [
            batch(&[(Some(1), "a", None), (Some(2), "b", None)]),
            batch(&[
                (Some(2), "c", None),
                (Some(2), "d", None),
                (Some(3), "e", None),
            ]),
        ];
        let prefix = |rows: &[(Option<i32>, &str, Option<f64>)]| {
            first_non_prefix_row(&reference, &[batch(rows)], &by_k()).unwrap()
        };
        // An exact prefix, of any length
        assert_eq!(prefix(&[]), None);
        assert_eq!(prefix(&[(Some(1), "a", None)]), None);
        assert_eq!(
            prefix(&[
                (Some(1), "a", None),
                (Some(2), "b", None),
                (Some(2), "c", None),
                (Some(2), "d", None),
                (Some(3), "e", None),
            ]),
            None
        );
        // Tied rows in another order, or other tied rows at the end
        assert_eq!(
            prefix(&[
                (Some(1), "a", None),
                (Some(2), "d", None),
                (Some(2), "b", None)
            ]),
            None
        );
        assert_eq!(prefix(&[(Some(1), "a", None), (Some(2), "c", None)]), None);
        // A row that is not first in sort order
        assert_eq!(prefix(&[(Some(2), "b", None)]), Some(0));
        // A row skipped before a later sort key
        assert_eq!(
            prefix(&[(Some(1), "a", None), (Some(3), "e", None)]),
            Some(1)
        );
        // A tied row that is not in the reference
        assert_eq!(
            prefix(&[(Some(1), "a", None), (Some(2), "z", None)]),
            Some(1)
        );
        // A tied row repeated more often than in the reference
        assert_eq!(
            prefix(&[
                (Some(1), "a", None),
                (Some(2), "b", None),
                (Some(2), "b", None)
            ]),
            Some(1)
        );
        // Longer than the reference
        let mut longer: Vec<_> = reference.to_vec();
        longer.push(batch(&[(Some(4), "f", None)]));
        assert_eq!(
            first_non_prefix_row(&reference, &longer, &by_k()).unwrap(),
            Some(5)
        );
    }

    #[test]
    fn prefix_respects_sort_options_and_nulls() {
        let descending = LexOrdering::new(vec![PhysicalSortExpr::new(
            col("k", &schema()).unwrap(),
            arrow::compute::SortOptions {
                descending: true,
                nulls_first: true,
            },
        )])
        .unwrap();
        let reference = [batch(&[
            (None, "a", None),
            (None, "b", None),
            (Some(2), "c", None),
            (Some(1), "d", None),
        ])];
        let candidate = [batch(&[
            (None, "b", None),
            (None, "a", None),
            (Some(2), "c", None),
        ])];
        assert_eq!(
            first_non_prefix_row(&reference, &candidate, &descending).unwrap(),
            None
        );
        let candidate = [batch(&[(None, "a", None), (Some(1), "d", None)])];
        assert_eq!(
            first_non_prefix_row(&reference, &candidate, &descending).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn every_generated_type_can_be_compared() {
        use crate::physical_plan::fixtures::SourceSpec;
        use arrow::datatypes::TimeUnit;

        // The types `SourceSpec` generates
        let types = [
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("+01:00".into())),
            DataType::Decimal128(10, 2),
        ];
        let fields: Vec<Field> = types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), true))
            .collect();
        let generated = SourceSpec::new(Arc::new(Schema::new(fields)))
            .with_partition_rows(&[50])
            .build()
            .unwrap();
        let batches = &generated.partitions()[0];
        assert!(same_rows_in_order(batches, batches).unwrap());
        let reversed: Vec<RecordBatch> = batches.iter().rev().cloned().collect();
        assert!(same_rows(batches, &reversed).unwrap());

        // `PlaceholderRowExec` produces `Null` columns
        let nulls = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Null, true)])),
            vec![arrow::array::new_null_array(&DataType::Null, 3)],
        )
        .unwrap();
        let nulls = std::slice::from_ref(&nulls);
        assert!(same_rows(nulls, nulls).unwrap());
    }
}
