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

//! This module provides the bisect function, which implements binary search.

use crate::{DataFusionError, Result, ScalarValue};
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::compute;
use arrow::compute::{lexicographical_partition_ranges, SortColumn, SortOptions};
use arrow::datatypes::UInt32Type;
use arrow::record_batch::RecordBatch;
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithLocation};
use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;
use std::ops::Range;

/// Given column vectors, returns row at `idx`.
pub fn get_row_at_idx(columns: &[ArrayRef], idx: usize) -> Result<Vec<ScalarValue>> {
    columns
        .iter()
        .map(|arr| ScalarValue::try_from_array(arr, idx))
        .collect()
}

/// Construct a new RecordBatch from the rows of the `record_batch` at the `indices`.
pub fn get_record_batch_at_indices(
    record_batch: &RecordBatch,
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<RecordBatch> {
    let new_columns = get_arrayref_at_indices(record_batch.columns(), indices)?;
    RecordBatch::try_new(record_batch.schema(), new_columns)
        .map_err(DataFusionError::ArrowError)
}

/// This function compares two tuples depending on the given sort options.
pub fn compare_rows(
    x: &[ScalarValue],
    y: &[ScalarValue],
    sort_options: &[SortOptions],
) -> Result<Ordering> {
    let zip_it = x.iter().zip(y.iter()).zip(sort_options.iter());
    // Preserving lexical ordering.
    for ((lhs, rhs), sort_options) in zip_it {
        // Consider all combinations of NULLS FIRST/LAST and ASC/DESC configurations.
        let result = match (lhs.is_null(), rhs.is_null(), sort_options.nulls_first) {
            (true, false, false) | (false, true, true) => Ordering::Greater,
            (true, false, true) | (false, true, false) => Ordering::Less,
            (false, false, _) => if sort_options.descending {
                rhs.partial_cmp(lhs)
            } else {
                lhs.partial_cmp(rhs)
            }
            .ok_or_else(|| {
                DataFusionError::Internal("Column array shouldn't be empty".to_string())
            })?,
            (true, true, _) => continue,
        };
        if result != Ordering::Equal {
            return Ok(result);
        }
    }
    Ok(Ordering::Equal)
}

/// This function searches for a tuple of given values (`target`) among the given
/// rows (`item_columns`) using the bisection algorithm. It assumes that `item_columns`
/// is sorted according to `sort_options` and returns the insertion index of `target`.
/// Template argument `SIDE` being `true`/`false` means left/right insertion.
pub fn bisect<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    sort_options: &[SortOptions],
) -> Result<usize> {
    let low: usize = 0;
    let high: usize = item_columns
        .get(0)
        .ok_or_else(|| {
            DataFusionError::Internal("Column array shouldn't be empty".to_string())
        })?
        .len();
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
        let cmp = compare_rows(current, target, sort_options)?;
        Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
    };
    find_bisect_point(item_columns, target, compare_fn, low, high)
}

/// This function searches for a tuple of given values (`target`) among a slice of
/// the given rows (`item_columns`) using the bisection algorithm. The slice starts
/// at the index `low` and ends at the index `high`. The boolean-valued function
/// `compare_fn` specifies whether we bisect on the left (by returning `false`),
/// or on the right (by returning `true`) when we compare the target value with
/// the current value as we iteratively bisect the input.
pub fn find_bisect_point<F>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    compare_fn: F,
    mut low: usize,
    mut high: usize,
) -> Result<usize>
where
    F: Fn(&[ScalarValue], &[ScalarValue]) -> Result<bool>,
{
    while low < high {
        let mid = ((high - low) / 2) + low;
        let val = get_row_at_idx(item_columns, mid)?;
        if compare_fn(&val, target)? {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

/// This function searches for a tuple of given values (`target`) among the given
/// rows (`item_columns`) via a linear scan. It assumes that `item_columns` is sorted
/// according to `sort_options` and returns the insertion index of `target`.
/// Template argument `SIDE` being `true`/`false` means left/right insertion.
pub fn linear_search<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    sort_options: &[SortOptions],
) -> Result<usize> {
    let low: usize = 0;
    let high: usize = item_columns
        .get(0)
        .ok_or_else(|| {
            DataFusionError::Internal("Column array shouldn't be empty".to_string())
        })?
        .len();
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
        let cmp = compare_rows(current, target, sort_options)?;
        Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
    };
    search_in_slice(item_columns, target, compare_fn, low, high)
}

/// This function searches for a tuple of given values (`target`) among a slice of
/// the given rows (`item_columns`) via a linear scan. The slice starts at the index
/// `low` and ends at the index `high`. The boolean-valued function `compare_fn`
/// specifies the stopping criterion.
pub fn search_in_slice<F>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    compare_fn: F,
    mut low: usize,
    high: usize,
) -> Result<usize>
where
    F: Fn(&[ScalarValue], &[ScalarValue]) -> Result<bool>,
{
    while low < high {
        let val = get_row_at_idx(item_columns, low)?;
        if !compare_fn(&val, target)? {
            break;
        }
        low += 1;
    }
    Ok(low)
}

/// This function finds the partition points according to `partition_columns`.
/// If there are no sort columns, then the result will be a single element
/// vector containing one partition range spanning all data.
pub fn evaluate_partition_ranges(
    num_rows: usize,
    partition_columns: &[SortColumn],
) -> Result<Vec<Range<usize>>> {
    Ok(if partition_columns.is_empty() {
        vec![Range {
            start: 0,
            end: num_rows,
        }]
    } else {
        lexicographical_partition_ranges(partition_columns)?.collect()
    })
}

/// Wraps identifier string in double quotes, escaping any double quotes in
/// the identifier by replacing it with two double quotes
///
/// e.g. identifier `tab.le"name` becomes `"tab.le""name"`
pub fn quote_identifier(s: &str) -> Cow<str> {
    if needs_quotes(s) {
        Cow::Owned(format!("\"{}\"", s.replace('"', "\"\"")))
    } else {
        Cow::Borrowed(s)
    }
}

/// returns true if this identifier needs quotes
fn needs_quotes(s: &str) -> bool {
    let mut chars = s.chars();

    // first char can not be a number unless escaped
    if let Some(first_char) = chars.next() {
        if !(first_char.is_ascii_lowercase() || first_char == '_') {
            return true;
        }
    }

    !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

// TODO: remove when can use https://github.com/sqlparser-rs/sqlparser-rs/issues/805
pub(crate) fn parse_identifiers(s: &str) -> Result<Vec<Ident>> {
    let dialect = GenericDialect;
    let mut parser = Parser::new(&dialect).try_with_sql(s)?;
    let mut idents = vec![];

    // expecting at least one word for identifier
    match parser.next_token_no_skip() {
        Some(TokenWithLocation {
            token: Token::Word(w),
            ..
        }) => idents.push(w.to_ident()),
        Some(TokenWithLocation { token, .. }) => {
            return Err(ParserError::ParserError(format!(
                "Unexpected token in identifier: {token}"
            )))?
        }
        None => {
            return Err(ParserError::ParserError(
                "Empty input when parsing identifier".to_string(),
            ))?
        }
    };

    while let Some(TokenWithLocation { token, .. }) = parser.next_token_no_skip() {
        match token {
            // ensure that optional period is succeeded by another identifier
            Token::Period => match parser.next_token_no_skip() {
                Some(TokenWithLocation {
                    token: Token::Word(w),
                    ..
                }) => idents.push(w.to_ident()),
                Some(TokenWithLocation { token, .. }) => {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token following period in identifier: {token}"
                    )))?
                }
                None => {
                    return Err(ParserError::ParserError(
                        "Trailing period in identifier".to_string(),
                    ))?
                }
            },
            _ => {
                return Err(ParserError::ParserError(format!(
                    "Unexpected token in identifier: {token}"
                )))?
            }
        }
    }
    Ok(idents)
}

/// Construct a new [`Vec`] of [`ArrayRef`] from the rows of the `arrays` at the `indices`.
pub fn get_arrayref_at_indices(
    arrays: &[ArrayRef],
    indices: &PrimitiveArray<UInt32Type>,
) -> Result<Vec<ArrayRef>> {
    arrays
        .iter()
        .map(|array| {
            compute::take(
                array.as_ref(),
                indices,
                None, // None: no index check
            )
            .map_err(DataFusionError::ArrowError)
        })
        .collect()
}

pub(crate) fn parse_identifiers_normalized(s: &str) -> Vec<String> {
    parse_identifiers(s)
        .unwrap_or_default()
        .into_iter()
        .map(|id| match id.quote_style {
            Some(_) => id.value,
            None => id.value.to_ascii_lowercase(),
        })
        .collect::<Vec<_>>()
}

/// This function "takes" the elements at `indices` from the slice `items`.
pub fn get_at_indices<T: Clone, I: Borrow<usize>>(
    items: &[T],
    indices: impl IntoIterator<Item = I>,
) -> Result<Vec<T>> {
    indices
        .into_iter()
        .map(|idx| items.get(*idx.borrow()).cloned())
        .collect::<Option<Vec<T>>>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "Expects indices to be in the range of searched vector".to_string(),
            )
        })
}

/// This function finds the longest prefix of the form 0, 1, 2, ... within the
/// collection `sequence`. Examples:
/// - For 0, 1, 2, 4, 5; we would produce 3, meaning 0, 1, 2 is the longest satisfying
/// prefix.
/// - For 1, 2, 3, 4; we would produce 0, meaning there is no such prefix.
pub fn longest_consecutive_prefix<T: Borrow<usize>>(
    sequence: impl IntoIterator<Item = T>,
) -> usize {
    let mut count = 0;
    for item in sequence {
        if !count.eq(item.borrow()) {
            break;
        }
        count += 1;
    }
    count
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;
    use arrow_array::Array;
    use std::ops::Range;
    use std::sync::Arc;

    use crate::from_slice::FromSlice;
    use crate::ScalarValue;
    use crate::ScalarValue::Null;

    use super::*;

    #[test]
    fn test_bisect_linear_left_and_right() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];
        let search_tuple: Vec<ScalarValue> = vec![
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(3.0)),
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(8.0)),
        ];
        let ords = [
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ];
        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);
        Ok(())
    }

    #[test]
    fn vector_ord() {
        assert!(vec![1, 0, 0, 0, 0, 0, 0, 1] < vec![1, 0, 0, 0, 0, 0, 0, 2]);
        assert!(vec![1, 0, 0, 0, 0, 0, 1, 1] > vec![1, 0, 0, 0, 0, 0, 0, 2]);
        assert!(
            vec![
                ScalarValue::Int32(Some(2)),
                Null,
                ScalarValue::Int32(Some(0)),
            ] < vec![
                ScalarValue::Int32(Some(2)),
                Null,
                ScalarValue::Int32(Some(1)),
            ]
        );
        assert!(
            vec![
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(None),
                ScalarValue::Int32(Some(0)),
            ] < vec![
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(None),
                ScalarValue::Int32(Some(1)),
            ]
        );
    }

    #[test]
    fn ord_same_type() {
        assert!((ScalarValue::Int32(Some(2)) < ScalarValue::Int32(Some(3))));
    }

    #[test]
    fn test_bisect_linear_left_and_right_diff_sort() -> Result<()> {
        // Descending, left
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice([
            4.0, 3.0, 2.0, 1.0, 0.0,
        ]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(4.0))];
        let ords = [SortOptions {
            descending: true,
            nulls_first: true,
        }];
        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 0);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 0);

        // Descending, right
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice([
            4.0, 3.0, 2.0, 1.0, 0.0,
        ]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(4.0))];
        let ords = [SortOptions {
            descending: true,
            nulls_first: true,
        }];
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);

        // Ascending, left
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 1);

        // Ascending, right
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([10.0, 9.0, 8.0, 7.5, 7., 6.])),
        ];
        let search_tuple: Vec<ScalarValue> = vec![
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(8.0)),
        ];
        let ords = [
            SortOptions {
                descending: false,
                nulls_first: true,
            },
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        ];
        let res = bisect::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);
        let res = linear_search::<false>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 3);

        let res = bisect::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        let res = linear_search::<true>(&arrays, &search_tuple, &ords)?;
        assert_eq!(res, 2);
        Ok(())
    }

    #[test]
    fn test_evaluate_partition_ranges() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([1.0, 1.0, 1.0, 2.0, 2.0, 2.0])),
            Arc::new(Float64Array::from_slice([4.0, 4.0, 3.0, 2.0, 1.0, 1.0])),
        ];
        let n_row = arrays[0].len();
        let options: Vec<SortOptions> = vec![
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];
        let sort_columns = arrays
            .into_iter()
            .zip(options)
            .map(|(values, options)| SortColumn {
                values,
                options: Some(options),
            })
            .collect::<Vec<_>>();
        let ranges = evaluate_partition_ranges(n_row, &sort_columns)?;
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], Range { start: 0, end: 2 });
        assert_eq!(ranges[1], Range { start: 2, end: 3 });
        assert_eq!(ranges[2], Range { start: 3, end: 4 });
        assert_eq!(ranges[3], Range { start: 4, end: 6 });
        Ok(())
    }

    #[test]
    fn test_parse_identifiers() -> Result<()> {
        let s = "CATALOG.\"F(o)o. \"\"bar\".table";
        let actual = parse_identifiers(s)?;
        let expected = vec![
            Ident {
                value: "CATALOG".to_string(),
                quote_style: None,
            },
            Ident {
                value: "F(o)o. \"bar".to_string(),
                quote_style: Some('"'),
            },
            Ident {
                value: "table".to_string(),
                quote_style: None,
            },
        ];
        assert_eq!(expected, actual);

        let s = "";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Empty input when parsing identifier\"))",
            format!("{err:?}")
        );

        let s = "*schema.table";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Unexpected token in identifier: *\"))",
            format!("{err:?}")
        );

        let s = "schema.table*";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Unexpected token in identifier: *\"))",
            format!("{err:?}")
        );

        let s = "schema.table.";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Trailing period in identifier\"))",
            format!("{err:?}")
        );

        let s = "schema.*";
        let err = parse_identifiers(s).expect_err("didn't fail to parse");
        assert_eq!(
            "SQL(ParserError(\"Unexpected token following period in identifier: *\"))",
            format!("{err:?}")
        );

        Ok(())
    }

    #[test]
    fn test_quote_identifier() -> Result<()> {
        let cases = vec![
            ("foo", r#"foo"#),
            ("_foo", r#"_foo"#),
            ("foo_bar", r#"foo_bar"#),
            ("foo-bar", r#""foo-bar""#),
            // name itself has a period, needs to be quoted
            ("foo.bar", r#""foo.bar""#),
            ("Foo", r#""Foo""#),
            ("Foo.Bar", r#""Foo.Bar""#),
            // name starting with a number needs to be quoted
            ("test1", r#"test1"#),
            ("1test", r#""1test""#),
        ];

        for (identifier, quoted_identifier) in cases {
            println!("input: \n{identifier}\nquoted_identifier:\n{quoted_identifier}");

            assert_eq!(quote_identifier(identifier), quoted_identifier);

            // When parsing the quoted identifier, it should be a
            // a single identifier without normalization, and not in multiple parts
            let quote_style = if quoted_identifier.starts_with('"') {
                Some('"')
            } else {
                None
            };

            let expected_parsed = vec![Ident {
                value: identifier.to_string(),
                quote_style,
            }];

            assert_eq!(
                parse_identifiers(quoted_identifier).unwrap(),
                expected_parsed
            );
        }

        Ok(())
    }

    #[test]
    fn test_get_arrayref_at_indices() -> Result<()> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice([2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice([15.0, 13.0, 8.0, 5., 0.0])),
        ];

        let row_indices_vec: Vec<Vec<u32>> = vec![
            // Get rows 0 and 1
            vec![0, 1],
            // Get rows 0 and 1
            vec![0, 2],
            // Get rows 1 and 3
            vec![1, 3],
            // Get rows 2 and 4
            vec![2, 4],
        ];
        for row_indices in row_indices_vec {
            let indices = PrimitiveArray::from_iter_values(row_indices.iter().cloned());
            let chunk = get_arrayref_at_indices(&arrays, &indices)?;
            for (arr_orig, arr_chunk) in arrays.iter().zip(&chunk) {
                for (idx, orig_idx) in row_indices.iter().enumerate() {
                    let res1 = ScalarValue::try_from_array(arr_orig, *orig_idx as usize)?;
                    let res2 = ScalarValue::try_from_array(arr_chunk, idx)?;
                    assert_eq!(res1, res2);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_get_at_indices() -> Result<()> {
        let in_vec = vec![1, 2, 3, 4, 5, 6, 7];
        assert_eq!(get_at_indices(&in_vec, [0, 2])?, vec![1, 3]);
        assert_eq!(get_at_indices(&in_vec, [4, 2])?, vec![5, 3]);
        // 7 is outside the range
        assert!(get_at_indices(&in_vec, [7]).is_err());
        Ok(())
    }

    #[test]
    fn test_longest_consecutive_prefix() {
        assert_eq!(longest_consecutive_prefix([0, 3, 4]), 1);
        assert_eq!(longest_consecutive_prefix([0, 1, 3, 4]), 2);
        assert_eq!(longest_consecutive_prefix([0, 1, 2, 3, 4]), 5);
        assert_eq!(longest_consecutive_prefix([1, 2, 3, 4]), 0);
    }
}
