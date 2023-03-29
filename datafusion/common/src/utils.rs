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
use arrow::array::ArrayRef;
use arrow::compute::{lexicographical_partition_ranges, SortColumn, SortOptions};
use sqlparser::ast::Ident;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, TokenWithLocation};
use std::cmp::Ordering;
use std::ops::Range;

/// Given column vectors, returns row at `idx`.
pub fn get_row_at_idx(columns: &[ArrayRef], idx: usize) -> Result<Vec<ScalarValue>> {
    columns
        .iter()
        .map(|arr| ScalarValue::try_from_array(arr, idx))
        .collect()
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

/// Wraps identifier string in double quotes, escaping any double quotes in
/// the identifier by replacing it with two double quotes
///
/// e.g. identifier `tab.le"name` becomes `"tab.le""name"`
pub fn quote_identifier(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
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

// Find the largest range that satisfy 0,1,2 .. n in the `in1`
// For 0,1,2,4,5 we would produce 3. meaning 0,1,2 is the largest consecutive range (starting from zero).
// For 1,2,3,4 we would produce 0. Meaning there is no consecutive range (starting from zero).
pub fn calc_ordering_range(in1: &[usize]) -> usize {
    let mut count = 0;
    for (idx, elem) in in1.iter().enumerate() {
        if idx != *elem {
            break;
        } else {
            count += 1
        }
    }
    count
}

/// Create a new vector from the elements at the `indices` of `searched` vector
pub fn get_at_indices<T: Clone>(searched: &[T], indices: &[usize]) -> Vec<T> {
    let mut result = vec![];
    for idx in indices {
        result.push(searched[*idx].clone());
    }
    result
}

/// evaluate the partition points given the sort columns; if the sort columns are
/// empty then the result will be a single element vec of the whole column rows.
pub fn evaluate_partition_points(
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

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;
    use std::sync::Arc;

    use crate::from_slice::FromSlice;
    use crate::ScalarValue;
    use crate::ScalarValue::Null;

    use super::*;

    #[test]
    fn test_calc_ordering_range() -> Result<()> {
        assert_eq!(calc_ordering_range(&[0, 3, 4]), 1);
        assert_eq!(calc_ordering_range(&[0, 1, 3, 4]), 2);
        assert_eq!(calc_ordering_range(&[0, 1, 2, 3, 4]), 5);
        assert_eq!(calc_ordering_range(&[1, 2, 3, 4]), 0);
        Ok(())
    }

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
}
