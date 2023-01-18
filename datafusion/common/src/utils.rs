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
use arrow::compute::SortOptions;
use std::cmp::Ordering;

/// Given column vectors, returns row at `idx`
fn get_row_at_idx(item_columns: &[ArrayRef], idx: usize) -> Result<Vec<ScalarValue>> {
    item_columns
        .iter()
        .map(|arr| ScalarValue::try_from_array(arr, idx))
        .collect::<Result<Vec<ScalarValue>>>()
}

/// This function compares two tuples depending on the given sort options.
fn compare(
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

/// This function implements both bisect_left and bisect_right, having the same
/// semantics with the Python Standard Library. To use bisect_left, supply true
/// as the template argument. To use bisect_right, supply false as the template argument.
/// It searches `item_columns` between rows `low` and `high`.
pub fn bisect<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    sort_options: &[SortOptions],
    low: usize,
    high: usize,
) -> Result<usize> {
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
        let cmp = compare(current, target, sort_options)?;
        Ok(if SIDE { cmp.is_lt() } else { cmp.is_le() })
    };
    find_bisect_point(item_columns, target, compare_fn, low, high)
}

/// This function searches for a tuple of target values among the given rows using the bisection algorithm.
/// The boolean-valued function `compare_fn` specifies whether we bisect on the left (with return value `false`),
/// or on the right (with return value `true`) as we compare the target value with the current value as we iteratively
/// bisect the input.
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

/// This function implements linear_search, It searches `item_columns` between rows `low` and `high`.
/// It assumes `item_columns` is sorted according to `sort_options`
/// and returns insertion position of the `target` in the `item_columns`.
/// `SIDE` is `true` means left insertion is applied.
/// `SIDE` is `false` means right insertion is applied.
pub fn linear_search<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    sort_options: &[SortOptions],
    mut low: usize,
    high: usize,
) -> Result<usize> {
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| {
        let cmp = compare(current, target, sort_options)?;
        Ok::<bool, DataFusionError>(if SIDE { cmp.is_lt() } else { cmp.is_le() })
    };
    while low < high {
        let val = get_row_at_idx(item_columns, low)?;
        if !compare_fn(&val, target)? {
            break;
        }
        low += 1;
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Float64Array};
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
        let n_row = arrays[0].len();
        let res: usize = bisect::<true>(&arrays, &search_tuple, &ords, 0, n_row)?;
        assert_eq!(res, 2);
        let res: usize = bisect::<false>(&arrays, &search_tuple, &ords, 0, n_row)?;
        assert_eq!(res, 3);
        let res: usize = linear_search::<true>(&arrays, &search_tuple, &ords, 0, n_row)?;
        assert_eq!(res, 2);
        let res: usize = linear_search::<false>(&arrays, &search_tuple, &ords, 0, n_row)?;
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
        let res: usize =
            bisect::<true>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 0);
        let res: usize =
            linear_search::<true>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
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
        let res: usize =
            bisect::<false>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 1);
        let res: usize =
            linear_search::<false>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 1);

        // Ascending, left
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res: usize =
            bisect::<true>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 1);
        let res: usize =
            linear_search::<true>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 1);

        // Ascending, right
        let arrays: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from_slice([5.0, 7.0, 8.0, 9., 10.]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res: usize =
            bisect::<false>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 2);
        let res: usize =
            linear_search::<false>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
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
        let res: usize =
            bisect::<false>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 3);
        let res: usize =
            linear_search::<false>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 3);

        let res: usize =
            bisect::<true>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 2);
        let res: usize =
            linear_search::<true>(&arrays, &search_tuple, &ords, 0, arrays[0].len())?;
        assert_eq!(res, 2);
        Ok(())
    }
}
