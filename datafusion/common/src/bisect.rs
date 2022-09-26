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

/// This function implements both bisect_left and bisect_right, having the same
/// semantics with the Python Standard Library. To use bisect_left, supply true
/// as the template argument. To use bisect_right, supply false as the template argument.
// Since these functions  have a lot of code in common we have decided to implement with single function
// where we separate left and right with compile time lookup.
pub fn bisect<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
    descending_order_columns: &[SortOptions],
) -> Result<usize> {
    let mut low: usize = 0;
    let mut high: usize = item_columns
        .get(0)
        .ok_or_else(|| {
            DataFusionError::Internal("Column array shouldn't be empty".to_string())
        })?
        .len();
    // It defines a comparison operator for order type (descending or ascending).
    let comparison_operator = |x: &[ScalarValue], y: &[ScalarValue]| {
        let zip_it = x.iter().zip(y.iter()).zip(descending_order_columns.iter());
        let mut res = Ordering::Equal;
        // Preserving lexical ordering.
        for ((lhs, rhs), sort_options) in zip_it {
            if lhs == rhs {
                continue;
            } else {
                // We compare left and right hand side accordingly. This makes binary search algorithm
                // robust to null_first and descending cases.

                res = match (
                    lhs.is_null(),
                    rhs.is_null(),
                    sort_options.nulls_first,
                    sort_options.descending,
                ) {
                    (false, false, _, false) => lhs.partial_cmp(rhs).unwrap(),
                    (false, false, _, true) => lhs.partial_cmp(rhs).unwrap().reverse(),
                    (true, false, false, true) => Ordering::Greater,
                    (true, false, false, false) => Ordering::Greater,
                    (false, true, true, false) => Ordering::Greater,
                    (false, true, true, true) => Ordering::Greater,
                    (true, false, true, false) => Ordering::Less,
                    (true, false, true, true) => Ordering::Less,
                    (false, true, false, true) => Ordering::Less,
                    (false, true, false, false) => Ordering::Less,
                    (true, true, _, _) => Ordering::Less,
                };
                break;
            }
        }
        res
    };
    while low < high {
        let mid = ((high - low) / 2) + low;
        let val = item_columns
            .iter()
            .map(|arr| ScalarValue::try_from_array(arr, mid))
            .collect::<Result<Vec<ScalarValue>>>()?;
        // flag true means left, false means right
        let flag = if SIDE {
            comparison_operator(&val, target).is_lt()
        } else {
            comparison_operator(&val, target).is_le()
        };
        if flag {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
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
    fn test_bisect_left_and_right() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice(&[5.0, 7.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice(&[2.0, 3.0, 3.0, 4.0, 5.0])),
            Arc::new(Float64Array::from_slice(&[5.0, 7.0, 8.0, 10., 11.0])),
            Arc::new(Float64Array::from_slice(&[15.0, 13.0, 8.0, 5., 0.0])),
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
        let res: usize = bisect::<true>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 2);
        let res: usize = bisect::<false>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 3);
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
    fn test_bisect_left_and_right_diff_sort() {
        // Descending, left
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice(&[
            4.0, 3.0, 2.0, 1.0, 0.0,
        ]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(4.0))];
        let ords = [SortOptions {
            descending: true,
            nulls_first: true,
        }];
        let res: usize = bisect::<true>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 0);

        // Descending, right
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice(&[
            4.0, 3.0, 2.0, 1.0, 0.0,
        ]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(4.0))];
        let ords = [SortOptions {
            descending: true,
            nulls_first: true,
        }];
        let res: usize = bisect::<false>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 1);

        // Ascending, left
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice(&[
            5.0, 7.0, 8.0, 9., 10.,
        ]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res: usize = bisect::<true>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 1);

        // Ascending, right
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from_slice(&[
            5.0, 7.0, 8.0, 9., 10.,
        ]))];
        let search_tuple: Vec<ScalarValue> = vec![ScalarValue::Float64(Some(7.0))];
        let ords = [SortOptions {
            descending: false,
            nulls_first: true,
        }];
        let res: usize = bisect::<false>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 2);

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from_slice(&[5.0, 7.0, 8.0, 8.0, 9., 10.])),
            Arc::new(Float64Array::from_slice(&[10.0, 9.0, 8.0, 7.5, 7., 6.])),
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
        let res: usize = bisect::<false>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 3);

        let res: usize = bisect::<true>(&arrays, &search_tuple, &ords).unwrap();
        assert_eq!(res, 2);
    }
}
