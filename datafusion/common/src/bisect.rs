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

/// This function implements both bisect_left and bisect_right, having the same
/// semantics with the Python Standard Library. To use bisect_left, supply true
/// as the template argument. To use bisect_right, supply false as the template argument.
// Since these functions  have a lot of code in common we have decided to implement with single function
// where we separate left and right with compile time lookup.
pub fn bisect<const SIDE: bool>(
    item_columns: &[ArrayRef],
    target: &[ScalarValue],
) -> Result<usize> {
    let mut low: usize = 0;
    let mut high: usize = item_columns
        .get(0)
        .ok_or_else(|| {
            DataFusionError::Internal("Column array shouldn't be empty".to_string())
        })?
        .len();
    while low < high {
        let mid = ((high - low) / 2) + low;
        let val = item_columns
            .iter()
            .map(|arr| ScalarValue::try_from_array(arr, mid))
            .collect::<Result<Vec<ScalarValue>>>()?;

        // flag true means left, false means right
        let flag = if SIDE {
            val[..] < *target
        } else {
            val[..] <= *target
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
            Arc::new(Float64Array::from_slice(&[2.0, 3.0, 3.0, 4.0, 0.0])),
            Arc::new(Float64Array::from_slice(&[5.0, 7.0, 8.0, 10., 0.0])),
            Arc::new(Float64Array::from_slice(&[5.0, 7.0, 8.0, 10., 0.0])),
        ];
        let search_tuple: Vec<ScalarValue> = vec![
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(3.0)),
            ScalarValue::Float64(Some(8.0)),
            ScalarValue::Float64(Some(8.0)),
        ];
        let res: usize = bisect::<true>(&arrays, &search_tuple).unwrap();
        assert_eq!(res, 2);
        let res: usize = bisect::<false>(&arrays, &search_tuple).unwrap();
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
    }

    #[test]
    fn ord_same_type() {
        assert!((ScalarValue::Int32(Some(2)) < ScalarValue::Int32(Some(3))));
    }
}
