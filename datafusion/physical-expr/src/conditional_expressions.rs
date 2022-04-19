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

use arrow::array::{new_null_array, Array, BooleanArray};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null, is_null};

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

/// coalesce evaluates to the first value which is not NULL
pub fn coalesce(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "coalesce was called with {} arguments. It requires at least 1.",
            args.len()
        )));
    }

    let return_type = args[0].data_type();
    let mut return_array = args.iter().filter_map(|x| match x {
        ColumnarValue::Array(array) => Some(array.len()),
        _ => None,
    });

    if let Some(size) = return_array.next() {
        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, size);
        let mut remainder = BooleanArray::from(vec![true; size]);

        for arg in args {
            match arg {
                ColumnarValue::Array(ref array) => {
                    let to_apply = and(&remainder, &is_not_null(array.as_ref())?)?;
                    current_value = zip(&to_apply, array, current_value.as_ref())?;
                    remainder = and(&remainder, &is_null(array)?)?;
                }
                ColumnarValue::Scalar(value) => {
                    if value.is_null() {
                        continue;
                    } else {
                        let last_value = value.to_array_of_size(size);
                        current_value =
                            zip(&remainder, &last_value, current_value.as_ref())?;
                        break;
                    }
                }
            }
            if remainder.iter().all(|x| x == Some(false)) {
                break;
            }
        }
        Ok(ColumnarValue::Array(current_value))
    } else {
        let result = args
            .iter()
            .filter_map(|x| match x {
                ColumnarValue::Scalar(s) if !s.is_null() => Some(x.clone()),
                _ => None,
            })
            .next()
            .unwrap_or_else(|| args[0].clone());
        Ok(result)
    }
}
