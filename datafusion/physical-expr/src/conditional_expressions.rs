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

use std::sync::Arc;

use arrow::array::{new_null_array, Array, BooleanArray};
use arrow::compute;
use arrow::compute::kernels::zip::zip;
use arrow::datatypes::DataType;

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

    let size = match args[0] {
        ColumnarValue::Array(ref a) => a.len(),
        ColumnarValue::Scalar(ref _s) => 1,
    };
    let mut res = new_null_array(&args[0].data_type(), size);

    for column_value in args {
        for i in 0..size {
            match column_value {
                ColumnarValue::Array(array_ref) => {
                    let curr_null_mask = compute::is_null(res.as_ref())?;
                    let arr_not_null_mask = compute::is_not_null(array_ref)?;
                    let bool_mask = compute::and(&curr_null_mask, &arr_not_null_mask)?;
                    res = zip(&bool_mask, array_ref, &res)?;
                }
                ColumnarValue::Scalar(scalar) => {
                    if !scalar.is_null() && res.is_null(i) {
                        let vec: Vec<bool> =
                            (0..size).into_iter().map(|j| j == i).collect();
                        let bool_arr = BooleanArray::from(vec);
                        res =
                            zip(&bool_arr, scalar.to_array_of_size(size).as_ref(), &res)?;
                        continue;
                    }
                }
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(res)))
}

/// Currently supported types by the coalesce function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
pub static SUPPORTED_COALESCE_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8,
    DataType::LargeUtf8,
];
