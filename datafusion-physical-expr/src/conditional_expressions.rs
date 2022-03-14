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

use arrow::array::Array;
use arrow::datatypes::DataType;

use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

pub fn coalesce(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // do not accept 0 arguments.
    if args.is_empty() {
        return Err(DataFusionError::Internal(format!(
            "coalesce was called with {} arguments. It requires at least 1.",
            args.len()
        )));
    }

    // let (lhs, rhs) = (&args[0], &args[1]);
    //
    // match (lhs, rhs) {
    //     (ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)) => {
    //         let cond_array = binary_array_op_scalar!(lhs, rhs.clone(), eq).unwrap()?;
    //
    //         let array = primitive_bool_array_op!(lhs, *cond_array, nullif)?;
    //
    //         Ok(ColumnarValue::Array(array))
    //     }
    //     (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
    //         // Get args0 == args1 evaluated and produce a boolean array
    //         let cond_array = binary_array_op!(lhs, rhs, eq)?;
    //
    //         // Now, invoke nullif on the result
    //         let array = primitive_bool_array_op!(lhs, *cond_array, nullif)?;
    //         Ok(ColumnarValue::Array(array))
    //     }
    //     _ => Err(DataFusionError::NotImplemented(
    //         "coalesce does not support a literal as first argument".to_string(),
    //     )),
    // }

    let mut res = vec![];
    let size = match args[0] {
        ColumnarValue::Array(ref a) => a.len(),
        ColumnarValue::Scalar(ref _s) => 1,
    };

    for i in 0..size {
        let mut value = ScalarValue::try_from(&args[0].data_type())?;
        for column_value in args {
            match column_value {
                ColumnarValue::Array(array_ref) => {
                    if array_ref.is_valid(i) {
                        value = ScalarValue::try_from_array(array_ref, i)?;
                        break;
                    }
                }
                ColumnarValue::Scalar(scalar) => {
                    if !scalar.is_null() {
                        value = scalar.clone();
                        break;
                    }
                }
            }
        }
        res.push(value);
    }

    Ok(ColumnarValue::Array(Arc::new(ScalarValue::iter_to_array(
        res,
    )?)))
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
