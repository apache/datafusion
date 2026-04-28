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

//! Shared helpers for scale-taking math functions (`round`, `ceil`, ...).

use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::ColumnarValue;

/// Extract the `scale` (decimal places) argument from `args[1]`.
///
/// - Returns `Some(0)` when the function is invoked with a single argument
///   (no scale provided), which matches Spark's default scale of `0`.
/// - Returns `Some(value)` for any non-NULL signed/unsigned integer scalar.
/// - Returns `None` when the scale argument is NULL — Spark returns NULL for
///   `round(expr, NULL)` / `ceil(expr, NULL)`.
/// - Returns an error for unsupported types or out-of-range integers.
pub(crate) fn get_scale(fn_name: &str, args: &[ColumnarValue]) -> Result<Option<i32>> {
    if args.len() < 2 {
        return Ok(Some(0));
    }

    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int8(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::Int16(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => Ok(Some(*v)),
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| out_of_range_err(fn_name, *v)),
        ColumnarValue::Scalar(ScalarValue::UInt8(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::UInt16(Some(v))) => Ok(Some(i32::from(*v))),
        ColumnarValue::Scalar(ScalarValue::UInt32(Some(v))) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| out_of_range_err(fn_name, *v)),
        ColumnarValue::Scalar(ScalarValue::UInt64(Some(v))) => i32::try_from(*v)
            .map(Some)
            .map_err(|_| out_of_range_err(fn_name, *v)),
        ColumnarValue::Scalar(sv) if sv.is_null() => Ok(None),
        other => exec_err!(
            "Unsupported type for {fn_name} scale: {}",
            other.data_type()
        ),
    }
}

fn out_of_range_err<T: std::fmt::Display>(
    fn_name: &str,
    v: T,
) -> datafusion_common::DataFusionError {
    (exec_err!("{fn_name} scale {v} is out of supported i32 range") as Result<(), _>)
        .unwrap_err()
}
