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

use arrow::array::{ArrayRef, AsArray, Int64Array, PrimitiveArray, new_null_array};
use arrow::compute::try_binary;
use arrow::datatypes::{DataType, Int64Type};
use arrow::error::ArrowError;
use std::any::Any;
use std::mem::swap;
use std::sync::Arc;

use datafusion_common::{Result, ScalarValue, exec_err, internal_datafusion_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the greatest common divisor of `expression_x` and `expression_y`. Returns 0 if both inputs are zero.",
    syntax_example = "gcd(expression_x, expression_y)",
    sql_example = r#"```sql
> SELECT gcd(48, 18);
+------------+
| gcd(48,18) |
+------------+
| 6          |
+------------+
```"#,
    standard_argument(name = "expression_x", prefix = "First numeric"),
    standard_argument(name = "expression_y", prefix = "Second numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct GcdFunc {
    signature: Signature,
}

impl Default for GcdFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GcdFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for GcdFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "gcd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 2] = args.args.try_into().map_err(|_| {
            internal_datafusion_err!("Expected 2 arguments for function gcd")
        })?;

        match args {
            [ColumnarValue::Array(a), ColumnarValue::Array(b)] => {
                compute_gcd_for_arrays(&a, &b)
            }
            [
                ColumnarValue::Scalar(ScalarValue::Int64(a)),
                ColumnarValue::Scalar(ScalarValue::Int64(b)),
            ] => match (a, b) {
                (Some(a), Some(b)) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                    Some(compute_gcd(a, b)?),
                ))),
                _ => Ok(ColumnarValue::Scalar(ScalarValue::Int64(None))),
            },
            [
                ColumnarValue::Array(a),
                ColumnarValue::Scalar(ScalarValue::Int64(b)),
            ] => compute_gcd_with_scalar(&a, b),
            [
                ColumnarValue::Scalar(ScalarValue::Int64(a)),
                ColumnarValue::Array(b),
            ] => compute_gcd_with_scalar(&b, a),
            _ => exec_err!("Unsupported argument types for function gcd"),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn compute_gcd_for_arrays(a: &ArrayRef, b: &ArrayRef) -> Result<ColumnarValue> {
    let a = a.as_primitive::<Int64Type>();
    let b = b.as_primitive::<Int64Type>();
    try_binary(a, b, compute_gcd)
        .map(|arr: PrimitiveArray<Int64Type>| {
            ColumnarValue::Array(Arc::new(arr) as ArrayRef)
        })
        .map_err(Into::into) // convert ArrowError to DataFusionError
}

fn compute_gcd_with_scalar(arr: &ArrayRef, scalar: Option<i64>) -> Result<ColumnarValue> {
    match scalar {
        Some(scalar_value) => {
            let result: Result<Int64Array> = arr
                .as_primitive::<Int64Type>()
                .iter()
                .map(|val| match val {
                    Some(val) => Ok(Some(compute_gcd(val, scalar_value)?)),
                    _ => Ok(None),
                })
                .collect();

            result.map(|arr| ColumnarValue::Array(Arc::new(arr) as ArrayRef))
        }
        None => Ok(ColumnarValue::Array(new_null_array(
            &DataType::Int64,
            arr.len(),
        ))),
    }
}

/// Computes gcd of two unsigned integers using Binary GCD algorithm.
pub(super) fn unsigned_gcd(mut a: u64, mut b: u64) -> u64 {
    if a == 0 {
        return b;
    }
    if b == 0 {
        return a;
    }

    let shift = (a | b).trailing_zeros();
    a >>= a.trailing_zeros();
    loop {
        b >>= b.trailing_zeros();
        if a > b {
            swap(&mut a, &mut b);
        }
        b -= a;
        if b == 0 {
            return a << shift;
        }
    }
}

/// Computes greatest common divisor using Binary GCD algorithm.
pub fn compute_gcd(x: i64, y: i64) -> Result<i64, ArrowError> {
    let a = x.unsigned_abs();
    let b = y.unsigned_abs();
    let r = unsigned_gcd(a, b);
    // gcd(i64::MIN, i64::MIN) = i64::MIN.unsigned_abs() cannot fit into i64
    r.try_into().map_err(|_| {
        ArrowError::ComputeError(format!("Signed integer overflow in GCD({x}, {y})"))
    })
}
