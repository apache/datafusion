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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float16Array, Float32Array, Float64Array};
use arrow::datatypes::DataType::{Float16, Float32, Float64};
use arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type};
use datafusion_common::{Result, ScalarValue, exec_err, utils::take_function_args};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = r#"Returns the first argument if it's not _NaN_.
Returns the second argument otherwise."#,
    syntax_example = "nanvl(expression_x, expression_y)",
    sql_example = r#"```sql
> SELECT nanvl(0, 5);
+------------+
| nanvl(0,5) |
+------------+
| 0          |
+------------+
```"#,
    argument(
        name = "expression_x",
        description = "Numeric expression to return if it's not _NaN_. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "expression_y",
        description = "Numeric expression to return if the first expression is _NaN_. Can be a constant, column, or function, and any combination of arithmetic operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NanvlFunc {
    signature: Signature,
}

impl Default for NanvlFunc {
    fn default() -> Self {
        NanvlFunc::new()
    }
}

impl NanvlFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Float16, Float16]),
                    Exact(vec![Float32, Float32]),
                    Exact(vec![Float64, Float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NanvlFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nanvl"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            Float16 => Ok(Float16),
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [x, y] = take_function_args(self.name(), args.args)?;

        match (x, y) {
            (ColumnarValue::Scalar(ScalarValue::Float16(Some(v))), y) if v.is_nan() => {
                Ok(y)
            }
            (ColumnarValue::Scalar(ScalarValue::Float32(Some(v))), y) if v.is_nan() => {
                Ok(y)
            }
            (ColumnarValue::Scalar(ScalarValue::Float64(Some(v))), y) if v.is_nan() => {
                Ok(y)
            }
            (x @ ColumnarValue::Scalar(_), _) => Ok(x),
            (x, y) => {
                let args = ColumnarValue::values_to_arrays(&[x, y])?;
                Ok(ColumnarValue::Array(nanvl(&args)?))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Nanvl SQL function
///
/// - x is NaN -> output is y (which may itself be NULL)
/// - otherwise -> output is x (which may itself be NULL)
fn nanvl(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Float64 => {
            let x = args[0].as_primitive::<Float64Type>();
            let y = args[1].as_primitive::<Float64Type>();
            let result: Float64Array = x
                .iter()
                .zip(y.iter())
                .map(|(x_value, y_value)| match x_value {
                    Some(x_value) if x_value.is_nan() => y_value,
                    _ => x_value,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        Float32 => {
            let x = args[0].as_primitive::<Float32Type>();
            let y = args[1].as_primitive::<Float32Type>();
            let result: Float32Array = x
                .iter()
                .zip(y.iter())
                .map(|(x_value, y_value)| match x_value {
                    Some(x_value) if x_value.is_nan() => y_value,
                    _ => x_value,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        Float16 => {
            let x = args[0].as_primitive::<Float16Type>();
            let y = args[1].as_primitive::<Float16Type>();
            let result: Float16Array = x
                .iter()
                .zip(y.iter())
                .map(|(x_value, y_value)| match x_value {
                    Some(x_value) if x_value.is_nan() => y_value,
                    _ => x_value,
                })
                .collect();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function nanvl"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::nanvl::nanvl;

    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};

    #[test]
    fn test_nanvl_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, 3.0, f64::NAN])), // x
            Arc::new(Float64Array::from(vec![5.0, 6.0, f64::NAN, f64::NAN])), // y
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float64_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }

    #[test]
    fn test_nanvl_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0, f32::NAN])), // x
            Arc::new(Float32Array::from(vec![5.0, 6.0, f32::NAN, f32::NAN])), // y
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float32_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }
}
