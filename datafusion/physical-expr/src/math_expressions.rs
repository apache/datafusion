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

//! Math expressions

use std::any::type_name;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::{BooleanArray, Float32Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use arrow_array::Array;

use datafusion_common::{exec_err, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $TYPE =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
                Ok(Arc::new(res))
            }
            _ => exec_err!("Invalid data type for {}", $NAME),
        }
    }};
}

macro_rules! unary_primitive_array_op {
    ($VALUE:expr, $NAME:expr, $FUNC:ident) => {{
        match ($VALUE) {
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Float32 => {
                    let result = downcast_compute_op!(array, $NAME, $FUNC, Float32Array);
                    Ok(ColumnarValue::Array(result?))
                }
                DataType::Float64 => {
                    let result = downcast_compute_op!(array, $NAME, $FUNC, Float64Array);
                    Ok(ColumnarValue::Array(result?))
                }
                other => {
                    exec_err!("Unsupported data type {:?} for function {}", other, $NAME)
                }
            },
            ColumnarValue::Scalar(a) => match a {
                ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float32(a.map(|x| x.$FUNC())),
                )),
                ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(a.map(|x| x.$FUNC())),
                )),
                _ => exec_err!(
                    "Unsupported data type {:?} for function {}",
                    ($VALUE).data_type(),
                    $NAME
                ),
            },
        }
    }};
}

macro_rules! math_unary_function {
    ($NAME:expr, $FUNC:ident) => {
        /// mathematical function that accepts f32 or f64 and returns f64
        pub fn $FUNC(args: &[ColumnarValue]) -> Result<ColumnarValue> {
            unary_primitive_array_op!(&args[0], $NAME, $FUNC)
        }
    };
}

macro_rules! downcast_arg {
    ($ARG:expr, $NAME:expr, $ARRAY_TYPE:ident) => {{
        $ARG.as_any().downcast_ref::<$ARRAY_TYPE>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} from {} to {}",
                $NAME,
                $ARG.data_type(),
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! make_function_scalar_inputs {
    ($ARG: expr, $NAME:expr, $ARRAY_TYPE:ident, $FUNC: block) => {{
        let arg = downcast_arg!($ARG, $NAME, $ARRAY_TYPE);

        arg.iter()
            .map(|a| match a {
                Some(a) => Some($FUNC(a)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE>()
    }};
}

macro_rules! make_function_scalar_inputs_return_type {
    ($ARG: expr, $NAME:expr, $ARGS_TYPE:ident, $RETURN_TYPE:ident, $FUNC: block) => {{
        let arg = downcast_arg!($ARG, $NAME, $ARGS_TYPE);

        arg.iter()
            .map(|a| match a {
                Some(a) => Some($FUNC(a)),
                _ => None,
            })
            .collect::<$RETURN_TYPE>()
    }};
}

math_unary_function!("ceil", ceil);
math_unary_function!("exp", exp);

/// Factorial SQL function
pub fn factorial(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Int64 => Ok(Arc::new(make_function_scalar_inputs!(
            &args[0],
            "value",
            Int64Array,
            { |value: i64| { (1..=value).product() } }
        )) as ArrayRef),
        other => exec_err!("Unsupported data type {other:?} for function factorial."),
    }
}

/// Isnan SQL function
pub fn isnan(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float64Array,
            BooleanArray,
            { f64::is_nan }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float32Array,
            BooleanArray,
            { f32::is_nan }
        )) as ArrayRef),

        other => exec_err!("Unsupported data type {other:?} for function isnan"),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array;

    use datafusion_common::cast::{as_boolean_array, as_int64_array};

    use super::*;

    #[test]
    fn test_factorial_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 4])), // input
        ];

        let result = factorial(&args).expect("failed to initialize function factorial");
        let ints =
            as_int64_array(&result).expect("failed to initialize function factorial");

        let expected = Int64Array::from(vec![1, 1, 2, 24]);

        assert_eq!(ints, &expected);
    }

    #[test]
    fn test_isnan_f64() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            1.0,
            f64::NAN,
            3.0,
            -f64::NAN,
        ]))];

        let result = isnan(&args).expect("failed to initialize function isnan");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function isnan");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }

    #[test]
    fn test_isnan_f32() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float32Array::from(vec![
            1.0,
            f32::NAN,
            3.0,
            f32::NAN,
        ]))];

        let result = isnan(&args).expect("failed to initialize function isnan");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function isnan");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }
}
