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

use arrow::array::ArrayRef;
use arrow::array::{Float32Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use rand::{thread_rng, Rng};
use std::any::type_name;
use std::iter;
use std::sync::Arc;

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $TYPE =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC());
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
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
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    other, $NAME,
                ))),
            },
            ColumnarValue::Scalar(a) => match a {
                ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float32(a.map(|x| x.$FUNC())),
                )),
                ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(a.map(|x| x.$FUNC())),
                )),
                _ => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    ($VALUE).data_type(),
                    $NAME,
                ))),
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
                "could not cast {} to {}",
                $NAME,
                type_name::<$ARRAY_TYPE>()
            ))
        })?
    }};
}

macro_rules! make_function_inputs2 {
    ($ARG1: expr, $ARG2: expr, $NAME1:expr, $NAME2: expr, $ARRAY_TYPE:ident, $FUNC: block) => {{
        let arg1 = downcast_arg!($ARG1, $NAME1, $ARRAY_TYPE);
        let arg2 = downcast_arg!($ARG2, $NAME2, $ARRAY_TYPE);

        arg1.iter()
            .zip(arg2.iter())
            .map(|(a1, a2)| match (a1, a2) {
                (Some(a1), Some(a2)) => Some($FUNC(a1, a2.try_into().ok()?)),
                _ => None,
            })
            .collect::<$ARRAY_TYPE>()
    }};
}

math_unary_function!("sqrt", sqrt);
math_unary_function!("sin", sin);
math_unary_function!("cos", cos);
math_unary_function!("tan", tan);
math_unary_function!("asin", asin);
math_unary_function!("acos", acos);
math_unary_function!("atan", atan);
math_unary_function!("floor", floor);
math_unary_function!("ceil", ceil);
math_unary_function!("round", round);
math_unary_function!("trunc", trunc);
math_unary_function!("abs", abs);
math_unary_function!("signum", signum);
math_unary_function!("exp", exp);
math_unary_function!("ln", ln);
math_unary_function!("log2", log2);
math_unary_function!("log10", log10);

/// random SQL function
pub fn random(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let len: usize = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        _ => {
            return Err(DataFusionError::Internal(
                "Expect random function to take no param".to_string(),
            ))
        }
    };
    let mut rng = thread_rng();
    let values = iter::repeat_with(|| rng.gen_range(0.0..1.0)).take(len);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

pub fn power(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "base",
            "exponent",
            Float64Array,
            { f64::powf }
        )) as ArrayRef),

        DataType::Int64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "base",
            "exponent",
            Int64Array,
            { i64::pow }
        )) as ArrayRef),

        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {:?} for function power",
            other
        ))),
    }
}

pub fn atan2(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Float64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "y",
            "x",
            Float64Array,
            { f64::atan2 }
        )) as ArrayRef),

        DataType::Float32 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "y",
            "x",
            Float32Array,
            { f32::atan2 }
        )) as ArrayRef),

        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {:?} for function atan2",
            other
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use arrow::array::{Array, Float64Array, NullArray};

    #[test]
    fn test_random_expression() {
        let args = vec![ColumnarValue::Array(Arc::new(NullArray::new(1)))];
        let array = random(&args).expect("fail").into_array(1);
        let floats = array.as_any().downcast_ref::<Float64Array>().expect("fail");

        assert_eq!(floats.len(), 1);
        assert!(0.0 <= floats.value(0) && floats.value(0) < 1.0);
    }

    #[test]
    fn test_atan2_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![2.0, -3.0, 4.0, -5.0])), // y
            Arc::new(Float64Array::from(vec![1.0, 2.0, -3.0, -4.0])), // x
        ];

        let result = atan2(&args).expect("fail");
        let floats = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("fail");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), (2.0_f64).atan2(1.0));
        assert_eq!(floats.value(1), (-3.0_f64).atan2(2.0));
        assert_eq!(floats.value(2), (4.0_f64).atan2(-3.0));
        assert_eq!(floats.value(3), (-5.0_f64).atan2(-4.0));
    }

    #[test]
    fn test_atan2_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![2.0, -3.0, 4.0, -5.0])), // y
            Arc::new(Float32Array::from(vec![1.0, 2.0, -3.0, -4.0])), // x
        ];

        let result = atan2(&args).expect("fail");
        let floats = result
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("fail");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), (2.0_f32).atan2(1.0));
        assert_eq!(floats.value(1), (-3.0_f32).atan2(2.0));
        assert_eq!(floats.value(2), (4.0_f32).atan2(-3.0));
        assert_eq!(floats.value(3), (-5.0_f32).atan2(-4.0));
    }
}
