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

use arrow::array::{Array, ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type};
use arrow::error::ArrowError;
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, Result};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CeilFunc {
    signature: Signature,
}

impl Default for CeilFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CeilFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CeilFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128(precision, scale))
            }
            _ => Ok(DataType::Float64),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg] = take_function_args(self.name(), arg_types)?;

        let coerced = match arg {
            DataType::Null => DataType::Float64,
            DataType::Float32 | DataType::Float64 => arg.clone(),
            DataType::Decimal128(_, _) => arg.clone(),
            DataType::Float16
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DataType::Float64,
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };

        Ok(vec![coerced])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let value = &args[0];

        let result: ArrayRef = match value.data_type() {
            DataType::Float64 => Arc::new(
                value
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(f64::ceil),
            ),
            DataType::Float32 => Arc::new(
                value
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(f32::ceil),
            ),
            DataType::Decimal128(_, scale) => {
                apply_decimal_op(value, *scale, ceil_decimal_value)?
            }
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        super::ceil_order(input)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        super::bounds::unbounded_bounds(inputs)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(super::get_ceil_doc())
    }
}

fn apply_decimal_op(
    array: &ArrayRef,
    scale: i8,
    op: fn(i128, i128) -> std::result::Result<i128, ArrowError>,
) -> Result<ArrayRef> {
    if scale <= 0 {
        return Ok(Arc::clone(array));
    }

    let factor = decimal_scale_factor(scale)?;
    let decimal = array.as_primitive::<Decimal128Type>();
    let data_type = array.data_type().clone();

    let result: PrimitiveArray<Decimal128Type> = decimal
        .try_unary(|value| op(value, factor))?
        .with_data_type(data_type);

    Ok(Arc::new(result))
}

fn decimal_scale_factor(scale: i8) -> Result<i128> {
    if scale < 0 {
        return exec_err!("Decimal scale {scale} must be non-negative");
    }
    let exponent = scale as u32;

    if let Some(value) = 10_i128.checked_pow(exponent) {
        Ok(value)
    } else {
        exec_err!("Decimal scale {scale} is too large for ceil")
    }
}

fn ceil_decimal_value(
    value: i128,
    factor: i128,
) -> std::result::Result<i128, ArrowError> {
    let remainder = value % factor;

    if remainder == 0 {
        return Ok(value);
    }

    if value >= 0 {
        let increment = factor - remainder;
        value.checked_add(increment).ok_or_else(|| {
            ArrowError::ComputeError("Decimal128 overflow while applying ceil".into())
        })
    } else {
        value.checked_sub(remainder).ok_or_else(|| {
            ArrowError::ComputeError("Decimal128 overflow while applying ceil".into())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Decimal128Array;
    use arrow::datatypes::Field;
    use datafusion_common::cast::as_decimal128_array;
    use datafusion_common::config::ConfigOptions;

    #[test]
    fn test_decimal128_ceil() {
        let data_type = DataType::Decimal128(10, 2);
        let input = Decimal128Array::from(vec![
            Some(1234),
            Some(-1234),
            Some(1200),
            Some(-1200),
            None,
        ])
        .with_precision_and_scale(10, 2)
        .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input) as ArrayRef)],
            arg_fields: vec![Field::new("a", data_type.clone(), true).into()],
            number_rows: 5,
            return_field: Field::new("f", data_type.clone(), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = CeilFunc::new()
            .invoke_with_args(args)
            .expect("ceil evaluation succeeded");

        let ColumnarValue::Array(result) = result else {
            panic!("expected array result");
        };

        let values = as_decimal128_array(&result).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(10, 2));
        assert_eq!(values.value(0), 1300);
        assert_eq!(values.value(1), -1200);
        assert_eq!(values.value(2), 1200);
        assert_eq!(values.value(3), -1200);
        assert!(values.is_null(4));
    }

    #[test]
    fn test_decimal128_ceil_zero_scale() {
        let data_type = DataType::Decimal128(6, 0);
        let input = Decimal128Array::from(vec![Some(12), Some(-13), None])
            .with_precision_and_scale(6, 0)
            .unwrap();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(input) as ArrayRef)],
            arg_fields: vec![Field::new("a", data_type.clone(), true).into()],
            number_rows: 3,
            return_field: Field::new("f", data_type.clone(), true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = CeilFunc::new()
            .invoke_with_args(args)
            .expect("ceil evaluation succeeded");

        let ColumnarValue::Array(result) = result else {
            panic!("expected array result");
        };

        let values = as_decimal128_array(&result).unwrap();
        assert_eq!(values.value(0), 12);
        assert_eq!(values.value(1), -13);
        assert!(values.is_null(2));
    }
}
