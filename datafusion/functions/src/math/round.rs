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

use crate::utils::make_scalar_function;

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::DataType::{
    Decimal128, Decimal256, Float32, Float64, Int32, Int64,
};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int32Type,
};
use arrow_buffer::i256;
use datafusion_common::{exec_datafusion_err, exec_err, Result, ScalarValue};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Rounds a number to the nearest integer.",
    syntax_example = "round(numeric_expression[, decimal_places])",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    argument(
        name = "decimal_places",
        description = "Optional. The number of decimal places to round to. Defaults to 0."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RoundFunc {
    signature: Signature,
}

impl Default for RoundFunc {
    fn default() -> Self {
        RoundFunc::new()
    }
}

impl RoundFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for RoundFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "round"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 && arg_types.len() != 2 {
            return exec_err!(
                "round function requires one or two arguments, got {}",
                arg_types.len()
            );
        }

        if arg_types.len() == 1 {
            match arg_types[0].clone() {
                Decimal128(p, s) => Ok(vec![Decimal128(p, s)]),
                Decimal256(p, s) => Ok(vec![Decimal256(p, s)]),
                Float32 => Ok(vec![Float32]),
                _ => Ok(vec![Float64]),
            }
        } else if arg_types.len() == 2 {
            match arg_types[0].clone() {
                Decimal128(p, s) => Ok(vec![Decimal128(p, s), Int64]),
                Decimal256(p, s) => Ok(vec![Decimal256(p, s), Int64]),
                Float32 => Ok(vec![Float32, Int64]),
                _ => Ok(vec![Float64, Int64]),
            }
        } else {
            exec_err!(
                "round function requires one or two arguments, got {}",
                arg_types.len()
            )
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            Float32 => Ok(Float32),
            Decimal128(p, s) => Ok(Decimal128(p, s)),
            Decimal256(p, s) => Ok(Decimal256(p, s)),
            _ => Ok(Float64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(round, vec![])(&args.args)
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // round preserves the order of the first argument
        let value = &input[0];
        let precision = input.get(1);

        if precision
            .map(|r| r.sort_properties.eq(&SortProperties::Singleton))
            .unwrap_or(true)
        {
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Round SQL function
pub fn round(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!(
            "round function requires one or two arguments, got {}",
            args.len()
        );
    }

    let mut decimal_places = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));

    if args.len() == 2 {
        decimal_places = ColumnarValue::Array(Arc::clone(&args[1]));
    }

    match args[0].data_type() {
        Float64 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places: i32 = decimal_places.try_into().map_err(|e| {
                    exec_datafusion_err!(
                        "Invalid value for decimal places: {decimal_places}: {e}"
                    )
                })?;

                let result = args[0]
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(|value: f64| {
                        (value * 10.0_f64.powi(decimal_places)).round()
                            / 10.0_f64.powi(decimal_places)
                    });
                Ok(Arc::new(result) as _)
            }
            ColumnarValue::Array(decimal_places) => {
                let options = CastOptions {
                    safe: false, // raise error if the cast is not possible
                    ..Default::default()
                };
                let decimal_places = cast_with_options(&decimal_places, &Int32, &options)
                    .map_err(|e| {
                        exec_datafusion_err!("Invalid values for decimal places: {e}")
                    })?;

                let values = args[0].as_primitive::<Float64Type>();
                let decimal_places = decimal_places.as_primitive::<Int32Type>();
                let result = arrow::compute::binary::<_, _, _, Float64Type>(
                    values,
                    decimal_places,
                    |value, decimal_places| {
                        (value * 10.0_f64.powi(decimal_places)).round()
                            / 10.0_f64.powi(decimal_places)
                    },
                )?;
                Ok(Arc::new(result) as _)
            }
            _ => {
                exec_err!("round function requires a scalar or array for decimal_places")
            }
        },

        Float32 => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places: i32 = decimal_places.try_into().map_err(|e| {
                    exec_datafusion_err!(
                        "Invalid value for decimal places: {decimal_places}: {e}"
                    )
                })?;
                let result = args[0]
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(|value: f32| {
                        (value * 10.0_f32.powi(decimal_places)).round()
                            / 10.0_f32.powi(decimal_places)
                    });
                Ok(Arc::new(result) as _)
            }
            ColumnarValue::Array(_) => {
                let ColumnarValue::Array(decimal_places) =
                    decimal_places.cast_to(&Int32, None).map_err(|e| {
                        exec_datafusion_err!("Invalid values for decimal places: {e}")
                    })?
                else {
                    panic!("Unexpected result of ColumnarValue::Array.cast")
                };

                let values = args[0].as_primitive::<Float32Type>();
                let decimal_places = decimal_places.as_primitive::<Int32Type>();
                let result: PrimitiveArray<Float32Type> = arrow::compute::binary(
                    values,
                    decimal_places,
                    |value, decimal_places| {
                        (value * 10.0_f32.powi(decimal_places)).round()
                            / 10.0_f32.powi(decimal_places)
                    },
                )?;
                Ok(Arc::new(result) as _)
            }
            _ => {
                exec_err!("round function requires a scalar or array for decimal_places")
            }
        },

        Decimal128(precision, scale) => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places: i32 = decimal_places.try_into().map_err(|e| {
                    exec_datafusion_err!(
                        "Invalid value for decimal places: {decimal_places}: {e}"
                    )
                })?;

                let values = args[0].as_primitive::<Decimal128Type>();
                let result = values.unary::<_, Decimal128Type>(|value| {
                    round_decimal128(value, *scale, decimal_places)
                });

                Ok(Arc::new(result.with_precision_and_scale(*precision, *scale)?) as _)
            }
            ColumnarValue::Array(decimal_places) => {
                let options = CastOptions {
                    safe: false, // raise error if the cast is not possible
                    ..Default::default()
                };
                let decimal_places = cast_with_options(&decimal_places, &Int32, &options)
                    .map_err(|e| {
                        exec_datafusion_err!("Invalid values for decimal places: {e}")
                    })?;

                let values = args[0].as_primitive::<Decimal128Type>();
                let decimal_places = decimal_places.as_primitive::<Int32Type>();
                let result = arrow::compute::binary::<_, _, _, Decimal128Type>(
                    values,
                    decimal_places,
                    |value, decimal_places| {
                        round_decimal128(value, *scale, decimal_places)
                    },
                )?;

                Ok(Arc::new(result.with_precision_and_scale(*precision, *scale)?) as _)
            }
            _ => {
                exec_err!("round function requires a scalar or array for decimal_places")
            }
        },

        Decimal256(precision, scale) => match decimal_places {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(decimal_places))) => {
                let decimal_places: i32 = decimal_places.try_into().map_err(|e| {
                    exec_datafusion_err!(
                        "Invalid value for decimal places: {decimal_places}: {e}"
                    )
                })?;

                let values = args[0].as_primitive::<Decimal256Type>();
                let result = values.unary::<_, Decimal256Type>(|value| {
                    round_decimal256(value, *scale, decimal_places)
                });

                Ok(Arc::new(result.with_precision_and_scale(*precision, *scale)?) as _)
            }
            ColumnarValue::Array(decimal_places) => {
                let options = CastOptions {
                    safe: false,
                    ..Default::default()
                };
                let decimal_places = cast_with_options(&decimal_places, &Int32, &options)
                    .map_err(|e| {
                        exec_datafusion_err!("Invalid values for decimal places: {e}")
                    })?;

                let values = args[0].as_primitive::<Decimal256Type>();
                let decimal_places = decimal_places.as_primitive::<Int32Type>();
                let result = arrow::compute::binary::<_, _, _, Decimal256Type>(
                    values,
                    decimal_places,
                    |value, decimal_places| {
                        round_decimal256(value, *scale, decimal_places)
                    },
                )?;

                Ok(Arc::new(result.with_precision_and_scale(*precision, *scale)?) as _)
            }
            _ => {
                exec_err!("round function requires a scalar or array for decimal_places")
            }
        },

        other => exec_err!("Unsupported data type {other:?} for function round"),
    }
}

#[inline]
fn round_decimal128(value: i128, current_scale: i8, decimal_places: i32) -> i128 {
    let scale_adjustment = current_scale as i32 - decimal_places;

    if scale_adjustment > 0 {
        let remove_factor = 10_i128.pow(scale_adjustment as u32);
        let half = remove_factor / 2;

        if value >= 0 {
            ((value + half) / remove_factor) * remove_factor
        } else {
            ((value - half) / remove_factor) * remove_factor
        }
    } else {
        value
    }
}

#[inline]
fn round_decimal256(value: i256, current_scale: i8, decimal_places: i32) -> i256 {
    let scale_adjustment = current_scale as i32 - decimal_places;

    if scale_adjustment > 0 {
        let remove_factor = i256::from_i128(10_i128.pow(scale_adjustment as u32));
        let half = remove_factor / i256::from_i128(2);

        if value >= i256::from_i128(0) {
            ((value + half) / remove_factor) * remove_factor
        } else {
            ((value - half) / remove_factor) * remove_factor
        }
    } else {
        value
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::round::round;

    use arrow::array::{
        ArrayRef, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
        Int64Array,
    };
    use arrow_buffer::i256;
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use datafusion_common::DataFusionError;

    #[test]
    fn test_round_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345; 10])), // input
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])), // decimal_places
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![
            125.0, 125.2, 125.23, 125.235, 125.2345, 125.2345, 130.0, 100.0, 0.0, 0.0,
        ]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345; 10])), // input
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])), // decimal_places
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![
            125.0, 125.2, 125.23, 125.235, 125.2345, 125.2345, 130.0, 100.0, 0.0, 0.0,
        ]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_one_input() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float32_array(&result).expect("failed to initialize function round");

        let expected = Float32Array::from(vec![125.0, 12.0, 1.0, 0.0]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f64_one_input() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345, 12.345, 1.234, 0.1234])), // input
        ];

        let result = round(&args).expect("failed to initialize function round");
        let floats =
            as_float64_array(&result).expect("failed to initialize function round");

        let expected = Float64Array::from(vec![125.0, 12.0, 1.0, 0.0]);

        assert_eq!(floats, &expected);
    }

    #[test]
    fn test_round_f32_cast_fail() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![125.2345])), // input
            Arc::new(Int64Array::from(vec![2147483648])), // decimal_places
        ];

        let result = round(&args);

        assert!(result.is_err());
        assert!(matches!(result, Err(DataFusionError::Execution { .. })));
    }

    #[test]
    fn test_round_decimal128() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(
                Decimal128Array::from(vec![1252345_i128; 10])
                    .with_precision_and_scale(10, 4)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])),
        ];

        let result = round(&args).expect("failed to initialize function round");
        let decimals = result.as_any().downcast_ref::<Decimal128Array>().unwrap();

        let expected = Decimal128Array::from(vec![
            1250000_i128,
            1252000_i128,
            1252300_i128,
            1252350_i128,
            1252345_i128,
            1252345_i128,
            1300000_i128,
            1000000_i128,
            0_i128,
            0_i128,
        ])
        .with_precision_and_scale(10, 4)
        .unwrap();

        assert_eq!(decimals, &expected);
    }

    #[test]
    fn test_round_decimal128_one_input() {
        let args: Vec<ArrayRef> = vec![Arc::new(
            Decimal128Array::from(vec![1252345_i128, 123450_i128, 12340_i128, 1234_i128])
                .with_precision_and_scale(10, 4)
                .unwrap(),
        )];

        let result = round(&args).expect("failed to initialize function round");
        let decimals = result.as_any().downcast_ref::<Decimal128Array>().unwrap();

        let expected =
            Decimal128Array::from(vec![1250000_i128, 120000_i128, 10000_i128, 0_i128])
                .with_precision_and_scale(10, 4)
                .unwrap();

        assert_eq!(decimals, &expected);
    }

    #[test]
    fn test_round_decimal256() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(
                Decimal256Array::from(vec![i256::from_i128(1252345_i128); 10])
                    .with_precision_and_scale(20, 4)
                    .unwrap(),
            ),
            Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4, 5, -1, -2, -3, -4])),
        ];

        let result = round(&args).expect("failed to initialize function round");
        let decimals = result.as_any().downcast_ref::<Decimal256Array>().unwrap();

        let expected = Decimal256Array::from(vec![
            i256::from_i128(1250000_i128),
            i256::from_i128(1252000_i128),
            i256::from_i128(1252300_i128),
            i256::from_i128(1252350_i128),
            i256::from_i128(1252345_i128),
            i256::from_i128(1252345_i128),
            i256::from_i128(1300000_i128),
            i256::from_i128(1000000_i128),
            i256::from_i128(0_i128),
            i256::from_i128(0_i128),
        ])
        .with_precision_and_scale(20, 4)
        .unwrap();

        assert_eq!(decimals, &expected);
    }

    #[test]
    fn test_round_decimal256_one_input() {
        let args: Vec<ArrayRef> = vec![Arc::new(
            Decimal256Array::from(vec![
                i256::from_i128(1252345_i128),
                i256::from_i128(123450_i128),
                i256::from_i128(12340_i128),
                i256::from_i128(1234_i128),
            ])
            .with_precision_and_scale(20, 4)
            .unwrap(),
        )];

        let result = round(&args).expect("failed to initialize function round");
        let decimals = result.as_any().downcast_ref::<Decimal256Array>().unwrap();

        let expected = Decimal256Array::from(vec![
            i256::from_i128(1250000_i128),
            i256::from_i128(120000_i128),
            i256::from_i128(10000_i128),
            i256::from_i128(0_i128),
        ])
        .with_precision_and_scale(20, 4)
        .unwrap();

        assert_eq!(decimals, &expected);
    }
}
