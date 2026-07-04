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

use std::ops::{Div, Mul};
use std::sync::Arc;

use crate::utils::{calculate_binary_decimal_math_cast, make_scalar_function};

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::DataType::{
    Decimal32, Decimal64, Decimal128, Decimal256, Float32, Float64,
};
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
    Float32Type, Float64Type, Int64Type,
};
use datafusion_common::ScalarValue::Int64;
use datafusion_common::types::{
    NativeType, logical_float32, logical_float64, logical_int64,
};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_expr_common::signature::{Coercion, TypeSignature, TypeSignatureClass};
use datafusion_macros::user_doc;
use num_traits::{One, Zero, pow};

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Truncates a number to a whole number or truncated to the specified decimal places.",
    syntax_example = "trunc(numeric_expression[, decimal_places])",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    argument(
        name = "decimal_places",
        description = r#"Optional. The number of decimal places to
  truncate to. Defaults to 0 (truncate to a whole number). If
  `decimal_places` is a positive integer, truncates digits to the
  right of the decimal point. If `decimal_places` is a negative
  integer, replaces digits to the left of the decimal point with `0`."#
    ),
    sql_example = r#"
  ```sql
  > SELECT trunc(42.738);
  +----------------+
  | trunc(42.738)  |
  +----------------+
  | 42             |
  +----------------+
  ```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TruncFunc {
    signature: Signature,
}

impl Default for TruncFunc {
    fn default() -> Self {
        TruncFunc::new()
    }
}

impl TruncFunc {
    pub fn new() -> Self {
        let decimal = Coercion::new_exact(TypeSignatureClass::Decimal);
        let decimal_places = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int64,
        );
        let float32 = Coercion::new_exact(TypeSignatureClass::Native(logical_float32()));
        let float64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            // math expressions expect 1 argument of type f64 or f32
            // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
            // return the best approximation for it (in f64).
            // We accept f32 because in this case it is clear that the best approximation
            // Decimal arguments are accepted to handle large values properly
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        decimal.clone(),
                        decimal_places.clone(),
                    ]),
                    TypeSignature::Coercible(vec![decimal]),
                    TypeSignature::Coercible(vec![
                        float32.clone(),
                        decimal_places.clone(),
                    ]),
                    TypeSignature::Coercible(vec![float32]),
                    TypeSignature::Coercible(vec![float64.clone(), decimal_places]),
                    TypeSignature::Coercible(vec![float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TruncFunc {
    fn name(&self) -> &str {
        "trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            Float32 => Ok(Float32),
            Float64 => Ok(Float64),
            dt if dt.is_decimal() => Ok(dt.clone()),
            DataType::Null => Ok(Float64),
            _ => plan_err!(
                "Unsupported data type {:?} for function {}",
                arg_types[0],
                self.name()
            ),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Extract precision from second argument (default 0)
        let precision = match args.args.get(1) {
            Some(ColumnarValue::Scalar(Int64(Some(p)))) => Some(*p),
            Some(ColumnarValue::Scalar(Int64(None))) => None, // null precision
            Some(ColumnarValue::Array(_)) => {
                // Precision is an array - use array path
                return make_scalar_function(trunc, vec![])(&args.args);
            }
            None => Some(0), // default precision
            Some(cv) => {
                return exec_err!(
                    "trunc function requires precision to be Int64, got {:?}",
                    cv.data_type()
                );
            }
        };

        // Scalar fast path using tuple matching for (value, precision)
        match (&args.args[0], precision) {
            // Null cases
            (ColumnarValue::Scalar(sv), _) if sv.is_null() => {
                ColumnarValue::Scalar(ScalarValue::Null).cast_to(args.return_type(), None)
            }
            (_, None) => {
                ColumnarValue::Scalar(ScalarValue::Null).cast_to(args.return_type(), None)
            }
            // Scalar cases
            (ColumnarValue::Scalar(ScalarValue::Float64(Some(v))), Some(p)) => Ok(
                ColumnarValue::Scalar(ScalarValue::Float64(Some(if p == 0 {
                    v.trunc()
                } else {
                    compute_truncate64(*v, p)
                }))),
            ),
            (ColumnarValue::Scalar(ScalarValue::Float32(Some(v))), Some(p)) => Ok(
                ColumnarValue::Scalar(ScalarValue::Float32(Some(if p == 0 {
                    v.trunc()
                } else {
                    compute_truncate32(*v, p)
                }))),
            ),
            (
                ColumnarValue::Scalar(ScalarValue::Decimal32(
                    Some(v),
                    lprecision,
                    lscale,
                )),
                Some(p),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal32(
                Some(compute_truncate_decimal::<Decimal32Type>(*v, *lscale, p)),
                *lprecision,
                *lscale,
            ))),
            (
                ColumnarValue::Scalar(ScalarValue::Decimal64(
                    Some(v),
                    lprecision,
                    lscale,
                )),
                Some(p),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal64(
                Some(compute_truncate_decimal::<Decimal64Type>(*v, *lscale, p)),
                *lprecision,
                *lscale,
            ))),
            (
                ColumnarValue::Scalar(ScalarValue::Decimal128(
                    Some(v),
                    lprecision,
                    lscale,
                )),
                Some(p),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                Some(compute_truncate_decimal::<Decimal128Type>(*v, *lscale, p)),
                *lprecision,
                *lscale,
            ))),
            (
                ColumnarValue::Scalar(ScalarValue::Decimal256(
                    Some(v),
                    lprecision,
                    lscale,
                )),
                Some(p),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                Some(compute_truncate_decimal::<Decimal256Type>(*v, *lscale, p)),
                *lprecision,
                *lscale,
            ))),

            // Array path for everything else
            _ => make_scalar_function(trunc, vec![])(&args.args),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // trunc preserves the order of the first argument
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

/// Truncate(numeric, decimalPrecision) and trunc(numeric) SQL function
fn trunc(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!(
            "truncate function requires one or two arguments, got {}",
            args.len()
        );
    }

    // If only one arg then invoke toolchain trunc(num) and precision = 0 by default
    // or then invoke the compute_truncate method to process precision
    let num = &args[0];
    let precision = if args.len() == 1 {
        ColumnarValue::Scalar(Int64(Some(0)))
    } else {
        ColumnarValue::Array(Arc::clone(&args[1]))
    };

    match num.data_type() {
        Float64 => match precision {
            ColumnarValue::Scalar(Int64(Some(0))) => {
                Ok(Arc::new(
                    args[0]
                        .as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(|x: f64| {
                            if x == 0_f64 { 0_f64 } else { x.trunc() }
                        }),
                ) as ArrayRef)
            }
            ColumnarValue::Array(precision) => {
                let num_array = num.as_primitive::<Float64Type>();
                let precision_array = precision.as_primitive::<Int64Type>();
                let result: PrimitiveArray<Float64Type> =
                    arrow::compute::binary(num_array, precision_array, |x, y| {
                        compute_truncate64(x, y)
                    })?;

                Ok(Arc::new(result) as ArrayRef)
            }
            _ => exec_err!("trunc function requires a scalar or array for precision"),
        },
        Float32 => match precision {
            ColumnarValue::Scalar(Int64(Some(0))) => {
                Ok(Arc::new(
                    args[0]
                        .as_primitive::<Float32Type>()
                        .unary::<_, Float32Type>(|x: f32| {
                            if x == 0_f32 { 0_f32 } else { x.trunc() }
                        }),
                ) as ArrayRef)
            }
            ColumnarValue::Array(precision) => {
                let num_array = num.as_primitive::<Float32Type>();
                let precision_array = precision.as_primitive::<Int64Type>();
                let result: PrimitiveArray<Float32Type> =
                    arrow::compute::binary(num_array, precision_array, |x, y| {
                        compute_truncate32(x, y)
                    })?;

                Ok(Arc::new(result) as ArrayRef)
            }
            _ => exec_err!("trunc function requires a scalar or array for precision"),
        },
        Decimal32(lprecision, lscale) => Ok(calculate_binary_decimal_math_cast::<
            Decimal32Type,
            Int64Type,
            Decimal32Type,
            _,
        >(
            num.as_ref(),
            &precision,
            |v, y| Ok(compute_truncate_decimal::<Decimal32Type>(v, *lscale, y)),
            *lprecision,
            *lscale,
            &DataType::Int64,
        )? as ArrayRef),
        Decimal64(lprecision, lscale) => Ok(calculate_binary_decimal_math_cast::<
            Decimal64Type,
            Int64Type,
            Decimal64Type,
            _,
        >(
            num.as_ref(),
            &precision,
            |v, y| Ok(compute_truncate_decimal::<Decimal64Type>(v, *lscale, y)),
            *lprecision,
            *lscale,
            &DataType::Int64,
        )? as ArrayRef),
        Decimal128(lprecision, lscale) => Ok(calculate_binary_decimal_math_cast::<
            Decimal128Type,
            Int64Type,
            Decimal128Type,
            _,
        >(
            num.as_ref(),
            &precision,
            |v, y| Ok(compute_truncate_decimal::<Decimal128Type>(v, *lscale, y)),
            *lprecision,
            *lscale,
            &DataType::Int64,
        )? as ArrayRef),
        Decimal256(lprecision, lscale) => Ok(calculate_binary_decimal_math_cast::<
            Decimal256Type,
            Int64Type,
            Decimal256Type,
            _,
        >(
            num.as_ref(),
            &precision,
            |v, y| Ok(compute_truncate_decimal::<Decimal256Type>(v, *lscale, y)),
            *lprecision,
            *lscale,
            &DataType::Int64,
        )? as ArrayRef),
        other => exec_err!("Unsupported data type {other:?} for function trunc"),
    }
}

fn compute_truncate32(x: f32, y: i64) -> f32 {
    let factor = 10.0_f32.powi(y as i32);
    (x * factor).trunc() / factor
}

fn compute_truncate64(x: f64, y: i64) -> f64 {
    let factor = 10.0_f64.powi(y as i32);
    (x * factor).trunc() / factor
}

/// Truncates a decimal value to `truncate_precision` fractional digits.
/// If `truncate_precision` is positive, clear that amount of trailing low-order digits
/// If `truncate_precision` is negative, it also clears digits before a decimal point
///
/// Example:
///   Truncating number 12.3456 (123456 as i128 with scale=4) to 1 digit produces 12.3.
///   It makes exp = 4-1 = 3; factor = 10^3 = 1000; result = (123456 / 1000) * 1000 = 123000
///   It is a decimal 12.3 with scale=4
///
///   Truncating number 12.3456 to -1 digit produces 10.0.
///   It makes exp = 4-(-1) = 5; factor = 10^5 = 100000; result = (123456 / 100000) * 100000 = 100000
///   It is a decimal 10.0 with scale=4
fn compute_truncate_decimal<T>(
    x: T::Native,
    scale: i8,
    truncate_precision: i64,
) -> T::Native
where
    T: DecimalType,
    T::Native: Copy + From<i32> + One + Zero + Div<Output = T::Native> + Mul,
{
    // How many trailing digits of decimal to clear
    let exp = (scale as i64).saturating_sub(truncate_precision);
    if exp <= 0 {
        // Keep more digits than actually stored, so nothing to truncate
        x
    } else if exp >= T::MAX_PRECISION as i64 {
        // Drop more digits that can be stored, return 0 without overflowing `pow`
        T::Native::zero()
    } else {
        let base = T::Native::from(10_i32);
        let exp = exp as usize;
        let factor = pow::<T::Native>(base, exp);
        // Result is (x / factor) * factor, so (x/factor) drops extra digits
        (x / factor) * factor
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::trunc::{compute_truncate_decimal, trunc};

    use arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array};
    use arrow::datatypes::Decimal128Type;
    use datafusion_common::cast::{as_float32_array, as_float64_array};

    #[test]
    fn test_truncate_32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                15.0,
                1_234.267_8,
                1_233.123_4,
                3.312_979_2,
                -21.123_4,
            ])),
            Arc::new(Int64Array::from(vec![0, 3, 2, 5, 6])),
        ];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float32_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 15.0);
        assert_eq!(floats.value(1), 1_234.267);
        assert_eq!(floats.value(2), 1_233.12);
        assert_eq!(floats.value(3), 3.312_97);
        assert_eq!(floats.value(4), -21.123_4);
    }

    #[test]
    fn test_truncate_64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                5.0,
                234.267_812_176,
                123.123_456_789,
                123.312_979_313_2,
                -321.123_1,
            ])),
            Arc::new(Int64Array::from(vec![0, 3, 2, 5, 6])),
        ];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float64_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 5.0);
        assert_eq!(floats.value(1), 234.267);
        assert_eq!(floats.value(2), 123.12);
        assert_eq!(floats.value(3), 123.312_97);
        assert_eq!(floats.value(4), -321.123_1);
    }

    #[test]
    fn test_truncate_64_one_arg() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            5.0,
            234.267_812,
            123.123_45,
            123.312_979_313_2,
            -321.123,
        ]))];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float64_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 5.0);
        assert_eq!(floats.value(1), 234.0);
        assert_eq!(floats.value(2), 123.0);
        assert_eq!(floats.value(3), 123.0);
        assert_eq!(floats.value(4), -321.0);
    }

    #[test]
    fn test_compute_truncate_decimal128() {
        // number 12.3456 (scale 4) truncated to 3 places = 12.345
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, 3),
            123_450
        );
        // number 12.3456 (scale 4) truncated to 1 place = 12.3
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, 1),
            123_000
        );

        // requesting more places = no change
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, 10),
            123_456
        );

        // truncating to 0 places = whole number 12
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, 0),
            120_000
        );

        // number 12.3456 (scale 2) truncated to -1 places = 10
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, -1),
            100_000
        );

        // number 12.3456 (scale 2) truncated to -3 places = 0
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, -3),
            0
        );

        // number 1234.56 (scale 2) truncated to -3 places = 1000
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 2, -3),
            100_000
        );

        // out of scale
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(123_456, 4, -900),
            0
        );

        // truncation rounds towards zero: -12.3456 = -12.345
        assert_eq!(
            compute_truncate_decimal::<Decimal128Type>(-123_456, 4, 3),
            -123_450
        );
    }
}
