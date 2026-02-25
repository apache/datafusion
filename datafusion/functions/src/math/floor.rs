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

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::{DecimalCast, rescale_decimal};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, DecimalType, Float32Type, Float64Type,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::preimage::PreimageResult;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
use num_traits::{CheckedAdd, Float, One};

use super::decimal::{apply_decimal_op, floor_decimal_value};

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the nearest integer less than or equal to a number.",
    syntax_example = "floor(numeric_expression)",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    sql_example = r#"```sql
> SELECT floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
| 3.0         |
+-------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FloorFunc {
    signature: Signature,
}

impl Default for FloorFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl FloorFunc {
    pub fn new() -> Self {
        let decimal_sig = Coercion::new_exact(TypeSignatureClass::Decimal);
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![decimal_sig]),
                    TypeSignature::Uniform(1, vec![DataType::Float64, DataType::Float32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

// ============ Macro for preimage bounds ============
/// Generates the code to call the appropriate bounds function and wrap results.
macro_rules! preimage_bounds {
    // Float types: call float_preimage_bounds and wrap in ScalarValue
    (float: $variant:ident, $value:expr) => {
        float_preimage_bounds($value).map(|(lo, hi)| {
            (
                ScalarValue::$variant(Some(lo)),
                ScalarValue::$variant(Some(hi)),
            )
        })
    };

    // Integer types: call int_preimage_bounds and wrap in ScalarValue
    (int: $variant:ident, $value:expr) => {
        int_preimage_bounds($value).map(|(lo, hi)| {
            (
                ScalarValue::$variant(Some(lo)),
                ScalarValue::$variant(Some(hi)),
            )
        })
    };

    // Decimal types: call decimal_preimage_bounds with precision/scale and wrap in ScalarValue
    (decimal: $variant:ident, $decimal_type:ty, $value:expr, $precision:expr, $scale:expr) => {
        decimal_preimage_bounds::<$decimal_type>($value, $precision, $scale).map(
            |(lo, hi)| {
                (
                    ScalarValue::$variant(Some(lo), $precision, $scale),
                    ScalarValue::$variant(Some(hi), $precision, $scale),
                )
            },
        )
    };
}

impl ScalarUDFImpl for FloorFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "floor"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Null => Ok(DataType::Float64),
            other => Ok(other.clone()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arg = &args.args[0];

        // Scalar fast path for float types - avoid array conversion overhead entirely
        if let ColumnarValue::Scalar(scalar) = arg {
            match scalar {
                ScalarValue::Float64(v) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(
                        v.map(f64::floor),
                    )));
                }
                ScalarValue::Float32(v) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float32(
                        v.map(f32::floor),
                    )));
                }
                ScalarValue::Null => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
                }
                // For decimals: convert to array of size 1, process, then extract scalar
                // This ensures we don't expand the array while reusing overflow validation
                _ => {}
            }
        }

        // Track if input was a scalar to convert back at the end
        let is_scalar = matches!(arg, ColumnarValue::Scalar(_));

        // Array path (also handles decimal scalars converted to size-1 arrays)
        let value = arg.to_array(args.number_rows)?;

        let result: ArrayRef = match value.data_type() {
            DataType::Float64 => Arc::new(
                value
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(f64::floor),
            ),
            DataType::Float32 => Arc::new(
                value
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(f32::floor),
            ),
            DataType::Null => {
                return Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)));
            }
            DataType::Decimal32(precision, scale) => {
                apply_decimal_op::<Decimal32Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    floor_decimal_value,
                )?
            }
            DataType::Decimal64(precision, scale) => {
                apply_decimal_op::<Decimal64Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    floor_decimal_value,
                )?
            }
            DataType::Decimal128(precision, scale) => {
                apply_decimal_op::<Decimal128Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    floor_decimal_value,
                )?
            }
            DataType::Decimal256(precision, scale) => {
                apply_decimal_op::<Decimal256Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    floor_decimal_value,
                )?
            }
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                );
            }
        };

        // If input was a scalar, convert result back to scalar
        if is_scalar {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        let data_type = inputs[0].data_type();
        Interval::make_unbounded(&data_type)
    }

    /// Compute the preimage for floor function.
    ///
    /// For `floor(x) = N`, the preimage is `x >= N AND x < N + 1`
    /// because floor(x) = N for all x in [N, N+1).
    ///
    /// This enables predicate pushdown optimizations, transforming:
    /// `floor(col) = 100` into `col >= 100 AND col < 101`
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        _info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        // floor takes exactly one argument and we do not expect to reach here with multiple arguments.
        debug_assert!(args.len() == 1, "floor() takes exactly one argument");

        let arg = args[0].clone();

        // Extract the literal value being compared to
        let Expr::Literal(lit_value, _) = lit_expr else {
            return Ok(PreimageResult::None);
        };

        // Compute lower bound (N) and upper bound (N + 1) using helper functions
        let Some((lower, upper)) = (match lit_value {
            // Floating-point types
            ScalarValue::Float64(Some(n)) => preimage_bounds!(float: Float64, *n),
            ScalarValue::Float32(Some(n)) => preimage_bounds!(float: Float32, *n),

            // Integer types (not reachable from SQL/SLT: floor() only accepts Float64/Float32/Decimal,
            // so the RHS literal is always coerced to one of those before preimage runs; kept for
            // programmatic use and unit tests)
            ScalarValue::Int8(Some(n)) => preimage_bounds!(int: Int8, *n),
            ScalarValue::Int16(Some(n)) => preimage_bounds!(int: Int16, *n),
            ScalarValue::Int32(Some(n)) => preimage_bounds!(int: Int32, *n),
            ScalarValue::Int64(Some(n)) => preimage_bounds!(int: Int64, *n),

            // Decimal types
            // DECIMAL(precision, scale) where precision ≤ 38 -> Decimal128(precision, scale)
            // DECIMAL(precision, scale) where precision > 38 -> Decimal256(precision, scale)
            // Decimal32 and Decimal64 are unreachable from SQL/SLT.
            ScalarValue::Decimal32(Some(n), precision, scale) => {
                preimage_bounds!(decimal: Decimal32, Decimal32Type, *n, *precision, *scale)
            }
            ScalarValue::Decimal64(Some(n), precision, scale) => {
                preimage_bounds!(decimal: Decimal64, Decimal64Type, *n, *precision, *scale)
            }
            ScalarValue::Decimal128(Some(n), precision, scale) => {
                preimage_bounds!(decimal: Decimal128, Decimal128Type, *n, *precision, *scale)
            }
            ScalarValue::Decimal256(Some(n), precision, scale) => {
                preimage_bounds!(decimal: Decimal256, Decimal256Type, *n, *precision, *scale)
            }

            // Unsupported types
            _ => None,
        }) else {
            return Ok(PreimageResult::None);
        };

        Ok(PreimageResult::Range {
            expr: arg,
            interval: Box::new(Interval::try_new(lower, upper)?),
        })
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

// ============ Helper functions for preimage bounds ============

/// Compute preimage bounds for floor function on floating-point types.
/// For floor(x) = n, the preimage is [n, n+1).
/// Returns None if:
/// - The value is non-finite (infinity, NaN)
/// - The value is not an integer (floor always returns integers, so floor(x) = 1.3 has no solution)
/// - Adding 1 would lose precision at extreme values
fn float_preimage_bounds<F: Float>(n: F) -> Option<(F, F)> {
    let one = F::one();
    // Check for non-finite values (infinity, NaN)
    if !n.is_finite() {
        return None;
    }
    // floor always returns an integer, so if n has a fractional part, there's no solution
    if n.fract() != F::zero() {
        return None;
    }
    // Check for precision loss at extreme values
    if n + one <= n {
        return None;
    }
    Some((n, n + one))
}

/// Compute preimage bounds for floor function on integer types.
/// For floor(x) = n, the preimage is [n, n+1).
/// Returns None if adding 1 would overflow.
fn int_preimage_bounds<I: CheckedAdd + One + Copy>(n: I) -> Option<(I, I)> {
    let upper = n.checked_add(&I::one())?;
    Some((n, upper))
}

/// Compute preimage bounds for floor function on decimal types.
/// For floor(x) = n, the preimage is [n, n+1).
/// Returns None if:
/// - The value has a fractional part (floor always returns integers)
/// - Adding 1 would overflow
fn decimal_preimage_bounds<D: DecimalType>(
    value: D::Native,
    precision: u8,
    scale: i8,
) -> Option<(D::Native, D::Native)>
where
    D::Native: DecimalCast + ArrowNativeTypeOp + std::ops::Rem<Output = D::Native>,
{
    // Use rescale_decimal to compute "1" at target scale (avoids manual pow)
    // Convert integer 1 (scale=0) to the target scale
    let one_scaled: D::Native = rescale_decimal::<D, D>(
        D::Native::ONE, // value = 1
        1,              // input_precision = 1
        0,              // input_scale = 0 (integer)
        precision,      // output_precision
        scale,          // output_scale
    )?;

    // floor always returns an integer, so if value has a fractional part, there's no solution
    // Check: value % one_scaled != 0 means fractional part exists
    if scale > 0 && value % one_scaled != D::Native::ZERO {
        return None;
    }

    // Compute upper bound using checked addition
    // Before preimage stage, the internal i128/i256(value) is validated based on the precision and scale.
    // MAX_DECIMAL128_FOR_EACH_PRECISION and MAX_DECIMAL256_FOR_EACH_PRECISION are used to validate the internal i128/i256.
    // Any invalid i128/i256 will not reach here.
    // Therefore, the add_checked will always succeed if tested via SQL/SLT path.
    let upper = value.add_checked(one_scaled).ok()?;

    Some((value, upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::i256;
    use datafusion_expr::col;

    /// Helper to test valid preimage cases that should return a Range
    fn assert_preimage_range(
        input: ScalarValue,
        expected_lower: ScalarValue,
        expected_upper: ScalarValue,
    ) {
        let floor_func = FloorFunc::new();
        let args = vec![col("x")];
        let lit_expr = Expr::Literal(input.clone(), None);
        let info = SimplifyContext::default();

        let result = floor_func.preimage(&args, &lit_expr, &info).unwrap();

        match result {
            PreimageResult::Range { expr, interval } => {
                assert_eq!(expr, col("x"));
                assert_eq!(interval.lower().clone(), expected_lower);
                assert_eq!(interval.upper().clone(), expected_upper);
            }
            PreimageResult::None => {
                panic!("Expected Range, got None for input {input:?}")
            }
        }
    }

    /// Helper to test cases that should return None
    fn assert_preimage_none(input: ScalarValue) {
        let floor_func = FloorFunc::new();
        let args = vec![col("x")];
        let lit_expr = Expr::Literal(input.clone(), None);
        let info = SimplifyContext::default();

        let result = floor_func.preimage(&args, &lit_expr, &info).unwrap();
        assert!(
            matches!(result, PreimageResult::None),
            "Expected None for input {input:?}"
        );
    }

    #[test]
    fn test_floor_preimage_valid_cases() {
        // Float64
        assert_preimage_range(
            ScalarValue::Float64(Some(100.0)),
            ScalarValue::Float64(Some(100.0)),
            ScalarValue::Float64(Some(101.0)),
        );
        // Float32
        assert_preimage_range(
            ScalarValue::Float32(Some(50.0)),
            ScalarValue::Float32(Some(50.0)),
            ScalarValue::Float32(Some(51.0)),
        );
        // Int64
        assert_preimage_range(
            ScalarValue::Int64(Some(42)),
            ScalarValue::Int64(Some(42)),
            ScalarValue::Int64(Some(43)),
        );
        // Int32
        assert_preimage_range(
            ScalarValue::Int32(Some(100)),
            ScalarValue::Int32(Some(100)),
            ScalarValue::Int32(Some(101)),
        );
        // Negative values
        assert_preimage_range(
            ScalarValue::Float64(Some(-5.0)),
            ScalarValue::Float64(Some(-5.0)),
            ScalarValue::Float64(Some(-4.0)),
        );
        // Zero
        assert_preimage_range(
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(1.0)),
        );
    }

    #[test]
    fn test_floor_preimage_non_integer_float() {
        // floor(x) = 1.3 has NO SOLUTION because floor always returns an integer
        // Therefore preimage should return None for non-integer literals
        assert_preimage_none(ScalarValue::Float64(Some(1.3)));
        assert_preimage_none(ScalarValue::Float64(Some(-2.5)));
        assert_preimage_none(ScalarValue::Float32(Some(3.7)));
    }

    #[test]
    fn test_floor_preimage_integer_overflow() {
        // All integer types at MAX value should return None
        assert_preimage_none(ScalarValue::Int64(Some(i64::MAX)));
        assert_preimage_none(ScalarValue::Int32(Some(i32::MAX)));
        assert_preimage_none(ScalarValue::Int16(Some(i16::MAX)));
        assert_preimage_none(ScalarValue::Int8(Some(i8::MAX)));
    }

    #[test]
    fn test_floor_preimage_float_edge_cases() {
        // Float64 edge cases
        assert_preimage_none(ScalarValue::Float64(Some(f64::INFINITY)));
        assert_preimage_none(ScalarValue::Float64(Some(f64::NEG_INFINITY)));
        assert_preimage_none(ScalarValue::Float64(Some(f64::NAN)));
        assert_preimage_none(ScalarValue::Float64(Some(f64::MAX))); // precision loss

        // Float32 edge cases
        assert_preimage_none(ScalarValue::Float32(Some(f32::INFINITY)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::NEG_INFINITY)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::NAN)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::MAX))); // precision loss
    }

    #[test]
    fn test_floor_preimage_null_values() {
        assert_preimage_none(ScalarValue::Float64(None));
        assert_preimage_none(ScalarValue::Float32(None));
        assert_preimage_none(ScalarValue::Int64(None));
    }

    // ============ Decimal32 Tests (mirrors float/int tests) ============

    #[test]
    fn test_floor_preimage_decimal_valid_cases() {
        // ===== Decimal32 =====
        // Positive integer decimal: 100.00 (scale=2, so raw=10000)
        // floor(x) = 100.00 -> x in [100.00, 101.00)
        assert_preimage_range(
            ScalarValue::Decimal32(Some(10000), 9, 2),
            ScalarValue::Decimal32(Some(10000), 9, 2), // 100.00
            ScalarValue::Decimal32(Some(10100), 9, 2), // 101.00
        );

        // Smaller positive: 50.00
        assert_preimage_range(
            ScalarValue::Decimal32(Some(5000), 9, 2),
            ScalarValue::Decimal32(Some(5000), 9, 2), // 50.00
            ScalarValue::Decimal32(Some(5100), 9, 2), // 51.00
        );

        // Negative integer decimal: -5.00
        assert_preimage_range(
            ScalarValue::Decimal32(Some(-500), 9, 2),
            ScalarValue::Decimal32(Some(-500), 9, 2), // -5.00
            ScalarValue::Decimal32(Some(-400), 9, 2), // -4.00
        );

        // Zero: 0.00
        assert_preimage_range(
            ScalarValue::Decimal32(Some(0), 9, 2),
            ScalarValue::Decimal32(Some(0), 9, 2), // 0.00
            ScalarValue::Decimal32(Some(100), 9, 2), // 1.00
        );

        // Scale 0 (pure integer): 42
        assert_preimage_range(
            ScalarValue::Decimal32(Some(42), 9, 0),
            ScalarValue::Decimal32(Some(42), 9, 0),
            ScalarValue::Decimal32(Some(43), 9, 0),
        );

        // ===== Decimal64 =====
        assert_preimage_range(
            ScalarValue::Decimal64(Some(10000), 18, 2),
            ScalarValue::Decimal64(Some(10000), 18, 2), // 100.00
            ScalarValue::Decimal64(Some(10100), 18, 2), // 101.00
        );

        // Negative
        assert_preimage_range(
            ScalarValue::Decimal64(Some(-500), 18, 2),
            ScalarValue::Decimal64(Some(-500), 18, 2), // -5.00
            ScalarValue::Decimal64(Some(-400), 18, 2), // -4.00
        );

        // Zero
        assert_preimage_range(
            ScalarValue::Decimal64(Some(0), 18, 2),
            ScalarValue::Decimal64(Some(0), 18, 2),
            ScalarValue::Decimal64(Some(100), 18, 2),
        );

        // ===== Decimal128 =====
        assert_preimage_range(
            ScalarValue::Decimal128(Some(10000), 38, 2),
            ScalarValue::Decimal128(Some(10000), 38, 2), // 100.00
            ScalarValue::Decimal128(Some(10100), 38, 2), // 101.00
        );

        // Negative
        assert_preimage_range(
            ScalarValue::Decimal128(Some(-500), 38, 2),
            ScalarValue::Decimal128(Some(-500), 38, 2), // -5.00
            ScalarValue::Decimal128(Some(-400), 38, 2), // -4.00
        );

        // Zero
        assert_preimage_range(
            ScalarValue::Decimal128(Some(0), 38, 2),
            ScalarValue::Decimal128(Some(0), 38, 2),
            ScalarValue::Decimal128(Some(100), 38, 2),
        );

        // ===== Decimal256 =====
        assert_preimage_range(
            ScalarValue::Decimal256(Some(i256::from(10000)), 76, 2),
            ScalarValue::Decimal256(Some(i256::from(10000)), 76, 2), // 100.00
            ScalarValue::Decimal256(Some(i256::from(10100)), 76, 2), // 101.00
        );

        // Negative
        assert_preimage_range(
            ScalarValue::Decimal256(Some(i256::from(-500)), 76, 2),
            ScalarValue::Decimal256(Some(i256::from(-500)), 76, 2), // -5.00
            ScalarValue::Decimal256(Some(i256::from(-400)), 76, 2), // -4.00
        );

        // Zero
        assert_preimage_range(
            ScalarValue::Decimal256(Some(i256::ZERO), 76, 2),
            ScalarValue::Decimal256(Some(i256::ZERO), 76, 2),
            ScalarValue::Decimal256(Some(i256::from(100)), 76, 2),
        );
    }

    #[test]
    fn test_floor_preimage_decimal_non_integer() {
        // floor(x) = 1.30 has NO SOLUTION because floor always returns an integer
        // Therefore preimage should return None for non-integer decimals

        // Decimal32
        assert_preimage_none(ScalarValue::Decimal32(Some(130), 9, 2)); // 1.30
        assert_preimage_none(ScalarValue::Decimal32(Some(-250), 9, 2)); // -2.50
        assert_preimage_none(ScalarValue::Decimal32(Some(370), 9, 2)); // 3.70
        assert_preimage_none(ScalarValue::Decimal32(Some(1), 9, 2)); // 0.01

        // Decimal64
        assert_preimage_none(ScalarValue::Decimal64(Some(130), 18, 2)); // 1.30
        assert_preimage_none(ScalarValue::Decimal64(Some(-250), 18, 2)); // -2.50

        // Decimal128
        assert_preimage_none(ScalarValue::Decimal128(Some(130), 38, 2)); // 1.30
        assert_preimage_none(ScalarValue::Decimal128(Some(-250), 38, 2)); // -2.50

        // Decimal256
        assert_preimage_none(ScalarValue::Decimal256(Some(i256::from(130)), 76, 2)); // 1.30
        assert_preimage_none(ScalarValue::Decimal256(Some(i256::from(-250)), 76, 2)); // -2.50

        // Decimal32: i32::MAX - 50
        // This return None because the value is not an integer, not because it is out of range.
        assert_preimage_none(ScalarValue::Decimal32(Some(i32::MAX - 50), 10, 2));

        // Decimal64: i64::MAX - 50
        // This return None because the value is not an integer, not because it is out of range.
        assert_preimage_none(ScalarValue::Decimal64(Some(i64::MAX - 50), 19, 2));
    }

    #[test]
    fn test_floor_preimage_decimal_overflow() {
        // Test near MAX where adding scale_factor would overflow

        // Decimal32: i32::MAX
        assert_preimage_none(ScalarValue::Decimal32(Some(i32::MAX), 10, 0));

        // Decimal64: i64::MAX
        assert_preimage_none(ScalarValue::Decimal64(Some(i64::MAX), 19, 0));
    }

    #[test]
    fn test_floor_preimage_decimal_edge_cases() {
        // ===== Decimal32 =====
        // Large value that doesn't overflow
        // Decimal(9,2) max value is 9,999,999.99 (stored as 999,999,999)
        // Use a large value that fits Decimal(9,2) and is divisible by 100
        let safe_max_aligned_32 = 999_999_900; // 9,999,999.00
        assert_preimage_range(
            ScalarValue::Decimal32(Some(safe_max_aligned_32), 9, 2),
            ScalarValue::Decimal32(Some(safe_max_aligned_32), 9, 2),
            ScalarValue::Decimal32(Some(safe_max_aligned_32 + 100), 9, 2),
        );

        // Negative edge: use a large negative value that fits Decimal(9,2)
        // Decimal(9,2) min value is -9,999,999.99 (stored as -999,999,999)
        let min_aligned_32 = -999_999_900; // -9,999,999.00
        assert_preimage_range(
            ScalarValue::Decimal32(Some(min_aligned_32), 9, 2),
            ScalarValue::Decimal32(Some(min_aligned_32), 9, 2),
            ScalarValue::Decimal32(Some(min_aligned_32 + 100), 9, 2),
        );
    }

    #[test]
    fn test_floor_preimage_decimal_null() {
        assert_preimage_none(ScalarValue::Decimal32(None, 9, 2));
        assert_preimage_none(ScalarValue::Decimal64(None, 18, 2));
        assert_preimage_none(ScalarValue::Decimal128(None, 38, 2));
        assert_preimage_none(ScalarValue::Decimal256(None, 76, 2));
    }
}
