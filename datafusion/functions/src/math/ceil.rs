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
use num_traits::{Float, One};

use super::decimal::{apply_decimal_op, ceil_decimal_value};

/// Extends `num_traits::Float` with `next_up()` so generic preimage helpers
/// can call it without specializing per concrete float type.
trait FloatExt: Float {
    fn next_up(self) -> Self;
}

impl FloatExt for f64 {
    #[inline]
    fn next_up(self) -> f64 {
        f64::next_up(self)
    }
}

impl FloatExt for f32 {
    #[inline]
    fn next_up(self) -> f32 {
        f32::next_up(self)
    }
}

// ============ Macro for ceil preimage bounds ============
/// Dispatches to the appropriate bounds helper and wraps results in ScalarValue.
macro_rules! ceil_preimage_bounds {
    (float: $variant:ident, $value:expr) => {
        ceil_preimage_float($value).map(|(lo, hi)| {
            (
                ScalarValue::$variant(Some(lo)),
                ScalarValue::$variant(Some(hi)),
            )
        })
    };
    (decimal: $variant:ident, $decimal_type:ty, $value:expr, $precision:expr, $scale:expr) => {
        ceil_preimage_decimal::<$decimal_type>($value, $precision, $scale).map(
            |(lo, hi)| {
                (
                    ScalarValue::$variant(Some(lo), $precision, $scale),
                    ScalarValue::$variant(Some(hi), $precision, $scale),
                )
            },
        )
    };
}

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the nearest integer greater than or equal to a number.",
    syntax_example = "ceil(numeric_expression)",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    sql_example = r#"```sql
> SELECT ceil(3.14);
+------------+
| ceil(3.14) |
+------------+
| 4.0        |
+------------+
```"#
)]
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

impl ScalarUDFImpl for CeilFunc {
    fn name(&self) -> &str {
        "ceil"
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
                        v.map(f64::ceil),
                    )));
                }
                ScalarValue::Float32(v) => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Float32(
                        v.map(f32::ceil),
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
                    .unary::<_, Float64Type>(f64::ceil),
            ),
            DataType::Float32 => Arc::new(
                value
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(f32::ceil),
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
                    ceil_decimal_value,
                )?
            }
            DataType::Decimal64(precision, scale) => {
                apply_decimal_op::<Decimal64Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
                )?
            }
            DataType::Decimal128(precision, scale) => {
                apply_decimal_op::<Decimal128Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
                )?
            }
            DataType::Decimal256(precision, scale) => {
                apply_decimal_op::<Decimal256Type, _>(
                    &value,
                    *precision,
                    *scale,
                    self.name(),
                    ceil_decimal_value,
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

    /// Compute the preimage for `ceil(col) = literal`.
    ///
    /// `ceil(x) = N` iff `x ∈ (N-1, N]`. Since the preimage API expects a
    /// half-open `[lower, upper)` interval, both boundaries are shifted by
    /// one ULP using `next_up()`:
    ///
    ///   `(N-1, N]  →  [next_up(N-1), next_up(N))`
    ///
    /// For decimal types the "ULP" is one scale unit (the smallest
    /// representable step at the column's scale).
    fn preimage(
        &self,
        args: &[Expr],
        lit_expr: &Expr,
        _info: &SimplifyContext,
    ) -> Result<PreimageResult> {
        debug_assert!(args.len() == 1, "ceil() takes exactly one argument");

        let arg = args[0].clone();

        let Expr::Literal(lit_value, _) = lit_expr else {
            return Ok(PreimageResult::None);
        };

        let Some((lower, upper)) = (match lit_value {
            ScalarValue::Float64(Some(n)) => {
                ceil_preimage_bounds!(float: Float64, *n)
            }
            ScalarValue::Float32(Some(n)) => {
                ceil_preimage_bounds!(float: Float32, *n)
            }
            ScalarValue::Decimal32(Some(n), precision, scale) => {
                ceil_preimage_bounds!(decimal: Decimal32, Decimal32Type, *n, *precision, *scale)
            }
            ScalarValue::Decimal64(Some(n), precision, scale) => {
                ceil_preimage_bounds!(decimal: Decimal64, Decimal64Type, *n, *precision, *scale)
            }
            ScalarValue::Decimal128(Some(n), precision, scale) => {
                ceil_preimage_bounds!(decimal: Decimal128, Decimal128Type, *n, *precision, *scale)
            }
            ScalarValue::Decimal256(Some(n), precision, scale) => {
                ceil_preimage_bounds!(decimal: Decimal256, Decimal256Type, *n, *precision, *scale)
            }
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

// ============ Helper functions for ceil preimage bounds ============

/// Computes `[next_up(N-1), next_up(N))` for `ceil(x) = N` on floating-point types.
///
/// Returns `None` if:
/// - `N` is non-finite (±∞, NaN)
/// - `N` has a fractional part (ceil always returns integers)
/// - `N - 1` underflows to `N` due to precision loss at extreme magnitudes
fn ceil_preimage_float<F: Float + One + FloatExt>(n: F) -> Option<(F, F)> {
    if !n.is_finite() || n.fract() != F::zero() {
        return None;
    }
    let n_minus_one = n - F::one();
    // Guard: at very large magnitudes N-1 == N due to precision loss.
    // Mirroring floor's analogous guard for N+1 == N.
    if n_minus_one >= n {
        return None;
    }
    Some((n_minus_one.next_up(), n.next_up()))
}

/// Computes the preimage bounds for `ceil(x) = N` on decimal types.
///
/// `ceil(x) = N` iff `x ∈ (N-1, N]`. In decimal discrete steps the
/// interval is `[N_raw - one_scaled + 1, N_raw + 1)`.
///
/// Returns `None` if:
/// - `N` has a fractional part (ceil always returns integers)
/// - Arithmetic would overflow the native integer
fn ceil_preimage_decimal<D: DecimalType>(
    n_raw: D::Native,
    precision: u8,
    scale: i8,
) -> Option<(D::Native, D::Native)>
where
    D::Native: DecimalCast + ArrowNativeTypeOp + std::ops::Rem<Output = D::Native>,
{
    let one_scaled: D::Native =
        rescale_decimal::<D, D>(D::Native::ONE, 1, 0, precision, scale)?;

    // ceil always returns a whole-number decimal (raw value divisible by one_scaled).
    // e.g. ceil(5.30) = 6.00 (raw 600), never 6.30. A literal like 5.30 (raw 530)
    // cannot be a valid output, so its preimage is empty, thus return None.
    if scale > 0 && n_raw % one_scaled != D::Native::ZERO {
        return None;
    }

    // lower = N_raw - one_scaled + 1  (first decimal step strictly above N-1)
    // upper = N_raw + 1               (first decimal step strictly above N)
    let lower = n_raw
        .sub_checked(one_scaled)
        .ok()?
        .add_checked(D::Native::ONE)
        .ok()?;
    let upper = n_raw.add_checked(D::Native::ONE).ok()?;

    Some((lower, upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_buffer::i256;
    use datafusion_expr::col;

    fn assert_preimage_range(
        input: ScalarValue,
        expected_lower: ScalarValue,
        expected_upper: ScalarValue,
    ) {
        let ceil_func = CeilFunc::new();
        let args = vec![col("x")];
        let lit_expr = Expr::Literal(input.clone(), None);
        let info = SimplifyContext::default();

        let result = ceil_func.preimage(&args, &lit_expr, &info).unwrap();

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

    fn assert_preimage_none(input: ScalarValue) {
        let ceil_func = CeilFunc::new();
        let args = vec![col("x")];
        let lit_expr = Expr::Literal(input.clone(), None);
        let info = SimplifyContext::default();

        let result = ceil_func.preimage(&args, &lit_expr, &info).unwrap();
        assert!(
            matches!(result, PreimageResult::None),
            "Expected None for input {input:?}"
        );
    }

    #[test]
    fn test_ceil_preimage_float_positive() {
        // ceil(x) = 5 → x ∈ (4, 5] → [next_up(4), next_up(5))
        assert_preimage_range(
            ScalarValue::Float64(Some(5.0)),
            ScalarValue::Float64(Some(4.0_f64.next_up())),
            ScalarValue::Float64(Some(5.0_f64.next_up())),
        );
        assert_preimage_range(
            ScalarValue::Float32(Some(5.0)),
            ScalarValue::Float32(Some(4.0_f32.next_up())),
            ScalarValue::Float32(Some(5.0_f32.next_up())),
        );
        // ceil(x) = 100
        assert_preimage_range(
            ScalarValue::Float64(Some(100.0)),
            ScalarValue::Float64(Some(99.0_f64.next_up())),
            ScalarValue::Float64(Some(100.0_f64.next_up())),
        );
    }

    #[test]
    fn test_ceil_preimage_float_negative() {
        // ceil(x) = -4 → x ∈ (-5, -4] → [next_up(-5), next_up(-4))
        assert_preimage_range(
            ScalarValue::Float64(Some(-4.0)),
            ScalarValue::Float64(Some((-5.0_f64).next_up())),
            ScalarValue::Float64(Some((-4.0_f64).next_up())),
        );
    }

    #[test]
    fn test_ceil_preimage_float_zero() {
        // ceil(x) = 0 → x ∈ (-1, 0] → [next_up(-1), next_up(0))
        assert_preimage_range(
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some((-1.0_f64).next_up())),
            ScalarValue::Float64(Some(0.0_f64.next_up())),
        );
    }

    #[test]
    fn test_ceil_preimage_float_non_integer() {
        // ceil always returns integers; non-integer literal has no preimage
        assert_preimage_none(ScalarValue::Float64(Some(1.5)));
        assert_preimage_none(ScalarValue::Float64(Some(-2.3)));
        assert_preimage_none(ScalarValue::Float32(Some(3.7)));
    }

    #[test]
    fn test_ceil_preimage_float_edge_cases() {
        assert_preimage_none(ScalarValue::Float64(Some(f64::INFINITY)));
        assert_preimage_none(ScalarValue::Float64(Some(f64::NEG_INFINITY)));
        assert_preimage_none(ScalarValue::Float64(Some(f64::NAN)));
        // At f64::MIN (most negative finite), N-1 == N (precision loss) → None
        assert_preimage_none(ScalarValue::Float64(Some(f64::MIN)));
        // At f64::MAX, N-1 == N for the same reason → None
        assert_preimage_none(ScalarValue::Float64(Some(f64::MAX)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::INFINITY)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::NEG_INFINITY)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::NAN)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::MIN)));
        assert_preimage_none(ScalarValue::Float32(Some(f32::MAX)));
    }

    #[test]
    fn test_ceil_preimage_float_null() {
        assert_preimage_none(ScalarValue::Float64(None));
        assert_preimage_none(ScalarValue::Float32(None));
    }

    #[test]
    fn test_ceil_preimage_decimal_positive() {
        // ceil(x) = 5.00 (raw 500, scale=2) → x ∈ (4.00, 5.00] → [4.01, 5.01) raw [401, 501)
        assert_preimage_range(
            ScalarValue::Decimal128(Some(500), 10, 2),
            ScalarValue::Decimal128(Some(401), 10, 2), // next above 4.00
            ScalarValue::Decimal128(Some(501), 10, 2), // next above 5.00
        );
        // scale=0 (integer): ceil(x) = 42 → [42-1+1, 42+1) = [42, 43)
        assert_preimage_range(
            ScalarValue::Decimal128(Some(42), 10, 0),
            ScalarValue::Decimal128(Some(42), 10, 0),
            ScalarValue::Decimal128(Some(43), 10, 0),
        );
    }

    #[test]
    fn test_ceil_preimage_decimal_negative() {
        // ceil(x) = -5.00 → x ∈ (-6.00, -5.00] → [-5.99, -4.99) raw [-599, -499)
        assert_preimage_range(
            ScalarValue::Decimal128(Some(-500), 10, 2),
            ScalarValue::Decimal128(Some(-599), 10, 2),
            ScalarValue::Decimal128(Some(-499), 10, 2),
        );
    }

    #[test]
    fn test_ceil_preimage_decimal_zero() {
        // ceil(x) = 0.00 → x ∈ (-1.00, 0.00] → [-0.99, 0.01) raw [-99, 1)
        assert_preimage_range(
            ScalarValue::Decimal128(Some(0), 10, 2),
            ScalarValue::Decimal128(Some(-99), 10, 2),
            ScalarValue::Decimal128(Some(1), 10, 2),
        );
    }

    #[test]
    fn test_ceil_preimage_decimal_non_integer() {
        // 5.50 has a fractional part → no preimage
        assert_preimage_none(ScalarValue::Decimal128(Some(550), 10, 2));
        assert_preimage_none(ScalarValue::Decimal128(Some(-275), 10, 2));
        assert_preimage_none(ScalarValue::Decimal128(Some(1), 10, 2)); // 0.01
    }

    #[test]
    fn test_ceil_preimage_decimal_overflow() {
        // upper = N_raw + 1 overflows for MAX values
        assert_preimage_none(ScalarValue::Decimal32(Some(i32::MAX), 10, 0));
        assert_preimage_none(ScalarValue::Decimal64(Some(i64::MAX), 19, 0));
        // lower = N_raw - one_scaled + 1 overflows for very negative aligned values
        // i128::MIN is not divisible by 100 (scale=2), so use MIN + (MIN % 100).abs()
        // The simplest aligned value that causes subtraction overflow at scale=0 is i128::MIN.
        assert_preimage_none(ScalarValue::Decimal128(Some(i128::MIN), 38, 0));
    }

    #[test]
    fn test_ceil_preimage_decimal_all_variants() {
        // Decimal32
        assert_preimage_range(
            ScalarValue::Decimal32(Some(500), 9, 2),
            ScalarValue::Decimal32(Some(401), 9, 2),
            ScalarValue::Decimal32(Some(501), 9, 2),
        );
        // Decimal64
        assert_preimage_range(
            ScalarValue::Decimal64(Some(500), 18, 2),
            ScalarValue::Decimal64(Some(401), 18, 2),
            ScalarValue::Decimal64(Some(501), 18, 2),
        );
        // Decimal256
        assert_preimage_range(
            ScalarValue::Decimal256(Some(i256::from(500)), 76, 2),
            ScalarValue::Decimal256(Some(i256::from(401)), 76, 2),
            ScalarValue::Decimal256(Some(i256::from(501)), 76, 2),
        );
    }

    #[test]
    fn test_ceil_preimage_decimal_null() {
        assert_preimage_none(ScalarValue::Decimal32(None, 9, 2));
        assert_preimage_none(ScalarValue::Decimal64(None, 18, 2));
        assert_preimage_none(ScalarValue::Decimal128(None, 38, 2));
        assert_preimage_none(ScalarValue::Decimal256(None, 76, 2));
    }
}
