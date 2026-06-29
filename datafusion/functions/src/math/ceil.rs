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
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Float32Type,
    Float64Type,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use super::decimal::{apply_decimal_op, ceil_decimal_value};

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
        let [input] = inputs else {
            return exec_err!(
                "ceil expected 1 argument for bounds evaluation, got {}",
                inputs.len()
            );
        };
        let data_type = input.data_type();
        match (ceil_scalar(input.lower()), ceil_scalar(input.upper())) {
            // Both bounds finite → ceil preserves type and monotonicity, so
            // `try_new` always succeeds; let it propagate if that ever breaks.
            (Some(lo), Some(hi)) => Interval::try_new(lo, hi),
            _ => Interval::make_unbounded(&data_type),
        }
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        let [input_interval] = inputs else {
            return exec_err!(
                "ceil expected 1 argument for constraint propagation, got {}",
                inputs.len()
            );
        };
        // ceil(x) ∈ [N, M] → x ∈ (ceil(N)−1, floor(M)]
        // Normalize bounds to integers ceil can actually take before mapping back.
        let lo = match interval.lower() {
            ScalarValue::Float64(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float64(Some(n.ceil() - 1.0)))
            }
            ScalarValue::Float32(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float32(Some(n.ceil() - 1.0)))
            }
            _ => None,
        };
        let hi = match interval.upper() {
            ScalarValue::Float64(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float64(Some(n.floor())))
            }
            ScalarValue::Float32(Some(n)) if n.is_finite() => {
                Some(ScalarValue::Float32(Some(n.floor())))
            }
            _ => None,
        };
        // When BOTH bounds of the output are finite, the output interval
        // [N, M] admits an integer (and therefore a preimage) only when
        // `ceil(N) ≤ floor(M)`. With our transformed values that's
        // `lo + 1 ≤ hi`, i.e. `lo < hi`. If `lo ≥ hi`, the output contains
        // no integer (e.g. `ceil(x) ∈ [12.3, 12.7]` or `[13.1, 13.1]`), the
        // preimage is empty, and we can prune the branch.
        if let (Some(l), Some(h)) = (&lo, &hi)
            && l >= h
        {
            return Ok(None);
        }
        // If either side of the output is finite we can still narrow that side
        // of the input; the unknown side falls back to the input's own bound so
        // the intersect is a no-op on it.
        match (lo, hi) {
            (None, None) => Ok(Some(vec![(*input_interval).clone()])),
            (lo, hi) => {
                let constraint = Interval::try_new(
                    lo.unwrap_or_else(|| input_interval.lower().clone()),
                    hi.unwrap_or_else(|| input_interval.upper().clone()),
                )?;
                Ok(input_interval.intersect(constraint)?.map(|r| vec![r]))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// IEEE-754 signed-zero is intentionally preserved: `(-0.001).ceil() == -0.0`
/// (not `+0.0`), and `ScalarValue` compares bit-for-bit, so `Float64(-0.0)` is
/// structurally distinct from `Float64(+0.0)` even though numerically equal.
/// Do not "normalise" `-0.0 → +0.0` here without considering downstream
/// hashing, structural equality, and Arrow consistency — see the
/// `test_*_zero` and `test_*_singleton_negative_zero` regression tests.
fn ceil_scalar(v: &ScalarValue) -> Option<ScalarValue> {
    match v {
        ScalarValue::Float64(Some(f)) if f.is_finite() => {
            Some(ScalarValue::Float64(Some(f.ceil())))
        }
        ScalarValue::Float32(Some(f)) if f.is_finite() => {
            Some(ScalarValue::Float32(Some(f.ceil())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ceil() -> CeilFunc {
        CeilFunc::new()
    }

    fn f64_interval(lo: f64, hi: f64) -> Interval {
        Interval::try_new(
            ScalarValue::Float64(Some(lo)),
            ScalarValue::Float64(Some(hi)),
        )
        .unwrap()
    }

    fn f32_interval(lo: f32, hi: f32) -> Interval {
        Interval::try_new(
            ScalarValue::Float32(Some(lo)),
            ScalarValue::Float32(Some(hi)),
        )
        .unwrap()
    }

    fn unbounded_f64() -> Interval {
        Interval::make_unbounded(&DataType::Float64).unwrap()
    }

    fn unbounded_f32() -> Interval {
        Interval::make_unbounded(&DataType::Float32).unwrap()
    }

    // --- evaluate_bounds ---

    #[test]
    fn test_evaluate_bounds_basic() {
        // ceil([1.2, 3.7]) = [2.0, 4.0]
        let input = f64_interval(1.2, 3.7);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(2.0, 4.0));
    }

    #[test]
    fn test_evaluate_bounds_already_integer() {
        // ceil([2.0, 4.0]) = [2.0, 4.0]
        let input = f64_interval(2.0, 4.0);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(2.0, 4.0));
    }

    #[test]
    fn test_evaluate_bounds_f32() {
        // ceil([1.1f32, 2.9f32]) = [2.0f32, 3.0f32]
        let input = f32_interval(1.1, 2.9);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f32_interval(2.0, 3.0));
    }

    #[test]
    fn test_evaluate_bounds_unbounded_returns_unbounded() {
        let input = unbounded_f64();
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, unbounded_f64());
    }

    #[test]
    fn test_evaluate_bounds_negative() {
        // ceil([-3.7, -1.2]) = [-3.0, -1.0]
        let input = f64_interval(-3.7, -1.2);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(-3.0, -1.0));
    }

    // --- propagate_constraints ---

    #[test]
    fn test_propagate_constraints_basic() {
        // ceil(x) ∈ [13.0, 15.0] → x ∈ (12.0, 15.0]
        let output = f64_interval(13.0, 15.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(12.0, 15.0));
    }

    #[test]
    fn test_propagate_constraints_non_integer_bounds() {
        // ceil(x) ∈ [12.3, 14.7] — non-integer bounds are normalized:
        // lower: ceil(12.3)-1 = 13-1 = 12.0, upper: floor(14.7) = 14.0
        // → x ∈ (12.0, 14.0]
        let output = f64_interval(12.3, 14.7);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(12.0, 14.0));
    }

    #[test]
    fn test_propagate_constraints_f32() {
        // Same as basic but with Float32
        let output = f32_interval(5.0, 8.0);
        let input = unbounded_f32();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f32_interval(4.0, 8.0));
    }

    #[test]
    fn test_propagate_constraints_unbounded_output_no_change() {
        // No output constraint → input unchanged
        let output = unbounded_f64();
        let input = f64_interval(1.0, 10.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], input);
    }

    #[test]
    fn test_propagate_constraints_nan_output_no_change() {
        // NaN bounds → conservative: input unchanged
        let output = Interval::try_new(
            ScalarValue::Float64(Some(f64::NAN)),
            ScalarValue::Float64(Some(f64::NAN)),
        )
        .unwrap();
        let input = f64_interval(0.0, 100.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], input);
    }

    #[test]
    fn test_propagate_constraints_negative_range() {
        // ceil(x) ∈ [-3.0, -1.0] → x ∈ (-4.0, -1.0]
        let output = f64_interval(-3.0, -1.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(-4.0, -1.0));
    }

    #[test]
    fn test_propagate_constraints_one_sided_upper() {
        // Output (-Inf, 3.5] → x ≤ floor(3.5) = 3.0; input [0.0, 10.0] → [0.0, 3.0]
        let output = Interval::try_new(
            ScalarValue::Float64(None),
            ScalarValue::Float64(Some(3.5)),
        )
        .unwrap();
        let input = f64_interval(0.0, 10.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(0.0, 3.0));
    }

    #[test]
    fn test_propagate_constraints_one_sided_lower() {
        // Output [5.0, +Inf) → x > ceil(5.0) - 1 = 4.0; input [0.0, 10.0] → [4.0, 10.0]
        let output = Interval::try_new(
            ScalarValue::Float64(Some(5.0)),
            ScalarValue::Float64(None),
        )
        .unwrap();
        let input = f64_interval(0.0, 10.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(4.0, 10.0));
    }

    #[test]
    fn test_propagate_constraints_empty_intersection() {
        // x ∈ [5.0, 7.0], constraint ceil(x) ∈ [20.0, 30.0]
        // mapped input constraint: [19.0, 30.0] — no overlap with [5.0, 7.0]
        // → intersect returns None → Ok(None) (branch pruned)
        let output = f64_interval(20.0, 30.0);
        let input = f64_interval(5.0, 7.0);
        let result = ceil().propagate_constraints(&output, &[&input]).unwrap();
        assert!(result.is_none());
    }

    // --- Output contains no integer → preimage is empty, branch prunes ---

    /// `ceil(x) ∈ [12.3, 12.7]` — both bounds non-integer in the same gap
    /// (12, 13). No integer satisfies `ceil(x) ∈ [12.3, 12.7]`, so the
    /// preimage is empty and the branch must be pruned (`Ok(None)`).
    #[test]
    fn test_propagate_constraints_no_integer_in_output_positive_width() {
        let output = f64_interval(12.3, 12.7);
        let input = unbounded_f64();
        let result = ceil().propagate_constraints(&output, &[&input]).unwrap();
        assert!(result.is_none(), "expected branch pruned, got {result:?}");
    }

    /// `ceil(x) ∈ [13.1, 13.1]` — degenerate (singleton) non-integer interval.
    /// Same logic: `ceil` only produces integers, so no x maps into a
    /// non-integer singleton. Branch must be pruned.
    #[test]
    fn test_propagate_constraints_no_integer_in_output_degenerate() {
        let output = f64_interval(13.1, 13.1);
        let input = unbounded_f64();
        let result = ceil().propagate_constraints(&output, &[&input]).unwrap();
        assert!(result.is_none(), "expected branch pruned, got {result:?}");
    }

    /// `ceil(x) ∈ [13.1, 13.9]` — wider non-integer interval that still
    /// contains no integer. Branch must be pruned.
    #[test]
    fn test_propagate_constraints_no_integer_in_output_wider_gap() {
        let output = f64_interval(13.1, 13.9);
        let input = unbounded_f64();
        let result = ceil().propagate_constraints(&output, &[&input]).unwrap();
        assert!(result.is_none(), "expected branch pruned, got {result:?}");
    }

    /// Float32 variant of the impossibility detection — the same logic must
    /// apply regardless of float width.
    #[test]
    fn test_propagate_constraints_no_integer_in_output_f32() {
        let output = f32_interval(7.2, 7.8);
        let input = unbounded_f32();
        let result = ceil().propagate_constraints(&output, &[&input]).unwrap();
        assert!(result.is_none(), "expected branch pruned, got {result:?}");
    }

    // --- Cases that must NOT trigger the impossibility check ---

    /// Integer singleton `ceil(x) ∈ [12.0, 12.0]` is feasible: `ceil(12) = 12`.
    /// The check `lo >= hi` becomes `11 >= 12` (false), so we DO NOT prune.
    #[test]
    fn test_propagate_constraints_integer_singleton_is_feasible() {
        let output = f64_interval(12.0, 12.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(11.0, 12.0));
    }

    /// Boundary case `ceil(x) ∈ [12.0, 12.7]` — N integer, M non-integer in
    /// gap (12, 13). Feasible (x=12 → ceil=12 ∈ [12, 12.7]). Must NOT prune.
    /// Without the guard the check could over-fire if integer N is mishandled.
    #[test]
    fn test_propagate_constraints_integer_lower_non_integer_upper_feasible() {
        let output = f64_interval(12.0, 12.7);
        let input = f64_interval(0.0, 100.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        // lo = ceil(12)-1 = 11, hi = floor(12.7) = 12 → constraint [11, 12]
        // intersect with [0, 100] → [11, 12]
        assert_eq!(result[0], f64_interval(11.0, 12.0));
    }

    /// Boundary case `ceil(x) ∈ [12.3, 13.0]` — N non-integer, M integer.
    /// Feasible (x=13 → ceil=13 ∈ [12.3, 13]). Must NOT prune.
    #[test]
    fn test_propagate_constraints_non_integer_lower_integer_upper_feasible() {
        let output = f64_interval(12.3, 13.0);
        let input = f64_interval(0.0, 100.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        // lo = ceil(12.3)-1 = 12, hi = floor(13) = 13 → constraint [12, 13]
        // intersect with [0, 100] → [12, 13]
        assert_eq!(result[0], f64_interval(12.0, 13.0));
    }

    /// One-sided output `(-∞, 12.7]` — even though `floor(12.7) = 12` would
    /// look "narrow", the lower side is unbounded so the impossibility check
    /// must NOT fire (the guard is `(Some, Some)` only).
    #[test]
    fn test_propagate_constraints_one_sided_does_not_prune() {
        let output = Interval::try_new(
            ScalarValue::Float64(None),
            ScalarValue::Float64(Some(12.7)),
        )
        .unwrap();
        let input = f64_interval(0.0, 100.0);
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        // hi = floor(12.7) = 12; lower bound falls back to input lower (0.0)
        assert_eq!(result[0], f64_interval(0.0, 12.0));
    }

    // --- evaluate_bounds: intervals straddling integer boundaries (off-by-one
    //     prone). ceil is monotonic, so ceil([a, b]) = [ceil(a), ceil(b)]. ---

    /// `[1.999, 2.001]` — straddles integer 2. ceil(1.999)=2, ceil(2.001)=3.
    #[test]
    fn test_evaluate_bounds_straddles_integer() {
        let input = f64_interval(1.999, 2.001);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(2.0, 3.0));
    }

    /// `[-2.001, -1.999]` — negative variant of the same boundary case.
    /// ceil(-2.001) = -2, ceil(-1.999) = -1.
    #[test]
    fn test_evaluate_bounds_straddles_negative_integer() {
        let input = f64_interval(-2.001, -1.999);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        assert_eq!(result, f64_interval(-2.0, -1.0));
    }

    /// `[-0.001, 0.001]` — straddles zero, the sign-flip boundary.
    /// Note: `f64::ceil(-0.001)` returns IEEE-754 `-0.0` (sign-preserving),
    /// not `+0.0`. Numerically equivalent but bit-level distinct, so the
    /// expected lower bound is `-0.0`.
    #[test]
    fn test_evaluate_bounds_straddles_zero() {
        let input = f64_interval(-0.001, 0.001);
        let result = ceil().evaluate_bounds(&[&input]).unwrap();
        let expected = Interval::try_new(
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::Float64(Some(1.0)),
        )
        .unwrap();
        assert_eq!(result, expected);
    }

    // --- propagate_constraints: integer singletons at sign-sensitive points ---

    /// `ceil(x) ∈ [0, 0]` — zero is the sign-flip boundary. Real preimage is
    /// `(-1, 0]`, conservatively `[-1, 0]`.
    #[test]
    fn test_propagate_constraints_integer_singleton_zero() {
        let output = f64_interval(0.0, 0.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(-1.0, 0.0));
    }

    /// `ceil(x) ∈ [-3, -3]` — negative integer singleton. Real preimage is
    /// `(-4, -3]`, conservatively `[-4, -3]`.
    #[test]
    fn test_propagate_constraints_integer_singleton_negative() {
        let output = f64_interval(-3.0, -3.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(-4.0, -3.0));
    }

    // --- propagate_constraints: single-integer output where the integer is on
    //     the LOWER bound (the mirror of `non_integer_lower_integer_upper`) ---

    /// `ceil(x) ∈ [13.0, 13.7]` — exactly one integer (13) lies in the output.
    /// lo = ceil(13)-1 = 12, hi = floor(13.7) = 13 → constraint [12, 13].
    #[test]
    fn test_propagate_constraints_integer_lower_with_room_above() {
        let output = f64_interval(13.0, 13.7);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(12.0, 13.0));
    }

    // --- propagate_constraints: multiple integers and broad negative ranges ---

    /// `ceil(x) ∈ [12.3, 15.7]` — three integers (13, 14, 15) in the output.
    /// lo = ceil(12.3)-1 = 12, hi = floor(15.7) = 15 → [12, 15].
    #[test]
    fn test_propagate_constraints_multiple_integers_in_output() {
        let output = f64_interval(12.3, 15.7);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(12.0, 15.0));
    }

    /// `ceil(x) ∈ [-5, -3]` — broader negative range. lo = ceil(-5)-1 = -6,
    /// hi = floor(-3) = -3 → [-6, -3]. Mirror of `[3, 5] → [2, 5]`.
    #[test]
    fn test_propagate_constraints_negative_multi_integer_range() {
        let output = f64_interval(-5.0, -3.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        assert_eq!(result[0], f64_interval(-6.0, -3.0));
    }

    // --- propagate_constraints: pruning short-circuits intersect with a
    //     bounded input (no need to compute the intersection) ---

    /// `ceil(x) ∈ [12.3, 12.7]` with a bounded input `[5, 7]`: the new
    /// impossibility check fires BEFORE the intersect step, so the result is
    /// `Ok(None)` regardless of the input. Distinct from
    /// `test_propagate_constraints_empty_intersection`, which exercises the
    /// intersect-returns-None path.
    #[test]
    fn test_propagate_constraints_pruning_short_circuits_intersect() {
        let output = f64_interval(12.3, 12.7);
        let input = f64_interval(5.0, 7.0);
        let result = ceil().propagate_constraints(&output, &[&input]).unwrap();
        assert!(
            result.is_none(),
            "expected pruning to short-circuit intersect, got {result:?}"
        );
    }

    // --- Signed-zero edge cases: ScalarValue compares bit-for-bit, so the
    //     output preserves the IEEE-754 sign of zero. These tests document
    //     that behaviour so a future "normalise -0.0 → +0.0" refactor breaks
    //     them visibly. ---

    /// `ceil(x) ∈ [0.0, 0.0]` (positive zero singleton). Feasible: ceil(0)=0.
    /// lo = 0.0.ceil() - 1.0 = -1.0, hi = 0.0.floor() = +0.0 → `[-1.0, +0.0]`.
    #[test]
    fn test_propagate_constraints_singleton_positive_zero() {
        let output = f64_interval(0.0, 0.0);
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        let expected = Interval::try_new(
            ScalarValue::Float64(Some(-1.0)),
            ScalarValue::Float64(Some(0.0)),
        )
        .unwrap();
        assert_eq!(result[0], expected);
    }

    /// `ceil(x) ∈ [-0.0, -0.0]` (negative zero singleton). Same set
    /// numerically as `[+0.0, +0.0]`, but bit-distinct. ceil/floor preserve
    /// the negative sign, so the propagated upper bound is `-0.0`, not `+0.0`.
    #[test]
    fn test_propagate_constraints_singleton_negative_zero() {
        let output = Interval::try_new(
            ScalarValue::Float64(Some(-0.0)),
            ScalarValue::Float64(Some(-0.0)),
        )
        .unwrap();
        let input = unbounded_f64();
        let result = ceil()
            .propagate_constraints(&output, &[&input])
            .unwrap()
            .unwrap();
        let expected = Interval::try_new(
            ScalarValue::Float64(Some(-1.0)),
            ScalarValue::Float64(Some(-0.0)),
        )
        .unwrap();
        assert_eq!(result[0], expected);
    }
}
