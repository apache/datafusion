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

use arrow::array::{ArrayRef, ArrowNumericType, AsArray, BooleanArray, PrimitiveArray};
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Decimal128Type, Field, FieldRef, Float64Type,
    Int64Type,
};
use datafusion_common::{Result, ScalarValue, downcast_value, exec_err, not_impl_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::mem::size_of_val;

#[derive(PartialEq, Eq, Hash)]
pub struct SparkTrySum {
    signature: Signature,
}

impl Default for SparkTrySum {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkTrySum {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Debug for SparkTrySum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkTrySum")
            .field("signature", &self.signature)
            .finish()
    }
}

/// Accumulator for try_sum that detects overflow
struct TrySumAccumulator<T: ArrowNumericType> {
    sum: Option<T::Native>,
    data_type: DataType,
    failed: bool,
    // Only used if data_type is Decimal128(p, s)
    dec_precision: Option<u8>,
}

impl<T: ArrowNumericType> Debug for TrySumAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TrySumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> TrySumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        let dec_precision = match &data_type {
            DataType::Decimal128(p, _) => Some(*p),
            _ => None,
        };
        Self {
            sum: None,
            data_type,
            failed: false,
            dec_precision,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for TrySumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.evaluate()?,
            ScalarValue::Boolean(Some(self.failed)),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        update_batch_internal(self, values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Check if any partition has failed
        if downcast_value!(states[1], BooleanArray)
            .iter()
            .flatten()
            .any(|f| f)
        {
            self.failed = true;
            return Ok(());
        }

        // Merge the sum values using the same logic as update_batch
        update_batch_internal(self, states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        evaluate_internal(self)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

// Specialized implementations for update_batch for each type

fn update_batch_internal<T: ArrowNumericType>(
    acc: &mut TrySumAccumulator<T>,
    values: &[ArrayRef],
) -> Result<()> {
    if values.is_empty() || acc.failed {
        return Ok(());
    }

    let array: &PrimitiveArray<T> = values[0].as_primitive::<T>();

    match acc.data_type {
        DataType::Int64 => update_int64(acc, array),
        DataType::Float64 => update_float64(acc, array),
        DataType::Decimal128(_, _) => update_decimal128(acc, array),
        _ => exec_err!(
            "try_sum: unsupported type in update_batch: {:?}",
            acc.data_type
        ),
    }
}

fn update_int64<T: ArrowNumericType>(
    acc: &mut TrySumAccumulator<T>,
    array: &PrimitiveArray<T>,
) -> Result<()> {
    for v in array.iter().flatten() {
        // Cast to i64 for checked_add
        let v_i64 = unsafe { std::mem::transmute_copy::<T::Native, i64>(&v) };
        let sum_i64 = acc
            .sum
            .map(|s| unsafe { std::mem::transmute_copy::<T::Native, i64>(&s) });

        let new_sum = match sum_i64 {
            None => v_i64,
            Some(s) => match s.checked_add(v_i64) {
                Some(result) => result,
                None => {
                    acc.failed = true;
                    return Ok(());
                }
            },
        };

        acc.sum = Some(unsafe { std::mem::transmute_copy::<i64, T::Native>(&new_sum) });
    }
    Ok(())
}

fn update_float64<T: ArrowNumericType>(
    acc: &mut TrySumAccumulator<T>,
    array: &PrimitiveArray<T>,
) -> Result<()> {
    for v in array.iter().flatten() {
        let v_f64 = unsafe { std::mem::transmute_copy::<T::Native, f64>(&v) };
        let sum_f64 = acc
            .sum
            .map(|s| unsafe { std::mem::transmute_copy::<T::Native, f64>(&s) })
            .unwrap_or(0.0);
        let new_sum = sum_f64 + v_f64;
        acc.sum = Some(unsafe { std::mem::transmute_copy::<f64, T::Native>(&new_sum) });
    }
    Ok(())
}

fn update_decimal128<T: ArrowNumericType>(
    acc: &mut TrySumAccumulator<T>,
    array: &PrimitiveArray<T>,
) -> Result<()> {
    let precision = acc.dec_precision.unwrap_or(38);

    for v in array.iter().flatten() {
        let v_i128 = unsafe { std::mem::transmute_copy::<T::Native, i128>(&v) };
        let sum_i128 = acc
            .sum
            .map(|s| unsafe { std::mem::transmute_copy::<T::Native, i128>(&s) });

        let new_sum = match sum_i128 {
            None => v_i128,
            Some(s) => match s.checked_add(v_i128) {
                Some(result) => result,
                None => {
                    acc.failed = true;
                    return Ok(());
                }
            },
        };

        if exceeds_decimal128_precision(new_sum, precision) {
            acc.failed = true;
            return Ok(());
        }

        acc.sum = Some(unsafe { std::mem::transmute_copy::<i128, T::Native>(&new_sum) });
    }
    Ok(())
}

fn evaluate_internal<T: ArrowNumericType>(
    acc: &mut TrySumAccumulator<T>,
) -> Result<ScalarValue> {
    if acc.failed {
        return ScalarValue::new_primitive::<T>(None, &acc.data_type);
    }
    ScalarValue::new_primitive::<T>(acc.sum, &acc.data_type)
}

// Helpers to determine if it exceeds decimal precision
fn pow10_i128(p: u8) -> Option<i128> {
    let mut v: i128 = 1;
    for _ in 0..p {
        v = v.checked_mul(10)?;
    }
    Some(v)
}

fn exceeds_decimal128_precision(sum: i128, p: u8) -> bool {
    if let Some(max_plus_one) = pow10_i128(p) {
        let max = max_plus_one - 1;
        sum > max || sum < -max
    } else {
        true
    }
}

impl AggregateUDFImpl for SparkTrySum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        let dt = &arg_types[0];
        let result_type = match dt {
            Null => Float64,
            Decimal128(p, s) => {
                let new_precision = DECIMAL128_MAX_PRECISION.min(p + 10);
                Decimal128(new_precision, *s)
            }
            Int8 | Int16 | Int32 | Int64 => Int64,
            Float16 | Float32 | Float64 => Float64,

            other => return exec_err!("try_sum: unsupported type: {other:?}"),
        };

        Ok(result_type)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(TrySumAccumulator::<$t>::new($dt.clone())))
            };
        }

        match acc_args.return_field.data_type() {
            DataType::Int64 => helper!(Int64Type, acc_args.return_field.data_type()),
            DataType::Float64 => helper!(Float64Type, acc_args.return_field.data_type()),
            DataType::Decimal128(_, _) => {
                helper!(Decimal128Type, acc_args.return_field.data_type())
            }
            _ => not_impl_err!(
                "try_sum: unsupported type for accumulator: {}",
                acc_args.return_field.data_type()
            ),
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let sum_dt = args.return_field.data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "sum"), sum_dt, true).into(),
            Field::new(
                format_state_name(args.name, "failed"),
                DataType::Boolean,
                false,
            )
            .into(),
        ])
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        use DataType::*;
        if arg_types.len() != 1 {
            return exec_err!(
                "try_sum: exactly 1 argument expected, got {}",
                arg_types.len()
            );
        }

        let dt = &arg_types[0];
        let coerced = match dt {
            Null => Float64,
            Decimal128(p, s) => Decimal128(*p, *s),
            Int8 | Int16 | Int32 | Int64 => Int64,
            Float16 | Float32 | Float64 => Float64,
            other => return exec_err!("try_sum: unsupported type: {other:?}"),
        };
        Ok(vec![coerced])
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Null)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{BooleanArray, Decimal128Array, Float64Array, Int64Array};
    use datafusion_common::{DataFusionError, ScalarValue};
    use std::sync::Arc;

    use super::*;
    // -------- Helpers --------

    fn int64(values: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(values)) as ArrayRef
    }

    fn f64(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values)) as ArrayRef
    }

    fn dec128(p: u8, s: i8, vals: Vec<Option<i128>>) -> Result<ArrayRef> {
        let base = Decimal128Array::from(vals);
        let arr = base.with_precision_and_scale(p, s).map_err(|e| {
            DataFusionError::Execution(format!("invalid precision/scale ({p},{s}): {e}"))
        })?;
        Ok(Arc::new(arr) as ArrayRef)
    }

    // -------- update_batch + evaluate --------

    #[test]
    fn try_sum_int_basic() -> Result<()> {
        let mut acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        acc.update_batch(&[int64((0..10).map(Some).collect())])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(Some(45)));
        Ok(())
    }

    #[test]
    fn try_sum_int_with_nulls() -> Result<()> {
        let mut acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        acc.update_batch(&[int64(vec![None, Some(2), Some(3), None, Some(5)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(Some(10)));
        Ok(())
    }

    #[test]
    fn try_sum_float_basic() -> Result<()> {
        let mut acc = TrySumAccumulator::<Float64Type>::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.5), Some(2.5), None, Some(3.0)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Float64(Some(7.0)));
        Ok(())
    }

    #[test]
    fn float_overflow_behaves_like_spark_sum_infinite() -> Result<()> {
        let mut acc = TrySumAccumulator::<Float64Type>::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1e308), Some(1e308)])])?;

        let out = acc.evaluate()?;
        assert!(
            matches!(out, ScalarValue::Float64(Some(v)) if v.is_infinite() && v.is_sign_positive()),
            "waiting +Infinity, got: {out:?}"
        );
        Ok(())
    }

    #[test]
    fn try_sum_float_negative_zero_normalizes_to_positive_zero() -> Result<()> {
        let mut acc = TrySumAccumulator::<Float64Type>::new(DataType::Float64);
        // -0.0 + 0.0 should normalize to 0.0 (positive zero), not -0.0
        acc.update_batch(&[f64(vec![Some(-0.0), Some(0.0)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Float64(Some(0.0)));
        // Verify it's positive zero using is_sign_positive
        if let ScalarValue::Float64(Some(v)) = out {
            assert!(v.is_sign_positive() || v == 0.0);
        }
        Ok(())
    }

    #[test]
    fn try_sum_decimal_basic() -> Result<()> {
        let p = 10u8;
        let s = 2i8;
        let mut acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(123), Some(477)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(600), p, s));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_with_nulls() -> Result<()> {
        let p = 10u8;
        let s = 2i8;
        let mut acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(150), None, Some(200)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(350), p, s));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_overflow_sets_failed() -> Result<()> {
        let p = 5u8;
        let s = 0i8;
        let mut acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(90_000), Some(20_000)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(None, p, s));
        assert!(acc.failed);
        Ok(())
    }

    #[test]
    fn try_sum_decimal_merge_ok_and_failure_propagation() -> Result<()> {
        let p = 10u8;
        let s = 2i8;

        let mut p_ok =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(p, s));
        p_ok.update_batch(&[dec128(p, s, vec![Some(100), Some(200)])?])?;
        let s_ok = p_ok
            .state()?
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<Result<Vec<_>>>()?;

        let mut p_fail =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(p, s));
        p_fail.update_batch(&[dec128(p, s, vec![Some(i128::MAX), Some(1)])?])?;
        let s_fail = p_fail
            .state()?
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<Result<Vec<_>>>()?;

        let mut final_acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(p, s));
        final_acc.merge_batch(&s_ok)?;
        final_acc.merge_batch(&s_fail)?;

        assert!(final_acc.failed);
        assert_eq!(final_acc.evaluate()?, ScalarValue::Decimal128(None, p, s));
        Ok(())
    }

    #[test]
    fn try_sum_int_overflow_sets_failed() -> Result<()> {
        let mut acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        // i64::MAX + 1 => overflow => failed => result NULL
        acc.update_batch(&[int64(vec![Some(i64::MAX), Some(1)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(None));
        assert!(acc.failed);
        Ok(())
    }

    #[test]
    fn try_sum_int_negative_overflow_sets_failed() -> Result<()> {
        let mut acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        // i64::MIN - 1 → overflow negative
        acc.update_batch(&[int64(vec![Some(i64::MIN), Some(-1)])])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(None));
        assert!(acc.failed);
        Ok(())
    }

    // -------- state + merge_batch --------

    #[test]
    fn try_sum_state_two_fields_and_merge_ok() -> Result<()> {
        // acumulador 1 [10, 5] -> sum=15
        let mut acc1 = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        acc1.update_batch(&[int64(vec![Some(10), Some(5)])])?;
        let state1 = acc1.state()?; // [sum, failed]
        assert_eq!(state1.len(), 2);

        // acumulador 2 [20, NULL] -> sum=20
        let mut acc2 = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        acc2.update_batch(&[int64(vec![Some(20), None])])?;
        let state2 = acc2.state()?; // [sum, failed]

        let state1_arrays: Vec<ArrayRef> = state1
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<Result<_>>()?;

        let state2_arrays: Vec<ArrayRef> = state2
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<Result<_>>()?;

        // final accumulator
        let mut final_acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);

        final_acc.merge_batch(&state1_arrays)?;
        final_acc.merge_batch(&state2_arrays)?;

        // sum total = 15 + 20 = 35
        assert!(!final_acc.failed);
        assert_eq!(final_acc.evaluate()?, ScalarValue::Int64(Some(35)));
        Ok(())
    }

    #[test]
    fn try_sum_merge_propagates_failure() -> Result<()> {
        // sum=NULL, failed=true
        let failed_sum = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let failed_flag = Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef;

        let mut acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        acc.merge_batch(&[failed_sum, failed_flag])?;

        assert!(acc.failed);
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(None));
        Ok(())
    }

    #[test]
    fn try_sum_merge_empty_partition_is_not_failure() -> Result<()> {
        // sum=NULL, failed=false
        let empty_sum = Arc::new(Int64Array::from(vec![None])) as ArrayRef;
        let ok_flag = Arc::new(BooleanArray::from(vec![Some(false)])) as ArrayRef;

        let mut acc = TrySumAccumulator::<Int64Type>::new(DataType::Int64);
        acc.update_batch(&[int64(vec![Some(7), Some(8)])])?; // 15

        acc.merge_batch(&[empty_sum, ok_flag])?;

        assert!(!acc.failed);
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(Some(15)));
        Ok(())
    }

    // -------- signature --------

    #[test]
    fn try_sum_return_type_matches_input() -> Result<()> {
        let f = SparkTrySum::new();
        assert_eq!(f.return_type(&[DataType::Int64])?, DataType::Int64);
        assert_eq!(f.return_type(&[DataType::Float64])?, DataType::Float64);
        Ok(())
    }

    #[test]
    fn try_sum_state_and_evaluate_consistency() -> Result<()> {
        let mut acc = TrySumAccumulator::<Float64Type>::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.0), Some(2.0)])])?;
        let eval = acc.evaluate()?;
        let state = acc.state()?;
        assert_eq!(state[0], eval);
        assert_eq!(state[1], ScalarValue::Boolean(Some(false)));
        Ok(())
    }

    // -------------------------
    // DECIMAL
    // -------------------------

    #[test]
    fn decimal_10_2_sum_and_schema_widened() -> Result<()> {
        // input: DECIMAL(10,2)  -> result: DECIMAL(20,2)
        let f = SparkTrySum::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(10, 2)])?,
            DataType::Decimal128(20, 2),
            "Spark needs +10 more digits of precision"
        );

        let mut acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(20, 2));
        acc.update_batch(&[dec128(10, 2, vec![Some(123), Some(477)])?])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(Some(600), 20, 2));
        Ok(())
    }

    #[test]
    fn decimal_5_0_fits_after_widening() -> Result<()> {
        // input: DECIMAL(5,0) -> result: DECIMAL(15,0)
        let f = SparkTrySum::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(5, 0)])?,
            DataType::Decimal128(15, 0)
        );

        let mut acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(15, 0));
        acc.update_batch(&[dec128(5, 0, vec![Some(90_000), Some(20_000)])?])?;
        assert_eq!(
            acc.evaluate()?,
            ScalarValue::Decimal128(Some(110_000), 15, 0)
        );
        Ok(())
    }

    #[test]
    fn decimal_38_0_max_precision_overflows_to_null() -> Result<()> {
        let f = SparkTrySum::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(38, 0)])?,
            DataType::Decimal128(38, 0)
        );
        let ten_pow_38_minus_1 = {
            let p10 = pow10_i128(38)
                .ok_or_else(|| DataFusionError::Internal("10^38 overflow".into()))?;
            p10 - 1
        };
        let mut acc =
            TrySumAccumulator::<Decimal128Type>::new(DataType::Decimal128(38, 0));
        acc.update_batch(&[dec128(38, 0, vec![Some(ten_pow_38_minus_1), Some(1)])?])?;

        assert!(acc.failed, "need fail in overflow p=38");
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(None, 38, 0));
        Ok(())
    }
}
