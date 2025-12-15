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

use arrow::array::{
    Array, ArrayRef, Int64Array, PrimitiveArray,
    cast::AsArray,
    types::{Float64Type, Int64Type},
};
use arrow::array::{BooleanArray, Decimal128Array, Float64Array};
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DataType, Decimal128Type, Field, FieldRef,
};
use datafusion_common::{DataFusionError, Result, ScalarValue, downcast_value, exec_err};
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

#[derive(Debug, Clone)]
struct TrySumAccumulator {
    dtype: DataType,
    sum_i64: Option<i64>,
    sum_f64: Option<f64>,
    sum_dec128: Option<i128>,
    failed: bool,
}

impl TrySumAccumulator {
    fn new(dtype: DataType) -> Self {
        Self {
            dtype,
            sum_i64: None,
            sum_f64: None,
            sum_dec128: None,
            failed: false,
        }
    }

    fn null_of_dtype(&self) -> ScalarValue {
        match self.dtype {
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s),
            _ => ScalarValue::Null,
        }
    }

    fn update_i64(&mut self, arr: &PrimitiveArray<Int64Type>) {
        if self.failed {
            return;
        }

        for v in arr.iter().flatten() {
            match self.sum_i64 {
                None => self.sum_i64 = Some(v),
                Some(acc) => match acc.checked_add(v) {
                    Some(s) => self.sum_i64 = Some(s),
                    None => {
                        self.failed = true;
                        return;
                    }
                },
            };
        }
    }

    fn update_f64(&mut self, arr: &PrimitiveArray<Float64Type>) {
        if self.failed {
            return;
        }
        for v in arr.iter().flatten() {
            self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + v);
        }
    }

    fn update_dec128(&mut self, arr: &Decimal128Array, p: u8, _s: i8) {
        if self.failed {
            return;
        }

        for v in arr.iter().flatten() {
            let new_sum = match self.sum_dec128 {
                None => v,
                Some(acc) => match acc.checked_add(v) {
                    Some(sum) => sum,
                    None => {
                        self.failed = true;
                        return;
                    }
                },
            };

            if exceeds_decimal128_precision(new_sum, p) {
                self.failed = true;
                return;
            }

            self.sum_dec128 = Some(new_sum);
        }
    }
}

macro_rules! sum_scalar_match {
    ($self:ident, {
            $(
                $pat:pat => ($field:ident, $some_ctor:expr, $null_sv:expr)
            ),+ $(,)?
        }) => {{
            match &$self.dtype {
                $(
                    &$pat => {
                        match $self.$field {
                            Some(v) => $some_ctor(v),
                            None    => $null_sv,
                        }
                    }
                ),+,
                _ => ScalarValue::Null,
            }
        }};
}

impl Accumulator for TrySumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() || self.failed {
            return Ok(());
        }

        match self.dtype {
            DataType::Int64 => {
                let array = values[0].as_primitive::<Int64Type>();
                self.update_i64(array)
            }
            DataType::Float64 => {
                let array = values[0].as_primitive::<Float64Type>();
                self.update_f64(array)
            }
            DataType::Decimal128(p, s) => {
                let array = values[0].as_primitive::<Decimal128Type>();
                self.update_dec128(array, p, s);
            }
            ref dt => {
                return exec_err!("try_sum: unsupported type in update_batch: {dt:?}");
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.failed {
            return Ok(self.null_of_dtype());
        }
        let out = sum_scalar_match!(self, {
            DataType::Int64   => (sum_i64, |x| ScalarValue::Int64(Some(x)), ScalarValue::Int64(None)),
            DataType::Float64 => (sum_f64, |x| ScalarValue::Float64(Some(x)), ScalarValue::Float64(None)),
            DataType::Decimal128(p, s) => (sum_dec128,|x| ScalarValue::Decimal128(Some(x), p, s), ScalarValue::Decimal128(None, p, s)),
        });
        Ok(out)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sum_scalar = sum_scalar_match!(self, {
            DataType::Int64   => (sum_i64, |x| ScalarValue::Int64(Some(x)), ScalarValue::Int64(None)),
            DataType::Float64 => (sum_f64, |x| ScalarValue::Float64(Some(x)), ScalarValue::Float64(None)),
            DataType::Decimal128(p, s) => (sum_dec128,|x| ScalarValue::Decimal128(Some(x), p, s), ScalarValue::Decimal128(None, p, s)),
        });

        Ok(vec![
            if self.failed {
                self.null_of_dtype()
            } else {
                sum_scalar
            },
            ScalarValue::Boolean(Some(self.failed)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Check if any partition has failed
        let failed_arr = downcast_value!(states[1], BooleanArray);
        for failed in failed_arr.iter().flatten() {
            if failed {
                self.failed = true;
                return Ok(());
            }
        }

        // Merge the sum values using the same logic as update_batch
        match self.dtype {
            DataType::Int64 => {
                let array = downcast_value!(states[0], Int64Array);
                self.update_i64(array);
            }
            DataType::Float64 => {
                let array = downcast_value!(states[0], Float64Array);
                self.update_f64(array);
            }
            DataType::Decimal128(p, s) => {
                let array = downcast_value!(states[0], Decimal128Array);
                self.update_dec128(array, p, s);
            }
            ref dt => {
                return exec_err!("try_sum: type not supported in merge_batch: {dt:?}");
            }
        }

        Ok(())
    }
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
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Int64,
            Float16 | Float32 | Float64 => Float64,

            other => return exec_err!("try_sum: unsupported type: {other:?}"),
        };

        Ok(result_type)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let dtype = acc_args.return_field.data_type().clone();
        Ok(Box::new(TrySumAccumulator::new(dtype)))
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
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Int64,
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
    use arrow::array::BooleanArray;
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::array::Float64Array;
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
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        acc.update_batch(&[int64((0..10).map(Some).collect())])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(Some(45)));
        Ok(())
    }

    #[test]
    fn try_sum_int_with_nulls() -> Result<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        acc.update_batch(&[int64(vec![None, Some(2), Some(3), None, Some(5)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(Some(10)));
        Ok(())
    }

    #[test]
    fn try_sum_float_basic() -> Result<()> {
        let mut acc = TrySumAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1.5), Some(2.5), None, Some(3.0)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Float64(Some(7.0)));
        Ok(())
    }

    #[test]
    fn float_overflow_behaves_like_spark_sum_infinite() -> Result<()> {
        let mut acc = TrySumAccumulator::new(DataType::Float64);
        acc.update_batch(&[f64(vec![Some(1e308), Some(1e308)])])?;

        let out = acc.evaluate()?;
        assert!(
            matches!(out, ScalarValue::Float64(Some(v)) if v.is_infinite() && v.is_sign_positive()),
            "waiting +Infinity, got: {out:?}"
        );
        Ok(())
    }

    #[test]
    fn try_sum_decimal_basic() -> Result<()> {
        let p = 10u8;
        let s = 2i8;
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(123), Some(477)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(600), p, s));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_with_nulls() -> Result<()> {
        let p = 10u8;
        let s = 2i8;
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        acc.update_batch(&[dec128(p, s, vec![Some(150), None, Some(200)])?])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Decimal128(Some(350), p, s));
        Ok(())
    }

    #[test]
    fn try_sum_decimal_overflow_sets_failed() -> Result<()> {
        let p = 5u8;
        let s = 0i8;
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
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

        let mut p_ok = TrySumAccumulator::new(DataType::Decimal128(p, s));
        p_ok.update_batch(&[dec128(p, s, vec![Some(100), Some(200)])?])?;
        let s_ok = p_ok
            .state()?
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<Result<Vec<_>>>()?;

        let mut p_fail = TrySumAccumulator::new(DataType::Decimal128(p, s));
        p_fail.update_batch(&[dec128(p, s, vec![Some(i128::MAX), Some(1)])?])?;
        let s_fail = p_fail
            .state()?
            .into_iter()
            .map(|sv| sv.to_array())
            .collect::<Result<Vec<_>>>()?;

        let mut final_acc = TrySumAccumulator::new(DataType::Decimal128(p, s));
        final_acc.merge_batch(&s_ok)?;
        final_acc.merge_batch(&s_fail)?;

        assert!(final_acc.failed);
        assert_eq!(final_acc.evaluate()?, ScalarValue::Decimal128(None, p, s));
        Ok(())
    }

    #[test]
    fn try_sum_int_overflow_sets_failed() -> Result<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
        // i64::MAX + 1 => overflow => failed => result NULL
        acc.update_batch(&[int64(vec![Some(i64::MAX), Some(1)])])?;
        let out = acc.evaluate()?;
        assert_eq!(out, ScalarValue::Int64(None));
        assert!(acc.failed);
        Ok(())
    }

    #[test]
    fn try_sum_int_negative_overflow_sets_failed() -> Result<()> {
        let mut acc = TrySumAccumulator::new(DataType::Int64);
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
        let mut acc1 = TrySumAccumulator::new(DataType::Int64);
        acc1.update_batch(&[int64(vec![Some(10), Some(5)])])?;
        let state1 = acc1.state()?; // [sum, failed]
        assert_eq!(state1.len(), 2);

        // acumulador 2 [20, NULL] -> sum=20
        let mut acc2 = TrySumAccumulator::new(DataType::Int64);
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
        let mut final_acc = TrySumAccumulator::new(DataType::Int64);

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

        let mut acc = TrySumAccumulator::new(DataType::Int64);
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

        let mut acc = TrySumAccumulator::new(DataType::Int64);
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
        let mut acc = TrySumAccumulator::new(DataType::Float64);
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

        let mut acc = TrySumAccumulator::new(DataType::Decimal128(20, 2));
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

        let mut acc = TrySumAccumulator::new(DataType::Decimal128(15, 0));
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
        let mut acc = TrySumAccumulator::new(DataType::Decimal128(38, 0));
        acc.update_batch(&[dec128(38, 0, vec![Some(ten_pow_38_minus_1), Some(1)])?])?;

        assert!(acc.failed, "need fail in overflow p=38");
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(None, 38, 0));
        Ok(())
    }
}
