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
    cast::AsArray,
    types::{Float64Type, Int64Type},
    Array, ArrayRef, Int64Array, PrimitiveArray,
};
use arrow::array::{BooleanArray, Decimal128Array, Float64Array};
use arrow::datatypes::{DataType, Decimal128Type, Field, FieldRef};
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature};
use std::any::Any;
use std::fmt::{Debug, Formatter};

#[derive(PartialEq, Eq, Hash)]
pub struct TrySumFunction {
    signature: Signature,
}

impl Default for TrySumFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl TrySumFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Immutable),
        }
    }
}

impl Debug for TrySumFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrySumFunction")
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
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            self.sum_i64 = match self.sum_i64 {
                None => Some(v),
                Some(acc) => match acc.checked_add(v) {
                    Some(s) => Some(s),
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
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + v);
        }
    }

    fn update_dec128(&mut self, arr: &Decimal128Array, p: u8, _s: i8) {
        if self.failed {
            return;
        }
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let v = arr.value(i);
            self.sum_dec128 = match self.sum_dec128 {
                None => Some(v),
                Some(acc) => match acc.checked_add(v) {
                    Some(sum) => Some(sum),
                    None => {
                        self.failed = true;
                        return;
                    }
                },
            };
            if let Some(sum) = self.sum_dec128 {
                if exceeds_decimal128_precision(sum, p) {
                    self.failed = true;
                    return;
                }
            }
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
                return Err(DataFusionError::Plan(format!(
                    "try_sum: type not supported update_batch: {dt:?}"
                )))
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
        size_of::<Self>()
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
        // 1) merge flag failed
        let failed_arr = downcast_value!(states[1], BooleanArray);
        for i in 0..failed_arr.len() {
            if failed_arr.is_valid(i) && failed_arr.value(i) {
                self.failed = true;
                return Ok(());
            }
        }

        match self.dtype {
            DataType::Int64 => {
                let array = downcast_value!(states[0], Int64Array);
                for value in array.iter().flatten() {
                    self.sum_i64 = match self.sum_i64 {
                        None => Some(value),
                        Some(acc) => match acc.checked_add(value) {
                            Some(s) => Some(s),
                            None => {
                                self.failed = true;
                                return Ok(());
                            }
                        },
                    };
                }
            }
            DataType::Float64 => {
                let array = downcast_value!(states[0], Float64Array);
                for value in array.iter().flatten() {
                    self.sum_f64 = Some(self.sum_f64.unwrap_or(0.0) + value);
                }
            }
            DataType::Decimal128(p, _s) => {
                let array = downcast_value!(states[0], Decimal128Array);
                for value in array.iter().flatten() {
                    let next = match self.sum_dec128 {
                        None => value,
                        Some(acc) => match acc.checked_add(value) {
                            Some(sum) => sum,
                            None => {
                                self.failed = true;
                                return Ok(());
                            }
                        },
                    };

                    if exceeds_decimal128_precision(next, p) {
                        self.failed = true;
                        return Ok(());
                    }

                    self.sum_dec128 = Some(next);
                }
            }
            ref dt => {
                return Err(DataFusionError::Execution(format!(
                    "try_sum: type not suported in merge_batch: {dt:?}"
                )))
            }
        }

        Ok(())
    }
}

// Helpers to determine is exceeds decimal
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

impl AggregateUDFImpl for TrySumFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        use DataType::*;

        let dt = &arg_types[0];
        let result_type = match *dt {
            Null => Float64,
            Decimal128(p, s) => {
                let p2 = std::cmp::min(p + 10, 38);
                Decimal128(p2, s)
            }
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Int64,
            Float16 | Float32 | Float64 => Float64,

            ref other => {
                return Err(DataFusionError::Plan(format!(
                    "try_sum: unsupported type: {other:?}"
                )))
            }
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
            return Err(DataFusionError::Plan(format!(
                "try_sum: exactly 1 argument expected, got {}",
                arg_types.len()
            )));
        }

        let dt = &arg_types[0];
        let coerced = match dt {
            Null => Float64,
            Decimal128(p, s) => Decimal128(*p, *s),
            Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => Int64,
            Float16 | Float32 | Float64 => Float64,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "try_sum: unsupported type: {other:?}"
                )))
            }
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
    use datafusion_common::arrow::array::Float64Array;
    use datafusion_common::ScalarValue;
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
        let f = TrySumFunction::new();
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
        let f = TrySumFunction::new();
        assert_eq!(
            f.return_type(&[DataType::Decimal128(10, 2)])?,
            DataType::Decimal128(20, 2),
            "Spark need more precision in +10"
        );

        let mut acc = TrySumAccumulator::new(DataType::Decimal128(20, 2));
        acc.update_batch(&[dec128(10, 2, vec![Some(123), Some(477)])?])?;
        assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(Some(600), 20, 2));
        Ok(())
    }

    #[test]
    fn decimal_5_0_fits_after_widening() -> Result<()> {
        // input: DECIMAL(5,0) -> result: DECIMAL(15,0)
        let f = TrySumFunction::new();
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
        let f = TrySumFunction::new();
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
