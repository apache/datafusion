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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DataType,
};
use arrow_schema::{Field, Schema};

use datafusion_common::{
    downcast_value, internal_err, not_impl_err, plan_err, DataFusionError, ScalarValue,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::{INTEGERS, NUMERICS};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, Expr, Signature, TypeSignature,
    Volatility,
};
use datafusion_physical_expr_common::aggregate::tdigest::{
    TDigest, TryIntoF64, DEFAULT_MAX_SIZE,
};
use datafusion_physical_expr_common::utils::limited_convert_logical_expr_to_physical_expr;

make_udaf_expr_and_func!(
    ApproxPercentileCont,
    approx_percentile_cont,
    expression percentile,
    "Computes the approximate percentile continuous of a set of numbers",
    approx_percentile_cont_udaf
);

pub struct ApproxPercentileCont {
    signature: Signature,
}

impl Debug for ApproxPercentileCont {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ApproxPercentileCont")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ApproxPercentileCont {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxPercentileCont {
    /// Create a new [`ApproxPercentileCont`] aggregate function.
    pub fn new() -> Self {
        let mut variants = Vec::with_capacity(NUMERICS.len() * (INTEGERS.len() + 1));
        // Accept any numeric value paired with a float64 percentile
        for num in NUMERICS {
            variants.push(TypeSignature::Exact(vec![num.clone(), DataType::Float64]));
            // Additionally accept an integer number of centroids for T-Digest
            for int in INTEGERS {
                variants.push(TypeSignature::Exact(vec![
                    num.clone(),
                    DataType::Float64,
                    int.clone(),
                ]))
            }
        }
        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }

    pub(crate) fn create_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> datafusion_common::Result<ApproxPercentileAccumulator> {
        let percentile = validate_input_percentile_expr(&args.input_exprs[1])?;
        let tdigest_max_size = if args.input_exprs.len() == 3 {
            Some(validate_input_max_size_expr(&args.input_exprs[2])?)
        } else {
            None
        };

        let accumulator: ApproxPercentileAccumulator = match args.input_type {
            t @ (DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64) => {
                if let Some(max_size) = tdigest_max_size {
                    ApproxPercentileAccumulator::new_with_max_size(percentile, t.clone(), max_size)
                }else{
                    ApproxPercentileAccumulator::new(percentile, t.clone())

                }
            }
            other => {
                return not_impl_err!(
                    "Support for 'APPROX_PERCENTILE_CONT' for data type {other} is not implemented"
                )
            }
        };

        Ok(accumulator)
    }
}

fn get_lit_value(expr: &Expr) -> datafusion_common::Result<ScalarValue> {
    let empty_schema = Arc::new(Schema::empty());
    let empty_batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    let expr = limited_convert_logical_expr_to_physical_expr(expr, &empty_schema)?;
    let result = expr.evaluate(&empty_batch)?;
    match result {
        ColumnarValue::Array(_) => Err(DataFusionError::Internal(format!(
            "The expr {:?} can't be evaluated to scalar value",
            expr
        ))),
        ColumnarValue::Scalar(scalar_value) => Ok(scalar_value),
    }
}

fn validate_input_percentile_expr(expr: &Expr) -> datafusion_common::Result<f64> {
    let lit = get_lit_value(expr)?;
    let percentile = match &lit {
        ScalarValue::Float32(Some(q)) => *q as f64,
        ScalarValue::Float64(Some(q)) => *q,
        got => return not_impl_err!(
            "Percentile value for 'APPROX_PERCENTILE_CONT' must be Float32 or Float64 literal (got data type {})",
            got.data_type()
        )
    };

    // Ensure the percentile is between 0 and 1.
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}

fn validate_input_max_size_expr(expr: &Expr) -> datafusion_common::Result<usize> {
    let lit = get_lit_value(expr)?;
    let max_size = match &lit {
        ScalarValue::UInt8(Some(q)) => *q as usize,
        ScalarValue::UInt16(Some(q)) => *q as usize,
        ScalarValue::UInt32(Some(q)) => *q as usize,
        ScalarValue::UInt64(Some(q)) => *q as usize,
        ScalarValue::Int32(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int64(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int16(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int8(Some(q)) if *q > 0 => *q as usize,
        got => return not_impl_err!(
            "Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be UInt > 0 literal (got data type {}).",
            got.data_type()
        )
    };
    Ok(max_size)
}

impl AggregateUDFImpl for ApproxPercentileCont {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`datafusion_physical_expr_common::aggregate::tdigest::TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(
        &self,
        args: StateFieldsArgs,
    ) -> datafusion_common::Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "max_size"),
                DataType::UInt64,
                false,
            ),
            Field::new(
                format_state_name(args.name, "sum"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(args.name, "count"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(args.name, "max"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(args.name, "min"),
                DataType::Float64,
                false,
            ),
            Field::new_list(
                format_state_name(args.name, "centroids"),
                Field::new("item", DataType::Float64, true),
                false,
            ),
        ])
    }

    fn name(&self) -> &str {
        "approx_percentile_cont"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    #[inline]
    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> datafusion_common::Result<Box<dyn Accumulator>> {
        Ok(Box::new(self.create_accumulator(acc_args)?))
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("approx_percentile_cont requires numeric input types");
        }
        if arg_types.len() == 3 && !arg_types[2].is_integer() {
            return plan_err!(
                "approx_percentile_cont requires integer max_size input types"
            );
        }
        Ok(arg_types[0].clone())
    }
}

#[derive(Debug)]
pub struct ApproxPercentileAccumulator {
    digest: TDigest,
    percentile: f64,
    return_type: DataType,
}

impl ApproxPercentileAccumulator {
    pub fn new(percentile: f64, return_type: DataType) -> Self {
        Self {
            digest: TDigest::new(DEFAULT_MAX_SIZE),
            percentile,
            return_type,
        }
    }

    pub fn new_with_max_size(
        percentile: f64,
        return_type: DataType,
        max_size: usize,
    ) -> Self {
        Self {
            digest: TDigest::new(max_size),
            percentile,
            return_type,
        }
    }

    // public for approx_percentile_cont_with_weight
    pub fn merge_digests(&mut self, digests: &[TDigest]) {
        let digests = digests.iter().chain(std::iter::once(&self.digest));
        self.digest = TDigest::merge_digests(digests)
    }

    // public for approx_percentile_cont_with_weight
    pub fn convert_to_float(values: &ArrayRef) -> datafusion_common::Result<Vec<f64>> {
        match values.data_type() {
            DataType::Float64 => {
                let array = downcast_value!(values, Float64Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::Float32 => {
                let array = downcast_value!(values, Float32Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::Int64 => {
                let array = downcast_value!(values, Int64Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::Int32 => {
                let array = downcast_value!(values, Int32Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::Int16 => {
                let array = downcast_value!(values, Int16Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::Int8 => {
                let array = downcast_value!(values, Int8Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::UInt64 => {
                let array = downcast_value!(values, UInt64Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::UInt32 => {
                let array = downcast_value!(values, UInt32Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::UInt16 => {
                let array = downcast_value!(values, UInt16Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            DataType::UInt8 => {
                let array = downcast_value!(values, UInt8Array);
                Ok(array
                    .values()
                    .iter()
                    .filter_map(|v| v.try_as_f64().transpose())
                    .collect::<datafusion_common::Result<Vec<_>>>()?)
            }
            e => internal_err!(
                "APPROX_PERCENTILE_CONT is not expected to receive the type {e:?}"
            ),
        }
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state().into_iter().collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let values = &values[0];
        let sorted_values = &arrow::compute::sort(values, None)?;
        let sorted_values = ApproxPercentileAccumulator::convert_to_float(sorted_values)?;
        self.digest = self.digest.merge_sorted_f64(&sorted_values);
        Ok(())
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        if self.digest.count() == 0.0 {
            return ScalarValue::try_from(self.return_type.clone());
        }
        let q = self.digest.estimate_quantile(self.percentile);

        // These acceptable return types MUST match the validation in
        // ApproxPercentile::create_accumulator.
        Ok(match &self.return_type {
            DataType::Int8 => ScalarValue::Int8(Some(q as i8)),
            DataType::Int16 => ScalarValue::Int16(Some(q as i16)),
            DataType::Int32 => ScalarValue::Int32(Some(q as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(q as i64)),
            DataType::UInt8 => ScalarValue::UInt8(Some(q as u8)),
            DataType::UInt16 => ScalarValue::UInt16(Some(q as u16)),
            DataType::UInt32 => ScalarValue::UInt32(Some(q as u32)),
            DataType::UInt64 => ScalarValue::UInt64(Some(q as u64)),
            DataType::Float32 => ScalarValue::Float32(Some(q as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(q)),
            v => unreachable!("unexpected return type {:?}", v),
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let states = (0..states[0].len())
            .map(|index| {
                states
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<datafusion_common::Result<Vec<_>>>()
                    .map(|state| TDigest::from_scalar_state(&state))
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        self.merge_digests(&states);

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.digest.size()
            - std::mem::size_of_val(&self.digest)
            + self.return_type.size()
            - std::mem::size_of_val(&self.return_type)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use datafusion_physical_expr_common::aggregate::tdigest::TDigest;

    use crate::approx_percentile_cont::ApproxPercentileAccumulator;

    #[test]
    fn test_combine_approx_percentile_accumulator() {
        let mut digests: Vec<TDigest> = Vec::new();

        // one TDigest with 50_000 values from 1 to 1_000
        for _ in 1..=50 {
            let t = TDigest::new(100);
            let values: Vec<_> = (1..=1_000).map(f64::from).collect();
            let t = t.merge_unsorted_f64(values);
            digests.push(t)
        }

        let t1 = TDigest::merge_digests(&digests);
        let t2 = TDigest::merge_digests(&digests);

        let mut accumulator =
            ApproxPercentileAccumulator::new_with_max_size(0.5, DataType::Float64, 100);

        accumulator.merge_digests(&[t1]);
        assert_eq!(accumulator.digest.count(), 50_000.0);
        accumulator.merge_digests(&[t2]);
        assert_eq!(accumulator.digest.count(), 100_000.0);
    }
}
