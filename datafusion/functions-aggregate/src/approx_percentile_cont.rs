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

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::{can_cast_types, filter, is_not_null};
use arrow::datatypes::Float64Type;
use arrow::{array::ArrayRef, datatypes::DataType};
use arrow_schema::{Field, Schema};

use datafusion_common::{
    exec_err, internal_err, not_impl_datafusion_err, not_impl_err, plan_err, Result,
    ScalarValue,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, Expr, Signature, Volatility,
};
use datafusion_functions_aggregate_common::tdigest::{TDigest, DEFAULT_MAX_SIZE};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

create_func!(ApproxPercentileCont, approx_percentile_cont_udaf);

/// Computes the approximate percentile continuous of a set of numbers
pub fn approx_percentile_cont(
    expression: Expr,
    percentile: Expr,
    centroids: Option<Expr>,
) -> Expr {
    let args = if let Some(centroids) = centroids {
        vec![expression, percentile, centroids]
    } else {
        vec![expression, percentile]
    };
    approx_percentile_cont_udaf().call(args)
}

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
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }

    pub(crate) fn create_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<ApproxPercentileAccumulator> {
        let percentile = validate_input_percentile_expr(&args.exprs[1])?;
        let tdigest_max_size = if args.exprs.len() == 3 {
            Some(validate_input_max_size_expr(&args.exprs[2])?)
        } else {
            None
        };

        let data_type = args.exprs[0].data_type(args.schema)?;
        let accumulator: ApproxPercentileAccumulator = match data_type {
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
                    ApproxPercentileAccumulator::new_with_max_size(percentile, t, max_size)
                }else{
                    ApproxPercentileAccumulator::new(percentile, t)

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

fn get_scalar_value(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    if let ColumnarValue::Scalar(s) = expr.evaluate(&batch)? {
        Ok(s)
    } else {
        internal_err!("Didn't expect ColumnarValue::Array")
    }
}

fn validate_input_percentile_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    let percentile = match get_scalar_value(expr)
        .map_err(|_| not_impl_datafusion_err!("Percentile value for 'APPROX_PERCENTILE_CONT' must be a literal, got: {expr}"))? {
        ScalarValue::Float64(Some(value)) => {
            value
        }
        sv => {
            return internal_err!(
                "Percentile value for 'APPROX_PERCENTILE_CONT' should be coerced to f64 (got data type {})",
                sv.data_type()
            )
        }
    };

    // Ensure the percentile is between 0 and 1.
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}

fn validate_input_max_size_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    let max_size = match get_scalar_value(expr)
        .map_err(|_| not_impl_datafusion_err!("Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be a literal, got: {expr}"))? {
        ScalarValue::UInt64(Some(q)) => q as usize,
        sv => {
            return not_impl_err!(
                "Tdigest max_size value for 'APPROX_PERCENTILE_CONT' should be coerced to u64 literal (got data type {}).",
                sv.data_type()
            )
        },
    };

    Ok(max_size)
}

impl AggregateUDFImpl for ApproxPercentileCont {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
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
                DataType::UInt64,
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
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(self.create_accumulator(acc_args)?))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 && arg_types.len() != 3 {
            return exec_err!("Expect to get 2 or 3 args");
        }

        // Check `is_numeric` to filter out numeric string case
        if !arg_types[0].is_numeric()
            || !can_cast_types(&arg_types[0], &DataType::Float64)
        {
            return exec_err!("1st argument {} is not coercible to f64", arg_types[0]);
        }
        if !arg_types[1].is_numeric()
            || !can_cast_types(&arg_types[1], &DataType::Float64)
        {
            return exec_err!("2nd argument {} is not coercible to f64", arg_types[1]);
        }
        if arg_types.len() == 3
            && (!arg_types[2].is_integer()
                || !can_cast_types(&arg_types[2], &DataType::UInt64))
        {
            return exec_err!("3rd argument {} is not coercible to u64", arg_types[2]);
        }

        if arg_types.len() == 2 {
            Ok(vec![DataType::Float64; 2])
        } else {
            Ok(vec![DataType::Float64, DataType::Float64, DataType::UInt64])
        }
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
    pub fn convert_to_float(values: &ArrayRef) -> Result<Vec<f64>> {
        let array = values.as_primitive::<Float64Type>();
        Ok(array.values().as_ref().to_vec())
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state().into_iter().collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Remove any nulls before computing the percentile
        let mut values = Arc::clone(&values[0]);
        if values.nulls().is_some() {
            values = filter(&values, &is_not_null(&values)?)?;
        }
        let sorted_values = &arrow::compute::sort(&values, None)?;
        let sorted_values = sorted_values
            .as_primitive::<Float64Type>()
            .values()
            .as_ref()
            .to_vec();
        self.digest = self.digest.merge_sorted_f64(&sorted_values);
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.digest.count() == 0 {
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

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let states = (0..states[0].len())
            .map(|index| {
                states
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<Result<Vec<_>>>()
                    .map(|state| TDigest::from_scalar_state(&state))
            })
            .collect::<Result<Vec<_>>>()?;

        self.merge_digests(&states);

        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.digest.size()
            - std::mem::size_of_val(&self.digest)
            + self.return_type.size()
            - std::mem::size_of_val(&self.return_type)
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use datafusion_functions_aggregate_common::tdigest::TDigest;

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
        assert_eq!(accumulator.digest.count(), 50_000);
        accumulator.merge_digests(&[t2]);
        assert_eq!(accumulator.digest.count(), 100_000);
    }
}
