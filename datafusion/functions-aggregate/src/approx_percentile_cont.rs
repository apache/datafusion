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
use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, Float16Array};
use arrow::compute::{filter, is_not_null};
use arrow::datatypes::FieldRef;
use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field},
};
use datafusion_common::{
    downcast_value, internal_err, not_impl_err, plan_err, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::expr::{AggregateFunction, Sort};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::{INTEGERS, NUMERICS};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Expr, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions_aggregate_common::tdigest::{TDigest, DEFAULT_MAX_SIZE};
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::utils::{get_scalar_value, validate_percentile_expr};

create_func!(ApproxPercentileCont, approx_percentile_cont_udaf);

/// Computes the approximate percentile continuous of a set of numbers
pub fn approx_percentile_cont(
    order_by: Sort,
    percentile: Expr,
    centroids: Option<Expr>,
) -> Expr {
    let expr = order_by.expr.clone();

    let args = if let Some(centroids) = centroids {
        vec![expr, percentile, centroids]
    } else {
        vec![expr, percentile]
    };

    Expr::AggregateFunction(AggregateFunction::new_udf(
        approx_percentile_cont_udaf(),
        args,
        false,
        None,
        vec![order_by],
        None,
    ))
}

#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = "Returns the approximate percentile of input values using the t-digest algorithm.",
    syntax_example = "approx_percentile_cont(percentile [, centroids]) WITHIN GROUP (ORDER BY expression)",
    sql_example = r#"```sql
> SELECT approx_percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) FROM table_name;
+------------------------------------------------------------------+
| approx_percentile_cont(0.75) WITHIN GROUP (ORDER BY column_name) |
+------------------------------------------------------------------+
| 65.0                                                             |
+------------------------------------------------------------------+
> SELECT approx_percentile_cont(0.75, 100) WITHIN GROUP (ORDER BY column_name) FROM table_name;
+-----------------------------------------------------------------------+
| approx_percentile_cont(0.75, 100) WITHIN GROUP (ORDER BY column_name) |
+-----------------------------------------------------------------------+
| 65.0                                                                  |
+-----------------------------------------------------------------------+
```
An alternate syntax is also supported:
```sql
> SELECT approx_percentile_cont(column_name, 0.75) FROM table_name;
+-----------------------------------------------+
| approx_percentile_cont(column_name, 0.75)     |
+-----------------------------------------------+
| 65.0                                          |
+-----------------------------------------------+

> SELECT approx_percentile_cont(column_name, 0.75, 100) FROM table_name;
+----------------------------------------------------------+
| approx_percentile_cont(column_name, 0.75, 100)           |
+----------------------------------------------------------+
| 65.0                                                     |
+----------------------------------------------------------+
```
"#,
    standard_argument(name = "expression",),
    argument(
        name = "percentile",
        description = "Percentile to compute. Must be a float value between 0 and 1 (inclusive)."
    ),
    argument(
        name = "centroids",
        description = "Number of centroids to use in the t-digest algorithm. _Default is 100_. A higher number results in more accurate approximation but requires more memory."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxPercentileCont {
    signature: Signature,
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
        args: &AccumulatorArgs,
    ) -> Result<ApproxPercentileAccumulator> {
        let percentile =
            validate_percentile_expr(&args.exprs[1], "APPROX_PERCENTILE_CONT")?;

        let is_descending = args
            .order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false);

        let percentile = if is_descending {
            1.0 - percentile
        } else {
            percentile
        };

        let tdigest_max_size = if args.exprs.len() == 3 {
            Some(validate_input_max_size_expr(&args.exprs[2])?)
        } else {
            None
        };

        let data_type = args.expr_fields[0].data_type();
        let accumulator: ApproxPercentileAccumulator = match data_type {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64 => {
                if let Some(max_size) = tdigest_max_size {
                    ApproxPercentileAccumulator::new_with_max_size(percentile, data_type.clone(), max_size)
                } else {
                    ApproxPercentileAccumulator::new(percentile, data_type.clone())
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

fn validate_input_max_size_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    let scalar_value = get_scalar_value(expr).map_err(|_e| {
        DataFusionError::Plan(
            "Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be a literal"
                .to_string(),
        )
    })?;

    let max_size = match scalar_value {
        ScalarValue::UInt8(Some(q)) => q as usize,
        ScalarValue::UInt16(Some(q)) => q as usize,
        ScalarValue::UInt32(Some(q)) => q as usize,
        ScalarValue::UInt64(Some(q)) => q as usize,
        ScalarValue::Int32(Some(q)) if q > 0 => q as usize,
        ScalarValue::Int64(Some(q)) if q > 0 => q as usize,
        ScalarValue::Int16(Some(q)) if q > 0 => q as usize,
        ScalarValue::Int8(Some(q)) if q > 0 => q as usize,
        sv => {
            return plan_err!(
                "Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be UInt > 0 literal (got data type {}).",
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

    /// See [`TDigest::to_scalar_state()`] for a description of the serialized
    /// state.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
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
                Field::new_list_field(DataType::Float64, true),
                false,
            ),
        ]
        .into_iter()
        .map(Arc::new)
        .collect())
    }

    fn name(&self) -> &str {
        "approx_percentile_cont"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    #[inline]
    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(self.create_accumulator(&acc_args)?))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("approx_percentile_cont requires numeric input types");
        }
        if arg_types.len() == 3 && !arg_types[2].is_integer() {
            return plan_err!(
                "approx_percentile_cont requires integer centroids input types"
            );
        }
        Ok(arg_types[0].clone())
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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

    // pub(crate) for approx_percentile_cont_with_weight
    pub(crate) fn max_size(&self) -> usize {
        self.digest.max_size()
    }

    // pub(crate) for approx_percentile_cont_with_weight
    pub(crate) fn merge_digests(&mut self, digests: &[TDigest]) {
        let digests = digests.iter().chain(std::iter::once(&self.digest));
        self.digest = TDigest::merge_digests(digests)
    }

    // pub(crate) for approx_percentile_cont_with_weight
    pub(crate) fn convert_to_float(values: &ArrayRef) -> Result<Vec<f64>> {
        debug_assert!(
            values.null_count() == 0,
            "convert_to_float assumes nulls have already been filtered out"
        );
        match values.data_type() {
            DataType::Float64 => {
                let array = downcast_value!(values, Float64Array);
                Ok(array.values().iter().copied().collect::<Vec<_>>())
            }
            DataType::Float32 => {
                let array = downcast_value!(values, Float32Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::Float16 => {
                let array = downcast_value!(values, Float16Array);
                Ok(array
                    .values()
                    .iter()
                    .map(|v| v.to_f64())
                    .collect::<Vec<_>>())
            }
            DataType::Int64 => {
                let array = downcast_value!(values, Int64Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::Int32 => {
                let array = downcast_value!(values, Int32Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::Int16 => {
                let array = downcast_value!(values, Int16Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::Int8 => {
                let array = downcast_value!(values, Int8Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::UInt64 => {
                let array = downcast_value!(values, UInt64Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::UInt32 => {
                let array = downcast_value!(values, UInt32Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::UInt16 => {
                let array = downcast_value!(values, UInt16Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            DataType::UInt8 => {
                let array = downcast_value!(values, UInt8Array);
                Ok(array.values().iter().map(|v| *v as f64).collect::<Vec<_>>())
            }
            e => internal_err!(
                "APPROX_PERCENTILE_CONT is not expected to receive the type {e:?}"
            ),
        }
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state().into_iter().collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Remove any nulls before computing the percentile
        let mut values = Arc::clone(&values[0]);
        if values.null_count() > 0 {
            values = filter(&values, &is_not_null(&values)?)?;
        }
        let sorted_values = &arrow::compute::sort(&values, None)?;
        let sorted_values = ApproxPercentileAccumulator::convert_to_float(sorted_values)?;
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
            DataType::Float16 => ScalarValue::Float16(Some(half::f16::from_f64(q))),
            DataType::Float32 => ScalarValue::Float32(Some(q as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(q)),
            v => unreachable!("unexpected return type {}", v),
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
        size_of_val(self) + self.digest.size() - size_of_val(&self.digest)
            + self.return_type.size()
            - size_of_val(&self.return_type)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

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
