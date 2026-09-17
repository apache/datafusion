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

use std::fmt::Debug;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, Float16Array};
use arrow::compute::{filter, is_not_null};
use arrow::datatypes::FieldRef;
use arrow::{
    array::{ArrayRef, Float32Array, Float64Array},
    datatypes::{DataType, Field},
};
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, downcast_value, internal_err, not_impl_err,
    plan_err,
};
use datafusion_expr::DistinctHandling;
use datafusion_expr::expr::{AggregateFunction, Sort};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, Expr, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_functions_aggregate_common::tdigest::{DEFAULT_MAX_SIZE, TDigest};
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::utils::{PercentileParam, get_scalar_value};

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
        // Accept any numeric value paired with a float64 percentile
        let signature = Signature::one_of(
            vec![
                // 2 args - numeric, percentile (float)
                TypeSignature::Coercible(vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Float,
                        vec![TypeSignatureClass::Numeric],
                        NativeType::Float64,
                    ),
                    Coercion::new_implicit_native(
                        logical_float64(),
                        vec![TypeSignatureClass::Numeric],
                    ),
                ]),
                // 3 args - numeric, percentile (float), number of centroid for T-Digest (integer)
                TypeSignature::Coercible(vec![
                    Coercion::new_implicit(
                        TypeSignatureClass::Float,
                        vec![TypeSignatureClass::Numeric],
                        NativeType::Float64,
                    ),
                    Coercion::new_implicit_native(
                        logical_float64(),
                        vec![TypeSignatureClass::Numeric],
                    ),
                    Coercion::new_implicit(
                        TypeSignatureClass::Integer,
                        vec![TypeSignatureClass::Numeric],
                        NativeType::Int64,
                    ),
                ]),
            ],
            Volatility::Immutable,
        );
        Self { signature }
    }

    pub(crate) fn create_accumulator(
        &self,
        args: &AccumulatorArgs,
    ) -> Result<ApproxPercentileAccumulator> {
        let percentile =
            PercentileParam::try_new(&args.exprs[1], "APPROX_PERCENTILE_CONT")?;

        let is_descending = args
            .order_bys
            .first()
            .map(|sort_expr| sort_expr.options.descending)
            .unwrap_or(false);

        let tdigest_max_size = if args.exprs.len() == 3 {
            Some(validate_input_max_size_expr(&args.exprs[2])?)
        } else {
            None
        };

        let data_type = args.expr_fields[0].data_type();
        let accumulator: ApproxPercentileAccumulator = match data_type {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                if let Some(max_size) = tdigest_max_size {
                    ApproxPercentileAccumulator::new_with_max_size(
                        percentile,
                        is_descending,
                        data_type.clone(),
                        max_size,
                    )
                    .should_track_percentile()
                } else {
                    ApproxPercentileAccumulator::new(
                        percentile,
                        is_descending,
                        data_type.clone(),
                    )
                    .should_track_percentile()
                }
            }
            other => {
                return not_impl_err!(
                    "Support for 'APPROX_PERCENTILE_CONT' for data type {other} is not implemented"
                );
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
            );
        }
    };

    Ok(max_size)
}

impl AggregateUDFImpl for ApproxPercentileCont {
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
                Field::new_list_field(DataType::Float64, true),
                false,
            ),
            Field::new(
                format_state_name(args.name, "percentile"),
                DataType::Float64,
                true,
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
        // Defensive: the public signature already restricts callers to 2 or 3
        // arguments. This guards against aggregate planning accidentally
        // feeding state-field types (e.g. from `PartialReduce`) back into
        // `return_type`, which would otherwise silently choose the wrong type.
        if arg_types.len() > 3 {
            return plan_err!("approx_percentile_cont requires at most 3 arguments");
        }
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

    fn distinct_handling(&self) -> DistinctHandling {
        // Duplicate-sensitive, but the accumulator does not read
        // `is_distinct` and today silently returns the non-distinct answer.
        // The tag records the intent; enforcement is a follow-up change.
        DistinctHandling::Unsupported
    }
}

#[derive(Debug)]
pub struct ApproxPercentileAccumulator {
    digest: TDigest,
    percentile: PercentileParam,
    is_descending: bool,
    return_type: DataType,
    should_track_percentile: bool,
}

impl ApproxPercentileAccumulator {
    pub(crate) fn new(
        percentile: PercentileParam,
        is_descending: bool,
        return_type: DataType,
    ) -> Self {
        Self {
            digest: TDigest::new(DEFAULT_MAX_SIZE),
            percentile,
            is_descending,
            return_type,
            should_track_percentile: false,
        }
    }

    pub(crate) fn new_with_max_size(
        percentile: PercentileParam,
        is_descending: bool,
        return_type: DataType,
        max_size: usize,
    ) -> Self {
        Self {
            digest: TDigest::new(max_size),
            percentile,
            is_descending,
            return_type,
            should_track_percentile: false,
        }
    }

    pub(crate) fn should_track_percentile(mut self) -> Self {
        self.should_track_percentile = true;
        self
    }

    /// Callers must only invoke this when a percentile argument actually
    /// exists in the current batch.
    pub(crate) fn resolve_percentile(
        &mut self,
        percentile_array: &ArrayRef,
    ) -> Result<()> {
        self.percentile.resolve(percentile_array)
    }

    /// The percentile to use for quantile estimation, applying the `1.0 - p`
    /// flip for descending `WITHIN GROUP (ORDER BY ... DESC)`. Errors if the
    /// percentile has not been resolved yet.
    fn effective_percentile(&self) -> Result<f64> {
        let percentile = self.percentile.get()?;
        Ok(if self.is_descending {
            1.0 - percentile
        } else {
            percentile
        })
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
            e => internal_err!(
                "APPROX_PERCENTILE_CONT is not expected to receive the type {e:?}"
            ),
        }
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut state: Vec<ScalarValue> =
            self.digest.to_scalar_state().into_iter().collect();
        if self.should_track_percentile {
            state.push(ScalarValue::Float64(self.percentile.get().ok()));
        }
        Ok(state)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() > 1 {
            self.resolve_percentile(&values[1])?;
        }

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
        if self.digest.count() == 0.0 {
            return ScalarValue::try_from(self.return_type.clone());
        }
        let q = self.digest.estimate_quantile(self.effective_percentile()?);

        // These acceptable return types MUST match the validation in
        // ApproxPercentile::create_accumulator.
        Ok(match &self.return_type {
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

        if self.should_track_percentile
            && let Some(percentile_array) = states.get(6)
        {
            self.percentile.resolve(percentile_array)?;
        }

        let tdigest_states = &states[..6.min(states.len())];
        let states = (0..tdigest_states[0].len())
            .map(|index| {
                tdigest_states
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
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::DataType;

    use datafusion_functions_aggregate_common::tdigest::TDigest;

    use crate::approx_percentile_cont::ApproxPercentileAccumulator;
    use crate::utils::{PercentileParam, PercentileParamState};

    fn make_accumulator() -> ApproxPercentileAccumulator {
        ApproxPercentileAccumulator::new_with_max_size(
            PercentileParam {
                aggregate_fn_name: "APPROX_PERCENTILE_CONT".to_string(),
                state: PercentileParamState::Resolved(0.5),
            },
            false,
            DataType::Float64,
            100,
        )
    }

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

        let mut accumulator = make_accumulator();

        accumulator.merge_digests(&[t1]);
        assert_eq!(accumulator.digest.count(), 50_000.0);
        accumulator.merge_digests(&[t2]);
        assert_eq!(accumulator.digest.count(), 100_000.0);
    }

    #[test]
    fn test_resolve_percentile_from_deferred_column() {
        let mut accumulator =
            ApproxPercentileAccumulator::new_with_max_size(
                PercentileParam::try_new(
                    &datafusion_physical_expr::expressions::col(
                        "m",
                        &arrow::datatypes::Schema::new(vec![
                            arrow::datatypes::Field::new("m", DataType::Float64, false),
                        ]),
                    )
                    .unwrap(),
                    "APPROX_PERCENTILE_CONT",
                )
                .unwrap(),
                false,
                DataType::Float64,
                100,
            );

        let percentile_array: ArrayRef =
            Arc::new(Float64Array::from(vec![0.5, 0.5, 0.5]));
        accumulator.resolve_percentile(&percentile_array).unwrap();
        assert_eq!(accumulator.effective_percentile().unwrap(), 0.5);

        // A later batch that disagrees with the resolved value must error.
        let bad_array: ArrayRef = Arc::new(Float64Array::from(vec![0.9]));
        let err = accumulator.resolve_percentile(&bad_array).unwrap_err();
        assert!(
            err.to_string().contains("must be constant"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_resolve_percentile_non_constant_within_batch_errors() {
        let mut accumulator = make_accumulator();
        accumulator.percentile = PercentileParam::try_new(
            &datafusion_physical_expr::expressions::col(
                "m",
                &arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
                    "m",
                    DataType::Float64,
                    false,
                )]),
            )
            .unwrap(),
            "APPROX_PERCENTILE_CONT",
        )
        .unwrap();

        let percentile_array: ArrayRef = Arc::new(Float64Array::from(vec![0.1, 0.9]));
        let err = accumulator
            .resolve_percentile(&percentile_array)
            .unwrap_err();
        assert!(
            err.to_string().contains("must be constant"),
            "unexpected error: {err}"
        );
    }
}
