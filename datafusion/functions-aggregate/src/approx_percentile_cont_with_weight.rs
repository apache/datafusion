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
use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::compute::{and, filter, is_not_null};
use arrow::datatypes::FieldRef;
use arrow::{array::ArrayRef, datatypes::DataType};
use datafusion_common::ScalarValue;
use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_expr::expr::{AggregateFunction, Sort};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::{INTEGERS, NUMERICS};
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Expr, Signature, TypeSignature,
};
use datafusion_functions_aggregate_common::tdigest::{Centroid, TDigest};
use datafusion_macros::user_doc;

use crate::approx_percentile_cont::{ApproxPercentileAccumulator, ApproxPercentileCont};

create_func!(
    ApproxPercentileContWithWeight,
    approx_percentile_cont_with_weight_udaf
);

/// Computes the approximate percentile continuous with weight of a set of numbers
pub fn approx_percentile_cont_with_weight(
    order_by: Sort,
    weight: Expr,
    percentile: Expr,
    centroids: Option<Expr>,
) -> Expr {
    let expr = order_by.expr.clone();

    let args = if let Some(centroids) = centroids {
        vec![expr, weight, percentile, centroids]
    } else {
        vec![expr, weight, percentile]
    };

    Expr::AggregateFunction(AggregateFunction::new_udf(
        approx_percentile_cont_with_weight_udaf(),
        args,
        false,
        None,
        vec![order_by],
        None,
    ))
}

/// APPROX_PERCENTILE_CONT_WITH_WEIGHT aggregate expression
#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = "Returns the weighted approximate percentile of input values using the t-digest algorithm.",
    syntax_example = "approx_percentile_cont_with_weight(weight, percentile [, centroids]) WITHIN GROUP (ORDER BY expression)",
    sql_example = r#"```sql
> SELECT approx_percentile_cont_with_weight(weight_column, 0.90) WITHIN GROUP (ORDER BY column_name) FROM table_name;
+---------------------------------------------------------------------------------------------+
| approx_percentile_cont_with_weight(weight_column, 0.90) WITHIN GROUP (ORDER BY column_name) |
+---------------------------------------------------------------------------------------------+
| 78.5                                                                                        |
+---------------------------------------------------------------------------------------------+
> SELECT approx_percentile_cont_with_weight(weight_column, 0.90, 100) WITHIN GROUP (ORDER BY column_name) FROM table_name;
+--------------------------------------------------------------------------------------------------+
| approx_percentile_cont_with_weight(weight_column, 0.90, 100) WITHIN GROUP (ORDER BY column_name) |
+--------------------------------------------------------------------------------------------------+
| 78.5                                                                                             |
+--------------------------------------------------------------------------------------------------+
```
An alternative syntax is also supported:

```sql
> SELECT approx_percentile_cont_with_weight(column_name, weight_column, 0.90) FROM table_name;
+--------------------------------------------------+
| approx_percentile_cont_with_weight(column_name, weight_column, 0.90) |
+--------------------------------------------------+
| 78.5                                             |
+--------------------------------------------------+
```"#,
    standard_argument(name = "expression", prefix = "The"),
    argument(
        name = "weight",
        description = "Expression to use as weight. Can be a constant, column, or function, and any combination of arithmetic operators."
    ),
    argument(
        name = "percentile",
        description = "Percentile to compute. Must be a float value between 0 and 1 (inclusive)."
    ),
    argument(
        name = "centroids",
        description = "Number of centroids to use in the t-digest algorithm. _Default is 100_. A higher number results in more accurate approximation but requires more memory."
    )
)]
#[derive(PartialEq, Eq, Hash)]
pub struct ApproxPercentileContWithWeight {
    signature: Signature,
    approx_percentile_cont: ApproxPercentileCont,
}

impl Debug for ApproxPercentileContWithWeight {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApproxPercentileContWithWeight")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for ApproxPercentileContWithWeight {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxPercentileContWithWeight {
    /// Create a new [`ApproxPercentileContWithWeight`] aggregate function.
    pub fn new() -> Self {
        let mut variants = Vec::with_capacity(NUMERICS.len() * (INTEGERS.len() + 1));
        // Accept any numeric value paired with weight and float64 percentile
        for num in NUMERICS {
            variants.push(TypeSignature::Exact(vec![
                num.clone(),
                num.clone(),
                DataType::Float64,
            ]));
            // Additionally accept an integer number of centroids for T-Digest
            for int in INTEGERS {
                variants.push(TypeSignature::Exact(vec![
                    num.clone(),
                    num.clone(),
                    DataType::Float64,
                    int.clone(),
                ]));
            }
        }
        Self {
            signature: Signature::one_of(variants, Immutable),
            approx_percentile_cont: ApproxPercentileCont::new(),
        }
    }
}

impl AggregateUDFImpl for ApproxPercentileContWithWeight {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "approx_percentile_cont_with_weight"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!(
                "approx_percentile_cont_with_weight requires numeric input types"
            );
        }
        if !arg_types[1].is_numeric() {
            return plan_err!(
                "approx_percentile_cont_with_weight requires numeric weight input types"
            );
        }
        if arg_types[2] != DataType::Float64 {
            return plan_err!("approx_percentile_cont_with_weight requires float64 percentile input types");
        }
        if arg_types.len() == 4 && !arg_types[3].is_integer() {
            return plan_err!(
                "approx_percentile_cont_with_weight requires integer centroids input types"
            );
        }
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!(
                "approx_percentile_cont_with_weight(DISTINCT) aggregations are not available"
            );
        }

        if acc_args.exprs.len() != 3 && acc_args.exprs.len() != 4 {
            return plan_err!(
                "approx_percentile_cont_with_weight requires three or four arguments: value, weight, percentile[, centroids]"
            );
        }

        let sub_args = AccumulatorArgs {
            exprs: if acc_args.exprs.len() == 4 {
                &[
                    Arc::clone(&acc_args.exprs[0]), // value
                    Arc::clone(&acc_args.exprs[2]), // percentile
                    Arc::clone(&acc_args.exprs[3]), // centroids
                ]
            } else {
                &[
                    Arc::clone(&acc_args.exprs[0]), // value
                    Arc::clone(&acc_args.exprs[2]), // percentile
                ]
            },
            expr_fields: if acc_args.exprs.len() == 4 {
                &[
                    Arc::clone(&acc_args.expr_fields[0]), // value
                    Arc::clone(&acc_args.expr_fields[2]), // percentile
                    Arc::clone(&acc_args.expr_fields[3]), // centroids
                ]
            } else {
                &[
                    Arc::clone(&acc_args.expr_fields[0]), // value
                    Arc::clone(&acc_args.expr_fields[2]), // percentile
                ]
            },
            // Unchanged below; we list each field explicitly in case we ever add more
            // fields to AccumulatorArgs making it easier to see if changes are also
            // needed here.
            return_field: acc_args.return_field,
            schema: acc_args.schema,
            ignore_nulls: acc_args.ignore_nulls,
            order_bys: acc_args.order_bys,
            is_reversed: acc_args.is_reversed,
            name: acc_args.name,
            is_distinct: acc_args.is_distinct,
        };
        let approx_percentile_cont_accumulator =
            self.approx_percentile_cont.create_accumulator(&sub_args)?;
        let accumulator = ApproxPercentileWithWeightAccumulator::new(
            approx_percentile_cont_accumulator,
        );
        Ok(Box::new(accumulator))
    }

    /// See [`TDigest::to_scalar_state()`] for a description of the serialized
    /// state.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.approx_percentile_cont.state_fields(args)
    }

    fn supports_within_group_clause(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

#[derive(Debug)]
pub struct ApproxPercentileWithWeightAccumulator {
    approx_percentile_cont_accumulator: ApproxPercentileAccumulator,
}

impl ApproxPercentileWithWeightAccumulator {
    pub fn new(approx_percentile_cont_accumulator: ApproxPercentileAccumulator) -> Self {
        Self {
            approx_percentile_cont_accumulator,
        }
    }
}

impl Accumulator for ApproxPercentileWithWeightAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.approx_percentile_cont_accumulator.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let mut means = Arc::clone(&values[0]);
        let mut weights = Arc::clone(&values[1]);
        // If nulls are present in either array, need to filter those rows out in both arrays
        match (means.null_count() > 0, weights.null_count() > 0) {
            // Both have nulls
            (true, true) => {
                let predicate = and(&is_not_null(&means)?, &is_not_null(&weights)?)?;
                means = filter(&means, &predicate)?;
                weights = filter(&weights, &predicate)?;
            }
            // Only one has nulls
            (false, true) => {
                let predicate = &is_not_null(&weights)?;
                means = filter(&means, predicate)?;
                weights = filter(&weights, predicate)?;
            }
            (true, false) => {
                let predicate = &is_not_null(&means)?;
                means = filter(&means, predicate)?;
                weights = filter(&weights, predicate)?;
            }
            // No nulls
            (false, false) => {}
        }
        debug_assert_eq!(
            means.len(),
            weights.len(),
            "invalid number of values in means and weights"
        );
        let means_f64 = ApproxPercentileAccumulator::convert_to_float(&means)?;
        let weights_f64 = ApproxPercentileAccumulator::convert_to_float(&weights)?;
        let mut digests: Vec<TDigest> = vec![];
        for (mean, weight) in means_f64.iter().zip(weights_f64.iter()) {
            digests.push(TDigest::new_with_centroid(
                self.approx_percentile_cont_accumulator.max_size(),
                Centroid::new(*mean, *weight),
            ))
        }
        self.approx_percentile_cont_accumulator
            .merge_digests(&digests);
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.approx_percentile_cont_accumulator.evaluate()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.approx_percentile_cont_accumulator
            .merge_batch(states)?;

        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.approx_percentile_cont_accumulator)
            + self.approx_percentile_cont_accumulator.size()
    }
}
