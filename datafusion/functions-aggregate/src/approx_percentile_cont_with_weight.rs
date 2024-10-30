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
use std::mem::size_of_val;
use std::sync::{Arc, OnceLock};

use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field},
};

use datafusion_common::ScalarValue;
use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_expr::aggregate_doc_sections::DOC_SECTION_APPROXIMATE;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::Volatility::Immutable;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, TypeSignature,
};
use datafusion_functions_aggregate_common::tdigest::{
    Centroid, TDigest, DEFAULT_MAX_SIZE,
};

use crate::approx_percentile_cont::{ApproxPercentileAccumulator, ApproxPercentileCont};

make_udaf_expr_and_func!(
    ApproxPercentileContWithWeight,
    approx_percentile_cont_with_weight,
    expression weight percentile,
    "Computes the approximate percentile continuous with weight of a set of numbers",
    approx_percentile_cont_with_weight_udaf
);

/// APPROX_PERCENTILE_CONT_WITH_WEIGHT aggregate expression
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
        Self {
            signature: Signature::one_of(
                // Accept any numeric value paired with a float64 percentile
                NUMERICS
                    .iter()
                    .map(|t| {
                        TypeSignature::Exact(vec![
                            t.clone(),
                            t.clone(),
                            DataType::Float64,
                        ])
                    })
                    .collect(),
                Immutable,
            ),
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
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!(
                "approx_percentile_cont_with_weight(DISTINCT) aggregations are not available"
            );
        }

        if acc_args.exprs.len() != 3 {
            return plan_err!(
                "approx_percentile_cont_with_weight requires three arguments: value, weight, percentile"
            );
        }

        let sub_args = AccumulatorArgs {
            exprs: &[
                Arc::clone(&acc_args.exprs[0]),
                Arc::clone(&acc_args.exprs[2]),
            ],
            ..acc_args
        };
        let approx_percentile_cont_accumulator =
            self.approx_percentile_cont.create_accumulator(sub_args)?;
        let accumulator = ApproxPercentileWithWeightAccumulator::new(
            approx_percentile_cont_accumulator,
        );
        Ok(Box::new(accumulator))
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        self.approx_percentile_cont.state_fields(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_approx_percentile_cont_with_weight_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_approx_percentile_cont_with_weight_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_APPROXIMATE)
            .with_description(
                "Returns the weighted approximate percentile of input values using the t-digest algorithm.",
            )
            .with_syntax_example("approx_percentile_cont_with_weight(expression, weight, percentile)")
            .with_sql_example(r#"```sql
> SELECT approx_percentile_cont_with_weight(column_name, weight_column, 0.90) FROM table_name;
+----------------------------------------------------------------------+
| approx_percentile_cont_with_weight(column_name, weight_column, 0.90) |
+----------------------------------------------------------------------+
| 78.5                                                                 |
+----------------------------------------------------------------------+
```"#,
            )
            .with_standard_argument("expression", None)
            .with_argument("weight", "Expression to use as weight. Can be a constant, column, or function, and any combination of arithmetic operators.")
            .with_argument("percentile", "Percentile to compute. Must be a float value between 0 and 1 (inclusive).")
            .build()
            .unwrap()
    })
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
        let means = &values[0];
        let weights = &values[1];
        debug_assert_eq!(
            means.len(),
            weights.len(),
            "invalid number of values in means and weights"
        );
        let means_f64 = ApproxPercentileAccumulator::convert_to_float(means)?;
        let weights_f64 = ApproxPercentileAccumulator::convert_to_float(weights)?;
        let mut digests: Vec<TDigest> = vec![];
        for (mean, weight) in means_f64.iter().zip(weights_f64.iter()) {
            digests.push(TDigest::new_with_centroid(
                DEFAULT_MAX_SIZE,
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
