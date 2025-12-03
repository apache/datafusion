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

//! Defines physical expressions for APPROX_MEDIAN that can be evaluated MEDIAN at runtime during query execution

use arrow::datatypes::DataType::{Float64, UInt64};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::NativeType;
use datafusion_functions_aggregate_common::noop_accumulator::NoopAccumulator;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::{not_impl_err, Result};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use crate::approx_percentile_cont::ApproxPercentileAccumulator;

make_udaf_expr_and_func!(
    ApproxMedian,
    approx_median,
    expression,
    "Computes the approximate median of a set of numbers",
    approx_median_udaf
);

/// APPROX_MEDIAN aggregate expression
#[user_doc(
    doc_section(label = "Approximate Functions"),
    description = "Returns the approximate median (50th percentile) of input values. It is an alias of `approx_percentile_cont(0.5) WITHIN GROUP (ORDER BY x)`.",
    syntax_example = "approx_median(expression)",
    sql_example = r#"```sql
> SELECT approx_median(column_name) FROM table_name;
+-----------------------------------+
| approx_median(column_name)        |
+-----------------------------------+
| 23.5                              |
+-----------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ApproxMedian {
    signature: Signature,
}

impl Default for ApproxMedian {
    fn default() -> Self {
        Self::new()
    }
}

impl ApproxMedian {
    /// Create a new APPROX_MEDIAN aggregate function
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Integer,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Float,
                        vec![TypeSignatureClass::Decimal],
                        NativeType::Float64,
                    )]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ApproxMedian {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.input_fields[0].data_type().is_null() {
            Ok(vec![Field::new(
                format_state_name(args.name, self.name()),
                DataType::Null,
                true,
            )
            .into()])
        } else {
            Ok(vec![
                Field::new(format_state_name(args.name, "max_size"), UInt64, false),
                Field::new(format_state_name(args.name, "sum"), Float64, false),
                Field::new(format_state_name(args.name, "count"), UInt64, false),
                Field::new(format_state_name(args.name, "max"), Float64, false),
                Field::new(format_state_name(args.name, "min"), Float64, false),
                Field::new_list(
                    format_state_name(args.name, "centroids"),
                    Field::new_list_field(Float64, true),
                    false,
                ),
            ]
            .into_iter()
            .map(Arc::new)
            .collect())
        }
    }

    fn name(&self) -> &str {
        "approx_median"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!(
                "APPROX_MEDIAN(DISTINCT) aggregations are not available"
            );
        }

        if acc_args.expr_fields[0].data_type().is_null() {
            Ok(Box::new(NoopAccumulator::default()))
        } else {
            Ok(Box::new(ApproxPercentileAccumulator::new(
                0.5_f64,
                acc_args.expr_fields[0].data_type().clone(),
            )))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
