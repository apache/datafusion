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

use std::any::Any;
use std::fmt::Debug;

use arrow::datatypes::DataType::{Float64, UInt64};
use arrow::datatypes::{DataType, Field};

use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::type_coercion::aggregates::NUMERICS;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
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
    description = "Returns the approximate median (50th percentile) of input values. It is an alias of `approx_percentile_cont(x, 0.5)`.",
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
pub struct ApproxMedian {
    signature: Signature,
}

impl Debug for ApproxMedian {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("ApproxMedian")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
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
            signature: Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ApproxMedian {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
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
        ])
    }

    fn name(&self) -> &str {
        "approx_median"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("ApproxMedian requires numeric input types");
        }
        Ok(arg_types[0].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return not_impl_err!(
                "APPROX_MEDIAN(DISTINCT) aggregations are not available"
            );
        }

        Ok(Box::new(ApproxPercentileAccumulator::new(
            0.5_f64,
            acc_args.exprs[0].data_type(acc_args.schema)?,
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
