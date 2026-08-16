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
use std::sync::Arc;

use arrow::datatypes::DataType;

use arrow::datatypes::FieldRef;

use crate::percentile_cont::PercentileCont;
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_expr::GroupsAccumulator;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, Signature, TypeSignature,
    TypeSignatureClass, Volatility, function::AccumulatorArgs,
};
use datafusion_macros::user_doc;
use datafusion_physical_expr::expressions::lit;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

make_udaf_expr_and_func!(
    Median,
    median,
    expression,
    "Computes the median of a set of numbers",
    median_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the median value in the specified column.",
    syntax_example = "median(expression)",
    sql_example = r#"```sql
> SELECT median(column_name) FROM table_name;
+----------------------+
| median(column_name)   |
+----------------------+
| 45.5                 |
+----------------------+
```"#,
    standard_argument(name = "expression", prefix = "The")
)]
/// MEDIAN aggregate expression. If using the non-distinct variation, then this uses a
/// lot of memory because all values need to be stored in memory before a result can be
/// computed. If an approximation is sufficient then APPROX_MEDIAN provides a much more
/// efficient solution.
///
/// If using the distinct variation, the memory usage will be similarly high if the
/// cardinality is high as it stores all distinct values in memory before computing the
/// result, but if cardinality is low then memory usage will also be lower.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Median {
    signature: Signature,
    percentile_cont: PercentileCont,
}

impl Default for Median {
    fn default() -> Self {
        Self::new()
    }
}

impl Median {
    pub fn new() -> Self {
        Self {
            // Integer inputs are coerced to Float64 so the average of the two
            // middle values is not truncated. This matches DuckDB / PostgreSQL / Spark.
            // Float and Decimal inputs preserve their type.
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Decimal,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_exact(
                        TypeSignatureClass::Float,
                    )]),
                    TypeSignature::Coercible(vec![Coercion::new_implicit(
                        TypeSignatureClass::Native(logical_float64()),
                        vec![TypeSignatureClass::Integer],
                        NativeType::Float64,
                    )]),
                ],
                Volatility::Immutable,
            ),
            percentile_cont: PercentileCont::new(),
        }
    }
}

type PercentileExprsArgs = ([Arc<dyn PhysicalExpr>; 2], [FieldRef; 2]);

/// Build arguments for `percentile_cont` UDF
fn percentile_exprs_args(args: &AccumulatorArgs) -> Result<PercentileExprsArgs> {
    let percentile_expr = lit(0.5_f64);
    let percentile_field = percentile_expr.return_field(args.schema)?;
    Ok((
        [Arc::clone(&args.exprs[0]), percentile_expr],
        [Arc::clone(&args.expr_fields[0]), percentile_field],
    ))
}

impl AggregateUDFImpl for Median {
    fn name(&self) -> &str {
        "median"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.percentile_cont.return_type(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.percentile_cont.state_fields(args)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let num_args = args.exprs.len();
        assert_eq_or_internal_err!(
            num_args,
            1,
            "median should only have 1 arg, but found num args:{}",
            num_args
        );
        let (exprs, expr_fields) = percentile_exprs_args(&args)?;
        let sub_args = AccumulatorArgs {
            exprs: &exprs,
            expr_fields: &expr_fields,
            return_field: Arc::clone(&args.return_field),
            schema: args.schema,
            ignore_nulls: args.ignore_nulls,
            order_bys: args.order_bys,
            is_reversed: args.is_reversed,
            name: args.name,
            is_distinct: args.is_distinct,
        };
        self.percentile_cont.accumulator(sub_args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.percentile_cont.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let num_args = args.exprs.len();
        assert_eq_or_internal_err!(
            num_args,
            1,
            "median should only have 1 arg, but found num args:{}",
            num_args
        );
        let (exprs, expr_fields) = percentile_exprs_args(&args)?;
        let sub_args = AccumulatorArgs {
            exprs: &exprs,
            expr_fields: &expr_fields,
            return_field: Arc::clone(&args.return_field),
            schema: args.schema,
            ignore_nulls: args.ignore_nulls,
            order_bys: args.order_bys,
            is_reversed: args.is_reversed,
            name: args.name,
            is_distinct: args.is_distinct,
        };
        self.percentile_cont.create_groups_accumulator(sub_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
