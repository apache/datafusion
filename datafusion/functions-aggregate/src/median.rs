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

use arrow::datatypes::DataType;

use arrow::datatypes::FieldRef;

use crate::percentile_cont::{
    PercentileCont, create_percentile_accumulator, create_percentile_groups_accumulator,
};
use datafusion_common::Result;
use datafusion_common::types::logical_float64;
use datafusion_expr::GroupsAccumulator;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Coercion, Documentation, Signature, TypeSignature,
    TypeSignatureClass, Volatility, function::AccumulatorArgs,
};
use datafusion_macros::user_doc;

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
| median(column_name)  |
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
                    TypeSignature::Coercible(vec![Coercion::new_implicit_native(
                        logical_float64(),
                        vec![TypeSignatureClass::Integer],
                    )]),
                ],
                Volatility::Immutable,
            ),
            percentile_cont: PercentileCont::new(),
        }
    }
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
        create_percentile_accumulator(
            self.name(),
            0.5,
            args.expr_fields[0].data_type(),
            args.is_distinct,
        )
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.percentile_cont.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        create_percentile_groups_accumulator(
            self.name(),
            0.5,
            args.expr_fields[0].data_type(),
        )
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
