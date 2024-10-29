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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::fmt;
use std::sync::OnceLock;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use datafusion_common::{not_impl_err, Result};
use datafusion_expr::aggregate_doc_sections::DOC_SECTION_GENERAL;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};

make_udaf_expr_and_func!(
    Grouping,
    grouping,
    expression,
    "Returns 1 if the data is aggregated across the specified column or 0 for not aggregated in the result set.",
    grouping_udaf
);

pub struct Grouping {
    signature: Signature,
}

impl fmt::Debug for Grouping {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Grouping")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Grouping {
    fn default() -> Self {
        Self::new()
    }
}

impl Grouping {
    /// Create a new GROUPING aggregate function.
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Grouping {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "grouping"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "grouping"),
            DataType::Int32,
            true,
        )])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        not_impl_err!(
            "physical plan is not yet implemented for GROUPING aggregate function"
        )
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_grouping_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_grouping_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_GENERAL)
            .with_description(
                "Returns 1 if the data is aggregated across the specified column, or 0 if it is not aggregated in the result set.",
            )
            .with_syntax_example("grouping(expression)")
            .with_sql_example(r#"```sql
> SELECT column_name, GROUPING(column_name) AS group_column
  FROM table_name
  GROUP BY GROUPING SETS ((column_name), ());
+-------------+-------------+
| column_name | group_column |
+-------------+-------------+
| value1      | 0           |
| value2      | 0           |
| NULL        | 1           |
+-------------+-------------+
```"#, 
            )
            .with_argument("expression", "Expression to evaluate whether data is aggregated across the specified column. Can be a constant, column, or function.")
            .build()
            .unwrap()
    })
}
