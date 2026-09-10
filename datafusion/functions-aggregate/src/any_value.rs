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

//! Defines the ANY_VALUE aggregation.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, not_impl_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_macros::user_doc;

use crate::first_last::TrivialFirstValueAccumulator;

make_udaf_expr_and_func!(
    AnyValue,
    any_value,
    expression,
    "Returns an arbitrary non-null value",
    any_value_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns an arbitrary non-null value from a group, or NULL if the group contains only NULL values.",
    syntax_example = "any_value(expression)",
    sql_example = r#"```sql
> SELECT any_value(column_name) FROM table_name;
+------------------------+
| any_value(column_name) |
+------------------------+
| arbitrary_value        |
+------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct AnyValue {
    signature: Signature,
}

impl Default for AnyValue {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AnyValue {
    fn name(&self) -> &str {
        "any_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        not_impl_err!("Not called because return_field is implemented")
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        Ok(Arc::new(
            Field::new(self.name(), arg_fields[0].data_type().clone(), true)
                .with_metadata(arg_fields[0].metadata().clone()),
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        TrivialFirstValueAccumulator::try_new(acc_args.return_field.data_type(), true)
            .map(|acc| Box::new(acc) as _)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new(
                format_state_name(args.name, "any_value"),
                args.return_type().clone(),
                true,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "any_value_is_set"),
                DataType::Boolean,
                true,
            )
            .into(),
        ])
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
