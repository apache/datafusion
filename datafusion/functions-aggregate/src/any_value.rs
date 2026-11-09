// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Defines the ANY_VALUE aggregation.

use std::fmt::Debug;
use std::hash::Hash;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
    udaf_return_field_preserving_arg_metadata,
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        udaf_return_field_preserving_arg_metadata(self, arg_fields)
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

    // TODO: this is arguably `DistinctHandling::Insensitive` — the accumulator
    // ignores `is_distinct` and returns an unspecified input value either
    // way. Grouped with `first_value`/`last_value` and left at the default
    // `Sensitive` until that family is settled together.
}
