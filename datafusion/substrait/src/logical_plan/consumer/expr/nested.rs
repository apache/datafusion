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

use crate::logical_plan::consumer::SubstraitConsumer;
use crate::logical_plan::consumer::expr::from_substrait_rex;
use datafusion::common::{DFSchema, not_impl_err, substrait_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::Expr;
use substrait::proto::expression::Nested;
use substrait::proto::expression::nested::NestedType;

/// Convert a Substrait Nested expression (List, Struct, Map constructors) to a DataFusion Expr.
///
/// Nested::List is converted to a `make_array(...)` scalar function call.
pub async fn from_nested(
    consumer: &impl SubstraitConsumer,
    nested: &Nested,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let Some(nested_type) = nested.nested_type.as_ref() else {
        return substrait_err!("Nested expression must set nested_type");
    };

    match nested_type {
        NestedType::List(list) => {
            let mut args = Vec::with_capacity(list.values.len());
            for expr in &list.values {
                args.push(from_substrait_rex(consumer, expr, input_schema).await?);
            }

            let make_array_udf = consumer.get_function_registry().udf("make_array")?;
            Ok(Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    make_array_udf.to_owned(),
                    args,
                ),
            ))
        }
        NestedType::Struct(_) => {
            not_impl_err!("Nested Struct expression is not yet supported")
        }
        NestedType::Map(_) => {
            not_impl_err!("Nested Map expression is not yet supported")
        }
    }
}
