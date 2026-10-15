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
use datafusion::arrow::datatypes::Field;
use datafusion::common::{DFSchema, not_impl_err};
use datafusion::logical_expr::type_coercion::functions::UDFCoercionExt;
use datafusion::logical_expr::{Expr, ExprSchemable};
use std::sync::Arc;
use substrait::proto::FunctionArgument;
use substrait::proto::function_argument::ArgType;

/// Convert Substrait FunctionArguments to DataFusion Exprs
pub async fn from_substrait_func_args(
    consumer: &impl SubstraitConsumer,
    arguments: &Vec<FunctionArgument>,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Vec<Expr>> {
    let mut args: Vec<Expr> = Vec::with_capacity(arguments.len());
    for arg in arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => consumer.consume_expression(e, input_schema).await,
            _ => not_impl_err!("Function argument non-Value type not supported"),
        };
        args.push(arg_expr?);
    }
    Ok(args)
}

/// Identifies casts in function arguments that correspond to DataFusion's type
/// coercion rules and restores their `is_implicit` metadata to `true`.
///
/// Substrait expression serialization does not track engine-specific `is_implicit` flags
/// on cast expressions. When expressions are reconstructed by the Substrait consumer,
/// casts default to `is_implicit: false`. For function arguments where type coercion inserted
/// an implicit cast (e.g. `sum(UInt32)` coerced to `UInt64`), restoring `is_implicit: true`
/// ensures schema names and plan formatting match the original DataFusion plan without
/// injecting spurious projection aliases.
///
/// Note: If a user explicitly authored a cast whose target type coincides with the coercion
/// target (e.g. `avg(CAST(a AS DOUBLE))`), this heuristic initially marks it implicit.
/// The consumer's top-level schema reconciliation in `rename_expressions` uses `RelRoot.names`
/// to distinguish explicitly authored casts and restore their `is_implicit: false` flag.
pub(crate) fn mark_implicit_coercion_casts<F: UDFCoercionExt>(
    func: &F,
    args: &mut [Expr],
    input_schema: &DFSchema,
) -> datafusion::common::Result<()> {
    if !args
        .iter()
        .any(|arg| matches!(arg, Expr::Cast(_) | Expr::TryCast(_)))
    {
        return Ok(());
    }

    let mut uncoerced_fields = Vec::with_capacity(args.len());
    for arg in args.iter() {
        match arg {
            Expr::Cast(c) => {
                uncoerced_fields.push(Arc::new(Field::new(
                    "",
                    c.expr.get_type(input_schema)?,
                    c.field.is_nullable(),
                )));
            }
            Expr::TryCast(tc) => {
                uncoerced_fields.push(Arc::new(Field::new(
                    "",
                    tc.expr.get_type(input_schema)?,
                    tc.field.is_nullable(),
                )));
            }
            other => {
                uncoerced_fields.push(Arc::new(Field::new(
                    "",
                    other.get_type(input_schema)?,
                    other.nullable(input_schema)?,
                )));
            }
        }
    }

    if let Ok(coerced_fields) =
        datafusion::logical_expr::type_coercion::functions::fields_with_udf(
            &uncoerced_fields,
            func,
        )
    {
        if coerced_fields.len() == args.len() {
            for (arg, coerced_field) in args.iter_mut().zip(coerced_fields.iter()) {
                match arg {
                    Expr::Cast(c) => {
                        let inner_type = c.expr.get_type(input_schema)?;
                        if c.field.data_type() == coerced_field.data_type()
                            && &inner_type != c.field.data_type()
                        {
                            c.is_implicit = true;
                        }
                    }
                    Expr::TryCast(tc) => {
                        let inner_type = tc.expr.get_type(input_schema)?;
                        if tc.field.data_type() == coerced_field.data_type()
                            && &inner_type != tc.field.data_type()
                        {
                            tc.is_implicit = true;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
