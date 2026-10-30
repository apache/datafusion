// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::{DFSchema, not_impl_err};
use datafusion::logical_expr::Expr;
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
