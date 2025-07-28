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

use crate::logical_plan::consumer::{
    from_substrait_func_args, substrait_fun_name, SubstraitConsumer,
};
use datafusion::common::{not_impl_datafusion_err, plan_err, DFSchema, ScalarValue};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{expr, Expr, SortExpr};
use std::sync::Arc;
use substrait::proto::AggregateFunction;

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    consumer: &impl SubstraitConsumer,
    f: &AggregateFunction,
    input_schema: &DFSchema,
    filter: Option<Box<Expr>>,
    order_by: Vec<SortExpr>,
    distinct: bool,
) -> datafusion::common::Result<Arc<Expr>> {
    let Some(fn_signature) = consumer
        .get_extensions()
        .functions
        .get(&f.function_reference)
    else {
        return plan_err!(
            "Aggregate function not registered: function anchor = {:?}",
            f.function_reference
        );
    };

    let fn_name = substrait_fun_name(fn_signature);
    let udaf = consumer.get_function_registry().udaf(fn_name);
    let udaf = udaf.map_err(|_| {
        not_impl_datafusion_err!(
            "Aggregate function {} is not supported: function anchor = {:?}",
            fn_signature,
            f.function_reference
        )
    })?;

    let args = from_substrait_func_args(consumer, &f.arguments, input_schema).await?;

    // Datafusion does not support aggregate functions with no arguments, so
    // we inject a dummy argument that does not affect the query, but allows
    // us to bypass this limitation.
    let args = if udaf.name() == "count" && args.is_empty() {
        vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)]
    } else {
        args
    };

    Ok(Arc::new(Expr::AggregateFunction(
        expr::AggregateFunction::new_udf(udaf, args, distinct, filter, order_by, None),
    )))
}
