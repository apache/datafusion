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
    from_substrait_func_args, from_substrait_rex_vec, from_substrait_sorts,
    substrait_fun_name, SubstraitConsumer,
};
use datafusion::common::{
    not_impl_err, plan_datafusion_err, plan_err, substrait_err, DFSchema, ScalarValue,
};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr::WindowFunctionParams;
use datafusion::logical_expr::{
    expr, Expr, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use substrait::proto::expression::window_function::{Bound, BoundsType};
use substrait::proto::expression::WindowFunction;
use substrait::proto::expression::{
    window_function::bound as SubstraitBound, window_function::bound::Kind as BoundKind,
};

pub async fn from_window_function(
    consumer: &impl SubstraitConsumer,
    window: &WindowFunction,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let Some(fn_signature) = consumer
        .get_extensions()
        .functions
        .get(&window.function_reference)
    else {
        return plan_err!(
            "Window function not found: function reference = {:?}",
            window.function_reference
        );
    };
    let fn_name = substrait_fun_name(fn_signature);

    // check udwf first, then udaf, then built-in window and aggregate functions
    let fun = if let Ok(udwf) = consumer.get_function_registry().udwf(fn_name) {
        Ok(WindowFunctionDefinition::WindowUDF(udwf))
    } else if let Ok(udaf) = consumer.get_function_registry().udaf(fn_name) {
        Ok(WindowFunctionDefinition::AggregateUDF(udaf))
    } else {
        not_impl_err!(
            "Window function {} is not supported: function anchor = {:?}",
            fn_name,
            window.function_reference
        )
    }?;

    let mut order_by =
        from_substrait_sorts(consumer, &window.sorts, input_schema).await?;

    let bound_units = match BoundsType::try_from(window.bounds_type).map_err(|e| {
        plan_datafusion_err!("Invalid bound type {}: {e}", window.bounds_type)
    })? {
        BoundsType::Rows => WindowFrameUnits::Rows,
        BoundsType::Range => WindowFrameUnits::Range,
        BoundsType::Unspecified => {
            // If the plan does not specify the bounds type, then we use a simple logic to determine the units
            // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
            // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
            if order_by.is_empty() {
                WindowFrameUnits::Rows
            } else {
                WindowFrameUnits::Range
            }
        }
    };
    let window_frame = datafusion::logical_expr::WindowFrame::new_bounds(
        bound_units,
        from_substrait_bound(&window.lower_bound, true)?,
        from_substrait_bound(&window.upper_bound, false)?,
    );

    window_frame.regularize_order_bys(&mut order_by)?;

    // Datafusion does not support aggregate functions with no arguments, so
    // we inject a dummy argument that does not affect the query, but allows
    // us to bypass this limitation.
    let args = if fun.name() == "count" && window.arguments.is_empty() {
        vec![Expr::Literal(ScalarValue::Int64(Some(1)), None)]
    } else {
        from_substrait_func_args(consumer, &window.arguments, input_schema).await?
    };

    Ok(Expr::from(expr::WindowFunction {
        fun,
        params: WindowFunctionParams {
            args,
            partition_by: from_substrait_rex_vec(
                consumer,
                &window.partitions,
                input_schema,
            )
            .await?,
            order_by,
            window_frame,
            null_treatment: None,
            distinct: false,
        },
    }))
}

fn from_substrait_bound(
    bound: &Option<Bound>,
    is_lower: bool,
) -> datafusion::common::Result<WindowFrameBound> {
    match bound {
        Some(b) => match &b.kind {
            Some(k) => match k {
                BoundKind::CurrentRow(SubstraitBound::CurrentRow {}) => {
                    Ok(WindowFrameBound::CurrentRow)
                }
                BoundKind::Preceding(SubstraitBound::Preceding { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Preceding bound must be positive");
                    }
                    Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Following(SubstraitBound::Following { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Following bound must be positive");
                    }
                    Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Unbounded(SubstraitBound::Unbounded {}) => {
                    if is_lower {
                        Ok(WindowFrameBound::Preceding(ScalarValue::Null))
                    } else {
                        Ok(WindowFrameBound::Following(ScalarValue::Null))
                    }
                }
            },
            None => substrait_err!("WindowFunction missing Substrait Bound kind"),
        },
        None => {
            if is_lower {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            } else {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
        }
    }
}
