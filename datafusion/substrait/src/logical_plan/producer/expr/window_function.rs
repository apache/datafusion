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

use crate::logical_plan::producer::SubstraitProducer;
use crate::logical_plan::producer::utils::substrait_sort_field;
use datafusion::common::{DFSchemaRef, ScalarValue, not_impl_err};
use datafusion::logical_expr::expr::{WindowFunction, WindowFunctionParams};
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use substrait::proto::aggregate_function::AggregationInvocation;
use substrait::proto::expression::RexType;
use substrait::proto::expression::WindowFunction as SubstraitWindowFunction;
use substrait::proto::expression::window_function::bound as SubstraitBound;
use substrait::proto::expression::window_function::bound::Kind as BoundKind;
use substrait::proto::expression::window_function::{Bound, BoundsType};
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument, SortField};

pub fn from_window_function(
    producer: &mut impl SubstraitProducer,
    window_fn: &WindowFunction,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let WindowFunction {
        fun,
        params:
            WindowFunctionParams {
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
                distinct,
                filter,
            },
    } = window_fn;
    if let Some(null_treatment) = null_treatment {
        return not_impl_err!(
            "Window functions with {null_treatment} are not supported in Substrait"
        );
    }
    if filter.is_some() {
        return not_impl_err!(
            "Window functions with FILTER are not supported in Substrait"
        );
    }
    // function reference
    let function_anchor = producer.register_function(fun.to_string());
    // arguments
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }
    // partition by expressions
    let partition_by = partition_by
        .iter()
        .map(|e| producer.handle_expr(e, schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    // order by expressions
    let order_by = order_by
        .iter()
        .map(|e| substrait_sort_field(producer, e, schema))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    // window frame
    let bounds = to_substrait_bounds(window_frame)?;
    let bound_type = to_substrait_bound_type(window_frame)?;
    Ok(make_substrait_window_function(
        function_anchor,
        arguments,
        partition_by,
        order_by,
        bounds,
        bound_type,
        *distinct,
    ))
}

fn make_substrait_window_function(
    function_reference: u32,
    arguments: Vec<FunctionArgument>,
    partitions: Vec<Expression>,
    sorts: Vec<SortField>,
    bounds: (Bound, Bound),
    bounds_type: BoundsType,
    distinct: bool,
) -> Expression {
    #[expect(deprecated)]
    Expression {
        rex_type: Some(RexType::WindowFunction(SubstraitWindowFunction {
            function_reference,
            arguments,
            partitions,
            sorts,
            options: vec![],
            output_type: None,
            phase: 0, // default to AGGREGATION_PHASE_UNSPECIFIED
            invocation: if distinct {
                AggregationInvocation::Distinct as i32
            } else {
                AggregationInvocation::All as i32
            },
            lower_bound: Some(bounds.0),
            upper_bound: Some(bounds.1),
            args: vec![],
            bounds_type: bounds_type as i32,
        })),
    }
}

fn to_substrait_bound_type(
    window_frame: &WindowFrame,
) -> datafusion::common::Result<BoundsType> {
    match window_frame.units {
        WindowFrameUnits::Rows => Ok(BoundsType::Rows), // ROWS
        WindowFrameUnits::Range => Ok(BoundsType::Range), // RANGE
        // TODO: Support GROUPS
        unit => not_impl_err!("Unsupported window frame unit: {unit:?}"),
    }
}

fn to_substrait_bounds(
    window_frame: &WindowFrame,
) -> datafusion::common::Result<(Bound, Bound)> {
    Ok((
        to_substrait_bound(&window_frame.start_bound)?,
        to_substrait_bound(&window_frame.end_bound)?,
    ))
}

fn to_substrait_bound(bound: &WindowFrameBound) -> datafusion::common::Result<Bound> {
    if bound.is_unbounded() {
        return Ok(Bound {
            kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
        });
    }
    match bound {
        WindowFrameBound::CurrentRow => Ok(Bound {
            kind: Some(BoundKind::CurrentRow(SubstraitBound::CurrentRow {})),
        }),
        WindowFrameBound::Preceding(s) => Ok(Bound {
            kind: Some(BoundKind::Preceding(SubstraitBound::Preceding {
                offset: to_substrait_bound_offset(s)?,
            })),
        }),
        WindowFrameBound::Following(s) => Ok(Bound {
            kind: Some(BoundKind::Following(SubstraitBound::Following {
                offset: to_substrait_bound_offset(s)?,
            })),
        }),
    }
}

fn to_substrait_bound_offset(value: &ScalarValue) -> datafusion::common::Result<i64> {
    match value {
        ScalarValue::UInt8(Some(v)) => Ok((*v).into()),
        ScalarValue::UInt16(Some(v)) => Ok((*v).into()),
        ScalarValue::UInt32(Some(v)) => Ok((*v).into()),
        ScalarValue::UInt64(Some(v)) => match i64::try_from(*v) {
            Ok(v) => Ok(v),
            Err(_) => not_impl_err!("Unsupported Substrait window frame offset: {value}"),
        },
        ScalarValue::Int8(Some(v)) => Ok((*v).into()),
        ScalarValue::Int16(Some(v)) => Ok((*v).into()),
        ScalarValue::Int32(Some(v)) => Ok((*v).into()),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        _ => not_impl_err!("Unsupported Substrait window frame offset: {value}"),
    }
}
