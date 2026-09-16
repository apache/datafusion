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

//! Protobuf conversions shared by window execution plans.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{
    Result, ScalarValue, internal_datafusion_err, internal_err, not_impl_err,
};
use datafusion_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_physical_expr::window::SlidingAggregateWindowExpr;
use datafusion_physical_expr_common::sort_expr::{
    sort_exprs_try_from_proto, sort_exprs_try_to_proto,
};
use datafusion_proto_common::protobuf_common;
use datafusion_proto_models::protobuf::{self, physical_window_expr_node};

use super::{
    PlainAggregateWindowExpr, StandardWindowExpr, WindowExpr, WindowUDFExpr,
    create_window_expr, schema_add_window_field,
};

pub(super) fn encode_physical_window_expr(
    window_expr: &Arc<dyn WindowExpr>,
    ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
) -> Result<protobuf::PhysicalWindowExprNode> {
    let expr = window_expr.as_any();
    let mut args = window_expr.expressions().to_vec();
    let window_frame = window_expr.get_window_frame();
    let (window_function, fun_definition, ignore_nulls, distinct) =
        if let Some(plain) = expr.downcast_ref::<PlainAggregateWindowExpr>() {
            let aggregate_expr = plain.get_aggregate_expr();
            (
                physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
                    aggregate_expr.fun().name().to_string(),
                ),
                ctx.encode_udaf(aggregate_expr.fun())?,
                aggregate_expr.ignore_nulls(),
                aggregate_expr.is_distinct(),
            )
        } else if let Some(sliding) = expr.downcast_ref::<SlidingAggregateWindowExpr>() {
            let aggregate_expr = sliding.get_aggregate_expr();
            (
                physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
                    aggregate_expr.fun().name().to_string(),
                ),
                ctx.encode_udaf(aggregate_expr.fun())?,
                aggregate_expr.ignore_nulls(),
                aggregate_expr.is_distinct(),
            )
        } else if let Some(standard) = expr.downcast_ref::<StandardWindowExpr>() {
            if let Some(window_udf) = standard
                .get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
            {
                // `WindowUDFExpr::args` returns the full, unfiltered argument list so
                // every argument survives the round-trip.
                args = window_udf.args().to_vec();
                (
                    physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(
                        window_udf.fun().name().to_string(),
                    ),
                    ctx.encode_udwf(window_udf.fun().as_ref())?,
                    false,
                    false,
                )
            } else {
                return not_impl_err!(
                    "User-defined window function not supported: {window_expr:?}"
                );
            }
        } else {
            return not_impl_err!("WindowExpr not supported: {window_expr:?}");
        };

    let args = ctx.encode_expressions(&args)?;
    let partition_by = ctx.encode_expressions(window_expr.partition_by())?;
    let order_by = sort_exprs_try_to_proto(window_expr.order_by(), &ctx.expr_ctx())?;

    Ok(protobuf::PhysicalWindowExprNode {
        args,
        partition_by,
        order_by,
        window_frame: Some(encode_window_frame(window_frame.as_ref())?),
        window_function: Some(window_function),
        name: window_expr.name().to_string(),
        fun_definition,
        ignore_nulls,
        distinct,
    })
}

pub(super) fn decode_physical_window_expr(
    proto: &protobuf::PhysicalWindowExprNode,
    ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    let args = proto
        .args
        .iter()
        .map(|expr| ctx.decode_expr(expr, input_schema))
        .collect::<Result<Vec<_>>>()?;
    let partition_by = proto
        .partition_by
        .iter()
        .map(|expr| ctx.decode_expr(expr, input_schema))
        .collect::<Result<Vec<_>>>()?;
    let order_by =
        sort_exprs_try_from_proto(&proto.order_by, &ctx.expr_ctx(input_schema))?;
    let window_frame = proto
        .window_frame
        .as_ref()
        .map(decode_window_frame)
        .transpose()?
        .ok_or_else(|| {
            internal_datafusion_err!("Missing required field 'window_frame' in protobuf")
        })?;
    let function = match proto.window_function.as_ref() {
        Some(physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
            name,
        )) => WindowFunctionDefinition::AggregateUDF(
            ctx.decode_udaf(name, proto.fun_definition.as_deref())?,
        ),
        Some(physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(
            name,
        )) => WindowFunctionDefinition::WindowUDF(
            ctx.decode_udwf(name, proto.fun_definition.as_deref())?,
        ),
        None => {
            return internal_err!("Missing required field 'window_function' in protobuf");
        }
    };

    let name = proto.name.clone();
    // TODO: Remove extended_schema if functions are all UDAF
    let extended_schema = schema_add_window_field(&args, input_schema, &function, &name)?;
    create_window_expr(
        &function,
        name,
        &args,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        extended_schema,
        proto.ignore_nulls,
        proto.distinct,
        None,
    )
}

fn encode_window_frame(window_frame: &WindowFrame) -> Result<protobuf::WindowFrame> {
    let units = match window_frame.units {
        WindowFrameUnits::Rows => protobuf::WindowFrameUnits::Rows,
        WindowFrameUnits::Range => protobuf::WindowFrameUnits::Range,
        WindowFrameUnits::Groups => protobuf::WindowFrameUnits::Groups,
    };
    Ok(protobuf::WindowFrame {
        window_frame_units: units.into(),
        start_bound: Some(encode_window_frame_bound(&window_frame.start_bound)?),
        end_bound: Some(protobuf::window_frame::EndBound::Bound(
            encode_window_frame_bound(&window_frame.end_bound)?,
        )),
    })
}

fn encode_window_frame_bound(
    bound: &WindowFrameBound,
) -> Result<protobuf::WindowFrameBound> {
    let encode_value = |value: &ScalarValue| -> Result<protobuf_common::ScalarValue> {
        Ok(value.try_into()?)
    };
    Ok(match bound {
        WindowFrameBound::CurrentRow => protobuf::WindowFrameBound {
            window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow.into(),
            bound_value: None,
        },
        WindowFrameBound::Preceding(value) => protobuf::WindowFrameBound {
            window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
            bound_value: Some(encode_value(value)?),
        },
        WindowFrameBound::Following(value) => protobuf::WindowFrameBound {
            window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
            bound_value: Some(encode_value(value)?),
        },
    })
}

fn decode_window_frame(window_frame: &protobuf::WindowFrame) -> Result<WindowFrame> {
    let units = protobuf::WindowFrameUnits::try_from(window_frame.window_frame_units)
        .map_err(|_| {
            internal_datafusion_err!(
                "Received a WindowFrame message with unknown WindowFrameUnits {}",
                window_frame.window_frame_units
            )
        })?;
    let units = match units {
        protobuf::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
        protobuf::WindowFrameUnits::Range => WindowFrameUnits::Range,
        protobuf::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
    };
    let start_bound =
        decode_window_frame_bound(window_frame.start_bound.as_ref().ok_or_else(
            || internal_datafusion_err!("Missing start_bound in WindowFrame"),
        )?)?;
    let end_bound = window_frame
        .end_bound
        .as_ref()
        .map(|end_bound| match end_bound {
            protobuf::window_frame::EndBound::Bound(bound) => {
                decode_window_frame_bound(bound)
            }
        })
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);
    Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
}

fn decode_window_frame_bound(
    bound: &protobuf::WindowFrameBound,
) -> Result<WindowFrameBound> {
    let decode_value = |value: &protobuf_common::ScalarValue| -> Result<ScalarValue> {
        Ok(ScalarValue::try_from(value)?)
    };
    let bound_type = protobuf::WindowFrameBoundType::try_from(
        bound.window_frame_bound_type,
    )
    .map_err(|_| {
        internal_datafusion_err!(
            "Received a WindowFrameBound message with unknown WindowFrameBoundType {}",
            bound.window_frame_bound_type
        )
    })?;
    match bound_type {
        protobuf::WindowFrameBoundType::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        protobuf::WindowFrameBoundType::Preceding => match &bound.bound_value {
            Some(value) => Ok(WindowFrameBound::Preceding(decode_value(value)?)),
            None => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None))),
        },
        protobuf::WindowFrameBoundType::Following => match &bound.bound_value {
            Some(value) => Ok(WindowFrameBound::Following(decode_value(value)?)),
            None => Ok(WindowFrameBound::Following(ScalarValue::UInt64(None))),
        },
    }
}
