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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::ipc::writer::StreamWriter;
use datafusion_common::{Result, internal_datafusion_err, internal_err, not_impl_err};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_expr::WindowFrame;
use datafusion_physical_expr::window::{SlidingAggregateWindowExpr, StandardWindowExpr};
use datafusion_physical_expr::{HigherOrderFunctionExpr, ScalarFunctionExpr};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_plan::proto::ExecutionPlanEncodeCtx;
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use datafusion_physical_plan::windows::{PlainAggregateWindowExpr, WindowUDFExpr};
use datafusion_physical_plan::{Partitioning, PhysicalExpr, WindowExpr};

use super::{
    ConverterPlanEncoder, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension, encode_human_display_alias,
};
use crate::protobuf::{
    self, PhysicalSortExprNode, physical_aggregate_expr_node, physical_window_expr_node,
};

#[expect(clippy::needless_pass_by_value)]
pub fn serialize_physical_aggr_expr(
    aggr_expr: Arc<AggregateFunctionExpr>,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::PhysicalExprNode> {
    let expressions =
        serialize_physical_exprs(&aggr_expr.expressions(), codec, proto_converter)?;
    let order_bys = serialize_physical_sort_exprs(
        aggr_expr.order_bys().iter().cloned(),
        codec,
        proto_converter,
    )?;

    let name = aggr_expr.fun().name().to_string();
    let mut buf = Vec::new();
    codec.try_encode_udaf(aggr_expr.fun(), &mut buf)?;
    let human_display = match (aggr_expr.human_display(), aggr_expr.human_display_alias())
    {
        (Some(display), Some(alias)) => encode_human_display_alias(display, alias),
        (Some(display), None) => display.to_string(),
        (None, _) => String::new(),
    };
    Ok(protobuf::PhysicalExprNode {
        expr_id: None,
        expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
            protobuf::PhysicalAggregateExprNode {
                aggregate_function: Some(physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(name)),
                expr: expressions,
                ordering_req: order_bys,
                distinct: aggr_expr.is_distinct(),
                ignore_nulls: aggr_expr.ignore_nulls(),
                fun_definition: (!buf.is_empty()).then_some(buf),
                human_display,
                is_reversed: aggr_expr.is_reversed(),
            },
        )),
    })
}

fn serialize_physical_window_aggr_expr(
    aggr_expr: &AggregateFunctionExpr,
    _window_frame: &WindowFrame,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(physical_window_expr_node::WindowFunction, Option<Vec<u8>>)> {
    // Distinct and ignore_nulls are now supported in window expressions

    let mut buf = Vec::new();
    codec.try_encode_udaf(aggr_expr.fun(), &mut buf)?;
    Ok((
        physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
            aggr_expr.fun().name().to_string(),
        ),
        (!buf.is_empty()).then_some(buf),
    ))
}

pub fn serialize_physical_window_expr(
    window_expr: &Arc<dyn WindowExpr>,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::PhysicalWindowExprNode> {
    let expr = window_expr.as_any();
    let mut args = window_expr.expressions().to_vec();
    let window_frame = window_expr.get_window_frame();

    let (window_function, fun_definition, ignore_nulls, distinct) =
        if let Some(plain_aggr_window_expr) =
            expr.downcast_ref::<PlainAggregateWindowExpr>()
        {
            let aggr_expr = plain_aggr_window_expr.get_aggregate_expr();
            let (window_function, fun_definition) =
                serialize_physical_window_aggr_expr(aggr_expr, window_frame, codec)?;
            (
                window_function,
                fun_definition,
                aggr_expr.ignore_nulls(),
                aggr_expr.is_distinct(),
            )
        } else if let Some(sliding_aggr_window_expr) =
            expr.downcast_ref::<SlidingAggregateWindowExpr>()
        {
            let aggr_expr = sliding_aggr_window_expr.get_aggregate_expr();
            let (window_function, fun_definition) =
                serialize_physical_window_aggr_expr(aggr_expr, window_frame, codec)?;
            (
                window_function,
                fun_definition,
                aggr_expr.ignore_nulls(),
                aggr_expr.is_distinct(),
            )
        } else if let Some(udf_window_expr) = expr.downcast_ref::<StandardWindowExpr>() {
            if let Some(expr) = udf_window_expr
                .get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
            {
                let mut buf = Vec::new();
                codec.try_encode_udwf(expr.fun(), &mut buf)?;
                args = expr.args().to_vec();
                (
                    physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(
                        expr.fun().name().to_string(),
                    ),
                    (!buf.is_empty()).then_some(buf),
                    false, // WindowUDFExpr doesn't have ignore_nulls/distinct
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

    let args = serialize_physical_exprs(&args, codec, proto_converter)?;
    let partition_by =
        serialize_physical_exprs(window_expr.partition_by(), codec, proto_converter)?;
    let order_by = serialize_physical_sort_exprs(
        window_expr.order_by().to_vec(),
        codec,
        proto_converter,
    )?;
    let window_frame = protobuf::WindowFrame::try_from(window_frame.as_ref())
        .map_err(|e| internal_datafusion_err!("{e}"))?;

    Ok(protobuf::PhysicalWindowExprNode {
        args,
        partition_by,
        order_by,
        window_frame: Some(window_frame),
        window_function: Some(window_function),
        name: window_expr.name().to_string(),
        fun_definition,
        ignore_nulls,
        distinct,
    })
}

pub fn serialize_physical_sort_exprs<I>(
    sort_exprs: I,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Vec<PhysicalSortExprNode>>
where
    I: IntoIterator<Item = PhysicalSortExpr>,
{
    sort_exprs
        .into_iter()
        .map(|sort_expr| serialize_physical_sort_expr(sort_expr, codec, proto_converter))
        .collect()
}

pub fn serialize_physical_sort_expr(
    sort_expr: PhysicalSortExpr,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<PhysicalSortExprNode> {
    let PhysicalSortExpr { expr, options } = sort_expr;
    let expr = proto_converter.physical_expr_to_proto(&expr, codec)?;
    Ok(PhysicalSortExprNode {
        expr: Some(Box::new(expr)),
        asc: !options.descending,
        nulls_first: options.nulls_first,
    })
}

pub fn serialize_physical_exprs<'a, I>(
    values: I,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Vec<protobuf::PhysicalExprNode>>
where
    I: IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
{
    values
        .into_iter()
        .map(|value| proto_converter.physical_expr_to_proto(value, codec))
        .collect()
}

/// Serialize a `PhysicalExpr` to default protobuf representation.
///
/// If required, a [`PhysicalExtensionCodec`] can be provided which can handle
/// serialization of udfs requiring specialized serialization (see [`PhysicalExtensionCodec::try_encode_udf`])
pub fn serialize_physical_expr(
    value: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::PhysicalExprNode> {
    serialize_physical_expr_with_converter(
        value,
        codec,
        &DefaultPhysicalProtoConverter {},
    )
}

/// Concrete [`PhysicalExprEncode`] driver used to back
/// [`PhysicalExprEncodeCtx`] when expressions invoke `PhysicalExpr::to_proto`.
///
/// Wraps the existing extension codec + converter pair so individual
/// expressions can recurse into children without depending on
/// `datafusion-proto` directly.
///
/// [`PhysicalExprEncode`]: datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncode
/// [`PhysicalExprEncodeCtx`]: datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx
struct ConverterEncoder<'a> {
    codec: &'a dyn PhysicalExtensionCodec,
    proto_converter: &'a dyn PhysicalProtoConverterExtension,
}

impl datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncode
    for ConverterEncoder<'_>
{
    fn encode(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<protobuf::PhysicalExprNode> {
        self.proto_converter
            .physical_expr_to_proto(expr, self.codec)
    }
}

/// Serialize a `PhysicalExpr` to default protobuf representation.
///
/// If required, a [`PhysicalExtensionCodec`] can be provided which can handle
/// serialization of udfs requiring specialized serialization (see [`PhysicalExtensionCodec::try_encode_udf`]).
/// A [`PhysicalProtoConverterExtension`] can be provided to handle the
/// conversion process (see [`PhysicalProtoConverterExtension::physical_expr_to_proto`]).
pub fn serialize_physical_expr_with_converter(
    value: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::PhysicalExprNode> {
    let expr = value.as_ref();
    let expr_id = value.expression_id();

    // Give the expression a chance to serialize itself first. Returning
    // `Ok(Some(node))` lets expressions with private state (e.g.
    // `DynamicFilterPhysicalExpr`) avoid exposing pub-for-proto accessors.
    // `Ok(None)` falls through to the downcast chain below — that's the
    // default for built-in expressions which haven't been migrated yet.
    let encoder = ConverterEncoder {
        codec,
        proto_converter,
    };
    let ctx = datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx::new(&encoder);
    if let Some(node) = expr.try_to_proto(&ctx)? {
        return Ok(node);
    }

    if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let mut buf = Vec::new();
        codec.try_encode_udf(expr.fun(), &mut buf)?;
        Ok(protobuf::PhysicalExprNode {
            expr_id,
            expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                protobuf::PhysicalScalarUdfNode {
                    name: expr.name().to_string(),
                    args: serialize_physical_exprs(expr.args(), codec, proto_converter)?,
                    fun_definition: (!buf.is_empty()).then_some(buf),
                    return_type: Some(expr.return_type().try_into()?),
                    nullable: expr.nullable(),
                    return_field_name: expr
                        .return_field(&Schema::empty())?
                        .name()
                        .to_string(),
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<HigherOrderFunctionExpr>() {
        let mut buf = Vec::new();
        codec.try_encode_higher_order_function(expr.fun(), &mut buf)?;
        Ok(protobuf::PhysicalExprNode {
            expr_id,
            expr_type: Some(protobuf::physical_expr_node::ExprType::HigherOrderUdf(
                protobuf::PhysicalHigherOrderUdfNode {
                    name: expr.name().to_string(),
                    args: serialize_physical_exprs(expr.args(), codec, proto_converter)?,
                    fun_definition: (!buf.is_empty()).then_some(buf),
                },
            )),
        })
    } else {
        let mut buf: Vec<u8> = vec![];
        match codec.try_encode_expr(value, &mut buf, &ctx) {
            Ok(_) => {
                let inputs: Vec<protobuf::PhysicalExprNode> = value
                    .children()
                    .into_iter()
                    .map(|e| proto_converter.physical_expr_to_proto(e, codec))
                    .collect::<Result<_>>()?;
                Ok(protobuf::PhysicalExprNode {
                    expr_id,
                    expr_type: Some(protobuf::physical_expr_node::ExprType::Extension(
                        protobuf::PhysicalExtensionExprNode {
                            expr: buf,
                            inputs,
                            // The codec *is* the discriminator on this path,
                            // so no name is written. Expressions that want
                            // registry decoding emit their own node through
                            // `PhysicalExprEncodeCtx::encode_extension`.
                            expr_name: None,
                        },
                    )),
                })
            }
            Err(e) => internal_err!(
                "Unsupported physical expr and extension codec failed with [{e}]. Expr: {value:?}"
            ),
        }
    }
}

pub fn serialize_partitioning(
    partitioning: &Partitioning,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::Partitioning> {
    let encoder = ConverterEncoder {
        codec,
        proto_converter,
    };
    partitioning.try_to_proto(
        &datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx::new(&encoder),
    )
}

pub fn serialize_file_scan_config(
    conf: &FileScanConfig,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::FileScanExecConf> {
    let encoder = ConverterPlanEncoder {
        codec,
        proto_converter,
    };
    conf.try_to_proto(&ExecutionPlanEncodeCtx::new(&encoder))
}

pub fn serialize_maybe_filter(
    expr: Option<Arc<dyn PhysicalExpr>>,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::MaybeFilter> {
    match expr {
        None => Ok(protobuf::MaybeFilter { expr: None }),
        Some(expr) => Ok(protobuf::MaybeFilter {
            expr: Some(proto_converter.physical_expr_to_proto(&expr, codec)?),
        }),
    }
}

#[deprecated(
    since = "55.0.0",
    note = "unused by DataFusion; `MemorySourceConfig` serializes its record batches itself via `DataSource::try_to_proto`"
)]
pub fn serialize_record_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Ok(vec![]);
    }
    let schema = batches[0].schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(buf)
}
