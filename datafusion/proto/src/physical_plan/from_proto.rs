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

//! Serde code to convert from protocol buffers to Rust data structures.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::SortOptions;
use arrow::datatypes::{Field, Schema};
use arrow::ipc::reader::StreamReader;
use datafusion_common::{Result, internal_datafusion_err, not_impl_err};
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::{FunctionRegistry, TaskContext};
use datafusion_expr::WindowFunctionDefinition;
use datafusion_physical_expr::expressions::{LambdaExpr, LambdaVariable};
use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion_physical_expr::{
    HigherOrderFunctionExpr, PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion_physical_plan::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
    LikeExpr, Literal, NegativeExpr, NotExpr, SqlSimilarToPattern, TryCastExpr,
    UnKnownColumn,
};
use datafusion_physical_plan::joins::HashExpr;
use datafusion_physical_plan::proto::ExecutionPlanDecodeCtx;
use datafusion_physical_plan::repartition::RangeExpr;
use datafusion_physical_plan::windows::{create_window_expr, schema_add_window_field};
use datafusion_physical_plan::{Partitioning, PhysicalExpr, WindowExpr};
use datafusion_proto_common::common::proto_error;

use super::{
    ConverterPlanDecoder, DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalPlanDecodeContext, PhysicalProtoConverterExtension,
};
use crate::protobuf::physical_expr_node::ExprType;
use crate::{convert_required, protobuf};
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;

/// Parses a physical sort expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical sort expression node
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///   when performing type coercion.
/// * `ctx` - Decode context carrying the task context, extension codec, and
///   any scoped state needed during recursive deserialization.
/// * `proto_converter` - Converter hooks used for recursive physical plan and
///   expression deserialization.
pub fn parse_physical_sort_expr(
    proto: &protobuf::PhysicalSortExprNode,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<PhysicalSortExpr> {
    if let Some(expr) = &proto.expr {
        let expr =
            proto_converter.proto_to_physical_expr(expr.as_ref(), input_schema, ctx)?;
        let options = SortOptions {
            descending: !proto.asc,
            nulls_first: proto.nulls_first,
        };
        Ok(PhysicalSortExpr { expr, options })
    } else {
        Err(proto_error("Unexpected empty physical expression"))
    }
}

/// Parses a physical sort expressions from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with vector of physical sort expression node
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///   when performing type coercion.
/// * `ctx` - Decode context carrying the task context, extension codec, and
///   any scoped state needed during recursive deserialization.
/// * `proto_converter` - Converter hooks used for recursive physical plan and
///   expression deserialization.
pub fn parse_physical_sort_exprs(
    proto: &[protobuf::PhysicalSortExprNode],
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Vec<PhysicalSortExpr>> {
    proto
        .iter()
        .map(|sort_expr| {
            parse_physical_sort_expr(sort_expr, ctx, input_schema, proto_converter)
        })
        .collect()
}

/// Parses a physical window expr from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical window expression node.
/// * `name` - Name of the window expression.
/// * `input_schema` - The Arrow schema for the input, used for determining
///   expression data types when performing type coercion.
/// * `ctx` - Decode context carrying the task context, extension codec, and
///   any scoped state needed during recursive deserialization.
/// * `proto_converter` - Converter hooks used for recursive physical plan and
///   expression deserialization.
pub fn parse_physical_window_expr(
    proto: &protobuf::PhysicalWindowExprNode,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Arc<dyn WindowExpr>> {
    let window_node_expr =
        parse_physical_exprs(&proto.args, ctx, input_schema, proto_converter)?;
    let partition_by =
        parse_physical_exprs(&proto.partition_by, ctx, input_schema, proto_converter)?;

    let order_by =
        parse_physical_sort_exprs(&proto.order_by, ctx, input_schema, proto_converter)?;

    let window_frame = proto
        .window_frame
        .as_ref()
        .map(|wf| datafusion_expr::WindowFrame::try_from(wf.clone()))
        .transpose()
        .map_err(|e| internal_datafusion_err!("{e}"))?
        .ok_or_else(|| {
            internal_datafusion_err!("Missing required field 'window_frame' in protobuf")
        })?;

    let fun = if let Some(window_func) = proto.window_function.as_ref() {
        match window_func {
            protobuf::physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(udaf_name) => {
                WindowFunctionDefinition::AggregateUDF(match &proto.fun_definition {
                    Some(buf) => ctx.codec().try_decode_udaf(udaf_name, buf)?,
                    None => ctx
                        .task_ctx()
                        .udaf(udaf_name)
                        .or_else(|_| ctx.codec().try_decode_udaf(udaf_name, &[]))?,
                })
            }
            protobuf::physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(udwf_name) => {
                WindowFunctionDefinition::WindowUDF(match &proto.fun_definition {
                    Some(buf) => ctx.codec().try_decode_udwf(udwf_name, buf)?,
                    None => ctx
                        .task_ctx()
                        .udwf(udwf_name)
                        .or_else(|_| ctx.codec().try_decode_udwf(udwf_name, &[]))?
                })
            }
        }
    } else {
        return Err(proto_error("Missing required field in protobuf"));
    };

    let name = proto.name.clone();
    // TODO: Remove extended_schema if functions are all UDAF
    let extended_schema =
        schema_add_window_field(&window_node_expr, input_schema, &fun, &name)?;
    create_window_expr(
        &fun,
        name,
        &window_node_expr,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        extended_schema,
        proto.ignore_nulls,
        proto.distinct,
        None,
    )
}

pub fn parse_physical_exprs<'a, I>(
    protos: I,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Vec<Arc<dyn PhysicalExpr>>>
where
    I: IntoIterator<Item = &'a protobuf::PhysicalExprNode>,
{
    protos
        .into_iter()
        .map(|p| proto_converter.proto_to_physical_expr(p, input_schema, ctx))
        .collect::<Result<Vec<_>>>()
}

/// Parses a physical expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical expression node
/// * `ctx` - Task context used to resolve registered functions.
/// * `input_schema` - The Arrow schema for the input, used for determining
///   expression data types when performing type coercion.
/// * `codec` - Physical extension codec used to construct the root decode
///   context for deserialization.
pub fn parse_physical_expr(
    proto: &protobuf::PhysicalExprNode,
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let decode_ctx = PhysicalPlanDecodeContext::new(ctx, codec);
    parse_physical_expr_with_converter(
        proto,
        input_schema,
        &decode_ctx,
        &DefaultPhysicalProtoConverter {},
    )
}

/// Parses a physical expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical expression node
/// * `input_schema` - The Arrow schema for the input, used for determining
///   expression data types when performing type coercion.
/// * `ctx` - Decode context carrying the task context, extension codec, and
///   any scoped state needed during recursive deserialization.
/// * `proto_converter` - Converter hooks used for recursive physical plan and
///   expression deserialization.
pub fn parse_physical_expr_with_converter(
    proto: &protobuf::PhysicalExprNode,
    input_schema: &Schema,
    ctx: &PhysicalPlanDecodeContext<'_>,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    // Decoder context handed to per-expression `try_from_proto` constructors.
    // This is the new shape the codebase is migrating toward (see #21835);
    // the remaining `ExprType` variants stay matched inline until they migrate.
    let decoder = ConverterDecoder {
        ctx,
        proto_converter,
    };
    let decode_ctx =
        datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx::new(
            input_schema,
            &decoder,
        );

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        // Migrated expressions take the whole `PhysicalExprNode` and unwrap
        // their own `ExprType` variant — see #21835. This match only routes
        // to the right constructor.
        ExprType::Column(_) => Column::try_from_proto(proto, &decode_ctx)?,
        ExprType::UnknownColumn(_) => UnKnownColumn::try_from_proto(proto, &decode_ctx)?,
        ExprType::Literal(_) => Literal::try_from_proto(proto, &decode_ctx)?,
        ExprType::BinaryExpr(_) => BinaryExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::AggregateExpr(_) => {
            return not_impl_err!(
                "Cannot convert aggregate expr node to physical expression"
            );
        }
        ExprType::WindowExpr(_) => {
            return not_impl_err!(
                "Cannot convert window expr node to physical expression"
            );
        }
        ExprType::Sort(_) => {
            return not_impl_err!("Cannot convert sort expr node to physical expression");
        }
        ExprType::IsNullExpr(_) => IsNullExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::IsNotNullExpr(_) => IsNotNullExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::NotExpr(_) => NotExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::Negative(_) => NegativeExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::InList(_) => InListExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::Case(_) => CaseExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::Cast(_) => CastExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::TryCast(_) => TryCastExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::ScalarUdf(e) => {
            let udf = match &e.fun_definition {
                Some(buf) => ctx.codec().try_decode_udf(&e.name, buf)?,
                None => ctx
                    .task_ctx()
                    .udf(e.name.as_str())
                    .or_else(|_| ctx.codec().try_decode_udf(&e.name, &[]))?,
            };
            let scalar_fun_def = Arc::clone(&udf);

            let args = parse_physical_exprs(&e.args, ctx, input_schema, proto_converter)?;

            let config_options = Arc::clone(ctx.task_ctx().session_config().options());

            Arc::new(
                ScalarFunctionExpr::new(
                    e.name.as_str(),
                    scalar_fun_def,
                    args,
                    Field::new(
                        &e.return_field_name,
                        convert_required!(e.return_type)?,
                        true,
                    )
                    .into(),
                    config_options,
                )
                .with_nullable(e.nullable),
            )
        }
        ExprType::HigherOrderUdf(e) => {
            let func = match &e.fun_definition {
                Some(buf) => {
                    ctx.codec().try_decode_higher_order_function(&e.name, buf)?
                }
                None => ctx
                    .task_ctx()
                    .higher_order_function(e.name.as_str())
                    .or_else(|_| {
                        ctx.codec().try_decode_higher_order_function(&e.name, &[])
                    })?,
            };
            let func_def = Arc::clone(&func);

            let args = parse_physical_exprs(&e.args, ctx, input_schema, proto_converter)?;

            let config_options = Arc::clone(ctx.task_ctx().session_config().options());

            Arc::new(HigherOrderFunctionExpr::try_new_with_schema(
                func_def,
                args,
                input_schema,
                config_options,
            )?)
        }
        ExprType::LikeExpr(_) => LikeExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::HashExpr(_) => HashExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::RangeExpr(_) => RangeExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::ScalarSubquery(_) => {
            let results = ctx.scalar_subquery_results().ok_or_else(|| {
                proto_error(
                    "ScalarSubqueryExpr can only be deserialized as part \
                         of a surrounding ScalarSubqueryExec",
                )
            })?;
            ScalarSubqueryExpr::try_from_proto(proto, &decode_ctx, results)?
        }
        ExprType::DynamicFilter(_) => {
            DynamicFilterPhysicalExpr::try_from_proto(proto, &decode_ctx)?
        }
        ExprType::SqlSimilarToPattern(_) => {
            SqlSimilarToPattern::try_from_proto(proto, &decode_ctx)?
        }
        ExprType::Extension(extension) => {
            let inputs: Vec<Arc<dyn PhysicalExpr>> = extension
                .inputs
                .iter()
                .map(|e| proto_converter.proto_to_physical_expr(e, input_schema, ctx))
                .collect::<Result<_>>()?;
            ctx.codec().try_decode_expr(
                extension.expr.as_slice(),
                &inputs,
                &decode_ctx,
            )? as _
        }
        ExprType::Lambda(_) => LambdaExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::LambdaVariable(_) => {
            LambdaVariable::try_from_proto(proto, &decode_ctx)?
        }
    };

    Ok(pexpr)
}

pub fn parse_protobuf_hash_partitioning(
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Option<Partitioning>> {
    // Delegate to the shared decoder rather than keep a second copy of the hash
    // wire format: a partition count that does not fit in `usize` (a 32-bit
    // target reading a plan written on a 64-bit one) is then an error here too
    // instead of a panic.
    let hash = partitioning.map(|hash_part| protobuf::Partitioning {
        partition_method: Some(protobuf::partitioning::PartitionMethod::Hash(
            hash_part.clone(),
        )),
    });
    parse_protobuf_partitioning(hash.as_ref(), ctx, input_schema, proto_converter)
}

pub fn parse_protobuf_partitioning(
    partitioning: Option<&protobuf::Partitioning>,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Option<Partitioning>> {
    let decoder = ConverterDecoder {
        ctx,
        proto_converter,
    };
    let decode_ctx =
        datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx::new(
            input_schema,
            &decoder,
        );
    partitioning
        .map(|partitioning| Partitioning::try_from_proto(partitioning, &decode_ctx))
        .transpose()
        .map(Option::flatten)
}
#[deprecated(
    since = "55.0.0",
    note = "unused by DataFusion; use `FileScanConfig::parse_table_schema_from_proto` to reconstruct the full table schema"
)]
pub fn parse_protobuf_file_scan_schema(
    proto: &protobuf::FileScanExecConf,
) -> Result<Arc<Schema>> {
    Ok(Arc::new(convert_required!(proto.schema)?))
}

/// Parses a TableSchema from protobuf, extracting the file schema and partition columns
pub fn parse_table_schema_from_proto(
    proto: &protobuf::FileScanExecConf,
) -> Result<TableSchema> {
    FileScanConfig::parse_table_schema_from_proto(proto)
}

pub fn parse_protobuf_file_scan_config(
    proto: &protobuf::FileScanExecConf,
    ctx: &PhysicalPlanDecodeContext<'_>,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    file_source: Arc<dyn FileSource>,
) -> Result<FileScanConfig> {
    let decoder = ConverterPlanDecoder {
        ctx,
        proto_converter,
    };
    FileScanConfig::try_from_proto(
        proto,
        &ExecutionPlanDecodeCtx::new(&decoder),
        file_source,
    )
}

#[deprecated(
    since = "55.0.0",
    note = "unused by DataFusion; `MemorySourceConfig` deserializes its record batches itself via `MemorySourceConfig::try_from_proto`"
)]
pub fn parse_record_batches(buf: &[u8]) -> Result<Vec<RecordBatch>> {
    if buf.is_empty() {
        return Ok(vec![]);
    }
    let reader = StreamReader::try_new(buf, None)?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Concrete [`PhysicalExprDecode`] driver that backs
/// [`PhysicalExprDecodeCtx`] inside `parse_physical_expr_with_converter`.
///
/// Today this is a thin wrapper that re-enters the central match through
/// `proto_to_physical_expr`; once more expressions migrate, the central match
/// shrinks and a future builder-style decoder can take over.
///
/// [`PhysicalExprDecode`]: datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecode
/// [`PhysicalExprDecodeCtx`]: datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx
struct ConverterDecoder<'a, 'b> {
    ctx: &'a PhysicalPlanDecodeContext<'b>,
    proto_converter: &'a dyn PhysicalProtoConverterExtension,
}

impl datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecode
    for ConverterDecoder<'_, '_>
{
    fn decode(
        &self,
        node: &protobuf::PhysicalExprNode,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.proto_converter
            .proto_to_physical_expr(node, schema, self.ctx)
    }
}
