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
use chrono::{TimeZone, Utc};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, internal_datafusion_err, not_impl_err,
};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::file_sink_config::FileSinkConfig;
use datafusion_datasource::{FileRange, ListingTableUrl, PartitionedFile, TableSchema};
use datafusion_datasource_csv::file_format::CsvSink;
use datafusion_datasource_json::file_format::JsonSink;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::file_format::ParquetSink;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{FunctionRegistry, TaskContext};
use datafusion_expr::WindowFunctionDefinition;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::execution_props::SubqueryIndex;
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion_physical_plan::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
    LikeExpr, Literal, NegativeExpr, NotExpr, TryCastExpr, UnKnownColumn,
};
use datafusion_physical_plan::joins::HashExpr;
use datafusion_physical_plan::windows::{create_window_expr, schema_add_window_field};
use datafusion_physical_plan::{
    Partitioning, PhysicalExpr, RangePartitioning, SplitPoint, WindowExpr,
};
use datafusion_proto_common::common::proto_error;
use object_store::ObjectMeta;
use object_store::path::Path;

use super::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};
use crate::convert::TryFromProto;
use crate::protobuf::physical_expr_node::ExprType;
use crate::{convert_required, convert_required_proto, protobuf};
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
        .map(|wf| datafusion_expr::WindowFrame::try_from_proto(wf.clone()))
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
        ExprType::LikeExpr(_) => LikeExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::HashExpr(_) => HashExpr::try_from_proto(proto, &decode_ctx)?,
        ExprType::ScalarSubquery(sq) => {
            let data_type: arrow::datatypes::DataType = sq
                .data_type
                .as_ref()
                .ok_or_else(|| {
                    proto_error("Missing data_type in PhysicalScalarSubqueryExprNode")
                })?
                .try_into()?;
            let results = ctx.scalar_subquery_results().ok_or_else(|| {
                proto_error(
                    "ScalarSubqueryExpr can only be deserialized as part \
                         of a surrounding ScalarSubqueryExec",
                )
            })?;
            Arc::new(ScalarSubqueryExpr::new(
                data_type,
                sq.nullable,
                SubqueryIndex::new(sq.index as usize),
                results.clone(),
            ))
        }
        ExprType::DynamicFilter(_) => {
            DynamicFilterPhysicalExpr::try_from_proto(proto, &decode_ctx)?
        }
        ExprType::Extension(extension) => {
            let inputs: Vec<Arc<dyn PhysicalExpr>> = extension
                .inputs
                .iter()
                .map(|e| proto_converter.proto_to_physical_expr(e, input_schema, ctx))
                .collect::<Result<_>>()?;
            ctx.codec()
                .try_decode_expr(extension.expr.as_slice(), &inputs)? as _
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
    match partitioning {
        Some(hash_part) => {
            let expr = parse_physical_exprs(
                &hash_part.hash_expr,
                ctx,
                input_schema,
                proto_converter,
            )?;

            Ok(Some(Partitioning::Hash(
                expr,
                hash_part.partition_count.try_into().unwrap(),
            )))
        }
        None => Ok(None),
    }
}

pub fn parse_protobuf_partitioning(
    partitioning: Option<&protobuf::Partitioning>,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Option<Partitioning>> {
    match partitioning {
        Some(protobuf::Partitioning { partition_method }) => match partition_method {
            Some(protobuf::partitioning::PartitionMethod::RoundRobin(
                partition_count,
            )) => Ok(Some(Partitioning::RoundRobinBatch(
                *partition_count as usize,
            ))),
            Some(protobuf::partitioning::PartitionMethod::Hash(hash_repartition)) => {
                parse_protobuf_hash_partitioning(
                    Some(hash_repartition),
                    ctx,
                    input_schema,
                    proto_converter,
                )
            }
            Some(protobuf::partitioning::PartitionMethod::Range(range_partitioning)) => {
                Ok(Some(parse_protobuf_range_partitioning(
                    range_partitioning,
                    ctx,
                    input_schema,
                    proto_converter,
                )?))
            }
            Some(protobuf::partitioning::PartitionMethod::Unknown(partition_count)) => {
                Ok(Some(Partitioning::UnknownPartitioning(
                    *partition_count as usize,
                )))
            }
            None => Ok(None),
        },
        None => Ok(None),
    }
}

fn parse_protobuf_range_partitioning(
    range_partitioning: &protobuf::PhysicalRangePartitioning,
    ctx: &PhysicalPlanDecodeContext<'_>,
    input_schema: &Schema,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<Partitioning> {
    let sort_exprs = parse_physical_sort_exprs(
        &range_partitioning.sort_expr,
        ctx,
        input_schema,
        proto_converter,
    )?;
    let sort_expr_count = sort_exprs.len();
    let ordering = LexOrdering::new(sort_exprs).ok_or_else(|| {
        internal_datafusion_err!("Range partitioning requires non-empty ordering")
    })?;
    if ordering.len() != sort_expr_count {
        return Err(internal_datafusion_err!(
            "Range partitioning ordering must not contain duplicate expressions"
        ));
    }
    let split_points = range_partitioning
        .split_point
        .iter()
        .map(parse_protobuf_range_split_point)
        .collect::<Result<_>>()?;
    Ok(Partitioning::Range(RangePartitioning::try_new(
        ordering,
        split_points,
    )?))
}

fn parse_protobuf_range_split_point(
    split_point: &protobuf::PhysicalRangeSplitPoint,
) -> Result<SplitPoint> {
    let values = split_point
        .value
        .iter()
        .map(|value| ScalarValue::try_from(value).map_err(Into::into))
        .collect::<Result<_>>()?;
    Ok(SplitPoint::new(values))
}

pub fn parse_protobuf_file_scan_schema(
    proto: &protobuf::FileScanExecConf,
) -> Result<Arc<Schema>> {
    Ok(Arc::new(convert_required!(proto.schema)?))
}

/// Parses a TableSchema from protobuf, extracting the file schema and partition columns
pub fn parse_table_schema_from_proto(
    proto: &protobuf::FileScanExecConf,
) -> Result<TableSchema> {
    let schema: Arc<Schema> = parse_protobuf_file_scan_schema(proto)?;

    // Reacquire the partition column types from the schema before removing them below.
    let table_partition_cols = proto
        .table_partition_cols
        .iter()
        .map(|col| Ok(Arc::new(schema.field_with_name(col)?.clone())))
        .collect::<Result<Vec<_>>>()?;

    // Remove partition columns from the schema after recreating table_partition_cols
    // because the partition columns are not in the file. They are present to allow
    // the partition column types to be reconstructed after serde.
    let file_schema = Arc::new(
        Schema::new(
            schema
                .fields()
                .iter()
                .filter(|field| !table_partition_cols.contains(field))
                .cloned()
                .collect::<Vec<_>>(),
        )
        .with_metadata(schema.metadata.clone()),
    );

    Ok(TableSchema::builder(file_schema)
        .with_table_partition_cols(table_partition_cols)
        .build())
}

pub fn parse_protobuf_file_scan_config(
    proto: &protobuf::FileScanExecConf,
    ctx: &PhysicalPlanDecodeContext<'_>,
    proto_converter: &dyn PhysicalProtoConverterExtension,
    file_source: Arc<dyn FileSource>,
) -> Result<FileScanConfig> {
    let schema: Arc<Schema> = parse_protobuf_file_scan_schema(proto)?;

    let constraints = convert_required!(proto.constraints)?;
    let statistics = convert_required!(proto.statistics)?;

    let file_groups = proto
        .file_groups
        .iter()
        .map(FileGroup::try_from_proto)
        .collect::<Result<Vec<_>, _>>()?;

    let object_store_url = match proto.object_store_url.is_empty() {
        false => ObjectStoreUrl::parse(&proto.object_store_url)?,
        true => ObjectStoreUrl::local_filesystem(),
    };

    let mut output_ordering = vec![];
    for node_collection in &proto.output_ordering {
        let sort_exprs = parse_physical_sort_exprs(
            &node_collection.physical_sort_expr_nodes,
            ctx,
            &schema,
            proto_converter,
        )?;
        output_ordering.extend(LexOrdering::new(sort_exprs));
    }
    let output_partitioning = parse_protobuf_partitioning(
        proto.output_partitioning.as_ref(),
        ctx,
        &schema,
        proto_converter,
    )?;

    // Parse projection expressions if present and apply to file source
    let file_source = if let Some(proto_projection_exprs) = &proto.projection_exprs {
        let projection_exprs: Vec<ProjectionExpr> = proto_projection_exprs
            .projections
            .iter()
            .map(|proto_expr| {
                let expr = proto_converter.proto_to_physical_expr(
                    proto_expr.expr.as_ref().ok_or_else(|| {
                        internal_datafusion_err!("ProjectionExpr missing expr field")
                    })?,
                    &schema,
                    ctx,
                )?;
                Ok(ProjectionExpr::new(expr, proto_expr.alias.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        let projection_exprs = ProjectionExprs::new(projection_exprs);

        // Apply projection to file source
        file_source
            .try_pushdown_projection(&projection_exprs)?
            .unwrap_or(file_source)
    } else {
        file_source
    };

    let mut config_builder = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(file_groups)
        .with_constraints(constraints)
        .with_statistics(statistics)
        .with_limit(proto.limit.as_ref().map(|sl| sl.limit as usize))
        .with_output_ordering(output_ordering)
        .with_output_partitioning(output_partitioning)
        .with_batch_size(proto.batch_size.map(|s| s as usize));
    if proto.partitioned_by_file_group.unwrap_or(false) {
        config_builder = config_builder.with_partitioned_by_file_group(true);
    }
    let config = config_builder.build();
    Ok(config)
}

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

impl TryFromProto<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = DataFusionError;

    fn try_from_proto(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        let mut pf = PartitionedFile::new_from_meta(ObjectMeta {
            location: Path::parse(val.path.as_str())
                .map_err(|e| proto_error(format!("Invalid object_store path: {e}")))?,
            last_modified: Utc.timestamp_nanos(val.last_modified_ns as i64),
            size: val.size,
            e_tag: None,
            version: None,
        })
        .with_partition_values(
            val.partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        );
        if let Some(proto_schema) = val.arrow_schema.as_ref() {
            pf = pf.with_arrow_schema(Arc::new(
                proto_schema.try_into().map_err(DataFusionError::from)?,
            ));
        }
        if let Some(range) = val.range.as_ref() {
            let file_range = FileRange::try_from_proto(range)?;
            pf = pf.with_range(file_range.start, file_range.end);
        }
        if let Some(proto_stats) = val.statistics.as_ref() {
            pf = pf.with_statistics(Arc::new(proto_stats.try_into()?));
        }
        Ok(pf)
    }
}

impl TryFromProto<&protobuf::FileRange> for FileRange {
    type Error = DataFusionError;

    fn try_from_proto(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFromProto<&protobuf::FileGroup> for FileGroup {
    type Error = DataFusionError;

    fn try_from_proto(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        let files = val
            .files
            .iter()
            .map(PartitionedFile::try_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileGroup::new(files))
    }
}

impl TryFromProto<&protobuf::JsonSink> for JsonSink {
    type Error = DataFusionError;

    fn try_from_proto(value: &protobuf::JsonSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required_proto!(FileSinkConfig, value.config)?,
            convert_required!(value.writer_options)?,
        ))
    }
}

#[cfg(feature = "parquet")]
impl TryFromProto<&protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from_proto(value: &protobuf::ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required_proto!(FileSinkConfig, value.config)?,
            convert_required!(value.parquet_options)?,
        ))
    }
}

impl TryFromProto<&protobuf::CsvSink> for CsvSink {
    type Error = DataFusionError;

    fn try_from_proto(value: &protobuf::CsvSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required_proto!(FileSinkConfig, value.config)?,
            convert_required!(value.writer_options)?,
        ))
    }
}

impl TryFromProto<&protobuf::FileSinkConfig> for FileSinkConfig {
    type Error = DataFusionError;

    fn try_from_proto(conf: &protobuf::FileSinkConfig) -> Result<Self, Self::Error> {
        let file_group = FileGroup::new(
            conf.file_groups
                .iter()
                .map(PartitionedFile::try_from_proto)
                .collect::<Result<Vec<_>>>()?,
        );
        let table_paths = conf
            .table_paths
            .iter()
            .map(ListingTableUrl::parse)
            .collect::<Result<Vec<_>>>()?;
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|protobuf::PartitionColumn { name, arrow_type }| {
                let data_type = convert_required!(arrow_type)?;
                Ok((name.clone(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;
        let insert_op = match conf.insert_op() {
            protobuf::InsertOp::Append => InsertOp::Append,
            protobuf::InsertOp::Overwrite => InsertOp::Overwrite,
            protobuf::InsertOp::Replace => InsertOp::Replace,
        };
        let file_output_mode = match conf.file_output_mode() {
            protobuf::FileOutputMode::Automatic => {
                datafusion_datasource::file_sink_config::FileOutputMode::Automatic
            }
            protobuf::FileOutputMode::SingleFile => {
                datafusion_datasource::file_sink_config::FileOutputMode::SingleFile
            }
            protobuf::FileOutputMode::Directory => {
                datafusion_datasource::file_sink_config::FileOutputMode::Directory
            }
        };
        Ok(Self {
            original_url: String::default(),
            object_store_url: ObjectStoreUrl::parse(&conf.object_store_url)?,
            file_group,
            table_paths,
            output_schema: Arc::new(convert_required!(conf.output_schema)?),
            table_partition_cols,
            insert_op,
            keep_partition_by_columns: conf.keep_partition_by_columns,
            file_extension: conf.file_extension.clone(),
            file_output_mode,
        })
    }
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn partitioned_file_path_roundtrip_percent_encoded() {
        let path_str = "foo/foo%2Fbar/baz%252Fqux";
        let pf = PartitionedFile::new_from_meta(ObjectMeta {
            location: Path::parse(path_str).unwrap(),
            last_modified: Utc.timestamp_nanos(1_000),
            size: 42,
            e_tag: None,
            version: None,
        });

        let proto = protobuf::PartitionedFile::try_from_proto(&pf).unwrap();
        assert_eq!(proto.path, path_str);

        let pf2 = PartitionedFile::try_from_proto(&proto).unwrap();
        assert_eq!(pf2.object_meta.location.as_ref(), path_str);
        assert_eq!(pf2.object_meta.location, pf.object_meta.location);
        assert_eq!(pf2.object_meta.size, pf.object_meta.size);
        assert_eq!(pf2.object_meta.last_modified, pf.object_meta.last_modified);
    }

    #[test]
    fn partitioned_file_arrow_schema_roundtrip() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::collections::HashMap;

        let arrow_schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([
                    ("field_meta".to_string(), "field_value".to_string()),
                ])),
            ],
            HashMap::from([("schema_meta".to_string(), "schema_value".to_string())]),
        ));
        let pf = PartitionedFile::new("foo/bar.parquet", 10)
            .with_arrow_schema(Arc::clone(&arrow_schema));

        let proto = protobuf::PartitionedFile::try_from_proto(&pf).unwrap();
        assert!(proto.arrow_schema.is_some());

        let decoded = PartitionedFile::try_from_proto(&proto).unwrap();
        assert_eq!(
            decoded.arrow_schema.as_ref().map(|s| s.as_ref()),
            Some(arrow_schema.as_ref())
        );
    }

    #[test]
    fn partitioned_file_from_proto_invalid_path() {
        let proto = protobuf::PartitionedFile {
            arrow_schema: None,
            path: "foo//bar".to_string(),
            size: 1,
            last_modified_ns: 0,
            partition_values: vec![],
            range: None,
            statistics: None,
        };

        let err = PartitionedFile::try_from_proto(&proto).unwrap_err();
        assert!(err.to_string().contains("Invalid object_store path"));
    }
}
