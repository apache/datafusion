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
use arrow::datatypes::Field;
use arrow::ipc::reader::StreamReader;
use chrono::{TimeZone, Utc};
use datafusion_expr::dml::InsertOp;
use object_store::path::Path;
use object_store::ObjectMeta;

use arrow::datatypes::Schema;
use datafusion_common::{internal_datafusion_err, not_impl_err, DataFusionError, Result};
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
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion_physical_plan::expressions::{
    in_list, BinaryExpr, CaseExpr, CastExpr, Column, IsNotNullExpr, IsNullExpr, LikeExpr,
    Literal, NegativeExpr, NotExpr, TryCastExpr, UnKnownColumn,
};
use datafusion_physical_plan::windows::{create_window_expr, schema_add_window_field};
use datafusion_physical_plan::{Partitioning, PhysicalExpr, WindowExpr};
use datafusion_proto_common::common::proto_error;

use crate::convert_required;
use crate::logical_plan::{self};
use crate::protobuf;
use crate::protobuf::physical_expr_node::ExprType;

use super::PhysicalExtensionCodec;

impl From<&protobuf::PhysicalColumn> for Column {
    fn from(c: &protobuf::PhysicalColumn) -> Column {
        Column::new(&c.name, c.index as usize)
    }
}

/// Parses a physical sort expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical sort expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///   when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_sort_expr(
    proto: &protobuf::PhysicalSortExprNode,
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<PhysicalSortExpr> {
    if let Some(expr) = &proto.expr {
        let expr = parse_physical_expr(expr.as_ref(), ctx, input_schema, codec)?;
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
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///   when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_sort_exprs(
    proto: &[protobuf::PhysicalSortExprNode],
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<PhysicalSortExpr>> {
    proto
        .iter()
        .map(|sort_expr| parse_physical_sort_expr(sort_expr, ctx, input_schema, codec))
        .collect()
}

/// Parses a physical window expr from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical window expression node.
/// * `name` - Name of the window expression.
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///   when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_window_expr(
    proto: &protobuf::PhysicalWindowExprNode,
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn WindowExpr>> {
    let window_node_expr = parse_physical_exprs(&proto.args, ctx, input_schema, codec)?;
    let partition_by =
        parse_physical_exprs(&proto.partition_by, ctx, input_schema, codec)?;

    let order_by = parse_physical_sort_exprs(&proto.order_by, ctx, input_schema, codec)?;

    let window_frame = proto
        .window_frame
        .as_ref()
        .map(|wf| wf.clone().try_into())
        .transpose()
        .map_err(|e| internal_datafusion_err!("{e}"))?
        .ok_or_else(|| {
            internal_datafusion_err!("Missing required field 'window_frame' in protobuf")
        })?;

    let fun = if let Some(window_func) = proto.window_function.as_ref() {
        match window_func {
            protobuf::physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(udaf_name) => {
                WindowFunctionDefinition::AggregateUDF(match &proto.fun_definition {
                    Some(buf) => codec.try_decode_udaf(udaf_name, buf)?,
                    None => ctx.udaf(udaf_name).or_else(|_| codec.try_decode_udaf(udaf_name, &[]))?,
                })
            }
            protobuf::physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(udwf_name) => {
                WindowFunctionDefinition::WindowUDF(match &proto.fun_definition {
                    Some(buf) => codec.try_decode_udwf(udwf_name, buf)?,
                    None => ctx.udwf(udwf_name).or_else(|_| codec.try_decode_udwf(udwf_name, &[]))?
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
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<Arc<dyn PhysicalExpr>>>
where
    I: IntoIterator<Item = &'a protobuf::PhysicalExprNode>,
{
    protos
        .into_iter()
        .map(|p| parse_physical_expr(p, ctx, input_schema, codec))
        .collect::<Result<Vec<_>>>()
}

/// Parses a physical expression from a protobuf.
///
/// # Arguments
///
/// * `proto` - Input proto with physical expression node
/// * `registry` - A registry knows how to build logical expressions out of user-defined function names
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///   when performing type coercion.
/// * `codec` - An extension codec used to decode custom UDFs.
pub fn parse_physical_expr(
    proto: &protobuf::PhysicalExprNode,
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| proto_error("Unexpected empty physical expression"))?;

    let pexpr: Arc<dyn PhysicalExpr> = match expr_type {
        ExprType::Column(c) => {
            let pcol: Column = c.into();
            Arc::new(pcol)
        }
        ExprType::UnknownColumn(c) => Arc::new(UnKnownColumn::new(&c.name)),
        ExprType::Literal(scalar) => Arc::new(Literal::new(scalar.try_into()?)),
        ExprType::BinaryExpr(binary_expr) => Arc::new(BinaryExpr::new(
            parse_required_physical_expr(
                binary_expr.l.as_deref(),
                ctx,
                "left",
                input_schema,
                codec,
            )?,
            logical_plan::from_proto::from_proto_binary_op(&binary_expr.op)?,
            parse_required_physical_expr(
                binary_expr.r.as_deref(),
                ctx,
                "right",
                input_schema,
                codec,
            )?,
        )),
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
        ExprType::IsNullExpr(e) => {
            Arc::new(IsNullExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?))
        }
        ExprType::IsNotNullExpr(e) => {
            Arc::new(IsNotNullExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?))
        }
        ExprType::NotExpr(e) => Arc::new(NotExpr::new(parse_required_physical_expr(
            e.expr.as_deref(),
            ctx,
            "expr",
            input_schema,
            codec,
        )?)),
        ExprType::Negative(e) => {
            Arc::new(NegativeExpr::new(parse_required_physical_expr(
                e.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?))
        }
        ExprType::InList(e) => in_list(
            parse_required_physical_expr(
                e.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?,
            parse_physical_exprs(&e.list, ctx, input_schema, codec)?,
            &e.negated,
            input_schema,
        )?,
        ExprType::Case(e) => Arc::new(CaseExpr::try_new(
            e.expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), ctx, input_schema, codec))
                .transpose()?,
            e.when_then_expr
                .iter()
                .map(|e| {
                    Ok((
                        parse_required_physical_expr(
                            e.when_expr.as_ref(),
                            ctx,
                            "when_expr",
                            input_schema,
                            codec,
                        )?,
                        parse_required_physical_expr(
                            e.then_expr.as_ref(),
                            ctx,
                            "then_expr",
                            input_schema,
                            codec,
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?,
            e.else_expr
                .as_ref()
                .map(|e| parse_physical_expr(e.as_ref(), ctx, input_schema, codec))
                .transpose()?,
        )?),
        ExprType::Cast(e) => Arc::new(CastExpr::new(
            parse_required_physical_expr(
                e.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?,
            convert_required!(e.arrow_type)?,
            None,
        )),
        ExprType::TryCast(e) => Arc::new(TryCastExpr::new(
            parse_required_physical_expr(
                e.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?,
            convert_required!(e.arrow_type)?,
        )),
        ExprType::ScalarUdf(e) => {
            let udf = match &e.fun_definition {
                Some(buf) => codec.try_decode_udf(&e.name, buf)?,
                None => ctx
                    .udf(e.name.as_str())
                    .or_else(|_| codec.try_decode_udf(&e.name, &[]))?,
            };
            let scalar_fun_def = Arc::clone(&udf);

            let args = parse_physical_exprs(&e.args, ctx, input_schema, codec)?;

            let config_options = Arc::clone(ctx.session_config().options());

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
        ExprType::LikeExpr(like_expr) => Arc::new(LikeExpr::new(
            like_expr.negated,
            like_expr.case_insensitive,
            parse_required_physical_expr(
                like_expr.expr.as_deref(),
                ctx,
                "expr",
                input_schema,
                codec,
            )?,
            parse_required_physical_expr(
                like_expr.pattern.as_deref(),
                ctx,
                "pattern",
                input_schema,
                codec,
            )?,
        )),
        ExprType::Extension(extension) => {
            let inputs: Vec<Arc<dyn PhysicalExpr>> = extension
                .inputs
                .iter()
                .map(|e| parse_physical_expr(e, ctx, input_schema, codec))
                .collect::<Result<_>>()?;
            (codec.try_decode_expr(extension.expr.as_slice(), &inputs)?) as _
        }
    };

    Ok(pexpr)
}

fn parse_required_physical_expr(
    expr: Option<&protobuf::PhysicalExprNode>,
    ctx: &TaskContext,
    field: &str,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.map(|e| parse_physical_expr(e, ctx, input_schema, codec))
        .transpose()?
        .ok_or_else(|| internal_datafusion_err!("Missing required field {field:?}"))
}

pub fn parse_protobuf_hash_partitioning(
    partitioning: Option<&protobuf::PhysicalHashRepartition>,
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Option<Partitioning>> {
    match partitioning {
        Some(hash_part) => {
            let expr =
                parse_physical_exprs(&hash_part.hash_expr, ctx, input_schema, codec)?;

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
    ctx: &TaskContext,
    input_schema: &Schema,
    codec: &dyn PhysicalExtensionCodec,
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
                    codec,
                )
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

    Ok(TableSchema::new(file_schema, table_partition_cols))
}

pub fn parse_protobuf_file_scan_config(
    proto: &protobuf::FileScanExecConf,
    ctx: &TaskContext,
    codec: &dyn PhysicalExtensionCodec,
    file_source: Arc<dyn FileSource>,
) -> Result<FileScanConfig> {
    let schema: Arc<Schema> = parse_protobuf_file_scan_schema(proto)?;

    let constraints = convert_required!(proto.constraints)?;
    let statistics = convert_required!(proto.statistics)?;

    let file_groups = proto
        .file_groups
        .iter()
        .map(|f| f.try_into())
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
            codec,
        )?;
        output_ordering.extend(LexOrdering::new(sort_exprs));
    }

    // Parse projection expressions if present and apply to file source
    let file_source = if let Some(proto_projection_exprs) = &proto.projection_exprs {
        let projection_exprs: Vec<ProjectionExpr> = proto_projection_exprs
            .projections
            .iter()
            .map(|proto_expr| {
                let expr = parse_physical_expr(
                    proto_expr.expr.as_ref().ok_or_else(|| {
                        internal_datafusion_err!("ProjectionExpr missing expr field")
                    })?,
                    ctx,
                    &schema,
                    codec,
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

    let config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(file_groups)
        .with_constraints(constraints)
        .with_statistics(statistics)
        .with_limit(proto.limit.as_ref().map(|sl| sl.limit as usize))
        .with_output_ordering(output_ordering)
        .with_batch_size(proto.batch_size.map(|s| s as usize))
        .build();
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

impl TryFrom<&protobuf::PartitionedFile> for PartitionedFile {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::PartitionedFile) -> Result<Self, Self::Error> {
        Ok(PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::parse(val.path.as_str()).map_err(|e| {
                    proto_error(format!("Invalid object_store path: {e}"))
                })?,
                last_modified: Utc.timestamp_nanos(val.last_modified_ns as i64),
                size: val.size,
                e_tag: None,
                version: None,
            },
            partition_values: val
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: val.range.as_ref().map(|v| v.try_into()).transpose()?,
            statistics: val
                .statistics
                .as_ref()
                .map(|v| v.try_into().map(Arc::new))
                .transpose()?,
            extensions: None,
            metadata_size_hint: None,
        })
    }
}

impl TryFrom<&protobuf::FileRange> for FileRange {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::FileRange) -> Result<Self, Self::Error> {
        Ok(FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&protobuf::FileGroup> for FileGroup {
    type Error = DataFusionError;

    fn try_from(val: &protobuf::FileGroup) -> Result<Self, Self::Error> {
        let files = val
            .files
            .iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileGroup::new(files))
    }
}

impl TryFrom<&protobuf::JsonSink> for JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::JsonSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required!(value.config)?,
            convert_required!(value.writer_options)?,
        ))
    }
}

#[cfg(feature = "parquet")]
impl TryFrom<&protobuf::ParquetSink> for ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required!(value.config)?,
            convert_required!(value.parquet_options)?,
        ))
    }
}

impl TryFrom<&protobuf::CsvSink> for CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &protobuf::CsvSink) -> Result<Self, Self::Error> {
        Ok(Self::new(
            convert_required!(value.config)?,
            convert_required!(value.writer_options)?,
        ))
    }
}

impl TryFrom<&protobuf::FileSinkConfig> for FileSinkConfig {
    type Error = DataFusionError;

    fn try_from(conf: &protobuf::FileSinkConfig) -> Result<Self, Self::Error> {
        let file_group = FileGroup::new(
            conf.file_groups
                .iter()
                .map(|f| f.try_into())
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use datafusion_datasource::PartitionedFile;
    use object_store::path::Path;
    use object_store::ObjectMeta;

    #[test]
    fn partitioned_file_path_roundtrip_percent_encoded() {
        let path_str = "foo/foo%2Fbar/baz%252Fqux";
        let pf = PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::parse(path_str).unwrap(),
                last_modified: Utc.timestamp_nanos(1_000),
                size: 42,
                e_tag: None,
                version: None,
            },
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        };

        let proto = protobuf::PartitionedFile::try_from(&pf).unwrap();
        assert_eq!(proto.path, path_str);

        let pf2 = PartitionedFile::try_from(&proto).unwrap();
        assert_eq!(pf2.object_meta.location.as_ref(), path_str);
        assert_eq!(pf2.object_meta.location, pf.object_meta.location);
        assert_eq!(pf2.object_meta.size, pf.object_meta.size);
        assert_eq!(pf2.object_meta.last_modified, pf.object_meta.last_modified);
    }

    #[test]
    fn partitioned_file_from_proto_invalid_path() {
        let proto = protobuf::PartitionedFile {
            path: "foo//bar".to_string(),
            size: 1,
            last_modified_ns: 0,
            partition_values: vec![],
            range: None,
            statistics: None,
        };

        let err = PartitionedFile::try_from(&proto).unwrap_err();
        assert!(err.to_string().contains("Invalid object_store path"));
    }
}
