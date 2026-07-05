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
use datafusion_common::{
    DataFusionError, Result, internal_datafusion_err, internal_err, not_impl_err,
};
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_sink_config::{FileSink, FileSinkConfig};
use datafusion_datasource::{FileRange, PartitionedFile};
use datafusion_datasource_csv::file_format::CsvSink;
use datafusion_datasource_json::file_format::JsonSink;
#[cfg(feature = "parquet")]
use datafusion_datasource_parquet::file_format::ParquetSink;
use datafusion_expr::WindowFrame;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion_physical_expr::window::{SlidingAggregateWindowExpr, StandardWindowExpr};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use datafusion_physical_plan::windows::{PlainAggregateWindowExpr, WindowUDFExpr};
use datafusion_physical_plan::{
    Partitioning, PhysicalExpr, RangePartitioning, SplitPoint, WindowExpr,
};

use super::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension, encode_human_display_alias,
};
use crate::convert::TryFromProto;
use crate::protobuf::{
    self, PhysicalSortExprNode, PhysicalSortExprNodeCollection,
    physical_aggregate_expr_node, physical_window_expr_node,
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
    let window_frame = protobuf::WindowFrame::try_from_proto(window_frame.as_ref())
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
    } else if let Some(expr) = expr.downcast_ref::<ScalarSubqueryExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id,
            expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarSubquery(
                protobuf::PhysicalScalarSubqueryExprNode {
                    data_type: Some(expr.data_type().try_into()?),
                    nullable: expr.nullable(),
                    index: expr.index().as_usize() as u32,
                },
            )),
        })
    } else {
        let mut buf: Vec<u8> = vec![];
        match codec.try_encode_expr(value, &mut buf) {
            Ok(_) => {
                let inputs: Vec<protobuf::PhysicalExprNode> = value
                    .children()
                    .into_iter()
                    .map(|e| proto_converter.physical_expr_to_proto(e, codec))
                    .collect::<Result<_>>()?;
                Ok(protobuf::PhysicalExprNode {
                    expr_id,
                    expr_type: Some(protobuf::physical_expr_node::ExprType::Extension(
                        protobuf::PhysicalExtensionExprNode { expr: buf, inputs },
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
    let serialized_partitioning = match partitioning {
        Partitioning::RoundRobinBatch(partition_count) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::RoundRobin(
                *partition_count as u64,
            )),
        },
        Partitioning::Hash(exprs, partition_count) => {
            let serialized_exprs =
                serialize_physical_exprs(exprs, codec, proto_converter)?;
            protobuf::Partitioning {
                partition_method: Some(protobuf::partitioning::PartitionMethod::Hash(
                    protobuf::PhysicalHashRepartition {
                        hash_expr: serialized_exprs,
                        partition_count: *partition_count as u64,
                    },
                )),
            }
        }
        Partitioning::Range(range) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::Range(
                serialize_range_partitioning(range, codec, proto_converter)?,
            )),
        },
        Partitioning::UnknownPartitioning(partition_count) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::Unknown(
                *partition_count as u64,
            )),
        },
    };
    Ok(serialized_partitioning)
}

fn serialize_range_partitioning(
    range: &RangePartitioning,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::PhysicalRangePartitioning> {
    Ok(protobuf::PhysicalRangePartitioning {
        sort_expr: serialize_physical_sort_exprs(
            range.ordering().iter().cloned(),
            codec,
            proto_converter,
        )?,
        split_point: range
            .split_points()
            .iter()
            .map(serialize_range_split_point)
            .collect::<Result<_>>()?,
    })
}

fn serialize_range_split_point(
    split_point: &SplitPoint,
) -> Result<protobuf::PhysicalRangeSplitPoint> {
    Ok(protobuf::PhysicalRangeSplitPoint {
        value: split_point
            .values()
            .iter()
            .map(|value| {
                TryInto::<datafusion_proto_common::ScalarValue>::try_into(value)
                    .map_err(Into::into)
            })
            .collect::<Result<_>>()?,
    })
}

impl TryFromProto<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = DataFusionError;

    fn try_from_proto(pf: &PartitionedFile) -> Result<Self> {
        let last_modified = pf.object_meta.last_modified;
        let last_modified_ns = last_modified.timestamp_nanos_opt().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid timestamp on PartitionedFile::ObjectMeta: {last_modified}"
            ))
        })? as u64;
        Ok(protobuf::PartitionedFile {
            arrow_schema: pf
                .arrow_schema
                .as_ref()
                .map(|s| s.as_ref().try_into())
                .transpose()?,
            path: pf.object_meta.location.as_ref().to_owned(),
            size: pf.object_meta.size,
            last_modified_ns,
            partition_values: pf
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: pf
                .range
                .as_ref()
                .map(protobuf::FileRange::try_from_proto)
                .transpose()?,
            statistics: pf.statistics.as_ref().map(|s| s.as_ref().into()),
        })
    }
}

impl TryFromProto<&FileRange> for protobuf::FileRange {
    type Error = DataFusionError;

    fn try_from_proto(value: &FileRange) -> Result<Self> {
        Ok(protobuf::FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFromProto<&[PartitionedFile]> for protobuf::FileGroup {
    type Error = DataFusionError;

    fn try_from_proto(gr: &[PartitionedFile]) -> Result<Self, Self::Error> {
        Ok(protobuf::FileGroup {
            files: gr
                .iter()
                .map(protobuf::PartitionedFile::try_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

pub fn serialize_file_scan_config(
    conf: &FileScanConfig,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::FileScanExecConf> {
    let file_groups = conf
        .file_groups
        .iter()
        .map(|p| protobuf::FileGroup::try_from_proto(p.files()))
        .collect::<Result<Vec<_>, _>>()?;

    let mut output_orderings = vec![];
    for order in &conf.output_ordering {
        let ordering =
            serialize_physical_sort_exprs(order.to_vec(), codec, proto_converter)?;
        output_orderings.push(ordering)
    }
    let output_partitioning = conf
        .output_partitioning
        .as_ref()
        .map(|partitioning| serialize_partitioning(partitioning, codec, proto_converter))
        .transpose()?;

    // Fields must be added to the schema so that they can persist in the protobuf,
    // and then they are to be removed from the schema in `parse_protobuf_file_scan_config`
    let mut fields = conf
        .file_schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(conf.table_partition_cols().iter().cloned());

    let schema = Arc::new(
        Schema::new(fields.clone()).with_metadata(conf.file_schema().metadata.clone()),
    );

    let projection_exprs = conf
        .file_source
        .projection()
        .as_ref()
        .map(|projection_exprs| {
            let projections = projection_exprs.iter().cloned().collect::<Vec<_>>();
            Ok::<_, DataFusionError>(protobuf::ProjectionExprs {
                projections: projections
                    .into_iter()
                    .map(|expr| {
                        Ok(protobuf::ProjectionExpr {
                            alias: expr.alias.to_string(),
                            expr: Some(
                                proto_converter
                                    .physical_expr_to_proto(&expr.expr, codec)?,
                            ),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .transpose()?;

    Ok(protobuf::FileScanExecConf {
        file_groups,
        statistics: Some((&conf.statistics()).into()),
        limit: conf.limit.map(|l| protobuf::ScanLimit { limit: l as u32 }),
        projection: vec![],
        schema: Some(schema.as_ref().try_into()?),
        table_partition_cols: conf
            .table_partition_cols()
            .iter()
            .map(|x| x.name().clone())
            .collect::<Vec<_>>(),
        object_store_url: conf.object_store_url.to_string(),
        output_ordering: output_orderings
            .into_iter()
            .map(|e| PhysicalSortExprNodeCollection {
                physical_sort_expr_nodes: e,
            })
            .collect::<Vec<_>>(),
        constraints: Some(conf.constraints.clone().into()),
        batch_size: conf.batch_size.map(|s| s as u64),
        projection_exprs,
        partitioned_by_file_group: Some(conf.partitioned_by_file_group),
        output_partitioning,
    })
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

impl TryFromProto<&JsonSink> for protobuf::JsonSink {
    type Error = DataFusionError;

    fn try_from_proto(value: &JsonSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(protobuf::FileSinkConfig::try_from_proto(value.config())?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

impl TryFromProto<&CsvSink> for protobuf::CsvSink {
    type Error = DataFusionError;

    fn try_from_proto(value: &CsvSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(protobuf::FileSinkConfig::try_from_proto(value.config())?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

#[cfg(feature = "parquet")]
impl TryFromProto<&ParquetSink> for protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from_proto(value: &ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(protobuf::FileSinkConfig::try_from_proto(value.config())?),
            parquet_options: Some(value.parquet_options().try_into()?),
        })
    }
}

impl TryFromProto<&FileSinkConfig> for protobuf::FileSinkConfig {
    type Error = DataFusionError;

    fn try_from_proto(conf: &FileSinkConfig) -> Result<Self, Self::Error> {
        let file_groups = conf
            .file_group
            .iter()
            .map(protobuf::PartitionedFile::try_from_proto)
            .collect::<Result<Vec<_>>>()?;
        let table_paths = conf
            .table_paths
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| {
                Ok(protobuf::PartitionColumn {
                    name: name.to_owned(),
                    arrow_type: Some(data_type.try_into()?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let file_output_mode = match conf.file_output_mode {
            datafusion_datasource::file_sink_config::FileOutputMode::Automatic => {
                protobuf::FileOutputMode::Automatic
            }
            datafusion_datasource::file_sink_config::FileOutputMode::SingleFile => {
                protobuf::FileOutputMode::SingleFile
            }
            datafusion_datasource::file_sink_config::FileOutputMode::Directory => {
                protobuf::FileOutputMode::Directory
            }
        };
        Ok(Self {
            object_store_url: conf.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(conf.output_schema.as_ref().try_into()?),
            table_partition_cols,
            keep_partition_by_columns: conf.keep_partition_by_columns,
            insert_op: conf.insert_op as i32,
            file_extension: conf.file_extension.to_string(),
            file_output_mode: file_output_mode.into(),
        })
    }
}
