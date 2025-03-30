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

#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::physical_plan::FileSink;
use datafusion::physical_expr::window::{SlidingAggregateWindowExpr, StandardWindowExpr};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr,
    Literal, NegativeExpr, NotExpr, TryCastExpr, UnKnownColumn,
};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::windows::{PlainAggregateWindowExpr, WindowUDFExpr};
use datafusion::physical_plan::{Partitioning, PhysicalExpr, WindowExpr};
use datafusion::{
    datasource::{
        file_format::{csv::CsvSink, json::JsonSink},
        listing::{FileRange, PartitionedFile},
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    physical_plan::expressions::LikeExpr,
};
use datafusion_common::{internal_err, not_impl_err, DataFusionError, Result};
use datafusion_expr::WindowFrame;

use crate::protobuf::{
    self, physical_aggregate_expr_node, physical_window_expr_node, PhysicalSortExprNode,
    PhysicalSortExprNodeCollection,
};

use super::PhysicalExtensionCodec;

pub fn serialize_physical_aggr_expr(
    aggr_expr: Arc<AggregateFunctionExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::PhysicalExprNode> {
    let expressions = serialize_physical_exprs(&aggr_expr.expressions(), codec)?;
    let ordering_req = match aggr_expr.order_bys() {
        Some(order) => order.clone(),
        None => LexOrdering::default(),
    };
    let ordering_req = serialize_physical_sort_exprs(ordering_req, codec)?;

    let name = aggr_expr.fun().name().to_string();
    let mut buf = Vec::new();
    codec.try_encode_udaf(aggr_expr.fun(), &mut buf)?;
    Ok(protobuf::PhysicalExprNode {
        expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
            protobuf::PhysicalAggregateExprNode {
                aggregate_function: Some(physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(name)),
                expr: expressions,
                ordering_req,
                distinct: aggr_expr.is_distinct(),
                ignore_nulls: aggr_expr.ignore_nulls(),
                fun_definition: (!buf.is_empty()).then_some(buf),
            },
        )),
    })
}

fn serialize_physical_window_aggr_expr(
    aggr_expr: &AggregateFunctionExpr,
    _window_frame: &WindowFrame,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(physical_window_expr_node::WindowFunction, Option<Vec<u8>>)> {
    if aggr_expr.is_distinct() || aggr_expr.ignore_nulls() {
        // TODO
        return not_impl_err!(
            "Distinct aggregate functions not supported in window expressions"
        );
    }

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
) -> Result<protobuf::PhysicalWindowExprNode> {
    let expr = window_expr.as_any();
    let args = window_expr.expressions().to_vec();
    let window_frame = window_expr.get_window_frame();

    let (window_function, fun_definition) = if let Some(plain_aggr_window_expr) =
        expr.downcast_ref::<PlainAggregateWindowExpr>()
    {
        serialize_physical_window_aggr_expr(
            plain_aggr_window_expr.get_aggregate_expr(),
            window_frame,
            codec,
        )?
    } else if let Some(sliding_aggr_window_expr) =
        expr.downcast_ref::<SlidingAggregateWindowExpr>()
    {
        serialize_physical_window_aggr_expr(
            sliding_aggr_window_expr.get_aggregate_expr(),
            window_frame,
            codec,
        )?
    } else if let Some(udf_window_expr) = expr.downcast_ref::<StandardWindowExpr>() {
        if let Some(expr) = udf_window_expr
            .get_standard_func_expr()
            .as_any()
            .downcast_ref::<WindowUDFExpr>()
        {
            let mut buf = Vec::new();
            codec.try_encode_udwf(expr.fun(), &mut buf)?;
            (
                physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(
                    expr.fun().name().to_string(),
                ),
                (!buf.is_empty()).then_some(buf),
            )
        } else {
            return not_impl_err!(
                "User-defined window function not supported: {window_expr:?}"
            );
        }
    } else {
        return not_impl_err!("WindowExpr not supported: {window_expr:?}");
    };

    let args = serialize_physical_exprs(&args, codec)?;
    let partition_by = serialize_physical_exprs(window_expr.partition_by(), codec)?;
    let order_by = serialize_physical_sort_exprs(window_expr.order_by().to_vec(), codec)?;
    let window_frame: protobuf::WindowFrame = window_frame
        .as_ref()
        .try_into()
        .map_err(|e| DataFusionError::Internal(format!("{e}")))?;

    Ok(protobuf::PhysicalWindowExprNode {
        args,
        partition_by,
        order_by,
        window_frame: Some(window_frame),
        window_function: Some(window_function),
        name: window_expr.name().to_string(),
        fun_definition,
    })
}

pub fn serialize_physical_sort_exprs<I>(
    sort_exprs: I,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<PhysicalSortExprNode>>
where
    I: IntoIterator<Item = PhysicalSortExpr>,
{
    sort_exprs
        .into_iter()
        .map(|sort_expr| serialize_physical_sort_expr(sort_expr, codec))
        .collect()
}

pub fn serialize_physical_sort_expr(
    sort_expr: PhysicalSortExpr,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<PhysicalSortExprNode> {
    let PhysicalSortExpr { expr, options } = sort_expr;
    let expr = serialize_physical_expr(&expr, codec)?;
    Ok(PhysicalSortExprNode {
        expr: Some(Box::new(expr)),
        asc: !options.descending,
        nulls_first: options.nulls_first,
    })
}

pub fn serialize_physical_exprs<'a, I>(
    values: I,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<protobuf::PhysicalExprNode>>
where
    I: IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
{
    values
        .into_iter()
        .map(|value| serialize_physical_expr(value, codec))
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
    let expr = value.as_any();

    if let Some(expr) = expr.downcast_ref::<Column>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Column(
                protobuf::PhysicalColumn {
                    name: expr.name().to_string(),
                    index: expr.index() as u32,
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<UnKnownColumn>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::UnknownColumn(
                protobuf::UnknownColumn {
                    name: expr.name().to_string(),
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        let binary_expr = Box::new(protobuf::PhysicalBinaryExprNode {
            l: Some(Box::new(serialize_physical_expr(expr.left(), codec)?)),
            r: Some(Box::new(serialize_physical_expr(expr.right(), codec)?)),
            op: format!("{:?}", expr.op()),
        });

        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::BinaryExpr(
                binary_expr,
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<CaseExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(
                protobuf::physical_expr_node::ExprType::Case(
                    Box::new(
                        protobuf::PhysicalCaseNode {
                            expr: expr
                                .expr()
                                .map(|exp| {
                                    serialize_physical_expr(exp, codec).map(Box::new)
                                })
                                .transpose()?,
                            when_then_expr: expr
                                .when_then_expr()
                                .iter()
                                .map(|(when_expr, then_expr)| {
                                    serialize_when_then_expr(when_expr, then_expr, codec)
                                })
                                .collect::<Result<
                                    Vec<protobuf::PhysicalWhenThen>,
                                    DataFusionError,
                                >>()?,
                            else_expr: expr
                                .else_expr()
                                .map(|a| serialize_physical_expr(a, codec).map(Box::new))
                                .transpose()?,
                        },
                    ),
                ),
            ),
        })
    } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::NotExpr(Box::new(
                protobuf::PhysicalNot {
                    expr: Some(Box::new(serialize_physical_expr(expr.arg(), codec)?)),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                Box::new(protobuf::PhysicalIsNull {
                    expr: Some(Box::new(serialize_physical_expr(expr.arg(), codec)?)),
                }),
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                Box::new(protobuf::PhysicalIsNotNull {
                    expr: Some(Box::new(serialize_physical_expr(expr.arg(), codec)?)),
                }),
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::InList(Box::new(
                protobuf::PhysicalInListNode {
                    expr: Some(Box::new(serialize_physical_expr(expr.expr(), codec)?)),
                    list: serialize_physical_exprs(expr.list(), codec)?,
                    negated: expr.negated(),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(Box::new(
                protobuf::PhysicalNegativeNode {
                    expr: Some(Box::new(serialize_physical_expr(expr.arg(), codec)?)),
                },
            ))),
        })
    } else if let Some(lit) = expr.downcast_ref::<Literal>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Literal(
                lit.value().try_into()?,
            )),
        })
    } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Cast(Box::new(
                protobuf::PhysicalCastNode {
                    expr: Some(Box::new(serialize_physical_expr(cast.expr(), codec)?)),
                    arrow_type: Some(cast.cast_type().try_into()?),
                },
            ))),
        })
    } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::TryCast(Box::new(
                protobuf::PhysicalTryCastNode {
                    expr: Some(Box::new(serialize_physical_expr(cast.expr(), codec)?)),
                    arrow_type: Some(cast.cast_type().try_into()?),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let mut buf = Vec::new();
        codec.try_encode_udf(expr.fun(), &mut buf)?;
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                protobuf::PhysicalScalarUdfNode {
                    name: expr.name().to_string(),
                    args: serialize_physical_exprs(expr.args(), codec)?,
                    fun_definition: (!buf.is_empty()).then_some(buf),
                    return_type: Some(expr.return_type().try_into()?),
                    nullable: expr.nullable(),
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<LikeExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::LikeExpr(Box::new(
                protobuf::PhysicalLikeExprNode {
                    negated: expr.negated(),
                    case_insensitive: expr.case_insensitive(),
                    expr: Some(Box::new(serialize_physical_expr(expr.expr(), codec)?)),
                    pattern: Some(Box::new(serialize_physical_expr(
                        expr.pattern(),
                        codec,
                    )?)),
                },
            ))),
        })
    } else {
        let mut buf: Vec<u8> = vec![];
        match codec.try_encode_expr(value, &mut buf) {
            Ok(_) => {
                let inputs: Vec<protobuf::PhysicalExprNode> = value
                    .children()
                    .into_iter()
                    .map(|e| serialize_physical_expr(e, codec))
                    .collect::<Result<_>>()?;
                Ok(protobuf::PhysicalExprNode {
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
) -> Result<protobuf::Partitioning> {
    let serialized_partitioning = match partitioning {
        Partitioning::RoundRobinBatch(partition_count) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::RoundRobin(
                *partition_count as u64,
            )),
        },
        Partitioning::Hash(exprs, partition_count) => {
            let serialized_exprs = serialize_physical_exprs(exprs, codec)?;
            protobuf::Partitioning {
                partition_method: Some(protobuf::partitioning::PartitionMethod::Hash(
                    protobuf::PhysicalHashRepartition {
                        hash_expr: serialized_exprs,
                        partition_count: *partition_count as u64,
                    },
                )),
            }
        }
        Partitioning::UnknownPartitioning(partition_count) => protobuf::Partitioning {
            partition_method: Some(protobuf::partitioning::PartitionMethod::Unknown(
                *partition_count as u64,
            )),
        },
    };
    Ok(serialized_partitioning)
}

fn serialize_when_then_expr(
    when_expr: &Arc<dyn PhysicalExpr>,
    then_expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::PhysicalWhenThen> {
    Ok(protobuf::PhysicalWhenThen {
        when_expr: Some(serialize_physical_expr(when_expr, codec)?),
        then_expr: Some(serialize_physical_expr(then_expr, codec)?),
    })
}

impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = DataFusionError;

    fn try_from(pf: &PartitionedFile) -> Result<Self> {
        let last_modified = pf.object_meta.last_modified;
        let last_modified_ns = last_modified.timestamp_nanos_opt().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid timestamp on PartitionedFile::ObjectMeta: {last_modified}"
            ))
        })? as u64;
        Ok(protobuf::PartitionedFile {
            path: pf.object_meta.location.as_ref().to_owned(),
            size: pf.object_meta.size as u64,
            last_modified_ns,
            partition_values: pf
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: pf.range.as_ref().map(|r| r.try_into()).transpose()?,
            statistics: pf.statistics.as_ref().map(|s| s.into()),
        })
    }
}

impl TryFrom<&FileRange> for protobuf::FileRange {
    type Error = DataFusionError;

    fn try_from(value: &FileRange) -> Result<Self> {
        Ok(protobuf::FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&[PartitionedFile]> for protobuf::FileGroup {
    type Error = DataFusionError;

    fn try_from(gr: &[PartitionedFile]) -> Result<Self, Self::Error> {
        Ok(protobuf::FileGroup {
            files: gr
                .iter()
                .map(|f| f.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

pub fn serialize_file_scan_config(
    conf: &FileScanConfig,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::FileScanExecConf> {
    let file_groups = conf
        .file_groups
        .iter()
        .map(|p| p.files().try_into())
        .collect::<Result<Vec<_>, _>>()?;

    let mut output_orderings = vec![];
    for order in &conf.output_ordering {
        let ordering = serialize_physical_sort_exprs(order.to_vec(), codec)?;
        output_orderings.push(ordering)
    }

    // Fields must be added to the schema so that they can persist in the protobuf,
    // and then they are to be removed from the schema in `parse_protobuf_file_scan_config`
    let mut fields = conf
        .file_schema
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(conf.table_partition_cols.iter().cloned().map(Arc::new));
    let schema = Arc::new(arrow::datatypes::Schema::new(fields.clone()));

    Ok(protobuf::FileScanExecConf {
        file_groups,
        statistics: Some((&conf.statistics).into()),
        limit: conf.limit.map(|l| protobuf::ScanLimit { limit: l as u32 }),
        projection: conf
            .projection
            .as_ref()
            .unwrap_or(&(0..schema.fields().len()).collect::<Vec<_>>())
            .iter()
            .map(|n| *n as u32)
            .collect(),
        schema: Some(schema.as_ref().try_into()?),
        table_partition_cols: conf
            .table_partition_cols
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
    })
}

pub fn serialize_maybe_filter(
    expr: Option<Arc<dyn PhysicalExpr>>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::MaybeFilter> {
    match expr {
        None => Ok(protobuf::MaybeFilter { expr: None }),
        Some(expr) => Ok(protobuf::MaybeFilter {
            expr: Some(serialize_physical_expr(&expr, codec)?),
        }),
    }
}

impl TryFrom<&JsonSink> for protobuf::JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &JsonSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

impl TryFrom<&CsvSink> for protobuf::CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &CsvSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

#[cfg(feature = "parquet")]
impl TryFrom<&ParquetSink> for protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            parquet_options: Some(value.parquet_options().try_into()?),
        })
    }
}

impl TryFrom<&FileSinkConfig> for protobuf::FileSinkConfig {
    type Error = DataFusionError;

    fn try_from(conf: &FileSinkConfig) -> Result<Self, Self::Error> {
        let file_groups = conf
            .file_group
            .iter()
            .map(TryInto::try_into)
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
        Ok(Self {
            object_store_url: conf.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(conf.output_schema.as_ref().try_into()?),
            table_partition_cols,
            keep_partition_by_columns: conf.keep_partition_by_columns,
            insert_op: conf.insert_op as i32,
            file_extension: conf.file_extension.to_string(),
        })
    }
}
