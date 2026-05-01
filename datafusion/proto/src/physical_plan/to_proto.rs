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
use datafusion_expr::WindowFrame;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion_physical_expr::window::{SlidingAggregateWindowExpr, StandardWindowExpr};
use datafusion_physical_expr_common::physical_expr::snapshot_physical_expr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_plan::expressions::{
    BinaryExpr, CaseExpr, CastExpr, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr,
    Literal, NegativeExpr, NotExpr, TryCastExpr, UnKnownColumn,
};
use datafusion_physical_plan::joins::{HashExpr, HashTableLookupExpr};
use datafusion_physical_plan::udaf::AggregateFunctionExpr;
use datafusion_physical_plan::windows::{PlainAggregateWindowExpr, WindowUDFExpr};
use datafusion_physical_plan::{Partitioning, PhysicalExpr, WindowExpr};

use super::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
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
                human_display: aggr_expr.human_display().to_string(),
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
    // Snapshot the expr in case it has dynamic predicate state so
    // it can be serialized
    let expr = snapshot_physical_expr(Arc::clone(value))?;

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
    if let Some(node) = expr.to_proto(&ctx)? {
        return Ok(node);
    }

    // HashTableLookupExpr is used for dynamic filter pushdown in hash joins.
    // It contains an Arc<dyn JoinHashMapType> (the build-side hash table) which
    // cannot be serialized - the hash table is a runtime structure built during
    // execution on the build side.
    //
    // We replace it with lit(true) which is safe because:
    // 1. The filter is a performance optimization, not a correctness requirement
    // 2. lit(true) passes all rows, so no valid rows are incorrectly filtered out
    // 3. The join itself will still produce correct results, just without the
    //    benefit of early filtering on the probe side
    //
    // In distributed execution, the remote worker won't have access to the hash
    // table anyway, so the best we can do is skip this optimization.
    if expr.downcast_ref::<HashTableLookupExpr>().is_some() {
        let value = datafusion_proto_common::ScalarValue {
            value: Some(datafusion_proto_common::scalar_value::Value::BoolValue(
                true,
            )),
        };
        return Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Literal(value)),
        });
    }

    if let Some(expr) = expr.downcast_ref::<UnKnownColumn>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::UnknownColumn(
                protobuf::UnknownColumn {
                    name: expr.name().to_string(),
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        // Linearize a nested binary expression tree of the same operator
        // into a flat vector of operands to avoid deep recursion in proto.
        let op = expr.op();
        let mut operand_refs: Vec<&Arc<dyn PhysicalExpr>> = vec![expr.right()];
        let mut current_expr: &BinaryExpr = expr;
        loop {
            match current_expr.left().downcast_ref::<BinaryExpr>() {
                Some(bin) if bin.op() == op => {
                    operand_refs.push(bin.right());
                    current_expr = bin;
                }
                _ => {
                    operand_refs.push(current_expr.left());
                    break;
                }
            }
        }

        // Reverse so operands are ordered from left innermost to right outermost
        operand_refs.reverse();

        let operands = operand_refs
            .iter()
            .map(|e| proto_converter.physical_expr_to_proto(e, codec))
            .collect::<Result<Vec<_>>>()?;

        let binary_expr = Box::new(protobuf::PhysicalBinaryExprNode {
            l: None,
            r: None,
            op: format!("{:?}", op),
            operands,
        });

        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::BinaryExpr(
                binary_expr,
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<CaseExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(
                protobuf::physical_expr_node::ExprType::Case(
                    Box::new(
                        protobuf::PhysicalCaseNode {
                            expr: expr
                                .expr()
                                .map(|exp| {
                                    proto_converter
                                        .physical_expr_to_proto(exp, codec)
                                        .map(Box::new)
                                })
                                .transpose()?,
                            when_then_expr: expr
                                .when_then_expr()
                                .iter()
                                .map(|(when_expr, then_expr)| {
                                    serialize_when_then_expr(
                                        when_expr,
                                        then_expr,
                                        codec,
                                        proto_converter,
                                    )
                                })
                                .collect::<Result<
                                    Vec<protobuf::PhysicalWhenThen>,
                                    DataFusionError,
                                >>()?,
                            else_expr: expr
                                .else_expr()
                                .map(|a| {
                                    proto_converter
                                        .physical_expr_to_proto(a, codec)
                                        .map(Box::new)
                                })
                                .transpose()?,
                        },
                    ),
                ),
            ),
        })
    } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::NotExpr(Box::new(
                protobuf::PhysicalNot {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.arg(), codec)?,
                    )),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                Box::new(protobuf::PhysicalIsNull {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.arg(), codec)?,
                    )),
                }),
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                Box::new(protobuf::PhysicalIsNotNull {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.arg(), codec)?,
                    )),
                }),
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::InList(Box::new(
                protobuf::PhysicalInListNode {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.expr(), codec)?,
                    )),
                    list: serialize_physical_exprs(expr.list(), codec, proto_converter)?,
                    negated: expr.negated(),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(Box::new(
                protobuf::PhysicalNegativeNode {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.arg(), codec)?,
                    )),
                },
            ))),
        })
    } else if let Some(lit) = expr.downcast_ref::<Literal>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Literal(
                lit.value().try_into()?,
            )),
        })
    } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::Cast(Box::new(
                protobuf::PhysicalCastNode {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(cast.expr(), codec)?,
                    )),
                    arrow_type: Some(cast.cast_type().try_into()?),
                },
            ))),
        })
    } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::TryCast(Box::new(
                protobuf::PhysicalTryCastNode {
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(cast.expr(), codec)?,
                    )),
                    arrow_type: Some(cast.cast_type().try_into()?),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let mut buf = Vec::new();
        codec.try_encode_udf(expr.fun(), &mut buf)?;
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
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
    } else if let Some(expr) = expr.downcast_ref::<LikeExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::LikeExpr(Box::new(
                protobuf::PhysicalLikeExprNode {
                    negated: expr.negated(),
                    case_insensitive: expr.case_insensitive(),
                    expr: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.expr(), codec)?,
                    )),
                    pattern: Some(Box::new(
                        proto_converter.physical_expr_to_proto(expr.pattern(), codec)?,
                    )),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<HashExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
            expr_type: Some(protobuf::physical_expr_node::ExprType::HashExpr(
                protobuf::PhysicalHashExprNode {
                    on_columns: serialize_physical_exprs(
                        expr.on_columns(),
                        codec,
                        proto_converter,
                    )?,
                    seed0: expr.seed(),
                    description: expr.description().to_string(),
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<ScalarSubqueryExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_id: None,
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
                    expr_id: None,
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
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::PhysicalWhenThen> {
    Ok(protobuf::PhysicalWhenThen {
        when_expr: Some(proto_converter.physical_expr_to_proto(when_expr, codec)?),
        then_expr: Some(proto_converter.physical_expr_to_proto(then_expr, codec)?),
    })
}

pub fn serialize_file_scan_config(
    conf: &FileScanConfig,
    codec: &dyn PhysicalExtensionCodec,
    proto_converter: &dyn PhysicalProtoConverterExtension,
) -> Result<protobuf::FileScanExecConf> {
    let file_groups = conf
        .file_groups
        .iter()
        .map(protobuf::FileGroup::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let mut output_orderings = vec![];
    for order in &conf.output_ordering {
        let ordering =
            serialize_physical_sort_exprs(order.to_vec(), codec, proto_converter)?;
        output_orderings.push(ordering)
    }

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
