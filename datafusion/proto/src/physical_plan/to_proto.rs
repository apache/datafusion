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
// under the License.language governing permissions and
// limitations under the License.

use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    sync::Arc,
};

use datafusion::physical_plan::expressions::{CastExpr, TryCastExpr};
use datafusion::physical_plan::ColumnStatistics;
use datafusion::physical_plan::{
    expressions::{
        CaseExpr, InListExpr, IsNotNullExpr, IsNullExpr, NegativeExpr, NotExpr,
    },
    Statistics,
};

use datafusion::datasource::listing::{FileRange, PartitionedFile};
use datafusion::datasource::physical_plan::FileScanConfig;

use datafusion::physical_plan::expressions::{Count, DistinctCount, Literal};

use datafusion::physical_plan::expressions::{
    Avg, BinaryExpr, BitAnd, BitOr, BitXor, BoolAnd, BoolOr, Column, LikeExpr, Max, Min,
    Sum,
};
use datafusion::physical_plan::{AggregateExpr, PhysicalExpr};

use crate::protobuf;
use crate::protobuf::{
    physical_aggregate_expr_node, PhysicalSortExprNode, PhysicalSortExprNodeCollection,
    ScalarValue,
};
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::physical_expr::expressions::{DateTimeIntervalExpr, GetIndexedFieldExpr};
use datafusion::physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::joins::utils::JoinSide;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion_common::{DataFusionError, Result};

impl TryFrom<Arc<dyn AggregateExpr>> for protobuf::PhysicalExprNode {
    type Error = DataFusionError;

    fn try_from(a: Arc<dyn AggregateExpr>) -> Result<Self, Self::Error> {
        use datafusion::physical_plan::expressions;
        use protobuf::AggregateFunction;

        let mut expressions: Vec<protobuf::PhysicalExprNode> = a
            .expressions()
            .iter()
            .map(|e| e.clone().try_into())
            .collect::<Result<Vec<_>>>()?;

        let mut distinct = false;
        let aggr_function = if a.as_any().downcast_ref::<Avg>().is_some() {
            Ok(AggregateFunction::Avg.into())
        } else if a.as_any().downcast_ref::<Sum>().is_some() {
            Ok(AggregateFunction::Sum.into())
        } else if a.as_any().downcast_ref::<Count>().is_some() {
            Ok(AggregateFunction::Count.into())
        } else if a.as_any().downcast_ref::<BitAnd>().is_some() {
            Ok(AggregateFunction::BitAnd.into())
        } else if a.as_any().downcast_ref::<BitOr>().is_some() {
            Ok(AggregateFunction::BitOr.into())
        } else if a.as_any().downcast_ref::<BitXor>().is_some() {
            Ok(AggregateFunction::BitXor.into())
        } else if a.as_any().downcast_ref::<BoolAnd>().is_some() {
            Ok(AggregateFunction::BoolAnd.into())
        } else if a.as_any().downcast_ref::<BoolOr>().is_some() {
            Ok(AggregateFunction::BoolOr.into())
        } else if a.as_any().downcast_ref::<DistinctCount>().is_some() {
            distinct = true;
            Ok(AggregateFunction::Count.into())
        } else if a.as_any().downcast_ref::<Min>().is_some() {
            Ok(AggregateFunction::Min.into())
        } else if a.as_any().downcast_ref::<Max>().is_some() {
            Ok(AggregateFunction::Max.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::ApproxDistinct>()
            .is_some()
        {
            Ok(AggregateFunction::ApproxDistinct.into())
        } else if a.as_any().downcast_ref::<expressions::ArrayAgg>().is_some() {
            Ok(AggregateFunction::ArrayAgg.into())
        } else if a.as_any().downcast_ref::<expressions::Variance>().is_some() {
            Ok(AggregateFunction::Variance.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::VariancePop>()
            .is_some()
        {
            Ok(AggregateFunction::VariancePop.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::Covariance>()
            .is_some()
        {
            Ok(AggregateFunction::Covariance.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::CovariancePop>()
            .is_some()
        {
            Ok(AggregateFunction::CovariancePop.into())
        } else if a.as_any().downcast_ref::<expressions::Stddev>().is_some() {
            Ok(AggregateFunction::Stddev.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::StddevPop>()
            .is_some()
        {
            Ok(AggregateFunction::StddevPop.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::Correlation>()
            .is_some()
        {
            Ok(AggregateFunction::Correlation.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::ApproxPercentileCont>()
            .is_some()
        {
            Ok(AggregateFunction::ApproxPercentileCont.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::ApproxPercentileContWithWeight>()
            .is_some()
        {
            Ok(AggregateFunction::ApproxPercentileContWithWeight.into())
        } else if a
            .as_any()
            .downcast_ref::<expressions::ApproxMedian>()
            .is_some()
        {
            Ok(AggregateFunction::ApproxMedian.into())
        } else if a.as_any().is::<expressions::FirstValue>() {
            Ok(AggregateFunction::FirstValueAgg.into())
        } else if a.as_any().is::<expressions::LastValue>() {
            Ok(AggregateFunction::LastValueAgg.into())
        } else {
            if let Some(a) = a.as_any().downcast_ref::<AggregateFunctionExpr>() {
                return Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                        protobuf::PhysicalAggregateExprNode {
                            aggregate_function: Some(physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(a.fun().name.clone())),
                            expr: expressions,
                            /// TODO: Add proper handling here
                            ordering_expr: vec![],
                            distinct,
                        },
                    )),
                });
            }

            Err(DataFusionError::NotImplemented(format!(
                "Aggregate function not supported: {a:?}"
            )))
        }?;

        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                protobuf::PhysicalAggregateExprNode {
                    aggregate_function: Some(
                        physical_aggregate_expr_node::AggregateFunction::AggrFunction(
                            aggr_function,
                        ),
                    ),
                    expr: expressions,
                    /// TODO: Add proper handling here
                    ordering_expr: vec![],
                    distinct,
                },
            )),
        })
    }
}

impl TryFrom<Arc<dyn PhysicalExpr>> for protobuf::PhysicalExprNode {
    type Error = DataFusionError;

    fn try_from(value: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
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
        } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
            let binary_expr = Box::new(protobuf::PhysicalBinaryExprNode {
                l: Some(Box::new(expr.left().to_owned().try_into()?)),
                r: Some(Box::new(expr.right().to_owned().try_into()?)),
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
                                    .map(|exp| exp.clone().try_into().map(Box::new))
                                    .transpose()?,
                                when_then_expr: expr
                                    .when_then_expr()
                                    .iter()
                                    .map(|(when_expr, then_expr)| {
                                        try_parse_when_then_expr(when_expr, then_expr)
                                    })
                                    .collect::<Result<
                                        Vec<protobuf::PhysicalWhenThen>,
                                        Self::Error,
                                    >>()?,
                                else_expr: expr
                                    .else_expr()
                                    .map(|a| a.clone().try_into().map(Box::new))
                                    .transpose()?,
                            },
                        ),
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::NotExpr(
                    Box::new(protobuf::PhysicalNot {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                    Box::new(protobuf::PhysicalIsNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                    Box::new(protobuf::PhysicalIsNotNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::InList(
                        Box::new(
                            protobuf::PhysicalInListNode {
                                expr: Some(Box::new(expr.expr().to_owned().try_into()?)),
                                list: expr
                                    .list()
                                    .iter()
                                    .map(|a| a.clone().try_into())
                                    .collect::<Result<
                                    Vec<protobuf::PhysicalExprNode>,
                                    Self::Error,
                                >>()?,
                                negated: expr.negated(),
                            },
                        ),
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(
                    Box::new(protobuf::PhysicalNegativeNode {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
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
                        expr: Some(Box::new(cast.expr().clone().try_into()?)),
                        arrow_type: Some(cast.cast_type().try_into()?),
                    },
                ))),
            })
        } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::TryCast(
                    Box::new(protobuf::PhysicalTryCastNode {
                        expr: Some(Box::new(cast.expr().clone().try_into()?)),
                        arrow_type: Some(cast.cast_type().try_into()?),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
            let args: Vec<protobuf::PhysicalExprNode> = expr
                .args()
                .iter()
                .map(|e| e.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            if let Ok(fun) = BuiltinScalarFunction::from_str(expr.name()) {
                let fun: protobuf::ScalarFunction = (&fun).try_into()?;

                Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(
                        protobuf::physical_expr_node::ExprType::ScalarFunction(
                            protobuf::PhysicalScalarFunctionNode {
                                name: expr.name().to_string(),
                                fun: fun.into(),
                                args,
                                return_type: Some(expr.return_type().try_into()?),
                            },
                        ),
                    ),
                })
            } else {
                Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                        protobuf::PhysicalScalarUdfNode {
                            name: expr.name().to_string(),
                            args,
                            return_type: Some(expr.return_type().try_into()?),
                        },
                    )),
                })
            }
        } else if let Some(expr) = expr.downcast_ref::<DateTimeIntervalExpr>() {
            let dti_expr = Box::new(protobuf::PhysicalDateTimeIntervalExprNode {
                l: Some(Box::new(expr.lhs().to_owned().try_into()?)),
                r: Some(Box::new(expr.rhs().to_owned().try_into()?)),
                op: format!("{:?}", expr.op()),
            });

            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::DateTimeIntervalExpr(
                        dti_expr,
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<LikeExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::LikeExpr(
                    Box::new(protobuf::PhysicalLikeExprNode {
                        negated: expr.negated(),
                        case_insensitive: expr.case_insensitive(),
                        expr: Some(Box::new(expr.expr().to_owned().try_into()?)),
                        pattern: Some(Box::new(expr.pattern().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<GetIndexedFieldExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::GetIndexedFieldExpr(
                        Box::new(protobuf::PhysicalGetIndexedFieldExprNode {
                            arg: Some(Box::new(expr.arg().to_owned().try_into()?)),
                            key: Some(ScalarValue::try_from(expr.key())?),
                        }),
                    ),
                ),
            })
        } else {
            Err(DataFusionError::Internal(format!(
                "physical_plan::to_proto() unsupported expression {value:?}"
            )))
        }
    }
}

fn try_parse_when_then_expr(
    when_expr: &Arc<dyn PhysicalExpr>,
    then_expr: &Arc<dyn PhysicalExpr>,
) -> Result<protobuf::PhysicalWhenThen> {
    Ok(protobuf::PhysicalWhenThen {
        when_expr: Some(when_expr.clone().try_into()?),
        then_expr: Some(then_expr.clone().try_into()?),
    })
}

impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = DataFusionError;

    fn try_from(pf: &PartitionedFile) -> Result<Self, Self::Error> {
        Ok(protobuf::PartitionedFile {
            path: pf.object_meta.location.as_ref().to_owned(),
            size: pf.object_meta.size as u64,
            last_modified_ns: pf.object_meta.last_modified.timestamp_nanos() as u64,
            partition_values: pf
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: pf.range.as_ref().map(|r| r.try_into()).transpose()?,
        })
    }
}

impl TryFrom<&FileRange> for protobuf::FileRange {
    type Error = DataFusionError;

    fn try_from(value: &FileRange) -> Result<Self, Self::Error> {
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

impl From<&ColumnStatistics> for protobuf::ColumnStats {
    fn from(cs: &ColumnStatistics) -> protobuf::ColumnStats {
        protobuf::ColumnStats {
            min_value: cs.min_value.as_ref().map(|m| m.try_into().unwrap()),
            max_value: cs.max_value.as_ref().map(|m| m.try_into().unwrap()),
            null_count: cs.null_count.map(|n| n as u32).unwrap_or(0),
            distinct_count: cs.distinct_count.map(|n| n as u32).unwrap_or(0),
        }
    }
}

impl From<&Statistics> for protobuf::Statistics {
    fn from(s: &Statistics) -> protobuf::Statistics {
        let none_value = -1_i64;
        let column_stats = match &s.column_statistics {
            None => vec![],
            Some(column_stats) => column_stats.iter().map(|s| s.into()).collect(),
        };
        protobuf::Statistics {
            num_rows: s.num_rows.map(|n| n as i64).unwrap_or(none_value),
            total_byte_size: s.total_byte_size.map(|n| n as i64).unwrap_or(none_value),
            column_stats,
            is_exact: s.is_exact,
        }
    }
}

impl TryFrom<&FileScanConfig> for protobuf::FileScanExecConf {
    type Error = DataFusionError;
    fn try_from(
        conf: &FileScanConfig,
    ) -> Result<protobuf::FileScanExecConf, Self::Error> {
        let file_groups = conf
            .file_groups
            .iter()
            .map(|p| p.as_slice().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        let mut output_orderings = vec![];
        for order in &conf.output_ordering {
            let expr_node_vec = order
                .iter()
                .map(|sort_expr| {
                    let expr = sort_expr.expr.clone().try_into()?;
                    Ok(PhysicalSortExprNode {
                        expr: Some(Box::new(expr)),
                        asc: !sort_expr.options.descending,
                        nulls_first: sort_expr.options.nulls_first,
                    })
                })
                .collect::<Result<Vec<PhysicalSortExprNode>>>()?;
            output_orderings.push(expr_node_vec)
        }

        Ok(protobuf::FileScanExecConf {
            file_groups,
            statistics: Some((&conf.statistics).into()),
            limit: conf.limit.map(|l| protobuf::ScanLimit { limit: l as u32 }),
            projection: conf
                .projection
                .as_ref()
                .unwrap_or(&vec![])
                .iter()
                .map(|n| *n as u32)
                .collect(),
            schema: Some(conf.file_schema.as_ref().try_into()?),
            table_partition_cols: conf
                .table_partition_cols
                .iter()
                .map(|x| x.0.clone())
                .collect::<Vec<_>>(),
            object_store_url: conf.object_store_url.to_string(),
            output_ordering: output_orderings
                .into_iter()
                .map(|e| PhysicalSortExprNodeCollection {
                    physical_sort_expr_nodes: e,
                })
                .collect::<Vec<_>>(),
        })
    }
}

impl From<JoinSide> for protobuf::JoinSide {
    fn from(t: JoinSide) -> Self {
        match t {
            JoinSide::Left => protobuf::JoinSide::LeftSide,
            JoinSide::Right => protobuf::JoinSide::RightSide,
        }
    }
}

impl TryFrom<Option<Arc<dyn PhysicalExpr>>> for protobuf::MaybeFilter {
    type Error = DataFusionError;

    fn try_from(expr: Option<Arc<dyn PhysicalExpr>>) -> Result<Self, Self::Error> {
        match expr {
            None => Ok(protobuf::MaybeFilter { expr: None }),
            Some(expr) => Ok(protobuf::MaybeFilter {
                expr: Some(expr.try_into()?),
            }),
        }
    }
}

impl TryFrom<Option<Vec<PhysicalSortExpr>>> for protobuf::MaybePhysicalSortExprs {
    type Error = DataFusionError;

    fn try_from(sort_exprs: Option<Vec<PhysicalSortExpr>>) -> Result<Self, Self::Error> {
        match sort_exprs {
            None => Ok(protobuf::MaybePhysicalSortExprs { sort_expr: vec![] }),
            Some(sort_exprs) => Ok(protobuf::MaybePhysicalSortExprs {
                sort_expr: sort_exprs
                    .into_iter()
                    .map(|sort_expr| sort_expr.try_into())
                    .collect::<Result<Vec<_>>>()?,
            }),
        }
    }
}

impl TryFrom<PhysicalSortExpr> for protobuf::PhysicalSortExprNode {
    type Error = DataFusionError;

    fn try_from(sort_expr: PhysicalSortExpr) -> std::result::Result<Self, Self::Error> {
        Ok(PhysicalSortExprNode {
            expr: Some(Box::new(sort_expr.expr.try_into()?)),
            asc: !sort_expr.options.descending,
            nulls_first: sort_expr.options.nulls_first,
        })
    }
}
