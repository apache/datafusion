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

use datafusion::physical_plan::{
    expressions::{
        ApproxDistinct, ApproxMedian, ApproxPercentileCont,
        ApproxPercentileContWithWeight, ArrayAgg, Correlation, Covariance, CovariancePop,
        DistinctArrayAgg, DistinctBitXor, DistinctSum, FirstValue, Grouping, LastValue,
        Median, OrderSensitiveArrayAgg, Regr, RegrType, Stddev, StddevPop, Variance,
        VariancePop,
    },
    windows::BuiltInWindowExpr,
    ColumnStatistics,
};
use datafusion::{
    physical_expr::window::NthValueKind,
    physical_plan::{
        expressions::{
            CaseExpr, CumeDist, InListExpr, IsNotNullExpr, IsNullExpr, NegativeExpr,
            NotExpr, NthValue, Ntile, Rank, RankType, RowNumber, WindowShift,
        },
        Statistics,
    },
};
use datafusion::{
    physical_expr::window::SlidingAggregateWindowExpr,
    physical_plan::{
        expressions::{CastExpr, TryCastExpr},
        windows::PlainAggregateWindowExpr,
        WindowExpr,
    },
};

use datafusion::datasource::listing::{FileRange, PartitionedFile};
use datafusion::datasource::physical_plan::FileScanConfig;

use datafusion::physical_plan::expressions::{Count, DistinctCount, Literal};

use datafusion::physical_plan::expressions::{
    Avg, BinaryExpr, BitAnd, BitOr, BitXor, BoolAnd, BoolOr, Column, LikeExpr, Max, Min,
    Sum,
};
use datafusion::physical_plan::{AggregateExpr, PhysicalExpr};

use crate::protobuf::{self, physical_window_expr_node};
use crate::protobuf::{
    physical_aggregate_expr_node, PhysicalSortExprNode, PhysicalSortExprNodeCollection,
    ScalarValue,
};
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::physical_expr::expressions::{GetFieldAccessExpr, GetIndexedFieldExpr};
use datafusion::physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::joins::utils::JoinSide;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion_common::{internal_err, not_impl_err, DataFusionError, Result};

impl TryFrom<Arc<dyn AggregateExpr>> for protobuf::PhysicalExprNode {
    type Error = DataFusionError;

    fn try_from(a: Arc<dyn AggregateExpr>) -> Result<Self, Self::Error> {
        let expressions: Vec<protobuf::PhysicalExprNode> = a
            .expressions()
            .iter()
            .map(|e| e.clone().try_into())
            .collect::<Result<Vec<_>>>()?;

        let ordering_req: Vec<protobuf::PhysicalSortExprNode> = a
            .order_bys()
            .unwrap_or(&[])
            .iter()
            .map(|e| e.clone().try_into())
            .collect::<Result<Vec<_>>>()?;

        if let Some(a) = a.as_any().downcast_ref::<AggregateFunctionExpr>() {
            return Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                        protobuf::PhysicalAggregateExprNode {
                            aggregate_function: Some(physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(a.fun().name.clone())),
                            expr: expressions,
                            ordering_req,
                            distinct: false,
                        },
                    )),
                });
        }

        let AggrFn {
            inner: aggr_function,
            distinct,
        } = aggr_expr_to_aggr_fn(a.as_ref())?;

        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                protobuf::PhysicalAggregateExprNode {
                    aggregate_function: Some(
                        physical_aggregate_expr_node::AggregateFunction::AggrFunction(
                            aggr_function as i32,
                        ),
                    ),
                    expr: expressions,
                    ordering_req,
                    distinct,
                },
            )),
        })
    }
}

impl TryFrom<Arc<dyn WindowExpr>> for protobuf::PhysicalWindowExprNode {
    type Error = DataFusionError;

    fn try_from(
        window_expr: Arc<dyn WindowExpr>,
    ) -> std::result::Result<Self, Self::Error> {
        let expr = window_expr.as_any();

        let mut args = window_expr.expressions().to_vec();
        let window_frame = window_expr.get_window_frame();

        let window_function = if let Some(built_in_window_expr) =
            expr.downcast_ref::<BuiltInWindowExpr>()
        {
            let expr = built_in_window_expr.get_built_in_func_expr();
            let built_in_fn_expr = expr.as_any();

            let builtin_fn = if built_in_fn_expr.downcast_ref::<RowNumber>().is_some() {
                protobuf::BuiltInWindowFunction::RowNumber
            } else if let Some(rank_expr) = built_in_fn_expr.downcast_ref::<Rank>() {
                match rank_expr.get_type() {
                    RankType::Basic => protobuf::BuiltInWindowFunction::Rank,
                    RankType::Dense => protobuf::BuiltInWindowFunction::DenseRank,
                    RankType::Percent => protobuf::BuiltInWindowFunction::PercentRank,
                }
            } else if built_in_fn_expr.downcast_ref::<CumeDist>().is_some() {
                protobuf::BuiltInWindowFunction::CumeDist
            } else if let Some(ntile_expr) = built_in_fn_expr.downcast_ref::<Ntile>() {
                args.insert(
                    0,
                    Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
                        ntile_expr.get_n() as i64,
                    )))),
                );
                protobuf::BuiltInWindowFunction::Ntile
            } else if let Some(window_shift_expr) =
                built_in_fn_expr.downcast_ref::<WindowShift>()
            {
                args.insert(
                    1,
                    Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
                        window_shift_expr.get_shift_offset(),
                    )))),
                );
                if let Some(default_value) = window_shift_expr.get_default_value() {
                    args.insert(2, Arc::new(Literal::new(default_value)));
                }
                if window_shift_expr.get_shift_offset() >= 0 {
                    protobuf::BuiltInWindowFunction::Lag
                } else {
                    protobuf::BuiltInWindowFunction::Lead
                }
            } else if let Some(nth_value_expr) =
                built_in_fn_expr.downcast_ref::<NthValue>()
            {
                match nth_value_expr.get_kind() {
                    NthValueKind::First => protobuf::BuiltInWindowFunction::FirstValue,
                    NthValueKind::Last => protobuf::BuiltInWindowFunction::LastValue,
                    NthValueKind::Nth(n) => {
                        args.insert(
                            1,
                            Arc::new(Literal::new(
                                datafusion_common::ScalarValue::Int64(Some(n as i64)),
                            )),
                        );
                        protobuf::BuiltInWindowFunction::NthValue
                    }
                }
            } else {
                return not_impl_err!("BuiltIn function not supported: {expr:?}");
            };

            physical_window_expr_node::WindowFunction::BuiltInFunction(builtin_fn as i32)
        } else if let Some(plain_aggr_window_expr) =
            expr.downcast_ref::<PlainAggregateWindowExpr>()
        {
            let AggrFn { inner, distinct } = aggr_expr_to_aggr_fn(
                plain_aggr_window_expr.get_aggregate_expr().as_ref(),
            )?;

            if distinct {
                // TODO
                return not_impl_err!(
                    "Distinct aggregate functions not supported in window expressions"
                );
            }

            if !window_frame.start_bound.is_unbounded() {
                return Err(DataFusionError::Internal(format!("Invalid PlainAggregateWindowExpr = {window_expr:?} with WindowFrame = {window_frame:?}")));
            }

            physical_window_expr_node::WindowFunction::AggrFunction(inner as i32)
        } else if let Some(sliding_aggr_window_expr) =
            expr.downcast_ref::<SlidingAggregateWindowExpr>()
        {
            let AggrFn { inner, distinct } = aggr_expr_to_aggr_fn(
                sliding_aggr_window_expr.get_aggregate_expr().as_ref(),
            )?;

            if distinct {
                // TODO
                return not_impl_err!(
                    "Distinct aggregate functions not supported in window expressions"
                );
            }

            if window_frame.start_bound.is_unbounded() {
                return Err(DataFusionError::Internal(format!("Invalid SlidingAggregateWindowExpr = {window_expr:?} with WindowFrame = {window_frame:?}")));
            }

            physical_window_expr_node::WindowFunction::AggrFunction(inner as i32)
        } else {
            return not_impl_err!("WindowExpr not supported: {window_expr:?}");
        };

        let args = args
            .into_iter()
            .map(|e| e.try_into())
            .collect::<Result<Vec<protobuf::PhysicalExprNode>>>()?;

        let partition_by = window_expr
            .partition_by()
            .iter()
            .map(|p| p.clone().try_into())
            .collect::<Result<Vec<protobuf::PhysicalExprNode>>>()?;

        let order_by = window_expr
            .order_by()
            .iter()
            .map(|o| o.clone().try_into())
            .collect::<Result<Vec<protobuf::PhysicalSortExprNode>>>()?;

        let window_frame: protobuf::WindowFrame = window_frame
            .as_ref()
            .try_into()
            .map_err(|e| DataFusionError::Internal(format!("{e}")))?;

        let name = window_expr.name().to_string();

        Ok(protobuf::PhysicalWindowExprNode {
            args,
            partition_by,
            order_by,
            window_frame: Some(window_frame),
            window_function: Some(window_function),
            name,
        })
    }
}

struct AggrFn {
    inner: protobuf::AggregateFunction,
    distinct: bool,
}

fn aggr_expr_to_aggr_fn(expr: &dyn AggregateExpr) -> Result<AggrFn> {
    let aggr_expr = expr.as_any();
    let mut distinct = false;

    let inner = if aggr_expr.downcast_ref::<Count>().is_some() {
        protobuf::AggregateFunction::Count
    } else if aggr_expr.downcast_ref::<DistinctCount>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::Count
    } else if aggr_expr.downcast_ref::<Grouping>().is_some() {
        protobuf::AggregateFunction::Grouping
    } else if aggr_expr.downcast_ref::<BitAnd>().is_some() {
        protobuf::AggregateFunction::BitAnd
    } else if aggr_expr.downcast_ref::<BitOr>().is_some() {
        protobuf::AggregateFunction::BitOr
    } else if aggr_expr.downcast_ref::<BitXor>().is_some() {
        protobuf::AggregateFunction::BitXor
    } else if aggr_expr.downcast_ref::<DistinctBitXor>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::BitXor
    } else if aggr_expr.downcast_ref::<BoolAnd>().is_some() {
        protobuf::AggregateFunction::BoolAnd
    } else if aggr_expr.downcast_ref::<BoolOr>().is_some() {
        protobuf::AggregateFunction::BoolOr
    } else if aggr_expr.downcast_ref::<Sum>().is_some() {
        protobuf::AggregateFunction::Sum
    } else if aggr_expr.downcast_ref::<DistinctSum>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::Sum
    } else if aggr_expr.downcast_ref::<ApproxDistinct>().is_some() {
        protobuf::AggregateFunction::ApproxDistinct
    } else if aggr_expr.downcast_ref::<ArrayAgg>().is_some() {
        protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<DistinctArrayAgg>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<OrderSensitiveArrayAgg>().is_some() {
        protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<Min>().is_some() {
        protobuf::AggregateFunction::Min
    } else if aggr_expr.downcast_ref::<Max>().is_some() {
        protobuf::AggregateFunction::Max
    } else if aggr_expr.downcast_ref::<Avg>().is_some() {
        protobuf::AggregateFunction::Avg
    } else if aggr_expr.downcast_ref::<Variance>().is_some() {
        protobuf::AggregateFunction::Variance
    } else if aggr_expr.downcast_ref::<VariancePop>().is_some() {
        protobuf::AggregateFunction::VariancePop
    } else if aggr_expr.downcast_ref::<Covariance>().is_some() {
        protobuf::AggregateFunction::Covariance
    } else if aggr_expr.downcast_ref::<CovariancePop>().is_some() {
        protobuf::AggregateFunction::CovariancePop
    } else if aggr_expr.downcast_ref::<Stddev>().is_some() {
        protobuf::AggregateFunction::Stddev
    } else if aggr_expr.downcast_ref::<StddevPop>().is_some() {
        protobuf::AggregateFunction::StddevPop
    } else if aggr_expr.downcast_ref::<Correlation>().is_some() {
        protobuf::AggregateFunction::Correlation
    } else if let Some(regr_expr) = aggr_expr.downcast_ref::<Regr>() {
        match regr_expr.get_regr_type() {
            RegrType::Slope => protobuf::AggregateFunction::RegrSlope,
            RegrType::Intercept => protobuf::AggregateFunction::RegrIntercept,
            RegrType::Count => protobuf::AggregateFunction::RegrCount,
            RegrType::R2 => protobuf::AggregateFunction::RegrR2,
            RegrType::AvgX => protobuf::AggregateFunction::RegrAvgx,
            RegrType::AvgY => protobuf::AggregateFunction::RegrAvgy,
            RegrType::SXX => protobuf::AggregateFunction::RegrSxx,
            RegrType::SYY => protobuf::AggregateFunction::RegrSyy,
            RegrType::SXY => protobuf::AggregateFunction::RegrSxy,
        }
    } else if aggr_expr.downcast_ref::<ApproxPercentileCont>().is_some() {
        protobuf::AggregateFunction::ApproxPercentileCont
    } else if aggr_expr
        .downcast_ref::<ApproxPercentileContWithWeight>()
        .is_some()
    {
        protobuf::AggregateFunction::ApproxPercentileContWithWeight
    } else if aggr_expr.downcast_ref::<ApproxMedian>().is_some() {
        protobuf::AggregateFunction::ApproxMedian
    } else if aggr_expr.downcast_ref::<Median>().is_some() {
        protobuf::AggregateFunction::Median
    } else if aggr_expr.downcast_ref::<FirstValue>().is_some() {
        protobuf::AggregateFunction::FirstValueAgg
    } else if aggr_expr.downcast_ref::<LastValue>().is_some() {
        protobuf::AggregateFunction::LastValueAgg
    } else {
        return not_impl_err!("Aggregate function not supported: {expr:?}");
    };

    Ok(AggrFn { inner, distinct })
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
            let field = match expr.field() {
                GetFieldAccessExpr::NamedStructField{name} => Some(
                    protobuf::physical_get_indexed_field_expr_node::Field::NamedStructFieldExpr(protobuf::NamedStructFieldExpr {
                        name: Some(ScalarValue::try_from(name)?)
                    })
                ),
                GetFieldAccessExpr::ListIndex{key} => Some(
                    protobuf::physical_get_indexed_field_expr_node::Field::ListIndexExpr(Box::new(protobuf::ListIndexExpr {
                        key: Some(Box::new(key.to_owned().try_into()?))
                    }))
                ),
                GetFieldAccessExpr::ListRange{start, stop} => Some(
                    protobuf::physical_get_indexed_field_expr_node::Field::ListRangeExpr(Box::new(protobuf::ListRangeExpr {
                        start: Some(Box::new(start.to_owned().try_into()?)),
                        stop: Some(Box::new(stop.to_owned().try_into()?)),
                    }))
                ),
            };

            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::GetIndexedFieldExpr(
                        Box::new(protobuf::PhysicalGetIndexedFieldExprNode {
                            arg: Some(Box::new(expr.arg().to_owned().try_into()?)),
                            field,
                        }),
                    ),
                ),
            })
        } else {
            internal_err!("physical_plan::to_proto() unsupported expression {value:?}")
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
            null_count: cs.null_count.map(|n| vec![n as u32]).unwrap_or(vec![]),
            distinct_count: cs.distinct_count.map(|n| vec![n as u32]).unwrap_or(vec![]),
        }
    }
}

impl From<&Statistics> for protobuf::Statistics {
    fn from(s: &Statistics) -> protobuf::Statistics {
        let none_value = -1_i64;
        let column_stats = s.column_statistics.iter().map(|s| s.into()).collect();
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
