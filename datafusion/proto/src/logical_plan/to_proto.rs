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

//! Code to convert Arrow schemas and DataFusion logical plans to protocol buffer format, allowing
//! DataFusion logical plans to be serialized and transmitted between
//! processes.

use std::collections::HashMap;

use datafusion_expr::expr::{
    self, AggregateFunctionParams, Alias, Between, BinaryExpr, Cast, GroupingSet, InList,
    Like, Placeholder, ScalarFunction, Unnest,
};
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::{Expr, SortExpr, TryCast, WindowFunctionDefinition};

use datafusion_common::DataFusionError as Error;

use crate::protobuf::{
    self, CubeNode, GroupingSetNode, LogicalExprList, PlaceholderNode, RollupNode,
};

use super::{AsLogicalPlan, LogicalExtensionCodec};
use crate::protobuf::LogicalPlanNode;

pub fn serialize_exprs<'a, I>(
    exprs: I,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<protobuf::LogicalExprNode>, Error>
where
    I: IntoIterator<Item = &'a Expr>,
{
    exprs
        .into_iter()
        .map(|expr| serialize_expr(expr, codec))
        .collect::<Result<Vec<_>, Error>>()
}

pub fn serialize_expr(
    expr: &Expr,
    codec: &dyn LogicalExtensionCodec,
) -> Result<protobuf::LogicalExprNode, Error> {
    use protobuf::logical_expr_node::ExprType;

    let expr_node = match expr {
        Expr::Column(c) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Column(c.into())),
        },
        Expr::Alias(Alias {
            expr,
            relation,
            name,
            metadata,
        }) => {
            let alias = Box::new(protobuf::AliasNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                relation: relation
                    .to_owned()
                    .map(|r| vec![r.into()])
                    .unwrap_or(vec![]),
                alias: name.to_owned(),
                metadata: metadata
                    .as_ref()
                    .map(|m| m.to_hashmap())
                    .unwrap_or(HashMap::new()),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Alias(alias)),
            }
        }
        Expr::Literal(value, _) => {
            let pb_value: protobuf::ScalarValue = value.try_into()?;
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Literal(pb_value)),
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Try to linerize a nested binary expression tree of the same operator
            // into a flat vector of expressions.
            let mut exprs = vec![right.as_ref()];
            let mut current_expr = left.as_ref();
            while let Expr::BinaryExpr(BinaryExpr {
                left,
                op: current_op,
                right,
            }) = current_expr
            {
                if current_op == op {
                    exprs.push(right.as_ref());
                    current_expr = left.as_ref();
                } else {
                    break;
                }
            }
            exprs.push(current_expr);

            let binary_expr = protobuf::BinaryExprNode {
                // We need to reverse exprs since operands are expected to be
                // linearized from left innermost to right outermost (but while
                // traversing the chain we do the exact opposite).
                operands: serialize_exprs(exprs.into_iter().rev(), codec)?,
                op: format!("{op:?}"),
            };
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::BinaryExpr(binary_expr)),
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if *case_insensitive {
                let pb = Box::new(protobuf::ILikeNode {
                    negated: *negated,
                    expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                    pattern: Some(Box::new(serialize_expr(pattern.as_ref(), codec)?)),
                    escape_char: escape_char.map(|ch| ch.to_string()).unwrap_or_default(),
                });

                protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Ilike(pb)),
                }
            } else {
                let pb = Box::new(protobuf::LikeNode {
                    negated: *negated,
                    expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                    pattern: Some(Box::new(serialize_expr(pattern.as_ref(), codec)?)),
                    escape_char: escape_char.map(|ch| ch.to_string()).unwrap_or_default(),
                });

                protobuf::LogicalExprNode {
                    expr_type: Some(ExprType::Like(pb)),
                }
            }
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive: _,
        }) => {
            let pb = Box::new(protobuf::SimilarToNode {
                negated: *negated,
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                pattern: Some(Box::new(serialize_expr(pattern.as_ref(), codec)?)),
                escape_char: escape_char.map(|ch| ch.to_string()).unwrap_or_default(),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::SimilarTo(pb)),
            }
        }
        Expr::WindowFunction(window_fun) => {
            let expr::WindowFunction {
                fun,
                params:
                    expr::WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        null_treatment,
                        distinct,
                        filter,
                    },
            } = window_fun.as_ref();
            let mut buf = Vec::new();
            let window_function = match fun {
                WindowFunctionDefinition::AggregateUDF(aggr_udf) => {
                    let _ = codec.try_encode_udaf(aggr_udf, &mut buf);
                    protobuf::window_expr_node::WindowFunction::Udaf(
                        aggr_udf.name().to_string(),
                    )
                }
                WindowFunctionDefinition::WindowUDF(window_udf) => {
                    let _ = codec.try_encode_udwf(window_udf, &mut buf);
                    protobuf::window_expr_node::WindowFunction::Udwf(
                        window_udf.name().to_string(),
                    )
                }
            };
            let fun_definition = (!buf.is_empty()).then_some(buf);
            let partition_by = serialize_exprs(partition_by, codec)?;
            let order_by = serialize_sorts(order_by, codec)?;

            let window_frame = Some(protobuf::WindowFrame::try_from(window_frame)?);

            let window_expr = protobuf::WindowExprNode {
                exprs: serialize_exprs(args, codec)?,
                window_function: Some(window_function),
                partition_by,
                order_by,
                window_frame,
                distinct: *distinct,
                filter: match filter {
                    Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                    None => None,
                },
                null_treatment: null_treatment
                    .map(|nt| protobuf::NullTreatment::from(nt).into()),
                fun_definition,
            };
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::WindowExpr(Box::new(window_expr))),
            }
        }
        Expr::AggregateFunction(expr::AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
        }) => {
            let mut buf = Vec::new();
            let _ = codec.try_encode_udaf(func, &mut buf);
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::AggregateUdfExpr(Box::new(
                    protobuf::AggregateUdfExprNode {
                        fun_name: func.name().to_string(),
                        args: serialize_exprs(args, codec)?,
                        distinct: *distinct,
                        filter: match filter {
                            Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                            None => None,
                        },
                        order_by: serialize_sorts(order_by, codec)?,
                        fun_definition: (!buf.is_empty()).then_some(buf),
                        null_treatment: null_treatment
                            .map(|nt| protobuf::NullTreatment::from(nt).into()),
                    },
                ))),
            }
        }

        Expr::ScalarVariable(_, _) => {
            return Err(Error::Plan(
                "Proto serialization error: Scalar Variable not supported".to_string(),
            ));
        }
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let mut buf = Vec::new();
            let _ = codec.try_encode_udf(func, &mut buf);
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::ScalarUdfExpr(protobuf::ScalarUdfExprNode {
                    fun_name: func.name().to_string(),
                    fun_definition: (!buf.is_empty()).then_some(buf),
                    args: serialize_exprs(args, codec)?,
                })),
            }
        }
        Expr::Not(expr) => {
            let expr = Box::new(protobuf::Not {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::NotExpr(expr)),
            }
        }
        Expr::IsNull(expr) => {
            let expr = Box::new(protobuf::IsNull {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNullExpr(expr)),
            }
        }
        Expr::IsNotNull(expr) => {
            let expr = Box::new(protobuf::IsNotNull {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotNullExpr(expr)),
            }
        }
        Expr::IsTrue(expr) => {
            let expr = Box::new(protobuf::IsTrue {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsTrue(expr)),
            }
        }
        Expr::IsFalse(expr) => {
            let expr = Box::new(protobuf::IsFalse {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsFalse(expr)),
            }
        }
        Expr::IsUnknown(expr) => {
            let expr = Box::new(protobuf::IsUnknown {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsUnknown(expr)),
            }
        }
        Expr::IsNotTrue(expr) => {
            let expr = Box::new(protobuf::IsNotTrue {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotTrue(expr)),
            }
        }
        Expr::IsNotFalse(expr) => {
            let expr = Box::new(protobuf::IsNotFalse {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotFalse(expr)),
            }
        }
        Expr::IsNotUnknown(expr) => {
            let expr = Box::new(protobuf::IsNotUnknown {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::IsNotUnknown(expr)),
            }
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = Box::new(protobuf::BetweenNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                negated: *negated,
                low: Some(Box::new(serialize_expr(low.as_ref(), codec)?)),
                high: Some(Box::new(serialize_expr(high.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Between(expr)),
            }
        }
        Expr::Case(case) => {
            let when_then_expr = case
                .when_then_expr
                .iter()
                .map(|(w, t)| {
                    Ok(protobuf::WhenThen {
                        when_expr: Some(serialize_expr(w.as_ref(), codec)?),
                        then_expr: Some(serialize_expr(t.as_ref(), codec)?),
                    })
                })
                .collect::<Result<Vec<protobuf::WhenThen>, Error>>()?;
            let expr = Box::new(protobuf::CaseNode {
                expr: match &case.expr {
                    Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                    None => None,
                },
                when_then_expr,
                else_expr: match &case.else_expr {
                    Some(e) => Some(Box::new(serialize_expr(e.as_ref(), codec)?)),
                    None => None,
                },
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Case(expr)),
            }
        }
        Expr::Cast(Cast { expr, field }) => {
            let expr = Box::new(protobuf::CastNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                arrow_type: Some(field.data_type().try_into()?),
                metadata: field.metadata().clone(),
                nullable: Some(field.is_nullable()),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Cast(expr)),
            }
        }
        Expr::TryCast(TryCast { expr, field }) => {
            let expr = Box::new(protobuf::TryCastNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                arrow_type: Some(field.data_type().try_into()?),
                metadata: field.metadata().clone(),
                nullable: Some(field.is_nullable()),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::TryCast(expr)),
            }
        }
        Expr::Negative(expr) => {
            let expr = Box::new(protobuf::NegativeNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Negative(expr)),
            }
        }
        Expr::Unnest(Unnest { expr }) => {
            let expr = protobuf::Unnest {
                exprs: vec![serialize_expr(expr.as_ref(), codec)?],
            };
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::Unnest(expr)),
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = Box::new(protobuf::InListNode {
                expr: Some(Box::new(serialize_expr(expr.as_ref(), codec)?)),
                list: serialize_exprs(list, codec)?,
                negated: *negated,
            });
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::InList(expr)),
            }
        }
        #[expect(deprecated)]
        Expr::Wildcard { qualifier, .. } => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Wildcard(protobuf::Wildcard {
                qualifier: qualifier.to_owned().map(protobuf::TableReference::from),
            })),
        },
        Expr::ScalarSubquery(subquery) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::ScalarSubqueryExpr(Box::new(
                protobuf::ScalarSubqueryExprNode {
                    subquery: Some(Box::new(serialize_subquery(subquery, codec)?)),
                },
            ))),
        },
        Expr::InSubquery(_)
        | Expr::Exists(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::SetComparison(_) => {
            return Err(Error::Plan(format!(
                "Proto serialization error: {expr} is not yet supported"
            )));
        }
        Expr::GroupingSet(GroupingSet::Cube(exprs)) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Cube(CubeNode {
                expr: serialize_exprs(exprs, codec)?,
            })),
        },
        Expr::GroupingSet(GroupingSet::Rollup(exprs)) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Rollup(RollupNode {
                expr: serialize_exprs(exprs, codec)?,
            })),
        },
        Expr::GroupingSet(GroupingSet::GroupingSets(exprs)) => {
            protobuf::LogicalExprNode {
                expr_type: Some(ExprType::GroupingSet(GroupingSetNode {
                    expr: exprs
                        .iter()
                        .map(|expr_list| {
                            Ok(LogicalExprList {
                                expr: serialize_exprs(expr_list, codec)?,
                            })
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                })),
            }
        }
        Expr::Placeholder(Placeholder { id, field }) => protobuf::LogicalExprNode {
            expr_type: Some(ExprType::Placeholder(PlaceholderNode {
                id: id.clone(),
                data_type: match field {
                    Some(field) => Some(field.data_type().try_into()?),
                    None => None,
                },
                nullable: field.as_ref().map(|f| f.is_nullable()),
                metadata: field
                    .as_ref()
                    .map(|f| f.metadata().clone())
                    .unwrap_or(HashMap::new()),
            })),
        },
        Expr::HigherOrderFunction(_) | Expr::Lambda(_) | Expr::LambdaVariable(_) => {
            return Err(Error::Plan(
                "Proto serialization error: Lambda not implemented".to_string(),
            ));
        }
    };

    Ok(expr_node)
}

fn serialize_subquery(
    subquery: &Subquery,
    codec: &dyn LogicalExtensionCodec,
) -> Result<protobuf::SubqueryNode, Error> {
    let plan = LogicalPlanNode::try_from_logical_plan(&subquery.subquery, codec)
        .map_err(|e| Error::Plan(e.to_string()))?;
    let outer_ref_columns = serialize_exprs(&subquery.outer_ref_columns, codec)?;
    Ok(protobuf::SubqueryNode {
        subquery: Some(Box::new(plan)),
        outer_ref_columns,
    })
}

pub fn serialize_sorts<'a, I>(
    sorts: I,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<protobuf::SortExprNode>, Error>
where
    I: IntoIterator<Item = &'a SortExpr>,
{
    sorts
        .into_iter()
        .map(|sort| {
            let SortExpr {
                expr,
                asc,
                nulls_first,
            } = sort;
            Ok(protobuf::SortExprNode {
                expr: Some(serialize_expr(expr, codec)?),
                asc: *asc,
                nulls_first: *nulls_first,
            })
        })
        .collect::<Result<Vec<_>, Error>>()
}
