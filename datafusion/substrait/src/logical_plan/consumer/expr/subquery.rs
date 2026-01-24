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

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::{DFSchema, Spans, substrait_datafusion_err, substrait_err};
use datafusion::logical_expr::expr::{Exists, InSubquery, SetComparison, SetQuantifier};
use datafusion::logical_expr::{Expr, Operator, Subquery};
use std::sync::Arc;
use substrait::proto::expression as substrait_expression;
use substrait::proto::expression::subquery::SubqueryType;
use substrait::proto::expression::subquery::set_comparison::{ComparisonOp, ReductionOp};
use substrait::proto::expression::subquery::set_predicate::PredicateOp;

pub async fn from_subquery(
    consumer: &impl SubstraitConsumer,
    subquery: &substrait_expression::Subquery,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    match &subquery.subquery_type {
        Some(subquery_type) => match subquery_type {
            SubqueryType::InPredicate(in_predicate) => {
                if in_predicate.needles.len() != 1 {
                    substrait_err!(
                        "InPredicate Subquery type must have exactly one Needle expression"
                    )
                } else {
                    let needle_expr = &in_predicate.needles[0];
                    let haystack_expr = &in_predicate.haystack;
                    if let Some(haystack_expr) = haystack_expr {
                        let haystack_expr = consumer.consume_rel(haystack_expr).await?;
                        let outer_refs = haystack_expr.all_out_ref_exprs();
                        Ok(Expr::InSubquery(InSubquery {
                            expr: Box::new(
                                consumer
                                    .consume_expression(needle_expr, input_schema)
                                    .await?,
                            ),
                            subquery: Subquery {
                                subquery: Arc::new(haystack_expr),
                                outer_ref_columns: outer_refs,
                                spans: Spans::new(),
                            },
                            negated: false,
                        }))
                    } else {
                        substrait_err!(
                            "InPredicate Subquery type must have a Haystack expression"
                        )
                    }
                }
            }
            SubqueryType::Scalar(query) => {
                let plan = consumer
                    .consume_rel(&(query.input.clone()).unwrap_or_default())
                    .await?;
                let outer_ref_columns = plan.all_out_ref_exprs();
                Ok(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(plan),
                    outer_ref_columns,
                    spans: Spans::new(),
                }))
            }
            SubqueryType::SetPredicate(predicate) => {
                match predicate.predicate_op() {
                    // exist
                    PredicateOp::Exists => {
                        let relation = &predicate.tuples;
                        let plan = consumer
                            .consume_rel(&relation.clone().unwrap_or_default())
                            .await?;
                        let outer_ref_columns = plan.all_out_ref_exprs();
                        Ok(Expr::Exists(Exists::new(
                            Subquery {
                                subquery: Arc::new(plan),
                                outer_ref_columns,
                                spans: Spans::new(),
                            },
                            false,
                        )))
                    }
                    other_type => substrait_err!(
                        "unimplemented type {other_type:?} for set predicate"
                    ),
                }
            }
            SubqueryType::SetComparison(comparison) => {
                let left = comparison.left.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("SetComparison requires a left expression")
                })?;
                let right = comparison.right.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("SetComparison requires a right relation")
                })?;
                let reduction_op = match ReductionOp::try_from(comparison.reduction_op) {
                    Ok(ReductionOp::Any) => SetQuantifier::Any,
                    Ok(ReductionOp::All) => SetQuantifier::All,
                    _ => {
                        return substrait_err!(
                            "Unsupported reduction op for SetComparison: {}",
                            comparison.reduction_op
                        );
                    }
                };
                let comparison_op = match ComparisonOp::try_from(comparison.comparison_op)
                {
                    Ok(ComparisonOp::Eq) => Operator::Eq,
                    Ok(ComparisonOp::Ne) => Operator::NotEq,
                    Ok(ComparisonOp::Lt) => Operator::Lt,
                    Ok(ComparisonOp::Gt) => Operator::Gt,
                    Ok(ComparisonOp::Le) => Operator::LtEq,
                    Ok(ComparisonOp::Ge) => Operator::GtEq,
                    _ => {
                        return substrait_err!(
                            "Unsupported comparison op for SetComparison: {}",
                            comparison.comparison_op
                        );
                    }
                };

                let left_expr = consumer.consume_expression(left, input_schema).await?;
                let plan = consumer.consume_rel(right).await?;
                let outer_ref_columns = plan.all_out_ref_exprs();

                Ok(Expr::SetComparison(SetComparison::new(
                    Box::new(left_expr),
                    Subquery {
                        subquery: Arc::new(plan),
                        outer_ref_columns,
                        spans: Spans::new(),
                    },
                    comparison_op,
                    reduction_op,
                )))
            }
        },
        None => {
            substrait_err!("Subquery expression without SubqueryType is not allowed")
        }
    }
}
