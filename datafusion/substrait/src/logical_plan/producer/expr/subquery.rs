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

use crate::logical_plan::producer::{SubstraitProducer, negate_if_needed};
use datafusion::common::{DFSchemaRef, substrait_err};
use datafusion::logical_expr::{Operator, Subquery};
use datafusion::logical_expr::expr::{Exists, InSubquery, SetComparison, SetQuantifier};
use substrait::proto::Expression;
use substrait::proto::expression::RexType;
use substrait::proto::expression::subquery::{InPredicate, Scalar, SetPredicate};
use substrait::proto::expression::subquery::set_comparison::{ComparisonOp, ReductionOp};

pub fn from_in_subquery(
    producer: &mut impl SubstraitProducer,
    subquery: &InSubquery,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let InSubquery {
        expr,
        subquery,
        negated,
    } = subquery;
    let substrait_expr = producer.handle_expr(expr, schema)?;

    let subquery_plan = producer.handle_plan(subquery.subquery.as_ref())?;

    let substrait_subquery = Expression {
        rex_type: Some(RexType::Subquery(Box::new(
            substrait::proto::expression::Subquery {
                subquery_type: Some(
                    substrait::proto::expression::subquery::SubqueryType::InPredicate(
                        Box::new(InPredicate {
                            needles: (vec![substrait_expr]),
                            haystack: Some(subquery_plan),
                        }),
                    ),
                ),
            },
        ))),
    };
    Ok(negate_if_needed(producer, substrait_subquery, *negated))
}

fn comparison_op_to_proto(op: &Operator) -> datafusion::common::Result<ComparisonOp> {
    match op {
        Operator::Eq => Ok(ComparisonOp::Eq),
        Operator::NotEq => Ok(ComparisonOp::Ne),
        Operator::Lt => Ok(ComparisonOp::Lt),
        Operator::Gt => Ok(ComparisonOp::Gt),
        Operator::LtEq => Ok(ComparisonOp::Le),
        Operator::GtEq => Ok(ComparisonOp::Ge),
        _ => substrait_err!("Unsupported operator {op:?} for SetComparison subquery"),
    }
}

fn reduction_op_to_proto(
    quantifier: &SetQuantifier,
) -> datafusion::common::Result<ReductionOp> {
    match quantifier {
        SetQuantifier::Any => Ok(ReductionOp::Any),
        SetQuantifier::All => Ok(ReductionOp::All),
    }
}

pub fn from_set_comparison(
    producer: &mut impl SubstraitProducer,
    set_comparison: &SetComparison,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let comparison_op = comparison_op_to_proto(&set_comparison.op)? as i32;
    let reduction_op = reduction_op_to_proto(&set_comparison.quantifier)? as i32;
    let left = producer.handle_expr(set_comparison.expr.as_ref(), schema)?;
    let subquery_plan =
        producer.handle_plan(set_comparison.subquery.subquery.as_ref())?;

    Ok(Expression {
        rex_type: Some(RexType::Subquery(Box::new(
            substrait::proto::expression::Subquery {
                subquery_type: Some(
                    substrait::proto::expression::subquery::SubqueryType::SetComparison(
                        Box::new(substrait::proto::expression::subquery::SetComparison {
                            reduction_op,
                            comparison_op,
                            left: Some(Box::new(left)),
                            right: Some(subquery_plan),
                        }),
                    ),
                ),
            },
        ))),
    })
}

/// Convert DataFusion ScalarSubquery to Substrait Scalar subquery type
pub fn from_scalar_subquery(
    producer: &mut impl SubstraitProducer,
    subquery: &Subquery,
    _schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let subquery_plan = producer.handle_plan(subquery.subquery.as_ref())?;

    Ok(Expression {
        rex_type: Some(RexType::Subquery(Box::new(
            substrait::proto::expression::Subquery {
                subquery_type: Some(
                    substrait::proto::expression::subquery::SubqueryType::Scalar(
                        Box::new(Scalar {
                            input: Some(subquery_plan),
                        }),
                    ),
                ),
            },
        ))),
    })
}

/// Convert DataFusion Exists expression to Substrait SetPredicate subquery type
pub fn from_exists(
    producer: &mut impl SubstraitProducer,
    exists: &Exists,
    _schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let subquery_plan = producer.handle_plan(exists.subquery.subquery.as_ref())?;

    let substrait_exists = Expression {
        rex_type: Some(RexType::Subquery(Box::new(
            substrait::proto::expression::Subquery {
                subquery_type: Some(
                    substrait::proto::expression::subquery::SubqueryType::SetPredicate(
                        Box::new(SetPredicate {
                            predicate_op: substrait::proto::expression::subquery::set_predicate::PredicateOp::Exists as i32,
                            tuples: Some(subquery_plan),
                        }),
                    ),
                ),
            },
        ))),
    };

    Ok(negate_if_needed(producer, substrait_exists, exists.negated))
}
