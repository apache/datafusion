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

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::Subquery;
use datafusion::logical_expr::expr::{Exists, InSubquery};
use substrait::proto::expression::subquery::{InPredicate, Scalar, SetPredicate};
use substrait::proto::expression::{RexType, ScalarFunction};
use substrait::proto::function_argument::ArgType;
use substrait::proto::{Expression, FunctionArgument};

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
    if *negated {
        let function_anchor = producer.register_function("not".to_string());

        #[expect(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_subquery)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_subquery)
    }
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

    // Handle negated EXISTS (NOT EXISTS)
    if exists.negated {
        let function_anchor = producer.register_function("not".to_string());

        #[expect(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_exists)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_exists)
    }
}
