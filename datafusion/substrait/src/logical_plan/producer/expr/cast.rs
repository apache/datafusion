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

use crate::logical_plan::producer::{SubstraitProducer, to_substrait_type};
use crate::variation_const::DEFAULT_TYPE_VARIATION_REF;
use datafusion::common::{DFSchemaRef, ScalarValue};
use datafusion::logical_expr::{Cast, Expr, TryCast};
use substrait::proto::Expression;
use substrait::proto::expression::cast::FailureBehavior;
use substrait::proto::expression::literal::LiteralType;
use substrait::proto::expression::{Literal, RexType};

pub fn from_cast(
    producer: &mut impl SubstraitProducer,
    cast: &Cast,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let Cast { expr, data_type } = cast;
    // since substrait Null must be typed, so if we see a cast(null, dt), we make it a typed null
    if let Expr::Literal(lit, _) = expr.as_ref() {
        // only the untyped(a null scalar value) null literal need this special handling
        // since all other kind of nulls are already typed and can be handled by substrait
        // e.g. null::<Int32Type> or null::<Utf8Type>
        if *lit == ScalarValue::Null {
            let lit = Literal {
                nullable: true,
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                literal_type: Some(LiteralType::Null(to_substrait_type(
                    producer, data_type, true,
                )?)),
            };
            return Ok(Expression {
                rex_type: Some(RexType::Literal(lit)),
            });
        }
    }
    Ok(Expression {
        rex_type: Some(RexType::Cast(Box::new(
            substrait::proto::expression::Cast {
                r#type: Some(to_substrait_type(producer, data_type, true)?),
                input: Some(Box::new(producer.handle_expr(expr, schema)?)),
                failure_behavior: FailureBehavior::ThrowException.into(),
            },
        ))),
    })
}

pub fn from_try_cast(
    producer: &mut impl SubstraitProducer,
    cast: &TryCast,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let TryCast { expr, data_type } = cast;
    Ok(Expression {
        rex_type: Some(RexType::Cast(Box::new(
            substrait::proto::expression::Cast {
                r#type: Some(to_substrait_type(producer, data_type, true)?),
                input: Some(Box::new(producer.handle_expr(expr, schema)?)),
                failure_behavior: FailureBehavior::ReturnNull.into(),
            },
        ))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::producer::{
        DefaultSubstraitProducer, to_substrait_extended_expr,
    };
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::DFSchema;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::ExprSchemable;
    use substrait::proto::expression_reference::ExprType;

    #[tokio::test]
    async fn fold_cast_null() {
        let state = SessionStateBuilder::default().build();
        let empty_schema = DFSchemaRef::new(DFSchema::empty());
        let field = Field::new("out", DataType::Int32, false);

        let mut producer = DefaultSubstraitProducer::new(&state);

        let expr = Expr::Literal(ScalarValue::Null, None)
            .cast_to(&DataType::Int32, &empty_schema)
            .unwrap();

        let typed_null =
            to_substrait_extended_expr(&[(&expr, &field)], &empty_schema, &state)
                .unwrap();

        if let ExprType::Expression(expr) =
            typed_null.referred_expr[0].expr_type.as_ref().unwrap()
        {
            let lit = Literal {
                nullable: true,
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                literal_type: Some(LiteralType::Null(
                    to_substrait_type(&mut producer, &DataType::Int32, true).unwrap(),
                )),
            };
            let expected = Expression {
                rex_type: Some(RexType::Literal(lit)),
            };
            assert_eq!(*expr, expected);
        } else {
            panic!("Expected expression type");
        }

        // a typed null should not be folded
        let expr = Expr::Literal(ScalarValue::Int64(None), None)
            .cast_to(&DataType::Int32, &empty_schema)
            .unwrap();

        let typed_null =
            to_substrait_extended_expr(&[(&expr, &field)], &empty_schema, &state)
                .unwrap();

        if let ExprType::Expression(expr) =
            typed_null.referred_expr[0].expr_type.as_ref().unwrap()
        {
            let cast_expr = substrait::proto::expression::Cast {
                r#type: Some(
                    to_substrait_type(&mut producer, &DataType::Int32, true).unwrap(),
                ),
                input: Some(Box::new(Expression {
                    rex_type: Some(RexType::Literal(Literal {
                        nullable: true,
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        literal_type: Some(LiteralType::Null(
                            to_substrait_type(&mut producer, &DataType::Int64, true)
                                .unwrap(),
                        )),
                    })),
                })),
                failure_behavior: FailureBehavior::ThrowException as i32,
            };
            let expected = Expression {
                rex_type: Some(RexType::Cast(Box::new(cast_expr))),
            };
            assert_eq!(*expr, expected);
        } else {
            panic!("Expected expression type");
        }
    }
}
