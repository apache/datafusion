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

mod aggregate_function;
mod cast;
mod extended_expression;
mod field_reference;
mod function_arguments;
mod if_then;
mod literal;
mod scalar_function;
mod singular_or_list;
mod subquery;
mod window_function;

pub use aggregate_function::*;
pub use cast::*;
pub use extended_expression::*;
pub use field_reference::*;
pub use function_arguments::*;
pub use if_then::*;
pub use literal::*;
pub use scalar_function::*;
pub use singular_or_list::*;
pub use subquery::*;
pub use window_function::*;

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::common::{substrait_err, DFSchema};
use datafusion::logical_expr::Expr;
use substrait::proto::expression::RexType;
use substrait::proto::Expression;

/// Convert Substrait Rex to DataFusion Expr
pub async fn from_substrait_rex(
    consumer: &impl SubstraitConsumer,
    expression: &Expression,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    match &expression.rex_type {
        Some(t) => match t {
            RexType::Literal(expr) => consumer.consume_literal(expr).await,
            RexType::Selection(expr) => {
                consumer.consume_field_reference(expr, input_schema).await
            }
            RexType::ScalarFunction(expr) => {
                consumer.consume_scalar_function(expr, input_schema).await
            }
            RexType::WindowFunction(expr) => {
                consumer.consume_window_function(expr, input_schema).await
            }
            RexType::IfThen(expr) => consumer.consume_if_then(expr, input_schema).await,
            RexType::SwitchExpression(expr) => {
                consumer.consume_switch(expr, input_schema).await
            }
            RexType::SingularOrList(expr) => {
                consumer.consume_singular_or_list(expr, input_schema).await
            }

            RexType::MultiOrList(expr) => {
                consumer.consume_multi_or_list(expr, input_schema).await
            }

            RexType::Cast(expr) => {
                consumer.consume_cast(expr.as_ref(), input_schema).await
            }

            RexType::Subquery(expr) => {
                consumer.consume_subquery(expr.as_ref(), input_schema).await
            }
            RexType::Nested(expr) => consumer.consume_nested(expr, input_schema).await,
            RexType::Enum(expr) => consumer.consume_enum(expr, input_schema).await,
            RexType::DynamicParameter(expr) => {
                consumer.consume_dynamic_parameter(expr, input_schema).await
            }
        },
        None => substrait_err!("Expression must set rex_type: {:?}", expression),
    }
}

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    consumer: &impl SubstraitConsumer,
    exprs: &Vec<Expression>,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Vec<Expr>> {
    let mut expressions: Vec<Expr> = vec![];
    for expr in exprs {
        let expression = consumer.consume_expression(expr, input_schema).await?;
        expressions.push(expression);
    }
    Ok(expressions)
}

#[cfg(test)]
mod tests {
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::utils::tests::test_consumer;
    use crate::logical_plan::consumer::*;
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::Expr;
    use substrait::proto::expression::window_function::BoundsType;
    use substrait::proto::expression::RexType;
    use substrait::proto::Expression;

    #[tokio::test]
    async fn window_function_with_range_unit_and_no_order_by(
    ) -> datafusion::common::Result<()> {
        let substrait = Expression {
            rex_type: Some(RexType::WindowFunction(
                substrait::proto::expression::WindowFunction {
                    function_reference: 0,
                    bounds_type: BoundsType::Range as i32,
                    sorts: vec![],
                    ..Default::default()
                },
            )),
        };

        let mut consumer = test_consumer();

        // Just registering a single function (index 0) so that the plan
        // does not throw a "function not found" error.
        let mut extensions = Extensions::default();
        extensions.register_function("count".to_string());
        consumer.extensions = &extensions;

        match from_substrait_rex(&consumer, &substrait, &DFSchema::empty()).await? {
            Expr::WindowFunction(window_function) => {
                assert_eq!(window_function.params.order_by.len(), 1)
            }
            _ => panic!("expr was not a WindowFunction"),
        };

        Ok(())
    }

    #[tokio::test]
    async fn window_function_with_count() -> datafusion::common::Result<()> {
        let substrait = Expression {
            rex_type: Some(RexType::WindowFunction(
                substrait::proto::expression::WindowFunction {
                    function_reference: 0,
                    ..Default::default()
                },
            )),
        };

        let mut consumer = test_consumer();

        let mut extensions = Extensions::default();
        extensions.register_function("count".to_string());
        consumer.extensions = &extensions;

        match from_substrait_rex(&consumer, &substrait, &DFSchema::empty()).await? {
            Expr::WindowFunction(window_function) => {
                assert_eq!(window_function.params.args.len(), 1)
            }
            _ => panic!("expr was not a WindowFunction"),
        };

        Ok(())
    }
}
