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
pub use field_reference::*;
pub use function_arguments::*;
pub use if_then::*;
pub use literal::*;
pub use scalar_function::*;
pub use singular_or_list::*;
pub use subquery::*;
pub use window_function::*;

use crate::extensions::Extensions;
use crate::logical_plan::consumer::{
    from_substrait_named_struct, rename_field, DefaultSubstraitConsumer,
    SubstraitConsumer,
};
use datafusion::arrow::datatypes::Field;
use datafusion::common::{not_impl_err, plan_err, substrait_err, DFSchema, DFSchemaRef};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, ExprSchemable};
use substrait::proto::expression::RexType;
use substrait::proto::expression_reference::ExprType;
use substrait::proto::{Expression, ExtendedExpression};

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
        None => substrait_err!("Expression must set rex_type: {expression:?}"),
    }
}

/// Convert Substrait ExtendedExpression to ExprContainer
///
/// A Substrait ExtendedExpression message contains one or more expressions,
/// with names for the outputs, and an input schema.  These pieces are all included
/// in the ExprContainer.
///
/// This is a top-level message and can be used to send expressions (not plans)
/// between systems.  This is often useful for scenarios like pushdown where filter
/// expressions need to be sent to remote systems.
pub async fn from_substrait_extended_expr(
    state: &SessionState,
    extended_expr: &ExtendedExpression,
) -> datafusion::common::Result<ExprContainer> {
    // Register function extension
    let extensions = Extensions::try_from(&extended_expr.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }

    let consumer = DefaultSubstraitConsumer {
        extensions: &extensions,
        state,
    };

    let input_schema = DFSchemaRef::new(match &extended_expr.base_schema {
        Some(base_schema) => from_substrait_named_struct(&consumer, base_schema),
        None => {
            plan_err!("required property `base_schema` missing from Substrait ExtendedExpression message")
        }
    }?);

    // Parse expressions
    let mut exprs = Vec::with_capacity(extended_expr.referred_expr.len());
    for (expr_idx, substrait_expr) in extended_expr.referred_expr.iter().enumerate() {
        let scalar_expr = match &substrait_expr.expr_type {
            Some(ExprType::Expression(scalar_expr)) => Ok(scalar_expr),
            Some(ExprType::Measure(_)) => {
                not_impl_err!("Measure expressions are not yet supported")
            }
            None => {
                plan_err!("required property `expr_type` missing from Substrait ExpressionReference message")
            }
        }?;
        let expr = consumer
            .consume_expression(scalar_expr, &input_schema)
            .await?;
        let output_field = expr.to_field(&input_schema)?.1;
        let mut names_idx = 0;
        let output_field = rename_field(
            &output_field,
            &substrait_expr.output_names,
            expr_idx,
            &mut names_idx,
        )?;
        exprs.push((expr, output_field));
    }

    Ok(ExprContainer {
        input_schema,
        exprs,
    })
}

/// An ExprContainer is a container for a collection of expressions with a common input schema
///
/// In addition, each expression is associated with a field, which defines the
/// expression's output.  The data type and nullability of the field are calculated from the
/// expression and the input schema.  However the names of the field (and its nested fields) are
/// derived from the Substrait message.
pub struct ExprContainer {
    /// The input schema for the expressions
    pub input_schema: DFSchemaRef,
    /// The expressions
    ///
    /// Each item contains an expression and the field that defines the expected nullability and name of the expr's output
    pub exprs: Vec<(Expr, Field)>,
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
        extensions.register_function("count");
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
        extensions.register_function("count");
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
