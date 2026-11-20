// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::literal::name_struct_literals_like_first_struct_expr;
use crate::logical_plan::consumer::{SubstraitConsumer, SubstraitConsumerContext};
use datafusion::common::{DFSchema, not_impl_err, substrait_err};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::Expr;
use substrait::proto::expression::Nested;
use substrait::proto::expression::nested::NestedType;

/// Converts a Substrait [Nested] expression into a DataFusion [Expr].
///
/// Substrait Nested expressions represent complex type constructors (list, struct, map)
/// where elements are full expressions rather than just literals. This is used by
/// producers that emit `Nested { list: ... }` for array construction, as opposed to
/// `Literal { list: ... }` which only supports scalar values.
pub async fn from_nested(
    consumer: &impl SubstraitConsumer,
    nested: &Nested,
    input_schema: &DFSchema,
    context: &SubstraitConsumerContext,
) -> datafusion::common::Result<Expr> {
    let Some(nested_type) = &nested.nested_type else {
        return substrait_err!("Nested expression requires a nested_type");
    };

    match nested_type {
        NestedType::List(list) => {
            if list.values.is_empty() {
                return substrait_err!(
                    "Empty Nested lists are not supported; use Literal.empty_list instead"
                );
            }

            let mut args = Vec::with_capacity(list.values.len());
            for value in &list.values {
                args.push(
                    consumer
                        .consume_expression(value, input_schema, context)
                        .await?,
                );
            }
            // the elements of a list share one type, so a struct literal element takes the
            // field names of a struct column element, as in `[s, {c0: 1, c1: 'x'}]`
            name_struct_literals_like_first_struct_expr(&mut args, input_schema);

            let make_array_udf = consumer.get_function_registry().udf("make_array")?;
            Ok(Expr::ScalarFunction(
                datafusion::logical_expr::expr::ScalarFunction::new_udf(
                    make_array_udf,
                    args,
                ),
            ))
        }
        NestedType::Struct(_) => {
            not_impl_err!("Nested struct expressions are not yet supported")
        }
        NestedType::Map(_) => {
            not_impl_err!("Nested map expressions are not yet supported")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::utils::tests::test_consumer;
    use crate::logical_plan::producer::substrait_field_ref;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::logical_expr::ExprSchemable;
    use substrait::proto::expression::Literal;
    use substrait::proto::expression::nested::List;
    use substrait::proto::{self, Expression};

    fn make_i64_literal(value: i64) -> Expression {
        Expression {
            rex_type: Some(proto::expression::RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(proto::expression::literal::LiteralType::I64(value)),
            })),
        }
    }

    #[tokio::test]
    async fn nested_list_struct_literal_takes_column_names()
    -> datafusion::common::Result<()> {
        // `[s, {0}]` with `s: Struct{a}`: the literal element is `Struct{c0}` until renamed
        let struct_type =
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Int64, true)]));
        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            "s",
            struct_type.clone(),
            true,
        )]))?;
        let struct_literal = Expression {
            rex_type: Some(proto::expression::RexType::Literal(Literal {
                nullable: false,
                type_variation_reference: 0,
                literal_type: Some(proto::expression::literal::LiteralType::Struct(
                    proto::expression::literal::Struct {
                        fields: vec![Literal {
                            nullable: false,
                            type_variation_reference: 0,
                            literal_type: Some(
                                proto::expression::literal::LiteralType::I64(0),
                            ),
                        }],
                    },
                )),
            })),
        };
        let nested = Nested {
            nullable: false,
            type_variation_reference: 0,
            nested_type: Some(NestedType::List(List {
                values: vec![substrait_field_ref(0)?, struct_literal],
            })),
        };

        let expr = from_nested(
            &test_consumer(),
            &nested,
            &schema,
            &SubstraitConsumerContext::default(),
        )
        .await?;
        let Expr::ScalarFunction(make_array) = expr else {
            panic!("expected a make_array call, got {expr}");
        };
        assert_eq!(make_array.args[1].get_type(&schema)?, struct_type);
        Ok(())
    }

    #[tokio::test]
    async fn nested_list_with_literals() -> datafusion::common::Result<()> {
        let consumer = test_consumer();
        let schema = DFSchema::empty();
        let nested = Nested {
            nullable: false,
            type_variation_reference: 0,
            nested_type: Some(NestedType::List(List {
                values: vec![
                    make_i64_literal(1),
                    make_i64_literal(2),
                    make_i64_literal(3),
                ],
            })),
        };

        let expr = from_nested(
            &consumer,
            &nested,
            &schema,
            &SubstraitConsumerContext::default(),
        )
        .await?;
        assert_eq!(
            format!("{expr}"),
            "make_array(Int64(1), Int64(2), Int64(3))"
        );

        Ok(())
    }

    #[tokio::test]
    async fn nested_list_empty_rejected() -> datafusion::common::Result<()> {
        let consumer = test_consumer();
        let schema = DFSchema::empty();
        let nested = Nested {
            nullable: true,
            type_variation_reference: 0,
            nested_type: Some(NestedType::List(List { values: vec![] })),
        };

        let result = from_nested(
            &consumer,
            &nested,
            &schema,
            &SubstraitConsumerContext::default(),
        )
        .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Empty Nested lists are not supported")
        );

        Ok(())
    }

    #[tokio::test]
    async fn nested_missing_type() -> datafusion::common::Result<()> {
        let consumer = test_consumer();
        let schema = DFSchema::empty();
        let nested = Nested {
            nullable: false,
            type_variation_reference: 0,
            nested_type: None,
        };

        let result = from_nested(
            &consumer,
            &nested,
            &schema,
            &SubstraitConsumerContext::default(),
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nested_type"));

        Ok(())
    }
}
