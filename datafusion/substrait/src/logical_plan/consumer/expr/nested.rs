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
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, ScalarValue, not_impl_err, substrait_err};
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
///
/// When all elements are literals, the result is a scalar literal (e.g. `ScalarValue::List`).
/// When elements include non-literal expressions (e.g. column references), the result uses
/// the `make_array` / `named_struct` UDFs to construct the value at execution time.
pub async fn from_nested(
    consumer: &impl SubstraitConsumer,
    nested: &Nested,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let Some(nested_type) = &nested.nested_type else {
        return substrait_err!("Nested expression requires a nested_type");
    };

    match nested_type {
        NestedType::List(list) => {
            let mut exprs = Vec::with_capacity(list.values.len());
            for value in &list.values {
                exprs.push(consumer.consume_expression(value, input_schema).await?);
            }

            // Fast path: if all expressions are literals, construct a ScalarValue::List directly
            let all_literals: Option<Vec<ScalarValue>> = exprs
                .iter()
                .map(|e| match e {
                    Expr::Literal(sv, _) => Some(sv.clone()),
                    _ => None,
                })
                .collect();

            if let Some(literals) = all_literals {
                // For empty lists, use Null as the element type. A surrounding Cast
                // in the Substrait plan can refine the type if needed.
                let element_type = literals
                    .first()
                    .map(|l| l.data_type())
                    .unwrap_or(DataType::Null);
                let arr = ScalarValue::new_list_nullable(&literals, &element_type);
                Ok(Expr::Literal(ScalarValue::List(arr), None))
            } else {
                // General case: use make_array UDF for non-literal expressions
                let make_array_udf =
                    consumer.get_function_registry().udf("make_array")?;
                Ok(Expr::ScalarFunction(
                    datafusion::logical_expr::expr::ScalarFunction::new_udf(
                        make_array_udf,
                        exprs,
                    ),
                ))
            }
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
    use datafusion::arrow::array::Array;
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

        let expr = from_nested(&consumer, &nested, &schema).await?;
        match &expr {
            Expr::Literal(ScalarValue::List(arr), _) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr.value_length(0), 3);
            }
            other => panic!("Expected Expr::Literal(List), got: {other:?}"),
        }
        assert_eq!(format!("{expr}"), "List([1, 2, 3])");

        Ok(())
    }

    #[tokio::test]
    async fn nested_list_empty() -> datafusion::common::Result<()> {
        let consumer = test_consumer();
        let schema = DFSchema::empty();
        let nested = Nested {
            nullable: true,
            type_variation_reference: 0,
            nested_type: Some(NestedType::List(List { values: vec![] })),
        };

        let expr = from_nested(&consumer, &nested, &schema).await?;
        match &expr {
            Expr::Literal(ScalarValue::List(arr), _) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr.value_length(0), 0);
            }
            other => panic!("Expected Expr::Literal(List), got: {other:?}"),
        }
        assert_eq!(format!("{expr}"), "List([])");

        Ok(())
    }

    #[tokio::test]
    async fn nested_list_missing_type() -> datafusion::common::Result<()> {
        let consumer = test_consumer();
        let schema = DFSchema::empty();
        let nested = Nested {
            nullable: false,
            type_variation_reference: 0,
            nested_type: None,
        };

        let result = from_nested(&consumer, &nested, &schema).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nested_type"),);

        Ok(())
    }
}
