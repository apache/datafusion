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

//! Simplifier for Physical Expressions

use arrow::datatypes::Schema;
use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    Result,
};
use std::sync::Arc;

use crate::{simplifier::not::simplify_not_expr, PhysicalExpr};

pub mod not;
pub mod unwrap_cast;

const MAX_LOOP_COUNT: usize = 5;

/// Simplifies physical expressions by applying various optimizations
///
/// This can be useful after adapting expressions from a table schema
/// to a file schema. For example, casts added to match the types may
/// potentially be unwrapped.
pub struct PhysicalExprSimplifier<'a> {
    schema: &'a Schema,
}

impl<'a> PhysicalExprSimplifier<'a> {
    /// Create a new physical expression simplifier
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// Simplify a physical expression
    pub fn simplify(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let mut current_expr = expr;
        let mut count = 0;
        while count < MAX_LOOP_COUNT {
            count += 1;
            let result = current_expr.rewrite(self)?;
            if !result.transformed {
                return Ok(result.data);
            }
            current_expr = result.data;
        }
        Ok(current_expr)
    }
}

impl<'a> TreeNodeRewriter for PhysicalExprSimplifier<'a> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        #[cfg(test)]
        let original_type = node.data_type(self.schema).unwrap();

        // Apply NOT expression simplification first, then unwrap cast optimization
        let rewritten =
            simplify_not_expr(&node, self.schema)?.transform_data(|node| {
                unwrap_cast::unwrap_cast_in_comparison(node, self.schema)
            })?;

        #[cfg(test)]
        assert_eq!(
            rewritten.data.data_type(self.schema).unwrap(),
            original_type,
            "Simplified expression should have the same data type as the original"
        );

        Ok(rewritten)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{
        col, in_list, lit, BinaryExpr, CastExpr, Literal, NotExpr, TryCastExpr,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int64, false),
            Field::new("c3", DataType::Utf8, false),
        ])
    }

    fn not_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
            Field::new("c", DataType::Int32, false),
        ])
    }

    /// Helper function to extract a Literal from a PhysicalExpr
    fn as_literal(expr: &Arc<dyn PhysicalExpr>) -> &Literal {
        expr.as_any()
            .downcast_ref::<Literal>()
            .unwrap_or_else(|| panic!("Expected Literal, got: {expr}"))
    }

    /// Helper function to extract a BinaryExpr from a PhysicalExpr
    fn as_binary(expr: &Arc<dyn PhysicalExpr>) -> &BinaryExpr {
        expr.as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap_or_else(|| panic!("Expected BinaryExpr, got: {expr}"))
    }

    /// Assert that simplifying `input` produces `expected`
    fn assert_not_simplify(
        simplifier: &mut PhysicalExprSimplifier,
        input: Arc<dyn PhysicalExpr>,
        expected: Arc<dyn PhysicalExpr>,
    ) {
        let result = simplifier.simplify(Arc::clone(&input)).unwrap();
        assert_eq!(
            &result,
            &expected,
            "Simplification should transform:\n  input: {input}\n  to:    {expected}\n  got:   {result}"
        );
    }

    #[test]
    fn test_simplify() {
        let schema = test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // Create: cast(c2 as INT32) != INT32(99)
        let column_expr = col("c2", &schema).unwrap();
        let cast_expr = Arc::new(CastExpr::new(column_expr, DataType::Int32, None));
        let literal_expr = lit(ScalarValue::Int32(Some(99)));
        let binary_expr =
            Arc::new(BinaryExpr::new(cast_expr, Operator::NotEq, literal_expr));

        // Apply full simplification (uses TreeNodeRewriter)
        let optimized = simplifier.simplify(binary_expr).unwrap();

        let optimized_binary = as_binary(&optimized);

        // Should be optimized to: c2 != INT64(99) (c2 is INT64, literal cast to match)
        let left_expr = optimized_binary.left();
        assert!(
            left_expr.as_any().downcast_ref::<CastExpr>().is_none()
                && left_expr.as_any().downcast_ref::<TryCastExpr>().is_none()
        );
        let right_literal = as_literal(optimized_binary.right());
        assert_eq!(right_literal.value(), &ScalarValue::Int64(Some(99)));
    }

    #[test]
    fn test_nested_expression_simplification() {
        let schema = test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // Create nested expression: (cast(c1 as INT64) > INT64(5)) OR (cast(c2 as INT32) <= INT32(10))
        let c1_expr = col("c1", &schema).unwrap();
        let c1_cast = Arc::new(CastExpr::new(c1_expr, DataType::Int64, None));
        let c1_literal = lit(ScalarValue::Int64(Some(5)));
        let c1_binary = Arc::new(BinaryExpr::new(c1_cast, Operator::Gt, c1_literal));

        let c2_expr = col("c2", &schema).unwrap();
        let c2_cast = Arc::new(CastExpr::new(c2_expr, DataType::Int32, None));
        let c2_literal = lit(ScalarValue::Int32(Some(10)));
        let c2_binary = Arc::new(BinaryExpr::new(c2_cast, Operator::LtEq, c2_literal));

        let or_expr = Arc::new(BinaryExpr::new(c1_binary, Operator::Or, c2_binary));

        // Apply simplification
        let optimized = simplifier.simplify(or_expr).unwrap();

        let or_binary = as_binary(&optimized);

        // Verify left side: c1 > INT32(5)
        let left_binary = as_binary(or_binary.left());
        let left_left_expr = left_binary.left();
        assert!(
            left_left_expr.as_any().downcast_ref::<CastExpr>().is_none()
                && left_left_expr
                    .as_any()
                    .downcast_ref::<TryCastExpr>()
                    .is_none()
        );
        let left_literal = as_literal(left_binary.right());
        assert_eq!(left_literal.value(), &ScalarValue::Int32(Some(5)));

        // Verify right side: c2 <= INT64(10)
        let right_binary = as_binary(or_binary.right());
        let right_left_expr = right_binary.left();
        assert!(
            right_left_expr
                .as_any()
                .downcast_ref::<CastExpr>()
                .is_none()
                && right_left_expr
                    .as_any()
                    .downcast_ref::<TryCastExpr>()
                    .is_none()
        );
        let right_literal = as_literal(right_binary.right());
        assert_eq!(right_literal.value(), &ScalarValue::Int64(Some(10)));
    }

    #[test]
    fn test_double_negation_elimination() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(NOT(c > 5)) -> c > 5
        let inner_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("c", &schema)?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(5))),
        ));
        let inner_not = Arc::new(NotExpr::new(Arc::clone(&inner_expr)));
        let double_not: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(inner_not));

        let expected = inner_expr;
        assert_not_simplify(&mut simplifier, double_not, expected);
        Ok(())
    }

    #[test]
    fn test_not_literal() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(TRUE) -> FALSE
        let not_true = Arc::new(NotExpr::new(lit(ScalarValue::Boolean(Some(true)))));
        let expected = lit(ScalarValue::Boolean(Some(false)));
        assert_not_simplify(&mut simplifier, not_true, expected);

        // NOT(FALSE) -> TRUE
        let not_false = Arc::new(NotExpr::new(lit(ScalarValue::Boolean(Some(false)))));
        let expected = lit(ScalarValue::Boolean(Some(true)));
        assert_not_simplify(&mut simplifier, not_false, expected);

        Ok(())
    }

    #[test]
    fn test_negate_comparison() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(c = 5) -> c != 5
        let not_eq = Arc::new(NotExpr::new(Arc::new(BinaryExpr::new(
            col("c", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Int32(Some(5))),
        ))));
        let expected = Arc::new(BinaryExpr::new(
            col("c", &schema)?,
            Operator::NotEq,
            lit(ScalarValue::Int32(Some(5))),
        ));
        assert_not_simplify(&mut simplifier, not_eq, expected);

        Ok(())
    }

    #[test]
    fn test_demorgans_law_and() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(a AND b) -> NOT a OR NOT b
        let and_expr = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::And,
            col("b", &schema)?,
        ));
        let not_and: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));

        let expected: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(NotExpr::new(col("a", &schema)?)),
            Operator::Or,
            Arc::new(NotExpr::new(col("b", &schema)?)),
        ));
        assert_not_simplify(&mut simplifier, not_and, expected);

        Ok(())
    }

    #[test]
    fn test_demorgans_law_or() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(a OR b) -> NOT a AND NOT b
        let or_expr = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::Or,
            col("b", &schema)?,
        ));
        let not_or: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(or_expr));

        let expected: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(NotExpr::new(col("a", &schema)?)),
            Operator::And,
            Arc::new(NotExpr::new(col("b", &schema)?)),
        ));
        assert_not_simplify(&mut simplifier, not_or, expected);

        Ok(())
    }

    #[test]
    fn test_demorgans_with_comparison_simplification() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(c = 1 AND c = 2) -> c != 1 OR c != 2
        let eq1 = Arc::new(BinaryExpr::new(
            col("c", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Int32(Some(1))),
        ));
        let eq2 = Arc::new(BinaryExpr::new(
            col("c", &schema)?,
            Operator::Eq,
            lit(ScalarValue::Int32(Some(2))),
        ));
        let and_expr = Arc::new(BinaryExpr::new(eq1, Operator::And, eq2));
        let not_and: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));

        let expected: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                col("c", &schema)?,
                Operator::NotEq,
                lit(ScalarValue::Int32(Some(1))),
            )),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                col("c", &schema)?,
                Operator::NotEq,
                lit(ScalarValue::Int32(Some(2))),
            )),
        ));
        assert_not_simplify(&mut simplifier, not_and, expected);

        Ok(())
    }

    #[test]
    fn test_not_of_not_and_not() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(NOT(a) AND NOT(b)) -> a OR b
        let not_a = Arc::new(NotExpr::new(col("a", &schema)?));
        let not_b = Arc::new(NotExpr::new(col("b", &schema)?));
        let and_expr = Arc::new(BinaryExpr::new(not_a, Operator::And, not_b));
        let not_and: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(and_expr));

        let expected: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("a", &schema)?,
            Operator::Or,
            col("b", &schema)?,
        ));
        assert_not_simplify(&mut simplifier, not_and, expected);

        Ok(())
    }

    #[test]
    fn test_not_in_list() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(c IN (1, 2, 3)) -> c NOT IN (1, 2, 3)
        let list = vec![
            lit(ScalarValue::Int32(Some(1))),
            lit(ScalarValue::Int32(Some(2))),
            lit(ScalarValue::Int32(Some(3))),
        ];
        let in_list_expr = in_list(col("c", &schema)?, list.clone(), &false, &schema)?;
        let not_in: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(in_list_expr));

        let expected = in_list(col("c", &schema)?, list, &true, &schema)?;
        assert_not_simplify(&mut simplifier, not_in, expected);

        Ok(())
    }

    #[test]
    fn test_not_not_in_list() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(c NOT IN (1, 2, 3)) -> c IN (1, 2, 3)
        let list = vec![
            lit(ScalarValue::Int32(Some(1))),
            lit(ScalarValue::Int32(Some(2))),
            lit(ScalarValue::Int32(Some(3))),
        ];
        let not_in_list_expr = in_list(col("c", &schema)?, list.clone(), &true, &schema)?;
        let not_not_in: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(not_in_list_expr));

        let expected = in_list(col("c", &schema)?, list, &false, &schema)?;
        assert_not_simplify(&mut simplifier, not_not_in, expected);

        Ok(())
    }

    #[test]
    fn test_double_not_in_list() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // NOT(NOT(c IN (1, 2, 3))) -> c IN (1, 2, 3)
        let list = vec![
            lit(ScalarValue::Int32(Some(1))),
            lit(ScalarValue::Int32(Some(2))),
            lit(ScalarValue::Int32(Some(3))),
        ];
        let in_list_expr = in_list(col("c", &schema)?, list.clone(), &false, &schema)?;
        let not_in = Arc::new(NotExpr::new(in_list_expr));
        let double_not: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(not_in));

        let expected = in_list(col("c", &schema)?, list, &false, &schema)?;
        assert_not_simplify(&mut simplifier, double_not, expected);

        Ok(())
    }

    #[test]
    fn test_deeply_nested_not() -> Result<()> {
        let schema = not_test_schema();
        let mut simplifier = PhysicalExprSimplifier::new(&schema);

        // Create a deeply nested NOT expression: NOT(NOT(NOT(...NOT(c > 5)...)))
        // This tests that we don't get stack overflow with many nested NOTs.
        // With recursive_protection enabled (default), this should work by
        // automatically growing the stack as needed.
        let inner_expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            col("c", &schema)?,
            Operator::Gt,
            lit(ScalarValue::Int32(Some(5))),
        ));

        let mut expr = Arc::clone(&inner_expr);
        // Create 200 layers of NOT to test deep recursion handling
        for _ in 0..200 {
            expr = Arc::new(NotExpr::new(expr));
        }

        // With 200 NOTs (even number), should simplify back to the original expression
        let expected = inner_expr;
        assert_not_simplify(&mut simplifier, Arc::clone(&expr), expected);

        // Manually dismantle the deep input expression to avoid Stack Overflow on Drop
        // If we just let `expr` go out of scope, Rust's recursive Drop will blow the stack
        // even with recursive_protection, because Drop doesn't use the #[recursive] attribute.
        // We peel off layers one by one to avoid deep recursion in Drop.
        while let Some(not_expr) = expr.as_any().downcast_ref::<NotExpr>() {
            // Clone the child (Arc increment).
            // Now child has 2 refs: one in parent, one in `child`.
            let child = Arc::clone(not_expr.arg());

            // Reassign `expr` to `child`.
            // This drops the old `expr` (Parent).
            // Parent refcount -> 0, Parent is dropped.
            // Parent drops its reference to Child.
            // Child refcount decrements 2 -> 1.
            // Child is NOT dropped recursively because we still hold it in `expr`
            expr = child;
        }

        Ok(())
    }
}
