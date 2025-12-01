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
        Ok(expr.rewrite(self)?.data)
    }
}

impl<'a> TreeNodeRewriter for PhysicalExprSimplifier<'a> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        // Apply NOT expression simplification first
        let not_expr_simplified = simplify_not_expr(&node, self.schema)?;
        let node = not_expr_simplified.data;
        let transformed = not_expr_simplified.transformed;

        // Apply unwrap cast optimization
        #[cfg(test)]
        let original_type = node.data_type(self.schema).unwrap();
        let unwrapped = unwrap_cast::unwrap_cast_in_comparison(node, self.schema)?;
        #[cfg(test)]
        assert_eq!(
            unwrapped.data.data_type(self.schema).unwrap(),
            original_type,
            "Simplified expression should have the same data type as the original"
        );
        // Combine transformation results
        let final_transformed = transformed || unwrapped.transformed;
        Ok(Transformed::new_transformed(
            unwrapped.data,
            final_transformed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{col, lit, BinaryExpr, CastExpr, Literal, TryCastExpr};
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

        let optimized_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Should be optimized to: c2 != INT64(99) (c2 is INT64, literal cast to match)
        let left_expr = optimized_binary.left();
        assert!(
            left_expr.as_any().downcast_ref::<CastExpr>().is_none()
                && left_expr.as_any().downcast_ref::<TryCastExpr>().is_none()
        );
        let right_literal = optimized_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
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

        let or_binary = optimized.as_any().downcast_ref::<BinaryExpr>().unwrap();

        // Verify left side: c1 > INT32(5)
        let left_binary = or_binary
            .left()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();
        let left_left_expr = left_binary.left();
        assert!(
            left_left_expr.as_any().downcast_ref::<CastExpr>().is_none()
                && left_left_expr
                    .as_any()
                    .downcast_ref::<TryCastExpr>()
                    .is_none()
        );
        let left_literal = left_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(left_literal.value(), &ScalarValue::Int32(Some(5)));

        // Verify right side: c2 <= INT64(10)
        let right_binary = or_binary
            .right()
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .unwrap();
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
        let right_literal = right_binary
            .right()
            .as_any()
            .downcast_ref::<Literal>()
            .unwrap();
        assert_eq!(right_literal.value(), &ScalarValue::Int64(Some(10)));
    }
}
