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

use std::sync::Arc;

use datafusion_common::{
    DataFusionError, HashMap,
    tree_node::{Transformed, TreeNodeRecursion, TreeNodeRewriter},
};
use datafusion_physical_expr::{PhysicalExpr, expressions::Column};

/// Rewrite column references in a physical expr according to a mapping.
///
/// This rewriter traverses the expression tree and replaces [`Column`] nodes
/// with the corresponding expression found in the `column_map`.
///
/// If a column is found in the map, it is replaced by the mapped expression.
/// If a column is NOT found in the map, a `DataFusionError::Internal` is
/// returned.
pub struct PhysicalColumnRewriter<'a> {
    /// Mapping from original column to new column.
    pub column_map: &'a HashMap<Column, Arc<dyn PhysicalExpr>>,
}

impl<'a> PhysicalColumnRewriter<'a> {
    /// Create a new PhysicalColumnRewriter with the given column mapping.
    pub fn new(column_map: &'a HashMap<Column, Arc<dyn PhysicalExpr>>) -> Self {
        Self { column_map }
    }
}

impl<'a> TreeNodeRewriter for PhysicalColumnRewriter<'a> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Self::Node,
    ) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(new_column) = self.column_map.get(column) {
                // jump to prevent rewriting the new sub-expression again
                return Ok(Transformed::new(
                    Arc::clone(new_column),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            } else {
                // Column not found in mapping
                return Err(DataFusionError::Internal(format!(
                    "Column {column:?} not found in column mapping {:?}",
                    self.column_map
                )));
            }
        }
        Ok(Transformed::no(node))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{DataFusionError, Result, tree_node::TreeNode};
    use datafusion_physical_expr::{
        PhysicalExpr,
        expressions::{Column, binary, col, lit},
    };
    use std::sync::Arc;

    /// Helper function to create a test schema
    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Int32, true),
            Field::new("new_col", DataType::Int32, true),
            Field::new("inner_col", DataType::Int32, true),
            Field::new("another_col", DataType::Int32, true),
        ]))
    }

    /// Helper function to create a complex nested expression with multiple columns
    /// Create: (col_a + col_b) * (col_c - col_d) + col_e
    fn create_complex_expression(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let col_a = col("a", schema).unwrap();
        let col_b = col("b", schema).unwrap();
        let col_c = col("c", schema).unwrap();
        let col_d = col("d", schema).unwrap();
        let col_e = col("e", schema).unwrap();

        let add_expr =
            binary(col_a, datafusion_expr::Operator::Plus, col_b, schema).unwrap();
        let sub_expr =
            binary(col_c, datafusion_expr::Operator::Minus, col_d, schema).unwrap();
        let mul_expr = binary(
            add_expr,
            datafusion_expr::Operator::Multiply,
            sub_expr,
            schema,
        )
        .unwrap();
        binary(mul_expr, datafusion_expr::Operator::Plus, col_e, schema).unwrap()
    }

    /// Helper function to create a deeply nested expression
    /// Create: col_a + (col_b + (col_c + (col_d + col_e)))
    fn create_deeply_nested_expression(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        let col_a = col("a", schema).unwrap();
        let col_b = col("b", schema).unwrap();
        let col_c = col("c", schema).unwrap();
        let col_d = col("d", schema).unwrap();
        let col_e = col("e", schema).unwrap();

        let inner1 =
            binary(col_d, datafusion_expr::Operator::Plus, col_e, schema).unwrap();
        let inner2 =
            binary(col_c, datafusion_expr::Operator::Plus, inner1, schema).unwrap();
        let inner3 =
            binary(col_b, datafusion_expr::Operator::Plus, inner2, schema).unwrap();
        binary(col_a, datafusion_expr::Operator::Plus, inner3, schema).unwrap()
    }

    #[test]
    fn test_simple_column_replacement_with_jump() -> Result<()> {
        let schema = create_test_schema();

        // Test that Jump prevents re-processing of replaced columns
        let mut column_map = HashMap::new();
        column_map.insert(Column::new_with_schema("a", &schema).unwrap(), lit(42i32));
        column_map.insert(
            Column::new_with_schema("b", &schema).unwrap(),
            lit("replaced_b"),
        );
        column_map.insert(
            Column::new_with_schema("c", &schema).unwrap(),
            col("c", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("d", &schema).unwrap(),
            col("d", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("e", &schema).unwrap(),
            col("e", &schema).unwrap(),
        );

        let mut rewriter = PhysicalColumnRewriter::new(&column_map);
        let expr = create_complex_expression(&schema);

        let result = expr.rewrite(&mut rewriter)?;

        // Verify the transformation occurred
        assert!(result.transformed);

        assert_eq!(
            format!("{}", result.data),
            "(42 + replaced_b) * (c@2 - d@3) + e@4"
        );

        Ok(())
    }

    #[test]
    fn test_nested_column_replacement_with_jump() -> Result<()> {
        let schema = create_test_schema();
        // Test Jump behavior with deeply nested expressions
        let mut column_map = HashMap::new();
        // Replace col_c with a complex expression containing new columns
        let replacement_expr = binary(
            lit(100i32),
            datafusion_expr::Operator::Plus,
            col("new_col", &schema).unwrap(),
            &schema,
        )
        .unwrap();
        column_map.insert(
            Column::new_with_schema("c", &schema).unwrap(),
            replacement_expr,
        );
        column_map.insert(
            Column::new_with_schema("a", &schema).unwrap(),
            col("a", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("b", &schema).unwrap(),
            col("b", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("d", &schema).unwrap(),
            col("d", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("e", &schema).unwrap(),
            col("e", &schema).unwrap(),
        );

        let mut rewriter = PhysicalColumnRewriter::new(&column_map);
        let expr = create_deeply_nested_expression(&schema);

        let result = expr.rewrite(&mut rewriter)?;

        // Verify transformation occurred
        assert!(result.transformed);

        assert_eq!(
            format!("{}", result.data),
            "a@0 + b@1 + 100 + new_col@5 + d@3 + e@4"
        );

        Ok(())
    }

    #[test]
    fn test_circular_reference_prevention() -> Result<()> {
        let schema = create_test_schema();
        // Test that Jump prevents infinite recursion with circular references
        let mut column_map = HashMap::new();

        // Create a circular reference: col_a -> col_b -> col_a (but Jump should prevent the second visit)
        column_map.insert(
            Column::new_with_schema("a", &schema).unwrap(),
            col("b", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("b", &schema).unwrap(),
            col("a", &schema).unwrap(),
        );

        let mut rewriter = PhysicalColumnRewriter::new(&column_map);

        // Start with an expression containing col_a
        let expr = binary(
            col("a", &schema).unwrap(),
            datafusion_expr::Operator::Plus,
            col("b", &schema).unwrap(),
            &schema,
        )
        .unwrap();

        let result = expr.rewrite(&mut rewriter)?;

        // Verify transformation occurred
        assert!(result.transformed);

        assert_eq!(format!("{}", result.data), "b@1 + a@0");

        Ok(())
    }

    #[test]
    fn test_multiple_replacements_in_same_expression() -> Result<()> {
        let schema = create_test_schema();
        // Test multiple column replacements in the same complex expression
        let mut column_map = HashMap::new();

        // Replace multiple columns with literals
        column_map.insert(Column::new_with_schema("a", &schema).unwrap(), lit(10i32));
        column_map.insert(Column::new_with_schema("c", &schema).unwrap(), lit(20i32));
        column_map.insert(Column::new_with_schema("e", &schema).unwrap(), lit(30i32));
        column_map.insert(
            Column::new_with_schema("b", &schema).unwrap(),
            col("b", &schema).unwrap(),
        );
        column_map.insert(
            Column::new_with_schema("d", &schema).unwrap(),
            col("d", &schema).unwrap(),
        );

        let mut rewriter = PhysicalColumnRewriter::new(&column_map);
        let expr = create_complex_expression(&schema); // (col_a + col_b) * (col_c - col_d) + col_e

        let result = expr.rewrite(&mut rewriter)?;

        // Verify transformation occurred
        assert!(result.transformed);
        assert_eq!(format!("{}", result.data), "(10 + b@1) * (20 - d@3) + 30");

        Ok(())
    }

    #[test]
    fn test_jump_with_complex_replacement_expression() -> Result<()> {
        let schema = create_test_schema();
        // Test Jump behavior when replacing with very complex expressions
        let mut column_map = HashMap::new();

        // Replace col_a with a complex nested expression
        let inner_expr = binary(
            lit(5i32),
            datafusion_expr::Operator::Multiply,
            col("a", &schema).unwrap(),
            &schema,
        )
        .unwrap();
        let middle_expr = binary(
            inner_expr,
            datafusion_expr::Operator::Plus,
            lit(3i32),
            &schema,
        )
        .unwrap();
        let complex_replacement = binary(
            middle_expr,
            datafusion_expr::Operator::Minus,
            col("another_col", &schema).unwrap(),
            &schema,
        )
        .unwrap();

        column_map.insert(
            Column::new_with_schema("a", &schema).unwrap(),
            complex_replacement,
        );
        column_map.insert(
            Column::new_with_schema("b", &schema).unwrap(),
            col("b", &schema).unwrap(),
        );

        let mut rewriter = PhysicalColumnRewriter::new(&column_map);

        // Create expression: col_a + col_b
        let expr = binary(
            col("a", &schema).unwrap(),
            datafusion_expr::Operator::Plus,
            col("b", &schema).unwrap(),
            &schema,
        )
        .unwrap();

        let result = expr.rewrite(&mut rewriter)?;

        assert_eq!(
            format!("{}", result.data),
            "5 * a@0 + 3 - another_col@7 + b@1"
        );

        // Verify transformation occurred
        assert!(result.transformed);

        Ok(())
    }

    #[test]
    fn test_unmapped_columns_detection() -> Result<()> {
        let schema = create_test_schema();
        let mut column_map = HashMap::new();

        // Only map col_a, leave col_b unmapped
        column_map.insert(Column::new_with_schema("a", &schema).unwrap(), lit(42i32));

        let mut rewriter = PhysicalColumnRewriter::new(&column_map);

        // Create expression: col_a + col_b
        let expr = binary(
            col("a", &schema).unwrap(),
            datafusion_expr::Operator::Plus,
            col("b", &schema).unwrap(),
            &schema,
        )
        .unwrap();

        let err = expr.rewrite(&mut rewriter).unwrap_err();
        assert!(matches!(err, DataFusionError::Internal(_)));

        Ok(())
    }
}
