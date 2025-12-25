use std::sync::Arc;

use datafusion_common::{
    HashMap,
    tree_node::{Transformed, TreeNodeRecursion, TreeNodeRewriter},
};
use datafusion_physical_expr::{
    PhysicalExpr,
    expressions::{Column, UnKnownColumn},
};

/// Rewrite column references in a physical expr according to a mapping.
pub struct PhysicalColumnRewriter {
    /// Mapping from original column to new column.
    pub column_map: HashMap<Column, Arc<dyn PhysicalExpr>>,
}

impl PhysicalColumnRewriter {
    /// Create a new PhysicalColumnRewriter with the given column mapping.
    pub fn new(column_map: HashMap<Column, Arc<dyn PhysicalExpr>>) -> Self {
        Self { column_map }
    }
}

impl TreeNodeRewriter for PhysicalColumnRewriter {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Self::Node,
    ) -> datafusion_common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(new_column) = self.column_map.get(column) {
                // jump to prevent rewriting the new sub-expression again
                return Ok(Transformed::new(
                    new_column.clone(),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            } else {
                return Ok(Transformed::yes(Arc::new(UnKnownColumn::new(
                    column.name(),
                ))));
            }
        }
        Ok(Transformed::no(node))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, tree_node::TreeNode};
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
        let final_expr =
            binary(mul_expr, datafusion_expr::Operator::Plus, col_e, schema).unwrap();

        final_expr
    }

    /// Helper function to create a deeply nested expression
    fn create_deeply_nested_expression(schema: &Schema) -> Arc<dyn PhysicalExpr> {
        // Create: col_a + (col_b + (col_c + (col_d + col_e)))
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
        let final_expr =
            binary(col_a, datafusion_expr::Operator::Plus, inner3, schema).unwrap();

        final_expr
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

        let mut rewriter = PhysicalColumnRewriter::new(column_map);
        let expr = create_complex_expression(&schema);

        let result = expr.rewrite(&mut rewriter)?;

        // Verify the transformation occurred
        assert!(result.transformed);

        assert_eq!(
            format!("{}", result.data),
            "(42 + replaced_b) * (c - d) + e"
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

        let mut rewriter = PhysicalColumnRewriter::new(column_map);
        let expr = create_deeply_nested_expression(&schema);

        let result = expr.rewrite(&mut rewriter)?;

        // Verify transformation occurred
        assert!(result.transformed);

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

        let mut rewriter = PhysicalColumnRewriter::new(column_map);

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

        let mut rewriter = PhysicalColumnRewriter::new(column_map);
        let expr = create_complex_expression(&schema); // (col_a + col_b) * (col_c - col_d) + col_e

        let result = expr.rewrite(&mut rewriter)?;

        // Verify transformation occurred
        assert!(result.transformed);
        assert_eq!(format!("{}", result.data), "(10 + b) * (20 - d) + 30");

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

        let mut rewriter = PhysicalColumnRewriter::new(column_map);

        // Create expression: col_a + col_b
        let expr = binary(
            col("a", &schema).unwrap(),
            datafusion_expr::Operator::Plus,
            col("b", &schema).unwrap(),
            &schema,
        )
        .unwrap();

        let result = expr.rewrite(&mut rewriter)?;

        dbg!(format!("{}", result.data));
        assert_eq!(
            format!("{}", result.data),
            "5 * a@0 + 3 - another_col@7 + b"
        );

        // Verify transformation occurred
        assert!(result.transformed);

        Ok(())
    }
}
