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

//! Physical expression schema rewriting utilities

use std::sync::Arc;

use arrow::compute::can_cast_types;
use arrow::datatypes::{FieldRef, Schema};
use datafusion_common::{
    exec_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result, ScalarValue,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::expressions::{self, CastExpr, Column};

/// Builder for rewriting physical expressions to match different schemas.
///
/// # Example
///
/// ```rust
/// use datafusion_physical_expr::schema_rewriter::PhysicalExprSchemaRewriter;
/// use arrow::datatypes::Schema;
///
/// # fn example(
/// #     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
/// #     physical_file_schema: &Schema,
/// #     logical_file_schema: &Schema,
/// # ) -> datafusion_common::Result<()> {
/// let rewriter = PhysicalExprSchemaRewriter::new(physical_file_schema, logical_file_schema);
/// let adapted_predicate = rewriter.rewrite(predicate)?;
/// # Ok(())
/// # }
/// ```
pub struct PhysicalExprSchemaRewriter<'a> {
    physical_file_schema: &'a Schema,
    logical_file_schema: &'a Schema,
    partition_fields: Vec<FieldRef>,
    partition_values: Vec<ScalarValue>,
}

impl<'a> PhysicalExprSchemaRewriter<'a> {
    /// Create a new schema rewriter with the given schemas
    pub fn new(
        physical_file_schema: &'a Schema,
        logical_file_schema: &'a Schema,
    ) -> Self {
        Self {
            physical_file_schema,
            logical_file_schema,
            partition_fields: Vec::new(),
            partition_values: Vec::new(),
        }
    }

    /// Add partition columns and their corresponding values
    ///
    /// When a column reference matches a partition field, it will be replaced
    /// with the corresponding literal value from partition_values.
    pub fn with_partition_columns(
        mut self,
        partition_fields: Vec<FieldRef>,
        partition_values: Vec<ScalarValue>,
    ) -> Self {
        self.partition_fields = partition_fields;
        self.partition_values = partition_values;
        self
    }

    /// Rewrite the given physical expression to match the target schema
    ///
    /// This method applies the following transformations:
    /// 1. Replaces partition column references with literal values
    /// 2. Handles missing columns by inserting null literals
    /// 3. Casts columns when logical and physical schemas have different types
    pub fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|expr| self.rewrite_expr(expr)).data()
    }

    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }

        Ok(Transformed::no(expr))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Check if this is a partition column
        if let Some(partition_value) = self.get_partition_value(column.name()) {
            return Ok(Transformed::yes(expressions::lit(partition_value)));
        }

        // Get the logical field for this column
        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(e) => {
                return Err(e.into());
            }
        };

        // Check if the column exists in the physical schema
        let physical_field =
            match self.physical_file_schema.field_with_name(column.name()) {
                Ok(field) => field,
                Err(_) => {
                    // Column is missing from physical schema
                    if !logical_field.is_nullable() {
                        return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        column.name()
                    );
                    }
                    // Fill in with null value
                    let null_value =
                        ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(Transformed::yes(expressions::lit(null_value)));
                }
            };

        // Check if casting is needed
        if logical_field.data_type() == physical_field.data_type() {
            return Ok(Transformed::no(expr));
        }

        // Perform type casting
        if !can_cast_types(physical_field.data_type(), logical_field.data_type()) {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        let cast_expr =
            Arc::new(CastExpr::new(expr, logical_field.data_type().clone(), None));

        Ok(Transformed::yes(cast_expr))
    }

    fn get_partition_value(&self, column_name: &str) -> Option<ScalarValue> {
        self.partition_fields
            .iter()
            .zip(self.partition_values.iter())
            .find(|(field, _)| field.name() == column_name)
            .map(|(_, value)| value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    fn create_test_schema() -> (Schema, Schema) {
        let physical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);

        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false), // Different type
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true), // Missing from physical
        ]);

        (physical_schema, logical_schema)
    }

    #[test]
    fn test_rewrite_column_with_type_cast() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("a", 0));

        let result = rewriter.rewrite(column_expr)?;

        // Should be wrapped in a cast expression
        assert!(result.as_any().downcast_ref::<CastExpr>().is_some());

        Ok(())
    }

    #[test]
    fn test_rewrite_missing_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("c", 2));

        let result = rewriter.rewrite(column_expr)?;

        // Should be replaced with a literal null
        if let Some(literal) = result.as_any().downcast_ref::<expressions::Literal>() {
            assert_eq!(*literal.value(), ScalarValue::Float64(None));
        } else {
            panic!("Expected literal expression");
        }

        Ok(())
    }

    #[test]
    fn test_rewrite_partition_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let partition_fields =
            vec![Arc::new(Field::new("partition_col", DataType::Utf8, false))];
        let partition_values = vec![ScalarValue::Utf8(Some("test_value".to_string()))];

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema)
            .with_partition_columns(partition_fields, partition_values);

        let column_expr = Arc::new(Column::new("partition_col", 0));
        let result = rewriter.rewrite(column_expr)?;

        // Should be replaced with the partition value
        if let Some(literal) = result.as_any().downcast_ref::<expressions::Literal>() {
            assert_eq!(
                *literal.value(),
                ScalarValue::Utf8(Some("test_value".to_string()))
            );
        } else {
            panic!("Expected literal expression");
        }

        Ok(())
    }

    #[test]
    fn test_rewrite_no_change_needed() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("b", 1));

        let result = rewriter.rewrite(column_expr.clone())?;

        // Should be the same expression (no transformation needed)
        // We compare the underlying pointer through the trait object
        assert!(std::ptr::eq(
            column_expr.as_ref() as *const dyn PhysicalExpr,
            result.as_ref() as *const dyn PhysicalExpr
        ));

        Ok(())
    }

    #[test]
    fn test_non_nullable_missing_column_error() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false), // Non-nullable missing column
        ]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("b", 1));

        let result = rewriter.rewrite(column_expr);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Non-nullable column 'b' is missing"));
    }
}
