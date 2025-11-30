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
use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion_common::{
    exec_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result, ScalarValue,
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::expressions::CastColumnExpr;
use datafusion_physical_expr::{
    expressions::{self, Column},
    ScalarFunctionExpr,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Trait for adapting physical expressions to match a target schema.
///
/// This is used in file scans to rewrite expressions so that they can be evaluated
/// against the physical schema of the file being scanned. It allows for handling
/// differences between logical and physical schemas, such as type mismatches or missing columns.
///
/// ## Overview
///
/// The `PhysicalExprAdapter` allows rewriting physical expressions to match different schemas, including:
///
/// - **Type casting**: When logical and physical schemas have different types, expressions are
///   automatically wrapped with cast operations. For example, `lit(ScalarValue::Int32(123)) = int64_column`
///   gets rewritten to `lit(ScalarValue::Int32(123)) = cast(int64_column, 'Int32')`.
///   Note that this does not attempt to simplify such expressions - that is done by shared simplifiers.
///
/// - **Missing columns**: When a column exists in the logical schema but not in the physical schema,
///   references to it are replaced with null literals.
///
/// - **Struct field access**: Expressions like `struct_column.field_that_is_missing_in_schema` are
///   rewritten to `null` when the field doesn't exist in the physical schema.
///
/// - **Partition columns**: Partition column references can be replaced with their literal values
///   when scanning specific partitions.
///
/// ## Custom Implementations
///
/// You can create a custom implementation of this trait to handle specific rewriting logic.
/// For example, to fill in missing columns with default values instead of nulls:
///
/// ```rust
/// use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
/// use arrow::datatypes::{Schema, Field, DataType, FieldRef, SchemaRef};
/// use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
/// use datafusion_common::{Result, ScalarValue, tree_node::{Transformed, TransformedResult, TreeNode}};
/// use datafusion_physical_expr::expressions::{self, Column};
/// use std::sync::Arc;
///
/// #[derive(Debug)]
/// pub struct CustomPhysicalExprAdapter {
///     logical_file_schema: SchemaRef,
///     physical_file_schema: SchemaRef,
/// }
///
/// impl PhysicalExprAdapter for CustomPhysicalExprAdapter {
///     fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
///         expr.transform(|expr| {
///             if let Some(column) = expr.as_any().downcast_ref::<Column>() {
///                 // Check if the column exists in the physical schema
///                 if self.physical_file_schema.index_of(column.name()).is_err() {
///                     // If the column is missing, fill it with a default value instead of null
///                     // The default value could be stored in the table schema's column metadata for example.
///                     let default_value = ScalarValue::Int32(Some(0));
///                     return Ok(Transformed::yes(expressions::lit(default_value)));
///                 }
///             }
///             // If the column exists, return it as is
///             Ok(Transformed::no(expr))
///         }).data()
///     }
///
///     fn with_partition_values(
///         &self,
///         partition_values: Vec<(FieldRef, ScalarValue)>,
///     ) -> Arc<dyn PhysicalExprAdapter> {
///         // For simplicity, this example ignores partition values
///         Arc::new(CustomPhysicalExprAdapter {
///             logical_file_schema: self.logical_file_schema.clone(),
///             physical_file_schema: self.physical_file_schema.clone(),
///         })
///     }
/// }
///
/// #[derive(Debug)]
/// pub struct CustomPhysicalExprAdapterFactory;
///
/// impl PhysicalExprAdapterFactory for CustomPhysicalExprAdapterFactory {
///     fn create(
///         &self,
///         logical_file_schema: SchemaRef,
///         physical_file_schema: SchemaRef,
///     ) -> Arc<dyn PhysicalExprAdapter> {
///         Arc::new(CustomPhysicalExprAdapter {
///             logical_file_schema,
///             physical_file_schema,
///         })
///     }
/// }
/// ```
pub trait PhysicalExprAdapter: Send + Sync + std::fmt::Debug {
    /// Rewrite a physical expression to match the target schema.
    ///
    /// This method should return a transformed expression that matches the target schema.
    ///
    /// Arguments:
    /// - `expr`: The physical expression to rewrite.
    /// - `logical_file_schema`: The logical schema of the table being queried, excluding any partition columns.
    /// - `physical_file_schema`: The physical schema of the file being scanned.
    /// - `partition_values`: Optional partition values to use for rewriting partition column references.
    ///   These are handled as if they were columns appended onto the logical file schema.
    ///
    /// Returns:
    /// - `Arc<dyn PhysicalExpr>`: The rewritten physical expression that can be evaluated against the physical schema.
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>;

    fn with_partition_values(
        &self,
        partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter>;
}

pub trait PhysicalExprAdapterFactory: Send + Sync + std::fmt::Debug {
    /// Create a new instance of the physical expression adapter.
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter>;
}

#[derive(Debug, Clone)]
pub struct DefaultPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DefaultPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(DefaultPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
            partition_values: Vec::new(),
        })
    }
}

/// Default implementation for rewriting physical expressions to match different schemas.
///
/// # Example
///
/// ```rust
/// use datafusion_physical_expr_adapter::{DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory};
/// use arrow::datatypes::Schema;
/// use std::sync::Arc;
///
/// # fn example(
/// #     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
/// #     physical_file_schema: &Schema,
/// #     logical_file_schema: &Schema,
/// # ) -> datafusion_common::Result<()> {
/// let factory = DefaultPhysicalExprAdapterFactory;
/// let adapter = factory.create(Arc::new(logical_file_schema.clone()), Arc::new(physical_file_schema.clone()));
/// let adapted_predicate = adapter.rewrite(predicate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DefaultPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    partition_values: Vec<(FieldRef, ScalarValue)>,
}

impl DefaultPhysicalExprAdapter {
    /// Create a new instance of the default physical expression adapter.
    ///
    /// This adapter rewrites expressions to match the physical schema of the file being scanned,
    /// handling type mismatches and missing columns by filling them with default values.
    pub fn new(logical_file_schema: SchemaRef, physical_file_schema: SchemaRef) -> Self {
        Self {
            logical_file_schema,
            physical_file_schema,
            partition_values: Vec::new(),
        }
    }
}

impl PhysicalExprAdapter for DefaultPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = DefaultPhysicalExprAdapterRewriter {
            logical_file_schema: &self.logical_file_schema,
            physical_file_schema: &self.physical_file_schema,
            partition_fields: &self.partition_values,
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }

    fn with_partition_values(
        &self,
        partition_values: Vec<(FieldRef, ScalarValue)>,
    ) -> Arc<dyn PhysicalExprAdapter> {
        Arc::new(DefaultPhysicalExprAdapter {
            partition_values,
            ..self.clone()
        })
    }
}

struct DefaultPhysicalExprAdapterRewriter<'a> {
    logical_file_schema: &'a Schema,
    physical_file_schema: &'a Schema,
    partition_fields: &'a [(FieldRef, ScalarValue)],
}

impl<'a> DefaultPhysicalExprAdapterRewriter<'a> {
    fn rewrite_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(transformed) = self.try_rewrite_struct_field_access(&expr)? {
            return Ok(Transformed::yes(transformed));
        }

        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            return self.rewrite_column(Arc::clone(&expr), column);
        }

        Ok(Transformed::no(expr))
    }

    /// Attempt to rewrite struct field access expressions to return null if the field does not exist in the physical schema.
    /// Note that this does *not* handle nested struct fields, only top-level struct field access.
    /// See <https://github.com/apache/datafusion/issues/17114> for more details.
    fn try_rewrite_struct_field_access(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let get_field_expr =
            match ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref()) {
                Some(expr) => expr,
                None => return Ok(None),
            };

        let source_expr = match get_field_expr.args().first() {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let field_name_expr = match get_field_expr.args().get(1) {
            Some(expr) => expr,
            None => return Ok(None),
        };

        let lit = match field_name_expr
            .as_any()
            .downcast_ref::<expressions::Literal>()
        {
            Some(lit) => lit,
            None => return Ok(None),
        };

        let field_name = match lit.value().try_as_str().flatten() {
            Some(name) => name,
            None => return Ok(None),
        };

        let column = match source_expr.as_any().downcast_ref::<Column>() {
            Some(column) => column,
            None => return Ok(None),
        };

        let physical_field =
            match self.physical_file_schema.field_with_name(column.name()) {
                Ok(field) => field,
                Err(_) => return Ok(None),
            };

        let physical_struct_fields = match physical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };

        if physical_struct_fields
            .iter()
            .any(|f| f.name() == field_name)
        {
            return Ok(None);
        }

        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(_) => return Ok(None),
        };

        let logical_struct_fields = match logical_field.data_type() {
            DataType::Struct(fields) => fields,
            _ => return Ok(None),
        };

        let logical_struct_field = match logical_struct_fields
            .iter()
            .find(|f| f.name() == field_name)
        {
            Some(field) => field,
            None => return Ok(None),
        };

        let null_value = ScalarValue::Null.cast_to(logical_struct_field.data_type())?;
        Ok(Some(expressions::lit(null_value)))
    }

    fn rewrite_column(
        &self,
        expr: Arc<dyn PhysicalExpr>,
        column: &Column,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // Get the logical field for this column if it exists in the logical schema
        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(e) => {
                // If the column is a partition field, we can use the partition value
                if let Some(partition_value) = self.get_partition_value(column.name()) {
                    return Ok(Transformed::yes(expressions::lit(partition_value)));
                }
                // This can be hit if a custom rewrite injected a reference to a column that doesn't exist in the logical schema.
                // For example, a pre-computed column that is kept only in the physical schema.
                // If the column exists in the physical schema, we can still use it.
                if let Ok(physical_field) =
                    self.physical_file_schema.field_with_name(column.name())
                {
                    // If the column exists in the physical schema, we can use it in place of the logical column.
                    // This is nice to users because if they do a rewrite that results in something like `physical_int32_col = 123u64`
                    // we'll at least handle the casts for them.
                    physical_field
                } else {
                    // A completely unknown column that doesn't exist in either schema!
                    // This should probably never be hit unless something upstream broke, but nonetheless it's better
                    // for us to return a handleable error than to panic / do something unexpected.
                    return Err(e.into());
                }
            }
        };

        // Check if the column exists in the physical schema
        let physical_column_index =
            match self.physical_file_schema.index_of(column.name()) {
                Ok(index) => index,
                Err(_) => {
                    if !logical_field.is_nullable() {
                        return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        column.name()
                    );
                    }
                    // If the column is missing from the physical schema fill it in with nulls as `SchemaAdapter` would do.
                    // TODO: do we need to sync this with what the `SchemaAdapter` actually does?
                    // While the default implementation fills in nulls in theory a custom `SchemaAdapter` could do something else!
                    // See https://github.com/apache/datafusion/issues/16527
                    let null_value =
                        ScalarValue::Null.cast_to(logical_field.data_type())?;
                    return Ok(Transformed::yes(expressions::lit(null_value)));
                }
            };
        let physical_field = self.physical_file_schema.field(physical_column_index);

        let column = match (
            column.index() == physical_column_index,
            logical_field.data_type() == physical_field.data_type(),
        ) {
            // If the column index matches and the data types match, we can use the column as is
            (true, true) => return Ok(Transformed::no(expr)),
            // If the indexes or data types do not match, we need to create a new column expression
            (true, _) => column.clone(),
            (false, _) => {
                Column::new_with_schema(logical_field.name(), self.physical_file_schema)?
            }
        };

        if logical_field.data_type() == physical_field.data_type() {
            // If the data types match, we can use the column as is
            return Ok(Transformed::yes(Arc::new(column)));
        }

        // We need to cast the column to the logical data type
        // TODO: add optimization to move the cast from the column to literal expressions in the case of `col = 123`
        // since that's much cheaper to evalaute.
        // See https://github.com/apache/datafusion/issues/15780#issuecomment-2824716928
        let is_compatible =
            can_cast_types(physical_field.data_type(), logical_field.data_type());
        if !is_compatible {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        let cast_expr = Arc::new(CastColumnExpr::new(
            Arc::new(column),
            Arc::new(physical_field.clone()),
            Arc::new(logical_field.clone()),
            None,
        ));

        Ok(Transformed::yes(cast_expr))
    }

    fn get_partition_value(&self, column_name: &str) -> Option<ScalarValue> {
        self.partition_fields
            .iter()
            .find(|(field, _)| field.name() == column_name)
            .map(|(_, value)| value.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{RecordBatch, RecordBatchOptions};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{assert_contains, record_batch, Result, ScalarValue};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{col, lit, Column, Literal};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use itertools::Itertools;
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
    fn test_rewrite_column_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("a", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        // Should be wrapped in a cast expression
        assert!(result.as_any().downcast_ref::<CastColumnExpr>().is_some());
    }

    #[test]
    fn test_rewrite_multi_column_expr_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();
        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));

        // Create a complex expression: (a + 5) OR (c > 0.0) that tests the recursive case of the rewriter
        let column_a = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let column_c = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;
        let expr = expressions::BinaryExpr::new(
            Arc::clone(&column_a),
            Operator::Plus,
            Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(5)))),
        );
        let expr = expressions::BinaryExpr::new(
            Arc::new(expr),
            Operator::Or,
            Arc::new(expressions::BinaryExpr::new(
                Arc::clone(&column_c),
                Operator::Gt,
                Arc::new(expressions::Literal::new(ScalarValue::Float64(Some(0.0)))),
            )),
        );

        let result = adapter.rewrite(Arc::new(expr)).unwrap();
        println!("Rewritten expression: {result}");

        let expected = expressions::BinaryExpr::new(
            Arc::new(CastColumnExpr::new(
                Arc::new(Column::new("a", 0)),
                Arc::new(Field::new("a", DataType::Int32, false)),
                Arc::new(Field::new("a", DataType::Int64, false)),
                None,
            )),
            Operator::Plus,
            Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(5)))),
        );
        let expected = Arc::new(expressions::BinaryExpr::new(
            Arc::new(expected),
            Operator::Or,
            Arc::new(expressions::BinaryExpr::new(
                lit(ScalarValue::Float64(None)), // c is missing, so it becomes null
                Operator::Gt,
                Arc::new(expressions::Literal::new(ScalarValue::Float64(Some(0.0)))),
            )),
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(
            result.to_string(),
            expected.to_string(),
            "The rewritten expression did not match the expected output"
        );
    }

    #[test]
    fn test_rewrite_struct_column_incompatible() {
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![Field::new("field1", DataType::Binary, true)].into()),
            true,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(vec![Field::new("field1", DataType::Int32, true)].into()),
            true,
        )]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("data", 0));

        let error_msg = adapter.rewrite(column_expr).unwrap_err().to_string();
        assert_contains!(error_msg, "Cannot cast column 'data'");
    }

    #[test]
    fn test_rewrite_struct_compatible_cast() {
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8View, true),
                ]
                .into(),
            ),
            false,
        )]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("data", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        let expected = Arc::new(CastColumnExpr::new(
            Arc::new(Column::new("data", 0)),
            Arc::new(Field::new(
                "data",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                false,
            )),
            Arc::new(Field::new(
                "data",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int64, false),
                        Field::new("name", DataType::Utf8View, true),
                    ]
                    .into(),
                ),
                false,
            )),
            None,
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_missing_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("c", 2));

        let result = adapter.rewrite(column_expr)?;

        // Should be replaced with a literal null
        if let Some(literal) = result.as_any().downcast_ref::<expressions::Literal>() {
            assert_eq!(*literal.value(), ScalarValue::Float64(None));
        } else {
            panic!("Expected literal expression");
        }

        Ok(())
    }

    #[test]
    fn test_rewrite_missing_column_non_nullable_error() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false), // Missing and non-nullable
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("b", 1));

        let error_msg = adapter.rewrite(column_expr).unwrap_err().to_string();
        assert_contains!(error_msg, "Non-nullable column 'b' is missing");
    }

    #[test]
    fn test_rewrite_missing_column_nullable() {
        let physical_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true), // Missing but nullable
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("b", 1));

        let result = adapter.rewrite(column_expr).unwrap();

        let expected =
            Arc::new(Literal::new(ScalarValue::Utf8(None))) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_partition_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let partition_field =
            Arc::new(Field::new("partition_col", DataType::Utf8, false));
        let partition_value = ScalarValue::Utf8(Some("test_value".to_string()));
        let partition_values = vec![(partition_field, partition_value)];

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let adapter = adapter.with_partition_values(partition_values);

        let column_expr = Arc::new(Column::new("partition_col", 0));
        let result = adapter.rewrite(column_expr)?;

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

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let result = adapter.rewrite(Arc::clone(&column_expr))?;

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

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory.create(Arc::new(logical_schema), Arc::new(physical_schema));
        let column_expr = Arc::new(Column::new("b", 1));

        let result = adapter.rewrite(column_expr);
        assert!(result.is_err());
        assert_contains!(
            result.unwrap_err().to_string(),
            "Non-nullable column 'b' is missing from the physical schema"
        );
    }

    /// Helper function to project expressions onto a RecordBatch
    fn batch_project(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        batch: &RecordBatch,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let arrays = expr
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(Into::into)
        }
    }

    /// Example showing how we can use the `DefaultPhysicalExprAdapter` to adapt RecordBatches during a scan
    /// to apply projections, type conversions and handling of missing columns all at once.
    #[test]
    fn test_adapt_batches() {
        let physical_batch = record_batch!(
            ("a", Int32, vec![Some(1), None, Some(3)]),
            ("extra", Utf8, vec![Some("x"), Some("y"), None])
        )
        .unwrap();

        let physical_schema = physical_batch.schema();

        let logical_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true), // Different type
            Field::new("b", DataType::Utf8, true),  // Missing from physical
        ]));

        let projection = vec![
            col("b", &logical_schema).unwrap(),
            col("a", &logical_schema).unwrap(),
        ];

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter =
            factory.create(Arc::clone(&logical_schema), Arc::clone(&physical_schema));

        let adapted_projection = projection
            .into_iter()
            .map(|expr| adapter.rewrite(expr).unwrap())
            .collect_vec();

        let adapted_schema = Arc::new(Schema::new(
            adapted_projection
                .iter()
                .map(|expr| expr.return_field(&physical_schema).unwrap())
                .collect_vec(),
        ));

        let res = batch_project(
            adapted_projection,
            &physical_batch,
            Arc::clone(&adapted_schema),
        )
        .unwrap();

        assert_eq!(res.num_columns(), 2);
        assert_eq!(res.column(0).data_type(), &DataType::Utf8);
        assert_eq!(res.column(1).data_type(), &DataType::Int64);
        assert_eq!(
            res.column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .iter()
                .collect_vec(),
            vec![None, None, None]
        );
        assert_eq!(
            res.column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .collect_vec(),
            vec![Some(1), None, Some(3)]
        );
    }

    #[test]
    fn test_try_rewrite_struct_field_access() {
        // Test the core logic of try_rewrite_struct_field_access
        let physical_schema = Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![Field::new("existing_field", DataType::Int32, true)].into(),
            ),
            true,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "struct_col",
            DataType::Struct(
                vec![
                    Field::new("existing_field", DataType::Int32, true),
                    Field::new("missing_field", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )]);

        let rewriter = DefaultPhysicalExprAdapterRewriter {
            logical_file_schema: &logical_schema,
            physical_file_schema: &physical_schema,
            partition_fields: &[],
        };

        // Test that when a field exists in physical schema, it returns None
        let column = Arc::new(Column::new("struct_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = rewriter.try_rewrite_struct_field_access(&column).unwrap();
        assert!(result.is_none());

        // The actual test for the get_field expression would require creating a proper ScalarFunctionExpr
        // with ScalarUDF, which is complex to set up in a unit test. The integration tests in
        // datafusion/core/tests/parquet/schema_adapter.rs provide better coverage for this functionality.
    }
}
