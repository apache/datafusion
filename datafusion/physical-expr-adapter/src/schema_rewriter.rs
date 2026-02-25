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

//! Physical expression schema rewriting utilities: [`PhysicalExprAdapter`],
//! [`PhysicalExprAdapterFactory`], default implementations,
//! and [`replace_columns_with_literals`].

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion_common::{
    Result, ScalarValue, exec_err,
    nested_struct::validate_struct_compatibility,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_physical_expr::PhysicalExprSimplifier;
use datafusion_physical_expr::expressions::CastColumnExpr;
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::{
    ScalarFunctionExpr,
    expressions::{self, Column},
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::Itertools;

/// Replace column references in the given physical expression with literal values.
///
/// Some use cases for this include:
/// - Partition column pruning: When scanning partitioned data, partition column references
///   can be replaced with their literal values for the specific partition being scanned.
/// - Constant folding: In some cases, columns that can be proven to be constant
///   from statistical analysis may be replaced with their literal values to optimize expression evaluation.
/// - Filling in non-null default values: in a custom [`PhysicalExprAdapter`] implementation,
///   column references can be replaced with default literal values instead of nulls.
///
/// # Arguments
/// - `expr`: The physical expression in which to replace column references.
/// - `replacements`: A mapping from column names to their corresponding literal `ScalarValue`s.
///   Accepts various HashMap types including `HashMap<&str, &ScalarValue>`,
///   `HashMap<String, ScalarValue>`, `HashMap<String, &ScalarValue>`, etc.
///
/// # Returns
/// - `Result<Arc<dyn PhysicalExpr>>`: The rewritten physical expression with columns replaced by literals.
pub fn replace_columns_with_literals<K, V>(
    expr: Arc<dyn PhysicalExpr>,
    replacements: &HashMap<K, V>,
) -> Result<Arc<dyn PhysicalExpr>>
where
    K: Borrow<str> + Eq + Hash,
    V: Borrow<ScalarValue>,
{
    expr.transform_down(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>()
            && let Some(replacement_value) = replacements.get(column.name())
        {
            return Ok(Transformed::yes(expressions::lit(
                replacement_value.borrow().clone(),
            )));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Trait for adapting [`PhysicalExpr`] expressions to match a target schema.
///
/// This is used in file scans to rewrite expressions so that they can be
/// evaluated against the physical schema of the file being scanned. It allows
/// for handling differences between logical and physical schemas, such as type
/// mismatches or missing columns common in [Schema evolution] scenarios.
///
/// [Schema evolution]: https://www.dremio.com/wiki/schema-evolution/
///
/// ## Default Implementations
///
/// The default implementation [`DefaultPhysicalExprAdapter`]  handles common
/// cases.
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
///     ) -> Result<Arc<dyn PhysicalExprAdapter>> {
///         Ok(Arc::new(CustomPhysicalExprAdapter {
///             logical_file_schema,
///             physical_file_schema,
///         }))
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
    ///
    /// See Also:
    /// - [`replace_columns_with_literals`]: for replacing partition column references with their literal values.
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Creates instances of [`PhysicalExprAdapter`] for given logical and physical schemas.
///
/// See [`DefaultPhysicalExprAdapterFactory`] for the default implementation.
pub trait PhysicalExprAdapterFactory: Send + Sync + std::fmt::Debug {
    /// Create a new instance of the physical expression adapter.
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>>;
}

#[derive(Debug, Clone)]
pub struct DefaultPhysicalExprAdapterFactory;

impl PhysicalExprAdapterFactory for DefaultPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(DefaultPhysicalExprAdapter {
            logical_file_schema,
            physical_file_schema,
        }))
    }
}

/// Default implementation of [`PhysicalExprAdapter`] for rewriting physical
/// expressions to match different schemas.
///
/// ## Overview
///
///  [`DefaultPhysicalExprAdapter`] rewrites physical expressions to match
///  different schemas, including:
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
/// - **Default column values**: Partition column references can be replaced with their literal values
///   when scanning specific partitions. See [`replace_columns_with_literals`] for more details.
///
/// # Example
///
/// ```rust
/// # use datafusion_physical_expr_adapter::{DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory};
/// # use arrow::datatypes::Schema;
/// # use std::sync::Arc;
/// #
/// # fn example(
/// #     predicate: std::sync::Arc<dyn datafusion_physical_expr_common::physical_expr::PhysicalExpr>,
/// #     physical_file_schema: &Schema,
/// #     logical_file_schema: &Schema,
/// # ) -> datafusion_common::Result<()> {
/// let factory = DefaultPhysicalExprAdapterFactory;
/// let adapter =
///     factory.create(Arc::new(logical_file_schema.clone()), Arc::new(physical_file_schema.clone()))?;
/// let adapted_predicate = adapter.rewrite(predicate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DefaultPhysicalExprAdapter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
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
        }
    }
}

impl PhysicalExprAdapter for DefaultPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        let rewriter = DefaultPhysicalExprAdapterRewriter {
            logical_file_schema: Arc::clone(&self.logical_file_schema),
            physical_file_schema: Arc::clone(&self.physical_file_schema),
        };
        expr.transform(|expr| rewriter.rewrite_expr(Arc::clone(&expr)))
            .data()
    }
}

struct DefaultPhysicalExprAdapterRewriter {
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl DefaultPhysicalExprAdapterRewriter {
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
        let physical_column_index = match self
            .physical_file_schema
            .index_of(column.name())
        {
            Ok(index) => index,
            Err(_) => {
                if !logical_field.is_nullable() {
                    return exec_err!(
                        "Non-nullable column '{}' is missing from the physical schema",
                        column.name()
                    );
                }
                // If the column is missing from the physical schema fill it in with nulls.
                // For a different behavior, provide a custom `PhysicalExprAdapter` implementation.
                let null_value = ScalarValue::Null.cast_to(logical_field.data_type())?;
                return Ok(Transformed::yes(expressions::lit(null_value)));
            }
        };
        let physical_field = self.physical_file_schema.field(physical_column_index);

        if column.index() == physical_column_index
            && logical_field.data_type() == physical_field.data_type()
        {
            return Ok(Transformed::no(expr));
        }

        let column = self.resolve_column(column, physical_column_index)?;

        if logical_field.data_type() == physical_field.data_type() {
            // If the data types match, we can use the column as is
            return Ok(Transformed::yes(Arc::new(column)));
        }

        // We need to cast the column to the logical data type
        // TODO: add optimization to move the cast from the column to literal expressions in the case of `col = 123`
        // since that's much cheaper to evalaute.
        // See https://github.com/apache/datafusion/issues/15780#issuecomment-2824716928
        self.create_cast_column_expr(column, logical_field)
    }

    /// Resolves a column expression, handling index and type mismatches.
    ///
    /// Returns the appropriate Column expression when the column's index or data type
    /// don't match the physical schema. Assumes that the early-exit case (both index
    /// and type match) has already been checked by the caller.
    fn resolve_column(
        &self,
        column: &Column,
        physical_column_index: usize,
    ) -> Result<Column> {
        if column.index() == physical_column_index {
            Ok(column.clone())
        } else {
            Column::new_with_schema(column.name(), self.physical_file_schema.as_ref())
        }
    }

    /// Validates type compatibility and creates a CastColumnExpr if needed.
    ///
    /// Checks whether the physical field can be cast to the logical field type,
    /// handling both struct and scalar types. Returns a CastColumnExpr with the
    /// appropriate configuration.
    fn create_cast_column_expr(
        &self,
        column: Column,
        logical_field: &Field,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        let actual_physical_field = self.physical_file_schema.field(column.index());

        // For struct types, use validate_struct_compatibility which handles:
        // - Missing fields in source (filled with nulls)
        // - Extra fields in source (ignored)
        // - Recursive validation of nested structs
        // For non-struct types, use Arrow's can_cast_types
        match (actual_physical_field.data_type(), logical_field.data_type()) {
            (DataType::Struct(physical_fields), DataType::Struct(logical_fields)) => {
                validate_struct_compatibility(
                    physical_fields.as_ref(),
                    logical_fields.as_ref(),
                )?;
            }
            _ => {
                let is_compatible = can_cast_types(
                    actual_physical_field.data_type(),
                    logical_field.data_type(),
                );
                if !is_compatible {
                    return exec_err!(
                        "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                        column.name(),
                        actual_physical_field.data_type(),
                        logical_field.data_type()
                    );
                }
            }
        }

        let cast_expr = Arc::new(CastColumnExpr::new(
            Arc::new(column),
            Arc::new(actual_physical_field.clone()),
            Arc::new(logical_field.clone()),
            None,
        ));

        Ok(Transformed::yes(cast_expr))
    }
}

/// Factory for creating [`BatchAdapter`] instances to adapt record batches
/// to a target schema.
///
/// This binds a target schema and allows creating adapters for different source schemas.
/// It handles:
/// - **Column reordering**: Columns are reordered to match the target schema
/// - **Type casting**: Automatic type conversion (e.g., Int32 to Int64)
/// - **Missing columns**: Nullable columns missing from source are filled with nulls
/// - **Struct field adaptation**: Nested struct fields are recursively adapted
///
/// ## Examples
///
/// ```rust
/// use arrow::array::{Int32Array, Int64Array, StringArray, RecordBatch};
/// use arrow::datatypes::{DataType, Field, Schema};
/// use datafusion_physical_expr_adapter::BatchAdapterFactory;
/// use std::sync::Arc;
///
/// // Target schema has different column order and types
/// let target_schema = Arc::new(Schema::new(vec![
///     Field::new("name", DataType::Utf8, true),
///     Field::new("id", DataType::Int64, false),    // Int64 in target
///     Field::new("score", DataType::Float64, true), // Missing from source
/// ]));
///
/// // Source schema has different column order and Int32 for id
/// let source_schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),    // Int32 in source
///     Field::new("name", DataType::Utf8, true),
///     // Note: 'score' column is missing from source
/// ]));
///
/// // Create factory with target schema
/// let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
///
/// // Create adapter for this specific source schema
/// let adapter = factory.make_adapter(Arc::clone(&source_schema)).unwrap();
///
/// // Create a source batch
/// let source_batch = RecordBatch::try_new(
///     source_schema,
///     vec![
///         Arc::new(Int32Array::from(vec![1, 2, 3])),
///         Arc::new(StringArray::from(vec!["Alice", "Bob", "Carol"])),
///     ],
/// ).unwrap();
///
/// // Adapt the batch to match target schema
/// let adapted = adapter.adapt_batch(&source_batch).unwrap();
///
/// assert_eq!(adapted.num_columns(), 3);
/// assert_eq!(adapted.column(0).data_type(), &DataType::Utf8);   // name
/// assert_eq!(adapted.column(1).data_type(), &DataType::Int64);  // id (cast from Int32)
/// assert_eq!(adapted.column(2).data_type(), &DataType::Float64); // score (filled with nulls)
/// ```
#[derive(Debug)]
pub struct BatchAdapterFactory {
    target_schema: SchemaRef,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
}

impl BatchAdapterFactory {
    /// Create a new [`BatchAdapterFactory`] with the given target schema.
    pub fn new(target_schema: SchemaRef) -> Self {
        let expr_adapter_factory = Arc::new(DefaultPhysicalExprAdapterFactory);
        Self {
            target_schema,
            expr_adapter_factory,
        }
    }

    /// Set a custom [`PhysicalExprAdapterFactory`] to use when adapting expressions.
    ///
    /// Use this to customize behavior when adapting batches, e.g. to fill in missing values
    /// with defaults instead of nulls.
    ///
    /// See [`PhysicalExprAdapter`] for more details.
    pub fn with_adapter_factory(
        self,
        factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        Self {
            expr_adapter_factory: factory,
            ..self
        }
    }

    /// Create a new [`BatchAdapter`] for the given source schema.
    ///
    /// Batches fed into this [`BatchAdapter`] *must* conform to the source schema,
    /// no validation is performed at runtime to minimize overheads.
    pub fn make_adapter(&self, source_schema: SchemaRef) -> Result<BatchAdapter> {
        let expr_adapter = self
            .expr_adapter_factory
            .create(Arc::clone(&self.target_schema), Arc::clone(&source_schema))?;

        let simplifier = PhysicalExprSimplifier::new(&self.target_schema);

        let projection = ProjectionExprs::from_indices(
            &(0..self.target_schema.fields().len()).collect_vec(),
            &self.target_schema,
        );

        let adapted = projection
            .try_map_exprs(|e| simplifier.simplify(expr_adapter.rewrite(e)?))?;
        let projector = adapted.make_projector(&source_schema)?;

        Ok(BatchAdapter { projector })
    }
}

/// Adapter for transforming record batches to match a target schema.
///
/// Create instances via [`BatchAdapterFactory`].
///
/// ## Performance
///
/// The adapter pre-computes the projection expressions during creation,
/// so the [`adapt_batch`](BatchAdapter::adapt_batch) call is efficient and suitable
/// for use in hot paths like streaming file scans.
#[derive(Debug)]
pub struct BatchAdapter {
    projector: Projector,
}

impl BatchAdapter {
    /// Adapt the given record batch to match the target schema.
    ///
    /// The input batch *must* conform to the source schema used when
    /// creating this adapter.
    pub fn adapt_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.projector.project_batch(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Int32Array, Int64Array, RecordBatch, RecordBatchOptions,
        StringArray, StringViewArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
    use datafusion_common::{Result, ScalarValue, assert_contains, record_batch};
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{Column, Literal, col, lit};
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
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("a", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        // Should be wrapped in a cast expression
        assert!(result.as_any().downcast_ref::<CastColumnExpr>().is_some());
    }

    #[test]
    fn test_rewrite_multi_column_expr_with_type_cast() {
        let (physical_schema, logical_schema) = create_test_schema();
        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();

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
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("data", 0));

        let error_msg = adapter.rewrite(column_expr).unwrap_err().to_string();
        // validate_struct_compatibility provides more specific error about which field can't be cast
        assert_contains!(
            error_msg,
            "Cannot cast struct field 'field1' from type Binary to type Int32"
        );
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
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("data", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        let physical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();
        let physical_field = Arc::new(Field::new(
            "data",
            DataType::Struct(physical_struct_fields),
            false,
        ));

        let logical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
        ]
        .into();
        let logical_field = Arc::new(Field::new(
            "data",
            DataType::Struct(logical_struct_fields),
            false,
        ));

        let expected = Arc::new(CastColumnExpr::new(
            Arc::new(Column::new("data", 0)),
            physical_field,
            logical_field,
            None,
        )) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_rewrite_missing_column() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
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
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
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
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
        let column_expr = Arc::new(Column::new("b", 1));

        let result = adapter.rewrite(column_expr).unwrap();

        let expected =
            Arc::new(Literal::new(ScalarValue::Utf8(None))) as Arc<dyn PhysicalExpr>;

        assert_eq!(result.to_string(), expected.to_string());
    }

    #[test]
    fn test_replace_columns_with_literals() -> Result<()> {
        let partition_value = ScalarValue::Utf8(Some("test_value".to_string()));
        let replacements = HashMap::from([("partition_col", &partition_value)]);

        let column_expr =
            Arc::new(Column::new("partition_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = replace_columns_with_literals(column_expr, &replacements)?;

        // Should be replaced with the partition value
        let literal = result
            .as_any()
            .downcast_ref::<expressions::Literal>()
            .expect("Expected literal expression");
        assert_eq!(*literal.value(), partition_value);

        Ok(())
    }

    #[test]
    fn test_replace_columns_with_literals_no_match() -> Result<()> {
        let value = ScalarValue::Utf8(Some("test_value".to_string()));
        let replacements = HashMap::from([("other_col", &value)]);

        let column_expr =
            Arc::new(Column::new("partition_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = replace_columns_with_literals(column_expr, &replacements)?;

        assert!(result.as_any().downcast_ref::<Column>().is_some());
        Ok(())
    }

    #[test]
    fn test_replace_columns_with_literals_nested_expr() -> Result<()> {
        let value_a = ScalarValue::Int64(Some(10));
        let value_b = ScalarValue::Int64(Some(20));
        let replacements = HashMap::from([("a", &value_a), ("b", &value_b)]);

        let expr = Arc::new(expressions::BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
        )) as Arc<dyn PhysicalExpr>;

        let result = replace_columns_with_literals(expr, &replacements)?;
        assert_eq!(result.to_string(), "10 + 20");

        Ok(())
    }

    #[test]
    fn test_rewrite_no_change_needed() -> Result<()> {
        let (physical_schema, logical_schema) = create_test_schema();

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
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
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();
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
        let adapter = factory
            .create(Arc::clone(&logical_schema), Arc::clone(&physical_schema))
            .unwrap();

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

    /// Test that struct columns are properly adapted including:
    /// - Type casting of subfields (Int32 -> Int64, Utf8 -> Utf8View)
    /// - Missing fields in logical schema are filled with nulls
    #[test]
    fn test_adapt_struct_batches() {
        // Physical struct: {id: Int32, name: Utf8}
        let physical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();

        let struct_array = StructArray::new(
            physical_struct_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as _,
                Arc::new(StringArray::from(vec![
                    Some("alice"),
                    None,
                    Some("charlie"),
                ])) as _,
            ],
            None,
        );

        let physical_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(physical_struct_fields),
            false,
        )]));

        let physical_batch = RecordBatch::try_new(
            Arc::clone(&physical_schema),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        // Logical struct: {id: Int64, name: Utf8View, extra: Boolean}
        // - id: cast from Int32 to Int64
        // - name: cast from Utf8 to Utf8View
        // - extra: missing from physical, should be filled with nulls
        let logical_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, true),
            Field::new("extra", DataType::Boolean, true), // New field, not in physical
        ]
        .into();

        let logical_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(logical_struct_fields),
            false,
        )]));

        let projection = vec![col("data", &logical_schema).unwrap()];

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::clone(&logical_schema), Arc::clone(&physical_schema))
            .unwrap();

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

        assert_eq!(res.num_columns(), 1);

        let result_struct = res
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Verify id field is cast to Int64
        let id_col = result_struct.column_by_name("id").unwrap();
        assert_eq!(id_col.data_type(), &DataType::Int64);
        let id_values = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            id_values.iter().collect_vec(),
            vec![Some(1), Some(2), Some(3)]
        );

        // Verify name field is cast to Utf8View
        let name_col = result_struct.column_by_name("name").unwrap();
        assert_eq!(name_col.data_type(), &DataType::Utf8View);
        let name_values = name_col.as_any().downcast_ref::<StringViewArray>().unwrap();
        assert_eq!(
            name_values.iter().collect_vec(),
            vec![Some("alice"), None, Some("charlie")]
        );

        // Verify extra field (missing from physical) is filled with nulls
        let extra_col = result_struct.column_by_name("extra").unwrap();
        assert_eq!(extra_col.data_type(), &DataType::Boolean);
        let extra_values = extra_col.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(extra_values.iter().collect_vec(), vec![None, None, None]);
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
            logical_file_schema: Arc::new(logical_schema),
            physical_file_schema: Arc::new(physical_schema),
        };

        // Test that when a field exists in physical schema, it returns None
        let column = Arc::new(Column::new("struct_col", 0)) as Arc<dyn PhysicalExpr>;
        let result = rewriter.try_rewrite_struct_field_access(&column).unwrap();
        assert!(result.is_none());

        // The actual test for the get_field expression would require creating a proper ScalarFunctionExpr
        // with ScalarUDF, which is complex to set up in a unit test. The integration tests in
        // datafusion/core/tests/parquet/schema_adapter.rs provide better coverage for this functionality.
    }

    // ============================================================================
    // BatchAdapterFactory and BatchAdapter tests
    // ============================================================================

    #[test]
    fn test_batch_adapter_factory_basic() {
        // Target schema
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        // Source schema with different column order and type
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false), // Int32 -> Int64
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
        let adapter = factory.make_adapter(Arc::clone(&source_schema)).unwrap();

        // Create source batch
        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("hello"), None, Some("world")])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let adapted = adapter.adapt_batch(&source_batch).unwrap();

        // Verify schema matches target
        assert_eq!(adapted.num_columns(), 2);
        assert_eq!(adapted.schema().field(0).name(), "a");
        assert_eq!(adapted.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(adapted.schema().field(1).name(), "b");
        assert_eq!(adapted.schema().field(1).data_type(), &DataType::Utf8);

        // Verify data
        let col_a = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col_a.iter().collect_vec(), vec![Some(1), Some(2), Some(3)]);

        let col_b = adapted
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            col_b.iter().collect_vec(),
            vec![Some("hello"), None, Some("world")]
        );
    }

    #[test]
    fn test_batch_adapter_factory_missing_column() {
        // Target schema with a column missing from source
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true), // exists in source
            Field::new("c", DataType::Float64, true), // missing from source
        ]));

        let source_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
        let adapter = factory.make_adapter(Arc::clone(&source_schema)).unwrap();

        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let adapted = adapter.adapt_batch(&source_batch).unwrap();

        assert_eq!(adapted.num_columns(), 3);

        // Missing column should be filled with nulls
        let col_c = adapted.column(2);
        assert_eq!(col_c.data_type(), &DataType::Float64);
        assert_eq!(col_c.null_count(), 2); // All nulls
    }

    #[test]
    fn test_batch_adapter_factory_with_struct() {
        // Target has struct with Int64 id
        let target_struct_fields: Fields = vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(target_struct_fields),
            false,
        )]));

        // Source has struct with Int32 id
        let source_struct_fields: Fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]
        .into();
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "data",
            DataType::Struct(source_struct_fields.clone()),
            false,
        )]));

        let struct_array = StructArray::new(
            source_struct_fields,
            vec![
                Arc::new(Int32Array::from(vec![10, 20])) as _,
                Arc::new(StringArray::from(vec!["a", "b"])) as _,
            ],
            None,
        );

        let source_batch = RecordBatch::try_new(
            Arc::clone(&source_schema),
            vec![Arc::new(struct_array)],
        )
        .unwrap();

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));
        let adapter = factory.make_adapter(source_schema).unwrap();
        let adapted = adapter.adapt_batch(&source_batch).unwrap();

        let result_struct = adapted
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        // Verify id was cast to Int64
        let id_col = result_struct.column_by_name("id").unwrap();
        assert_eq!(id_col.data_type(), &DataType::Int64);
        let id_values = id_col.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(id_values.iter().collect_vec(), vec![Some(10), Some(20)]);
    }

    #[test]
    fn test_batch_adapter_factory_identity() {
        // When source and target schemas are identical, should pass through efficiently
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&schema));
        let adapter = factory.make_adapter(Arc::clone(&schema)).unwrap();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let adapted = adapter.adapt_batch(&batch).unwrap();

        assert_eq!(adapted.num_columns(), 2);
        assert_eq!(adapted.schema().field(0).data_type(), &DataType::Int32);
        assert_eq!(adapted.schema().field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_batch_adapter_factory_reuse() {
        // Factory can create multiple adapters for different source schemas
        let target_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, true),
        ]));

        let factory = BatchAdapterFactory::new(Arc::clone(&target_schema));

        // First source schema
        let source1 = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, true),
        ]));
        let adapter1 = factory.make_adapter(source1).unwrap();

        // Second source schema (different order)
        let source2 = Arc::new(Schema::new(vec![
            Field::new("y", DataType::Utf8, true),
            Field::new("x", DataType::Int64, false),
        ]));
        let adapter2 = factory.make_adapter(source2).unwrap();

        // Both should work correctly
        assert!(format!("{:?}", adapter1).contains("BatchAdapter"));
        assert!(format!("{:?}", adapter2).contains("BatchAdapter"));
    }

    #[test]
    fn test_rewrite_column_index_and_type_mismatch() {
        let physical_schema = Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false), // Index 1
        ]);

        let logical_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false), // Index 0, Different Type
            Field::new("b", DataType::Utf8, true),
        ]);

        let factory = DefaultPhysicalExprAdapterFactory;
        let adapter = factory
            .create(Arc::new(logical_schema), Arc::new(physical_schema))
            .unwrap();

        // Logical column "a" is at index 0
        let column_expr = Arc::new(Column::new("a", 0));

        let result = adapter.rewrite(column_expr).unwrap();

        // Should be a CastColumnExpr
        let cast_expr = result
            .as_any()
            .downcast_ref::<CastColumnExpr>()
            .expect("Expected CastColumnExpr");

        // Verify the inner column points to the correct physical index (1)
        let inner_col = cast_expr
            .expr()
            .as_any()
            .downcast_ref::<Column>()
            .expect("Expected inner Column");
        assert_eq!(inner_col.name(), "a");
        assert_eq!(inner_col.index(), 1); // Physical index is 1

        // Verify cast types
        assert_eq!(
            cast_expr.data_type(&Schema::empty()).unwrap(),
            DataType::Int64
        );
    }
}
