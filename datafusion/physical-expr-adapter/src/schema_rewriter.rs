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

//! Physical expression schema rewriting utilities with struct support

use std::sync::Arc;

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::{
    exec_err, plan_err,
    tree_node::{Transformed, TransformedResult, TreeNode},
    Result, ScalarValue,
};
use datafusion_expr::ScalarUDF;
use datafusion_functions::core::{getfield::GetFieldFunc, r#struct::StructFunc};
use datafusion_physical_expr::{
    expressions::{self, CastExpr, Column},
    ScalarFunctionExpr,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Validates compatibility between source and target struct fields for casting operations.
///
/// This function implements comprehensive struct compatibility checking by examining:
/// - Field name matching between source and target structs  
/// - Type castability for each matching field (including recursive struct validation)
/// - Proper handling of missing fields (target fields not in source are allowed - filled with nulls)
/// - Proper handling of extra fields (source fields not in target are allowed - ignored)
///
/// # Compatibility Rules
/// - **Field Matching**: Fields are matched by name (case-sensitive)
/// - **Missing Target Fields**: Allowed - will be filled with null values during casting
/// - **Extra Source Fields**: Allowed - will be ignored during casting  
/// - **Type Compatibility**: Each matching field must be castable using Arrow's type system
/// - **Nested Structs**: Recursively validates nested struct compatibility
///
/// # Arguments
/// * `source_fields` - Fields from the source struct type
/// * `target_fields` - Fields from the target struct type
///
/// # Returns
/// * `Ok(true)` if the structs are compatible for casting
/// * `Err(DataFusionError)` with detailed error message if incompatible
///
/// # Examples
/// ```text
/// // Compatible: source has extra field, target has missing field
/// // Source: {a: i32, b: string, c: f64}  
/// // Target: {a: i64, d: bool}
/// // Result: Ok(true) - 'a' can cast i32->i64, 'b','c' ignored, 'd' filled with nulls
///
/// // Incompatible: matching field has incompatible types
/// // Source: {a: string}
/// // Target: {a: binary}
/// // Result: Err(...) - string cannot cast to binary
/// ```
pub fn validate_struct_compatibility(
    source_fields: &[FieldRef],
    target_fields: &[FieldRef],
) -> Result<bool> {
    // Check compatibility for each target field
    for target_field in target_fields {
        // Look for matching field in source by name
        if let Some(source_field) = source_fields
            .iter()
            .find(|f| f.name() == target_field.name())
        {
            // Check if the matching field types are compatible
            match (source_field.data_type(), target_field.data_type()) {
                // Recursively validate nested structs
                (DataType::Struct(source_nested), DataType::Struct(target_nested)) => {
                    validate_struct_compatibility(source_nested, target_nested)?;
                }
                // For non-struct types, use the existing castability check
                _ => {
                    if !can_cast_types(source_field.data_type(), target_field.data_type())
                    {
                        return plan_err!(
                            "Cannot cast struct field '{}' from type {:?} to type {:?}",
                            target_field.name(),
                            source_field.data_type(),
                            target_field.data_type()
                        );
                    }
                }
            }
        }
        // Missing fields in source are OK - they'll be filled with nulls
    }

    // Extra fields in source are OK - they'll be ignored
    Ok(true)
}

/// Build a struct expression by recursively extracting and rewriting fields from a source struct.
///
/// This function creates a new struct expression by:
/// 1. For each target field, checking if it exists in the source struct
/// 2. If it exists, using GetFieldFunc to extract it and recursively rewriting it
/// 3. If it doesn't exist, creating a null literal of the appropriate type
/// 4. Using StructFunc to combine all field expressions into the target struct
///
/// # Arguments
/// * `rewriter` - The schema rewriter instance for recursive rewriting
/// * `source_expr` - The source struct expression
/// * `source_fields` - Fields from the source struct type
/// * `target_fields` - Fields from the target struct type  
/// * `target_field_name` - Name of the target struct field (for error messages)
///
/// # Returns
/// A new struct expression matching the target field layout
fn build_struct_expr(
    rewriter: &PhysicalExprSchemaRewriter,
    source_expr: Arc<dyn PhysicalExpr>,
    source_fields: &[FieldRef],
    target_fields: &[FieldRef],
    _target_field_name: &str,
) -> Result<Arc<dyn PhysicalExpr>> {
    let mut field_exprs = Vec::new();

    for target_field in target_fields {
        let field_expr = if let Some(source_field) = source_fields
            .iter()
            .find(|f| f.name() == target_field.name())
        {
            // Field exists in source - extract it using GetFieldFunc
            let get_field_func = Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::new()));
            let field_name_literal =
                expressions::lit(ScalarValue::Utf8(Some(target_field.name().clone())));

            let get_field_expr = Arc::new(ScalarFunctionExpr::new(
                "get_field",
                get_field_func,
                vec![Arc::clone(&source_expr), field_name_literal],
                source_field.clone(),
            ));

            // Handle field type conversion based on source and target types
            match (source_field.data_type(), target_field.data_type()) {
                (
                    DataType::Struct(nested_source_fields),
                    DataType::Struct(nested_target_fields),
                ) => {
                    // For nested structs, recursively build the nested struct expression
                    build_struct_expr(
                        rewriter,
                        get_field_expr,
                        nested_source_fields,
                        nested_target_fields,
                        target_field.name(),
                    )?
                }
                _ if source_field.data_type() == target_field.data_type() => {
                    // Types match, no further rewriting needed
                    get_field_expr
                }
                _ => {
                    // Types differ, apply normal casting
                    Arc::new(CastExpr::new(
                        get_field_expr,
                        target_field.data_type().clone(),
                        None,
                    ))
                }
            }
        } else {
            // Field doesn't exist in source - create null literal
            let null_value = ScalarValue::Null.cast_to(target_field.data_type())?;
            expressions::lit(null_value)
        };

        field_exprs.push(field_expr);
    }

    // Build the final struct using StructFunc
    let struct_func = Arc::new(ScalarUDF::new_from_impl(StructFunc::new()));
    let return_field = Arc::new(arrow::datatypes::Field::new(
        "struct",
        DataType::Struct(target_fields.to_vec().into()),
        true,
    ));

    Ok(Arc::new(ScalarFunctionExpr::new(
        "struct",
        struct_func,
        field_exprs,
        return_field,
    )))
}

/// Builder for rewriting physical expressions to match different schemas.
///
/// # Example
///
/// ```rust
/// use datafusion_physical_expr_adapter::PhysicalExprSchemaRewriter;
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
    /// 4. Recursively rebuilds struct expressions with proper field mapping
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
        // Get the logical field for this column
        let logical_field = match self.logical_file_schema.field_with_name(column.name())
        {
            Ok(field) => field,
            Err(e) => {
                // If the column is a partition field, we can use the partition value
                if let Some(partition_value) = self.get_partition_value(column.name()) {
                    return Ok(Transformed::yes(expressions::lit(partition_value)));
                }
                // If the column is not found in the logical schema and is not a partition value, return an error
                // This should probably never be hit unless something upstream broke, but nontheless it's better
                // for us to return a handleable error than to panic / do something unexpected.
                return Err(e.into());
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

        // Check compatibility - for structs use our validation, for others use Arrow's can_cast_types
        let is_compatible = match (physical_field.data_type(), logical_field.data_type())
        {
            (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
                validate_struct_compatibility(source_fields, target_fields).is_ok()
            }
            _ => can_cast_types(physical_field.data_type(), logical_field.data_type()),
        };

        if !is_compatible {
            return exec_err!(
                "Cannot cast column '{}' from '{}' (physical data type) to '{}' (logical data type)",
                column.name(),
                physical_field.data_type(),
                logical_field.data_type()
            );
        }

        // For struct types, we need to recursively build the struct expression
        let cast_expr: Arc<dyn PhysicalExpr> =
            match (physical_field.data_type(), logical_field.data_type()) {
                (DataType::Struct(source_fields), DataType::Struct(target_fields)) => {
                    build_struct_expr(
                        self,
                        Arc::new(column),
                        source_fields,
                        target_fields,
                        logical_field.name(),
                    )?
                }
                _ => Arc::new(CastExpr::new(
                    Arc::new(column),
                    logical_field.data_type().clone(),
                    None,
                )),
            };

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
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{col, BinaryExpr};
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

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("a", 0));

        let result = rewriter.rewrite(column_expr).unwrap();

        // Should be wrapped in a cast expression
        assert!(result.as_any().downcast_ref::<CastExpr>().is_some());
    }

    #[test]
    fn test_validate_struct_compatibility_compatible() -> Result<()> {
        // Source struct: {a: Int32, b: Utf8}
        let source_fields = vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ];

        // Target struct: {a: Int64, c: Float64} (Int32 can cast to Int64, missing field c is OK)
        let target_fields = vec![
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Field::new("c", DataType::Float64, true)),
        ];

        let result = validate_struct_compatibility(&source_fields, &target_fields)?;
        assert!(result);
        Ok(())
    }

    #[test]
    fn test_validate_struct_compatibility_incompatible() {
        // Source struct: {a: Binary}
        let source_fields = vec![Arc::new(Field::new("a", DataType::Binary, true))];

        // Target struct: {a: Int32} (Binary cannot cast to Int32)
        let target_fields = vec![Arc::new(Field::new("a", DataType::Int32, true))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast struct field 'a'"));
        assert!(error_msg.contains("Binary"));
        assert!(error_msg.contains("Int32"));
    }

    #[test]
    fn test_validate_struct_compatibility_nested_structs() -> Result<()> {
        // Source nested struct: {info: {name: Utf8, age: Int32}}
        let source_nested_fields = vec![
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(Field::new("age", DataType::Int32, true)),
        ];
        let source_fields = vec![Arc::new(Field::new(
            "info",
            DataType::Struct(source_nested_fields.into()),
            true,
        ))];

        // Target nested struct: {info: {name: Utf8, age: Int64, location: Utf8}}
        // (Int32 can cast to Int64, missing location field is OK)
        let target_nested_fields = vec![
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(Field::new("age", DataType::Int64, true)),
            Arc::new(Field::new("location", DataType::Utf8, true)),
        ];
        let target_fields = vec![Arc::new(Field::new(
            "info",
            DataType::Struct(target_nested_fields.into()),
            true,
        ))];

        let result = validate_struct_compatibility(&source_fields, &target_fields)?;
        assert!(result);
        Ok(())
    }

    #[test]
    fn test_rewrite_struct_column_compatibility() -> Result<()> {
        // Test that struct compatibility validation is used in schema rewriting
        let physical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("field1", DataType::Int32, true),
                    Field::new("field2", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )]);

        let logical_schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(
                vec![
                    Field::new("field1", DataType::Int64, true), // Compatible cast
                    Field::new("field3", DataType::Float64, true), // Missing field, should be OK
                ]
                .into(),
            ),
            true,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("data", 0));

        // This should succeed because structs are compatible
        let result = rewriter.rewrite(column_expr);
        assert!(result.is_ok());

        // Should be wrapped in a struct function expression
        let struct_expr = result.unwrap();
        assert!(struct_expr
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some());

        Ok(())
    }

    #[test]
    fn test_rewrite_struct_column_incompatible() {
        // Test that incompatible struct fields are rejected
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

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);
        let column_expr = Arc::new(Column::new("data", 0));

        // This should fail because Binary cannot cast to Int32
        let result = rewriter.rewrite(column_expr);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast column 'data'"));
    }

    /// Test end-to-end struct evolution with simple field additions
    #[test]
    fn test_evolved_schema_struct_field_addition() -> Result<()> {
        // Physical schema: {user_info: {id: i32, name: string}}
        let physical_schema = Schema::new(vec![Field::new(
            "user_info",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )]);

        // Logical schema: {user_info: {id: i32, name: string, email: string}}
        // New field "email" added to struct
        let logical_schema = Schema::new(vec![Field::new(
            "user_info",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("email", DataType::Utf8, true), // New field
                ]
                .into(),
            ),
            false,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Test that we can rewrite a column reference
        let column_expr = Arc::new(Column::new("user_info", 0));
        let result = rewriter.rewrite(column_expr)?;

        // Should be a struct function expression
        assert!(result
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some());

        // Test that we can rewrite a predicate on existing fields
        let predicate = Arc::new(BinaryExpr::new(
            col("user_info", &logical_schema)?,
            Operator::IsNotDistinctFrom,
            expressions::lit(ScalarValue::Null),
        )) as Arc<dyn PhysicalExpr>;

        let rewritten_predicate = rewriter.rewrite(predicate)?;
        assert!(rewritten_predicate
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .is_some());

        Ok(())
    }

    /// Test end-to-end struct evolution with field type changes
    #[test]
    fn test_evolved_schema_struct_field_type_evolution() -> Result<()> {
        // Physical schema: {event_data: {timestamp: i64, count: i32}}
        let physical_schema = Schema::new(vec![Field::new(
            "event_data",
            DataType::Struct(
                vec![
                    Field::new("timestamp", DataType::Int64, false),
                    Field::new("count", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        )]);

        // Logical schema: {event_data: {timestamp: timestamp_ms, count: i64}}
        // timestamp field evolved from i64 to timestamp, count from i32 to i64
        let logical_schema = Schema::new(vec![Field::new(
            "event_data",
            DataType::Struct(
                vec![
                    Field::new(
                        "timestamp",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    Field::new("count", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Test column rewriting
        let column_expr = Arc::new(Column::new("event_data", 0));
        let result = rewriter.rewrite(column_expr)?;

        // Should be a struct function expression that handles the type conversions
        assert!(result
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some());

        Ok(())
    }

    /// Test end-to-end struct evolution with nested structs
    #[test]
    fn test_evolved_schema_nested_struct_evolution() -> Result<()> {
        // Physical schema: {
        //   metadata: {
        //     user: {id: i32, name: string},
        //     created_at: i64
        //   }
        // }
        let physical_schema = Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(
                vec![
                    Field::new(
                        "user",
                        DataType::Struct(
                            vec![
                                Field::new("id", DataType::Int32, false),
                                Field::new("name", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    ),
                    Field::new("created_at", DataType::Int64, false),
                ]
                .into(),
            ),
            false,
        )]);

        // Logical schema: {
        //   metadata: {
        //     user: {id: i64, name: string, email: string},
        //     created_at: timestamp_ms,
        //     version: i32
        //   }
        // }
        let logical_schema = Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(
                vec![
                    Field::new(
                        "user",
                        DataType::Struct(
                            vec![
                                Field::new("id", DataType::Int64, false), // Type change
                                Field::new("name", DataType::Utf8, true),
                                Field::new("email", DataType::Utf8, true), // New field
                            ]
                            .into(),
                        ),
                        false,
                    ),
                    Field::new(
                        "created_at",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ), // Type change
                    Field::new("version", DataType::Int32, true), // New field
                ]
                .into(),
            ),
            false,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Test that we can handle deeply nested struct evolution
        let column_expr = Arc::new(Column::new("metadata", 0));
        let result = rewriter.rewrite(column_expr)?;

        // Should be a struct function expression
        assert!(result
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some());

        Ok(())
    }

    /// Test end-to-end struct evolution with field removal (extra fields in source)
    #[test]
    fn test_evolved_schema_struct_field_removal() -> Result<()> {
        // Physical schema: {config: {debug_mode: bool, log_level: string, deprecated_flag: bool}}
        let physical_schema = Schema::new(vec![Field::new(
            "config",
            DataType::Struct(
                vec![
                    Field::new("debug_mode", DataType::Boolean, false),
                    Field::new("log_level", DataType::Utf8, true),
                    Field::new("deprecated_flag", DataType::Boolean, true), // This will be ignored
                ]
                .into(),
            ),
            false,
        )]);

        // Logical schema: {config: {debug_mode: bool, log_level: string}}
        // deprecated_flag removed from logical schema
        let logical_schema = Schema::new(vec![Field::new(
            "config",
            DataType::Struct(
                vec![
                    Field::new("debug_mode", DataType::Boolean, false),
                    Field::new("log_level", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Test that extra fields are properly ignored
        let column_expr = Arc::new(Column::new("config", 0));
        let result = rewriter.rewrite(column_expr)?;

        // Should be a struct function expression that ignores the deprecated field
        assert!(result
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some());

        Ok(())
    }

    /// Test end-to-end struct evolution with mixed scenarios (realistic data evolution)
    #[test]
    fn test_evolved_schema_complex_struct_evolution() -> Result<()> {
        // Simulate a realistic data evolution scenario:
        // Physical schema represents an older version of the data
        let physical_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "profile",
                DataType::Struct(
                    vec![
                        Field::new("username", DataType::Utf8, false),
                        Field::new("age", DataType::Int32, true),
                        Field::new("legacy_score", DataType::Float32, true), // Will be removed
                    ]
                    .into(),
                ),
                true,
            ),
        ]);

        // Logical schema represents the current/target version
        let logical_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false), // Type upgrade
            Field::new(
                "profile",
                DataType::Struct(
                    vec![
                        Field::new("username", DataType::Utf8, false),
                        Field::new("age", DataType::Int64, true), // Type upgrade
                        Field::new("email", DataType::Utf8, true), // New field
                        Field::new(
                            "preferences",
                            DataType::Struct(
                                vec![
                                    Field::new("theme", DataType::Utf8, true),
                                    Field::new("notifications", DataType::Boolean, true),
                                ]
                                .into(),
                            ),
                            true,
                        ), // New nested struct
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ), // New field
        ]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Test rewriting of simple field with type change
        let id_expr = Arc::new(Column::new("id", 0));
        let id_result = rewriter.rewrite(id_expr)?;
        assert!(id_result.as_any().downcast_ref::<CastExpr>().is_some());

        // Test rewriting of complex struct field
        let profile_expr = Arc::new(Column::new("profile", 1));
        let profile_result = rewriter.rewrite(profile_expr)?;
        assert!(profile_result
            .as_any()
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some());

        // Test rewriting of missing field (should become null)
        let created_at_expr = Arc::new(Column::new("created_at", 2));
        let created_at_result = rewriter.rewrite(created_at_expr)?;
        assert!(created_at_result
            .as_any()
            .downcast_ref::<datafusion_physical_expr::expressions::Literal>()
            .is_some());

        Ok(())
    }

    /// Test that struct evolution works correctly with predicates
    #[test]
    fn test_evolved_schema_struct_with_predicates() -> Result<()> {
        // Physical schema: {event: {type: string, data: {count: i32}}}
        let physical_schema = Schema::new(vec![Field::new(
            "event",
            DataType::Struct(
                vec![
                    Field::new("type", DataType::Utf8, false),
                    Field::new(
                        "data",
                        DataType::Struct(
                            vec![Field::new("count", DataType::Int32, false)].into(),
                        ),
                        false,
                    ),
                ]
                .into(),
            ),
            false,
        )]);

        // Logical schema: {event: {type: string, data: {count: i64, timestamp: i64}}}
        let logical_schema = Schema::new(vec![Field::new(
            "event",
            DataType::Struct(
                vec![
                    Field::new("type", DataType::Utf8, false),
                    Field::new(
                        "data",
                        DataType::Struct(
                            vec![
                                Field::new("count", DataType::Int64, false), // Type upgrade
                                Field::new("timestamp", DataType::Int64, true), // New field
                            ]
                            .into(),
                        ),
                        false,
                    ),
                ]
                .into(),
            ),
            false,
        )]);

        let rewriter = PhysicalExprSchemaRewriter::new(&physical_schema, &logical_schema);

        // Create a complex predicate that references the struct
        let predicate = Arc::new(BinaryExpr::new(
            col("event", &logical_schema)?,
            Operator::IsNotDistinctFrom,
            expressions::lit(ScalarValue::Null),
        )) as Arc<dyn PhysicalExpr>;

        let rewritten_predicate = rewriter.rewrite(predicate)?;

        // The predicate should be successfully rewritten
        assert!(rewritten_predicate
            .as_any()
            .downcast_ref::<BinaryExpr>()
            .is_some());

        Ok(())
    }
}
