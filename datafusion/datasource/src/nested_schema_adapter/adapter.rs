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

use crate::schema_adapter::{
    create_field_mapping, SchemaAdapter, SchemaMapper, SchemaMapping,
};
use arrow::{
    array::ArrayRef,
    datatypes::{DataType::Struct, Field, FieldRef, Schema, SchemaRef},
};
use datafusion_common::nested_struct::cast_column;
use datafusion_common::{plan_err, Result};
use std::sync::Arc;

/// A SchemaAdapter that handles schema evolution for nested struct types
#[derive(Debug, Clone)]
pub struct NestedStructSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetSource
    projected_table_schema: SchemaRef,
    /// The entire table schema for the table we're using this to adapt.
    ///
    /// This is used to evaluate any filters pushed down into the scan
    /// which may refer to columns that are not referred to anywhere
    /// else in the plan.
    table_schema: SchemaRef,
}

impl NestedStructSchemaAdapter {
    /// Create a new NestedStructSchemaAdapter with the target schema
    pub fn new(projected_table_schema: SchemaRef, table_schema: SchemaRef) -> Self {
        Self {
            projected_table_schema,
            table_schema,
        }
    }

    pub fn projected_table_schema(&self) -> &Schema {
        self.projected_table_schema.as_ref()
    }

    pub fn table_schema(&self) -> &Schema {
        self.table_schema.as_ref()
    }
}

impl SchemaAdapter for NestedStructSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field_name = self.table_schema.field(index).name();
        file_schema.index_of(field_name).ok()
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let (field_mappings, projection) = create_field_mapping(
            file_schema,
            &self.projected_table_schema,
            |file_field, table_field| {
                // Special handling for struct fields - validate internal structure compatibility
                match (file_field.data_type(), table_field.data_type()) {
                    (Struct(source_fields), Struct(target_fields)) => {
                        validate_struct_compatibility(source_fields, target_fields)
                    }
                    _ => crate::schema_adapter::can_cast_field(file_field, table_field),
                }
            },
        )?;

        Ok((
            Arc::new(SchemaMapping::new(
                Arc::clone(&self.projected_table_schema),
                field_mappings,
                Arc::new(|array: &ArrayRef, field: &Field| cast_column(array, field)),
            )),
            projection,
        ))
    }
}

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
/// ```ignore
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
fn validate_struct_compatibility(
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
                (Struct(source_nested), Struct(target_nested)) => {
                    validate_struct_compatibility(source_nested, target_nested)?;
                }
                // For non-struct types, use the existing castability check
                _ => {
                    if !arrow::compute::can_cast_types(
                        source_field.data_type(),
                        target_field.data_type(),
                    ) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_validate_struct_compatibility_incompatible_types() {
        // Source struct: {field1: String, field2: String}
        let source_fields = vec![
            Arc::new(Field::new("field1", DataType::Utf8, true)),
            Arc::new(Field::new("field2", DataType::Utf8, true)),
        ];

        // Target struct: {field1: UInt32}
        let target_fields = vec![Arc::new(Field::new("field1", DataType::UInt32, true))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast struct field 'field1'"));
        assert!(error_msg.contains("Utf8"));
        assert!(error_msg.contains("UInt32"));
    }

    #[test]
    fn test_validate_struct_compatibility_compatible_types() {
        // Source struct: {field1: Int32, field2: String}
        let source_fields = vec![
            Arc::new(Field::new("field1", DataType::Int32, true)),
            Arc::new(Field::new("field2", DataType::Utf8, true)),
        ];

        // Target struct: {field1: Int64} (Int32 can cast to Int64)
        let target_fields = vec![Arc::new(Field::new("field1", DataType::Int64, true))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_validate_struct_compatibility_missing_field_in_source() {
        // Source struct: {field2: String} (missing field1)
        let source_fields = vec![Arc::new(Field::new("field2", DataType::Utf8, true))];

        // Target struct: {field1: Int32}
        let target_fields = vec![Arc::new(Field::new("field1", DataType::Int32, true))];

        // Should be OK - missing fields will be filled with nulls
        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true);
    }
}
