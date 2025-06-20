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

use crate::error::{DataFusionError, Result, _plan_err};
use arrow::{
    array::{new_null_array, Array, ArrayRef, StructArray},
    compute::cast,
    datatypes::{DataType::Struct, Field, FieldRef},
};
use std::sync::Arc;

/// Cast a struct column to match target struct fields, handling nested structs recursively.
///
/// This function implements struct-to-struct casting with the assumption that **structs should
/// always be allowed to cast to other structs**. However, the source column must already be
/// a struct type - non-struct sources will result in an error.
///
/// ## Field Matching Strategy
/// - **By Name**: Source struct fields are matched to target fields by name (case-sensitive)
/// - **Type Adaptation**: When a matching field is found, it is recursively cast to the target field's type
/// - **Missing Fields**: Target fields not present in the source are filled with null values
/// - **Extra Fields**: Source fields not present in the target are ignored
///
/// ## Nested Struct Handling
/// - Nested structs are handled recursively using the same casting rules
/// - Each level of nesting follows the same field matching and null-filling strategy
/// - This allows for complex struct transformations while maintaining data integrity
///
/// # Arguments
/// * `source_col` - The source array to cast (must be a struct array)
/// * `target_fields` - The target struct field definitions to cast to
///
/// # Returns
/// A `Result<ArrayRef>` containing the cast struct array
///
/// # Errors
/// Returns a `DataFusionError::Plan` if the source column is not a struct type
fn cast_struct_column(
    source_col: &ArrayRef,
    target_fields: &[Arc<Field>],
) -> Result<ArrayRef> {
    if let Some(struct_array) = source_col.as_any().downcast_ref::<StructArray>() {
        let mut children: Vec<(Arc<Field>, Arc<dyn Array>)> = Vec::new();
        let num_rows = source_col.len();

        for target_child_field in target_fields {
            let field_arc = Arc::clone(target_child_field);
            match struct_array.column_by_name(target_child_field.name()) {
                Some(source_child_col) => {
                    let adapted_child =
                        cast_column(source_child_col, target_child_field)?;
                    children.push((field_arc, adapted_child));
                }
                None => {
                    children.push((
                        field_arc,
                        new_null_array(target_child_field.data_type(), num_rows),
                    ));
                }
            }
        }

        let struct_array = StructArray::from(children);
        Ok(Arc::new(struct_array))
    } else {
        // Return error if source is not a struct type
        Err(DataFusionError::Plan(format!(
            "Cannot cast column of type {:?} to struct type. Source must be a struct to cast to struct.",
            source_col.data_type()
        )))
    }
}

/// Cast a column to match the target field type, with special handling for nested structs.
///
/// This function serves as the main entry point for column casting operations. For struct
/// types, it enforces that **only struct columns can be cast to struct types**.
///
/// ## Casting Behavior
/// - **Struct Types**: Delegates to `cast_struct_column` for struct-to-struct casting only
/// - **Non-Struct Types**: Uses Arrow's standard `cast` function for primitive type conversions
///
/// ## Struct Casting Requirements
/// The struct casting logic requires that the source column must already be a struct type.
/// This makes the function useful for:
/// - Schema evolution scenarios where struct layouts change over time
/// - Data migration between different struct schemas  
/// - Type-safe data processing pipelines that maintain struct type integrity
///
/// # Arguments
/// * `source_col` - The source array to cast
/// * `target_field` - The target field definition (including type and metadata)
///
/// # Returns
/// A `Result<ArrayRef>` containing the cast array
///
/// # Errors
/// Returns an error if:
/// - Attempting to cast a non-struct column to a struct type
/// - Arrow's cast function fails for non-struct types
/// - Memory allocation fails during struct construction
/// - Invalid data type combinations are encountered
pub fn cast_column(source_col: &ArrayRef, target_field: &Field) -> Result<ArrayRef> {
    match target_field.data_type() {
        Struct(target_fields) => cast_struct_column(source_col, target_fields),
        _ => Ok(cast(source_col, target_field.data_type())?),
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
                (Struct(source_nested), Struct(target_nested)) => {
                    validate_struct_compatibility(source_nested, target_nested)?;
                }
                // For non-struct types, use the existing castability check
                _ => {
                    if !arrow::compute::can_cast_types(
                        source_field.data_type(),
                        target_field.data_type(),
                    ) {
                        return _plan_err!(
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
    use arrow::{
        array::{Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field},
    };
    /// Macro to extract and downcast a column from a StructArray
    macro_rules! get_column_as {
        ($struct_array:expr, $column_name:expr, $array_type:ty) => {
            $struct_array
                .column_by_name($column_name)
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap()
        };
    }

    #[test]
    fn test_cast_simple_column() {
        let source = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let target_field = Field::new("ints", DataType::Int64, true);
        let result = cast_column(&source, &target_field).unwrap();
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), 3);
    }

    #[test]
    fn test_cast_struct_with_missing_field() {
        let a_array = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let source_struct = StructArray::from(vec![(
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::clone(&a_array),
        )]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = Field::new(
            "s",
            Struct(
                vec![
                    Arc::new(Field::new("a", DataType::Int32, true)),
                    Arc::new(Field::new("b", DataType::Utf8, true)),
                ]
                .into(),
            ),
            true,
        );

        let result = cast_column(&source_col, &target_field).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.fields().len(), 2);
        let a_result = get_column_as!(&struct_array, "a", Int32Array);
        assert_eq!(a_result.value(0), 1);
        assert_eq!(a_result.value(1), 2);

        let b_result = get_column_as!(&struct_array, "b", StringArray);
        assert_eq!(b_result.len(), 2);
        assert!(b_result.is_null(0));
        assert!(b_result.is_null(1));
    }

    #[test]
    fn test_cast_struct_source_not_struct() {
        let source = Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef;
        let target_field = Field::new(
            "s",
            Struct(vec![Arc::new(Field::new("a", DataType::Int32, true))].into()),
            true,
        );

        let result = cast_column(&source, &target_field);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast column of type"));
        assert!(error_msg.contains("to struct type"));
        assert!(error_msg.contains("Source must be a struct"));
    }

    #[test]
    fn test_validate_struct_compatibility_incompatible_types() {
        // Source struct: {field1: Binary, field2: String}
        let source_fields = vec![
            Arc::new(Field::new("field1", DataType::Binary, true)),
            Arc::new(Field::new("field2", DataType::Utf8, true)),
        ];

        // Target struct: {field1: Int32}
        let target_fields = vec![Arc::new(Field::new("field1", DataType::Int32, true))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast struct field 'field1'"));
        assert!(error_msg.contains("Binary"));
        assert!(error_msg.contains("Int32"));
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
        assert!(result.unwrap());
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
        assert!(result.unwrap());
    }
}
