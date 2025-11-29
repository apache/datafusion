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

use crate::error::{_plan_err, Result};
use arrow::{
    array::{Array, ArrayRef, StructArray, new_null_array},
    compute::{CastOptions, cast_with_options},
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
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    if let Some(source_struct) = source_col.as_any().downcast_ref::<StructArray>() {
        validate_struct_compatibility(source_struct.fields(), target_fields)?;

        let mut fields: Vec<Arc<Field>> = Vec::with_capacity(target_fields.len());
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(target_fields.len());
        let num_rows = source_col.len();

        for target_child_field in target_fields {
            fields.push(Arc::clone(target_child_field));
            match source_struct.column_by_name(target_child_field.name()) {
                Some(source_child_col) => {
                    let adapted_child =
                        cast_column(source_child_col, target_child_field, cast_options)
                            .map_err(|e| {
                            e.context(format!(
                                "While casting struct field '{}'",
                                target_child_field.name()
                            ))
                        })?;
                    arrays.push(adapted_child);
                }
                None => {
                    arrays.push(new_null_array(target_child_field.data_type(), num_rows));
                }
            }
        }

        let struct_array =
            StructArray::new(fields.into(), arrays, source_struct.nulls().cloned());
        Ok(Arc::new(struct_array))
    } else {
        // Return error if source is not a struct type
        _plan_err!(
            "Cannot cast column of type {} to struct type. Source must be a struct to cast to struct.",
            source_col.data_type()
        )
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
/// ## Cast Options
/// The `cast_options` argument controls how Arrow handles values that cannot be represented
/// in the target type. When `safe` is `false` (DataFusion's default) the cast will return an
/// error if such a value is encountered. Setting `safe` to `true` instead produces `NULL`
/// for out-of-range or otherwise invalid values. The options also allow customizing how
/// temporal values are formatted when cast to strings.
///
/// ```
/// use arrow::array::{ArrayRef, Int64Array};
/// use arrow::compute::CastOptions;
/// use arrow::datatypes::{DataType, Field};
/// use datafusion_common::nested_struct::cast_column;
/// use std::sync::Arc;
///
/// let source: ArrayRef = Arc::new(Int64Array::from(vec![1, i64::MAX]));
/// let target = Field::new("ints", DataType::Int32, true);
/// // Permit lossy conversions by producing NULL on overflow instead of erroring
/// let options = CastOptions {
///     safe: true,
///     ..Default::default()
/// };
/// let result = cast_column(&source, &target, &options).unwrap();
/// assert!(result.is_null(1));
/// ```
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
/// * `cast_options` - Options that govern strictness and formatting of the cast
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
pub fn cast_column(
    source_col: &ArrayRef,
    target_field: &Field,
    cast_options: &CastOptions,
) -> Result<ArrayRef> {
    match target_field.data_type() {
        Struct(target_fields) => {
            cast_struct_column(source_col, target_fields, cast_options)
        }
        _ => Ok(cast_with_options(
            source_col,
            target_field.data_type(),
            cast_options,
        )?),
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
/// * `Ok(())` if the structs are compatible for casting
/// * `Err(DataFusionError)` with detailed error message if incompatible
///
/// # Examples
/// ```text
/// // Compatible: source has extra field, target has missing field
/// // Source: {a: i32, b: string, c: f64}
/// // Target: {a: i64, d: bool}
/// // Result: Ok(()) - 'a' can cast i32->i64, 'b','c' ignored, 'd' filled with nulls
///
/// // Incompatible: matching field has incompatible types
/// // Source: {a: string}
/// // Target: {a: binary}
/// // Result: Err(...) - string cannot cast to binary
/// ```
pub fn validate_struct_compatibility(
    source_fields: &[FieldRef],
    target_fields: &[FieldRef],
) -> Result<()> {
    // Check compatibility for each target field
    for target_field in target_fields {
        // Look for matching field in source by name
        if let Some(source_field) = source_fields
            .iter()
            .find(|f| f.name() == target_field.name())
        {
            // Ensure nullability is compatible. It is invalid to cast a nullable
            // source field to a non-nullable target field as this may discard
            // null values.
            if source_field.is_nullable() && !target_field.is_nullable() {
                return _plan_err!(
                    "Cannot cast nullable struct field '{}' to non-nullable field",
                    target_field.name()
                );
            }
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
                            "Cannot cast struct field '{}' from type {} to type {}",
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
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::format::DEFAULT_CAST_OPTIONS;
    use arrow::{
        array::{
            BinaryArray, Int32Array, Int32Builder, Int64Array, ListArray, MapArray,
            MapBuilder, StringArray, StringBuilder,
        },
        buffer::NullBuffer,
        datatypes::{DataType, Field, FieldRef, Int32Type},
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

    fn field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
    }

    fn non_null_field(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, false)
    }

    fn arc_field(name: &str, data_type: DataType) -> FieldRef {
        Arc::new(field(name, data_type))
    }

    fn struct_type(fields: Vec<Field>) -> DataType {
        Struct(fields.into())
    }

    fn struct_field(name: &str, fields: Vec<Field>) -> Field {
        field(name, struct_type(fields))
    }

    fn arc_struct_field(name: &str, fields: Vec<Field>) -> FieldRef {
        Arc::new(struct_field(name, fields))
    }

    #[test]
    fn test_cast_simple_column() {
        let source = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let target_field = field("ints", DataType::Int64);
        let result = cast_column(&source, &target_field, &DEFAULT_CAST_OPTIONS).unwrap();
        let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 1);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), 3);
    }

    #[test]
    fn test_cast_column_with_options() {
        let source = Arc::new(Int64Array::from(vec![1, i64::MAX])) as ArrayRef;
        let target_field = field("ints", DataType::Int32);

        let safe_opts = CastOptions {
            // safe: false - return Err for failure
            safe: false,
            ..DEFAULT_CAST_OPTIONS
        };
        assert!(cast_column(&source, &target_field, &safe_opts).is_err());

        let unsafe_opts = CastOptions {
            // safe: true - return Null for failure
            safe: true,
            ..DEFAULT_CAST_OPTIONS
        };
        let result = cast_column(&source, &target_field, &unsafe_opts).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.value(0), 1);
        assert!(result.is_null(1));
    }

    #[test]
    fn test_cast_struct_with_missing_field() {
        let a_array = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let source_struct = StructArray::from(vec![(
            arc_field("a", DataType::Int32),
            Arc::clone(&a_array),
        )]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![field("a", DataType::Int32), field("b", DataType::Utf8)],
        );

        let result =
            cast_column(&source_col, &target_field, &DEFAULT_CAST_OPTIONS).unwrap();
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
        let target_field = struct_field("s", vec![field("a", DataType::Int32)]);

        let result = cast_column(&source, &target_field, &DEFAULT_CAST_OPTIONS);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast column of type"));
        assert!(error_msg.contains("to struct type"));
        assert!(error_msg.contains("Source must be a struct"));
    }

    #[test]
    fn test_cast_struct_incompatible_child_type() {
        let a_array = Arc::new(BinaryArray::from(vec![
            Some(b"a".as_ref()),
            Some(b"b".as_ref()),
        ])) as ArrayRef;
        let source_struct =
            StructArray::from(vec![(arc_field("a", DataType::Binary), a_array)]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field("s", vec![field("a", DataType::Int32)]);

        let result = cast_column(&source_col, &target_field, &DEFAULT_CAST_OPTIONS);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast struct field 'a'"));
    }

    #[test]
    fn test_validate_struct_compatibility_incompatible_types() {
        // Source struct: {field1: Binary, field2: String}
        let source_fields = vec![
            arc_field("field1", DataType::Binary),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field1: Int32}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

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
            arc_field("field1", DataType::Int32),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field1: Int64} (Int32 can cast to Int64)
        let target_fields = vec![arc_field("field1", DataType::Int64)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_missing_field_in_source() {
        // Source struct: {field2: String} (missing field1)
        let source_fields = vec![arc_field("field2", DataType::Utf8)];

        // Target struct: {field1: Int32}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        // Should be OK - missing fields will be filled with nulls
        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_additional_field_in_source() {
        // Source struct: {field1: Int32, field2: String} (extra field2)
        let source_fields = vec![
            arc_field("field1", DataType::Int32),
            arc_field("field2", DataType::Utf8),
        ];

        // Target struct: {field1: Int32}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        // Should be OK - extra fields in source are ignored
        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_cast_struct_parent_nulls_retained() {
        let a_array = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let fields = vec![arc_field("a", DataType::Int32)];
        let nulls = Some(NullBuffer::from(vec![true, false]));
        let source_struct = StructArray::new(fields.clone().into(), vec![a_array], nulls);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field("s", vec![field("a", DataType::Int64)]);

        let result =
            cast_column(&source_col, &target_field, &DEFAULT_CAST_OPTIONS).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.null_count(), 1);
        assert!(struct_array.is_valid(0));
        assert!(struct_array.is_null(1));

        let a_result = get_column_as!(&struct_array, "a", Int64Array);
        assert_eq!(a_result.value(0), 1);
        assert_eq!(a_result.value(1), 2);
    }

    #[test]
    fn test_validate_struct_compatibility_nullable_to_non_nullable() {
        // Source struct: {field1: Int32 nullable}
        let source_fields = vec![arc_field("field1", DataType::Int32)];

        // Target struct: {field1: Int32 non-nullable}
        let target_fields = vec![Arc::new(non_null_field("field1", DataType::Int32))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("field1"));
        assert!(error_msg.contains("non-nullable"));
    }

    #[test]
    fn test_validate_struct_compatibility_non_nullable_to_nullable() {
        // Source struct: {field1: Int32 non-nullable}
        let source_fields = vec![Arc::new(non_null_field("field1", DataType::Int32))];

        // Target struct: {field1: Int32 nullable}
        let target_fields = vec![arc_field("field1", DataType::Int32)];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_struct_compatibility_nested_nullable_to_non_nullable() {
        // Source struct: {field1: {nested: Int32 nullable}}
        let source_fields = vec![Arc::new(non_null_field(
            "field1",
            struct_type(vec![field("nested", DataType::Int32)]),
        ))];

        // Target struct: {field1: {nested: Int32 non-nullable}}
        let target_fields = vec![Arc::new(non_null_field(
            "field1",
            struct_type(vec![non_null_field("nested", DataType::Int32)]),
        ))];

        let result = validate_struct_compatibility(&source_fields, &target_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("nested"));
        assert!(error_msg.contains("non-nullable"));
    }

    #[test]
    fn test_cast_nested_struct_with_extra_and_missing_fields() {
        // Source inner struct has fields a, b, extra
        let a = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![Some(2), Some(3)])) as ArrayRef;
        let extra = Arc::new(Int32Array::from(vec![Some(9), Some(10)])) as ArrayRef;

        let inner = StructArray::from(vec![
            (arc_field("a", DataType::Int32), a),
            (arc_field("b", DataType::Int32), b),
            (arc_field("extra", DataType::Int32), extra),
        ]);

        let source_struct = StructArray::from(vec![(
            arc_struct_field(
                "inner",
                vec![
                    field("a", DataType::Int32),
                    field("b", DataType::Int32),
                    field("extra", DataType::Int32),
                ],
            ),
            Arc::new(inner) as ArrayRef,
        )]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        // Target inner struct reorders fields, adds "missing", and drops "extra"
        let target_field = struct_field(
            "outer",
            vec![struct_field(
                "inner",
                vec![
                    field("b", DataType::Int64),
                    field("a", DataType::Int32),
                    field("missing", DataType::Int32),
                ],
            )],
        );

        let result =
            cast_column(&source_col, &target_field, &DEFAULT_CAST_OPTIONS).unwrap();
        let outer = result.as_any().downcast_ref::<StructArray>().unwrap();
        let inner = get_column_as!(&outer, "inner", StructArray);
        assert_eq!(inner.fields().len(), 3);

        let b = get_column_as!(inner, "b", Int64Array);
        assert_eq!(b.value(0), 2);
        assert_eq!(b.value(1), 3);
        assert!(!b.is_null(0));
        assert!(!b.is_null(1));

        let a = get_column_as!(inner, "a", Int32Array);
        assert_eq!(a.value(0), 1);
        assert!(a.is_null(1));

        let missing = get_column_as!(inner, "missing", Int32Array);
        assert!(missing.is_null(0));
        assert!(missing.is_null(1));
    }

    #[test]
    fn test_cast_struct_with_array_and_map_fields() {
        // Array field with second row null
        let arr_array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ])) as ArrayRef;

        // Map field with second row null
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();
        let mut map_builder = MapBuilder::new(None, string_builder, int_builder);
        map_builder.keys().append_value("a");
        map_builder.values().append_value(1);
        map_builder.append(true).unwrap();
        map_builder.append(false).unwrap();
        let map_array = Arc::new(map_builder.finish()) as ArrayRef;

        let source_struct = StructArray::from(vec![
            (
                arc_field(
                    "arr",
                    DataType::List(Arc::new(field("item", DataType::Int32))),
                ),
                arr_array,
            ),
            (
                arc_field(
                    "map",
                    DataType::Map(
                        Arc::new(non_null_field(
                            "entries",
                            struct_type(vec![
                                non_null_field("keys", DataType::Utf8),
                                field("values", DataType::Int32),
                            ]),
                        )),
                        false,
                    ),
                ),
                map_array,
            ),
        ]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![
                field(
                    "arr",
                    DataType::List(Arc::new(field("item", DataType::Int32))),
                ),
                field(
                    "map",
                    DataType::Map(
                        Arc::new(non_null_field(
                            "entries",
                            struct_type(vec![
                                non_null_field("keys", DataType::Utf8),
                                field("values", DataType::Int32),
                            ]),
                        )),
                        false,
                    ),
                ),
            ],
        );

        let result =
            cast_column(&source_col, &target_field, &DEFAULT_CAST_OPTIONS).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        let arr = get_column_as!(&struct_array, "arr", ListArray);
        assert!(!arr.is_null(0));
        assert!(arr.is_null(1));
        let arr0 = arr.value(0);
        let values = arr0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);

        let map = get_column_as!(&struct_array, "map", MapArray);
        assert!(!map.is_null(0));
        assert!(map.is_null(1));
        let map0 = map.value(0);
        let entries = map0.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = get_column_as!(entries, "keys", StringArray);
        let vals = get_column_as!(entries, "values", Int32Array);
        assert_eq!(keys.value(0), "a");
        assert_eq!(vals.value(0), 1);
    }

    #[test]
    fn test_cast_struct_field_order_differs() {
        let a = Arc::new(Int32Array::from(vec![Some(1), Some(2)])) as ArrayRef;
        let b = Arc::new(Int32Array::from(vec![Some(3), None])) as ArrayRef;

        let source_struct = StructArray::from(vec![
            (arc_field("a", DataType::Int32), a),
            (arc_field("b", DataType::Int32), b),
        ]);
        let source_col = Arc::new(source_struct) as ArrayRef;

        let target_field = struct_field(
            "s",
            vec![field("b", DataType::Int64), field("a", DataType::Int32)],
        );

        let result =
            cast_column(&source_col, &target_field, &DEFAULT_CAST_OPTIONS).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();

        let b_col = get_column_as!(&struct_array, "b", Int64Array);
        assert_eq!(b_col.value(0), 3);
        assert!(b_col.is_null(1));

        let a_col = get_column_as!(&struct_array, "a", Int32Array);
        assert_eq!(a_col.value(0), 1);
        assert_eq!(a_col.value(1), 2);
    }
}
