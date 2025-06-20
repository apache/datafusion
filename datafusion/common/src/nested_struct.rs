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

use crate::error::Result;
use arrow::{
    array::{new_null_array, Array, ArrayRef, StructArray},
    compute::cast,
    datatypes::{DataType::Struct, Field},
};
use std::sync::Arc;
/// Adapt a struct column to match the target field type, handling nested structs recursively
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
        // If source is not a struct, return null array with target struct type
        Ok(new_null_array(
            &Struct(target_fields.to_vec().into()),
            source_col.len(),
        ))
    }
}

/// Adapt a column to match the target field type, handling nested structs specially
pub fn cast_column(source_col: &ArrayRef, target_field: &Field) -> Result<ArrayRef> {
    match target_field.data_type() {
        Struct(target_fields) => cast_struct_column(source_col, target_fields),
        _ => Ok(cast(source_col, target_field.data_type())?),
    }
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

        let result = cast_column(&source, &target_field).unwrap();
        let struct_array = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_array.len(), 2);
        let a_result = get_column_as!(&struct_array, "a", Int32Array);
        assert_eq!(a_result.null_count(), 2);
    }
}
