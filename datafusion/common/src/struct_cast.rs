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

//! Utilities for casting struct arrays with field name matching

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray, new_null_array};
use arrow::compute::{CastOptions, kernels};
use arrow::datatypes::{DataType, Fields};

use crate::{DataFusionError, Result};

/// Cast a struct array to another struct type by aligning child arrays using
/// field names instead of their physical order.
///
/// This reorders or permutes the children to match the target schema, inserts
/// null arrays for missing fields, and applies the requested Arrow cast to each
/// field. It returns an error for duplicate source field names or if any child
/// cast fails.
///
/// If the source and target have no overlapping field names, falls back to
/// positional casting (matching fields by index, not name).
pub fn cast_struct_array_by_name(
    array: &ArrayRef,
    target_fields: &Fields,
    cast_options: &CastOptions<'static>,
) -> Result<ArrayRef> {
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Expected StructArray but got {:?}",
                array.data_type()
            ))
        })?;

    // Use the actual StructArray's fields to ensure indices match the physical layout
    let source_fields = struct_array.fields();

    // Check if any source field names match target field names
    let source_names: std::collections::HashSet<_> =
        source_fields.iter().map(|f| f.name()).collect();
    let has_name_overlap = target_fields
        .iter()
        .any(|f| source_names.contains(f.name()));

    // If no field names match, fall back to positional casting
    if !has_name_overlap {
        return Ok(kernels::cast::cast_with_options(
            array,
            &DataType::Struct(target_fields.clone()),
            cast_options,
        )?);
    }

    let mut source_by_name = source_fields
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().clone(), (idx, field)))
        .collect::<std::collections::HashMap<_, _>>();

    if source_by_name.len() != source_fields.len() {
        return Err(DataFusionError::Internal(
            "Duplicate field name found in struct".to_string(),
        ));
    }

    let mut reordered_children = Vec::with_capacity(target_fields.len());
    for target_field in target_fields {
        let casted_child = if let Some((idx, _)) =
            source_by_name.remove(target_field.name())
        {
            let child = Arc::clone(struct_array.column(idx));
            cast_array_with_name_matching(&child, target_field.data_type(), cast_options)?
        } else {
            // Missing field - create a null array of the target type
            new_null_array(target_field.data_type(), struct_array.len())
        };

        reordered_children.push(casted_child);
    }

    Ok(Arc::new(StructArray::new(
        target_fields.clone(),
        reordered_children,
        struct_array.nulls().cloned(),
    )))
}

/// Cast an array with name-based field matching for structs
fn cast_array_with_name_matching(
    array: &ArrayRef,
    cast_type: &DataType,
    cast_options: &CastOptions<'static>,
) -> Result<ArrayRef> {
    // If types are already equal, no cast needed
    if array.data_type() == cast_type {
        return Ok(Arc::clone(array));
    }

    match (array.data_type(), cast_type) {
        (DataType::Struct(_), DataType::Struct(target_fields)) => {
            cast_struct_array_by_name(array, target_fields, cast_options)
        }
        _ => Ok(kernels::cast::cast_with_options(
            array,
            cast_type,
            cast_options,
        )?),
    }
}
