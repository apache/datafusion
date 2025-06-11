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

use arrow::{
    array::{Array, ArrayRef, StructArray},
    compute::cast,
    datatypes::{DataType::Struct, Field},
};
use datafusion_common::{arrow::array::new_null_array, Result};
use std::sync::Arc;

/// Adapt a column to match the target field type, handling nested structs specially
pub(super) fn adapt_column(
    source_col: &ArrayRef,
    target_field: &Field,
) -> Result<ArrayRef> {
    match target_field.data_type() {
        Struct(target_fields) => {
            if let Some(struct_array) = source_col.as_any().downcast_ref::<StructArray>()
            {
                let mut children: Vec<(Arc<Field>, Arc<dyn Array>)> = Vec::new();
                let num_rows = source_col.len();

                for target_child_field in target_fields {
                    let field_arc = Arc::clone(target_child_field);
                    match struct_array.column_by_name(target_child_field.name()) {
                        Some(source_child_col) => {
                            let adapted_child =
                                adapt_column(source_child_col, target_child_field)?;
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
                Ok(new_null_array(target_field.data_type(), source_col.len()))
            }
        }
        _ => Ok(cast(source_col, target_field.data_type())?),
    }
}
