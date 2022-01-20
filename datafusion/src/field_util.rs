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

//! Utility functions for complex field access

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field};
use std::borrow::Borrow;

use crate::error::{DataFusionError, Result};
use crate::scalar::ScalarValue;

/// Returns the field access indexed by `key` from a [`DataType::List`] or [`DataType::Struct`]
/// # Error
/// Errors if
/// * the `data_type` is not a Struct or,
/// * there is no field key is not of the required index type
pub fn get_indexed_field(data_type: &DataType, key: &ScalarValue) -> Result<Field> {
    match (data_type, key) {
        (DataType::List(lt), ScalarValue::Int64(Some(i))) => {
            if *i < 0 {
                Err(DataFusionError::Plan(format!(
                    "List based indexed access requires a positive int, was {0}",
                    i
                )))
            } else {
                Ok(Field::new(&i.to_string(), lt.data_type().clone(), false))
            }
        }
        (DataType::Struct(fields), ScalarValue::Utf8(Some(s))) => {
            if s.is_empty() {
                Err(DataFusionError::Plan(
                    "Struct based indexed access requires a non empty string".to_string(),
                ))
            } else {
                let field = fields.iter().find(|f| f.name() == s);
                match field {
                    None => Err(DataFusionError::Plan(format!(
                        "Field {} not found in struct",
                        s
                    ))),
                    Some(f) => Ok(f.clone()),
                }
            }
        }
        (DataType::Struct(_), _) => Err(DataFusionError::Plan(
            "Only utf8 strings are valid as an indexed field in a struct".to_string(),
        )),
        (DataType::List(_), _) => Err(DataFusionError::Plan(
            "Only ints are valid as an indexed field in a list".to_string(),
        )),
        _ => Err(DataFusionError::Plan(
            "The expression to get an indexed field is only valid for `List` types"
                .to_string(),
        )),
    }
}

/// Imitate arrow-rs StructArray behavior by extending arrow2 StructArray
pub trait StructArrayExt {
    /// Return field names in this struct array
    fn column_names(&self) -> Vec<&str>;
    /// Return child array whose field name equals to column_name
    fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef>;
    /// Return the number of fields in this struct array
    fn num_columns(&self) -> usize;
    /// Return the column at the position
    fn column(&self, pos: usize) -> ArrayRef;
}

impl StructArrayExt for StructArray {
    fn column_names(&self) -> Vec<&str> {
        self.fields().iter().map(|f| f.name.as_str()).collect()
    }

    fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef> {
        self.fields()
            .iter()
            .position(|c| c.name() == column_name)
            .map(|pos| self.values()[pos].borrow())
    }

    fn num_columns(&self) -> usize {
        self.fields().len()
    }

    fn column(&self, pos: usize) -> ArrayRef {
        self.values()[pos].clone()
    }
}

/// Converts a list of field / array pairs to a struct array
pub fn struct_array_from(pairs: Vec<(Field, ArrayRef)>) -> StructArray {
    let fields: Vec<Field> = pairs.iter().map(|v| v.0.clone()).collect();
    let values = pairs.iter().map(|v| v.1.clone()).collect();
    StructArray::from_data(DataType::Struct(fields), values, None)
}
