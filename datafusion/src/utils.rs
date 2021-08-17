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

use arrow::datatypes::{DataType, Field};

use crate::error::{DataFusionError, Result};

/// Returns the first field named `name` from the fields of a [`DataType::Struct`].
/// # Error
/// Errors iff
/// * the `data_type` is not a Struct or,
/// * there is no field named `name`
pub fn get_field<'a>(data_type: &'a DataType, name: &str) -> Result<&'a Field> {
    match data_type {
        DataType::Struct(fields) | DataType::Union(fields) => {
            let maybe_field = fields.iter().find(|x| x.name() == name);
            if let Some(field) = maybe_field {
                Ok(field)
            } else {
                Err(DataFusionError::Plan(format!(
                    "The `Struct` has no field named \"{}\"",
                    name
                )))
            }
        }
        _ => Err(DataFusionError::Plan(
            "The expression to get a field is only valid for `Struct` or 'Union'"
                .to_string(),
        )),
    }
}
