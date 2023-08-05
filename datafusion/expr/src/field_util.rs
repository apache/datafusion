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

use arrow::datatypes::{DataType, Field};
use datafusion_common::{plan_err, DataFusionError, Result, ScalarValue};

/// Returns the field access indexed by `key` and/or `extra_key` from a [`DataType::List`] or [`DataType::Struct`]
/// # Error
/// Errors if
/// * the `data_type` is not a Struct or a List,
/// * the `data_type` of extra key does not match with `data_type` of key
/// * there is no field key is not of the required index type
pub fn get_indexed_field(
    data_type: &DataType,
    key: &(Option<DataType>, Option<ScalarValue>),
    extra_key: &Option<DataType>,
) -> Result<Field> {
    match (data_type, key) {
        (DataType::List(lt), (Some(DataType::Int64), None)) => {
            match extra_key {
                Some(DataType::Int64) => Ok(Field::new("list", data_type.clone(), true)),
                None => Ok(Field::new("list", lt.data_type().clone(), true)),
                _ => Err(DataFusionError::Plan(
                    "Only ints are valid as an indexed field in a list".to_string(),
                )),
            }
        }
        (DataType::Struct(fields), (None, Some(ScalarValue::Utf8(Some(s))))) => {
            if s.is_empty() {
                plan_err!(
                    "Struct based indexed access requires a non empty string"
                )
            } else {
                let field = fields.iter().find(|f| f.name() == s);
                field.ok_or(DataFusionError::Plan(format!("Field {s} not found in struct"))).map(|f| f.as_ref().clone())
            }
        }
        (DataType::Struct(_), _) => plan_err!(
            "Only utf8 strings are valid as an indexed field in a struct"
        ),
        (DataType::List(_), _) => plan_err!(
            "Only ints are valid as an indexed field in a list"
        ),
        (other, _) => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
    }
}
