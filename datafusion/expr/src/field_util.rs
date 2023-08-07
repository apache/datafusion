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

pub enum GetFieldAccessCharacteristic {
    /// returns the field `struct[field]`. For example `struct["name"]`
    NamedStructField { name: ScalarValue },
    /// single list index
    // list[i]
    ListIndex { key_dt: DataType },
    /// list range `list[i:j]`
    ListRange {
        start_dt: DataType,
        stop_dt: DataType,
    },
}

/// Returns the field access indexed by `key` and/or `extra_key` from a [`DataType::List`] or [`DataType::Struct`]
/// # Error
/// Errors if
/// * the `data_type` is not a Struct or a List,
/// * the `data_type` of extra key does not match with `data_type` of key
/// * there is no field key is not of the required index type
pub fn get_indexed_field(
    data_type: &DataType,
    field_characteristic: &GetFieldAccessCharacteristic,
) -> Result<Field> {
    match field_characteristic {
        GetFieldAccessCharacteristic::NamedStructField{ name } => {
            match (data_type, name) {
                (DataType::Struct(fields), ScalarValue::Utf8(Some(s))) => {
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
                (other, _) => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
            }
        }
        GetFieldAccessCharacteristic::ListIndex{ key_dt } => {
            match (data_type, key_dt) {
                (DataType::List(lt), DataType::Int64) => Ok(Field::new("list", lt.data_type().clone(), true)),
                (DataType::List(_), _) => plan_err!(
                    "Only ints are valid as an indexed field in a list"
                ),
                (other, _) => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
            }
        }
        GetFieldAccessCharacteristic::ListRange{ start_dt, stop_dt } => {
            match (data_type, start_dt, stop_dt) {
                (DataType::List(_), DataType::Int64, DataType::Int64) => Ok(Field::new("list", data_type.clone(), true)),
                (DataType::List(_), _, _) => plan_err!(
                    "Only ints are valid as an indexed field in a list"
                ),
                (other, _, _) => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
            }
        }
    }
}
