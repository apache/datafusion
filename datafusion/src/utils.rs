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

/// Returns the a field access indexed by `name` from a [`DataType::List`] or [`DataType::Dictionnary`].
/// # Error
/// Errors if
/// * the `data_type` is not a Struct or,
/// * there is no field key is not of the required index type
pub fn get_indexed_field<'a>(data_type: &'a DataType, key: &str) -> Result<Field> {
    match data_type {
        DataType::Dictionary(ref kt, ref vt) => {
            match kt.as_ref() {
                DataType::Utf8 => Ok(Field::new(key, *vt.clone(), true)),
                _ => Err(DataFusionError::Plan(format!("The key for a dictionary has to be an utf8 string, was : \"{}\"", key))),
            }
        },
        DataType::List(lt) => match key.parse::<usize>() {
            Ok(_) => Ok(Field::new(key, lt.data_type().clone(), false)),
            Err(_) => Err(DataFusionError::Plan(format!("The key for a list has to be an integer, was : \"{}\"", key))),
        },
        _ => Err(DataFusionError::Plan(
            "The expression to get an indexed field is only valid for `List` or 'Dictionary'"
                .to_string(),
        )),
    }
}
