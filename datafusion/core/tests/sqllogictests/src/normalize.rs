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

use std::sync::Arc;

use arrow::{
    array::{
        as_largestring_array, as_string_array, ArrayRef, LargeStringArray, StringArray,
    },
    datatypes::DataType,
    record_batch::RecordBatch,
};

/// Normalizes the content of a RecordBatch prior to printing.
///
/// This is to make the output comparable to the semi-standard .slt format
///
/// Normalizations applied:
/// 1. Null Values (TODO)
/// 2. [Empty Strings]
///
/// [Empty Strings]: https://duckdb.org/dev/sqllogictest/result_verification#null-values-and-empty-strings
pub fn normalize_batch(batch: RecordBatch) -> RecordBatch {
    let new_columns = batch
        .columns()
        .iter()
        .map(|array| {
            match array.data_type() {
                DataType::Utf8 => {
                    let arr: StringArray = as_string_array(array.as_ref())
                        .iter()
                        .map(normalize_string)
                        .collect();
                    Arc::new(arr) as ArrayRef
                }
                DataType::LargeUtf8 => {
                    let arr: LargeStringArray = as_largestring_array(array.as_ref())
                        .iter()
                        .map(normalize_string)
                        .collect();
                    Arc::new(arr) as ArrayRef
                }
                // todo normalize dictionary values

                // no normalization on this type
                _ => array.clone(),
            }
        })
        .collect();

    RecordBatch::try_new(batch.schema(), new_columns).expect("creating normalized batch")
}

fn normalize_string(v: Option<&str>) -> Option<&str> {
    v.map(|v| {
        // All empty strings are replaced with this value
        if v.is_empty() {
            "(empty)"
        } else {
            v
        }
    })
}
