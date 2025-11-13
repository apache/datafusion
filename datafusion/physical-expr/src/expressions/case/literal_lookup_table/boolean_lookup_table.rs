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

use crate::expressions::case::literal_lookup_table::WhenLiteralIndexMap;
use arrow::array::{ArrayRef, AsArray};
use datafusion_common::{internal_err, ScalarValue};

#[derive(Clone, Debug)]
pub(super) struct BooleanIndexMap {
    true_index: u32,
    false_index: u32,
    else_index: u32,
}

impl WhenLiteralIndexMap for BooleanIndexMap {
    fn try_new(
        unique_non_null_literals: Vec<ScalarValue>,
        else_index: u32,
    ) -> datafusion_common::Result<Self>
    where
        Self: Sized,
    {
        let mut true_index: Option<u32> = None;
        let mut false_index: Option<u32> = None;

        for (index, literal) in unique_non_null_literals.into_iter().enumerate() {
            match literal {
                ScalarValue::Boolean(Some(true)) => {
                    if true_index.is_some() {
                        return internal_err!(
                            "Duplicate true literal found in literals for BooleanIndexMap"
                        );
                    }
                    true_index = Some(index as u32);
                }
                ScalarValue::Boolean(Some(false)) => {
                    if false_index.is_some() {
                        return internal_err!(
                            "Duplicate false literal found in literals for BooleanIndexMap"
                        );
                    }
                    false_index = Some(index as u32);
                }
                ScalarValue::Boolean(None) => {
                    return internal_err!(
                        "Null literal found in non-null literals for BooleanIndexMap"
                    )
                }
                _ => {
                    return internal_err!(
                        "Non-boolean literal found in literals for BooleanIndexMap"
                    )
                }
            }
        }

        Ok(Self {
            true_index: true_index.unwrap_or(else_index),
            false_index: false_index.unwrap_or(else_index),
            else_index,
        })
    }

    fn map_to_indices(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<u32>> {
        Ok(array
            .as_boolean()
            .into_iter()
            .map(|value| match value {
                Some(true) => self.true_index,
                Some(false) => self.false_index,
                None => self.else_index,
            })
            .collect::<Vec<u32>>())
    }
}
