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
use datafusion_common::ScalarValue;

#[derive(Clone, Debug)]
pub(super) struct BooleanIndexMap {
    true_index: i32,
    false_index: i32,
    null_index: i32,
}

impl WhenLiteralIndexMap for BooleanIndexMap {
    fn try_new(
        literals: Vec<ScalarValue>,
        else_index: i32,
    ) -> datafusion_common::Result<Self>
    where
        Self: Sized,
    {
        fn get_first_index(
            literals: &[ScalarValue],
            target: Option<bool>,
        ) -> Option<i32> {
            literals
                .iter()
                .position(
                    |literal| matches!(literal, ScalarValue::Boolean(v) if v == &target),
                )
                .map(|pos| pos as i32)
        }

        Ok(Self {
            false_index: get_first_index(&literals, Some(false)).unwrap_or(else_index),
            true_index: get_first_index(&literals, Some(true)).unwrap_or(else_index),
            null_index: get_first_index(&literals, None).unwrap_or(else_index),
        })
    }

    fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>> {
        Ok(array
            .as_boolean()
            .into_iter()
            .map(|value| match value {
                Some(true) => self.true_index,
                Some(false) => self.false_index,
                None => self.null_index,
            })
            .collect::<Vec<i32>>())
    }
}
