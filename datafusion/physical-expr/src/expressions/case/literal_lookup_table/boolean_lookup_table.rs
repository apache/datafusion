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
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::DataType;
use datafusion_common::{ScalarValue, internal_err};

#[derive(Clone, Debug)]
pub(super) struct BooleanIndexMap {
    true_index: Option<u32>,
    false_index: Option<u32>,
}

impl BooleanIndexMap {
    /// Try creating a new lookup table from the given literals and else index
    /// The index of each literal in the vector is used as the mapped value in the lookup table.
    ///
    /// `literals` are guaranteed to be unique and non-nullable
    pub(super) fn try_new(
        unique_non_null_literals: Vec<ScalarValue>,
    ) -> datafusion_common::Result<Self> {
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
                    );
                }
                _ => {
                    return internal_err!(
                        "Non-boolean literal found in literals for BooleanIndexMap"
                    );
                }
            }
        }

        Ok(Self {
            true_index,
            false_index,
        })
    }

    fn map_boolean_array_to_when_indices(
        &self,
        array: &BooleanArray,
        else_index: u32,
    ) -> datafusion_common::Result<Vec<u32>> {
        let true_index = self.true_index.unwrap_or(else_index);
        let false_index = self.false_index.unwrap_or(else_index);

        Ok(array
            .into_iter()
            .map(|value| match value {
                Some(true) => true_index,
                Some(false) => false_index,
                None => else_index,
            })
            .collect::<Vec<u32>>())
    }
}

impl WhenLiteralIndexMap for BooleanIndexMap {
    fn map_to_when_indices(
        &self,
        array: &ArrayRef,
        else_index: u32,
    ) -> datafusion_common::Result<Vec<u32>> {
        match array.data_type() {
            DataType::Boolean => {
                self.map_boolean_array_to_when_indices(array.as_boolean(), else_index)
            }
            // We support dictionary boolean array as we create the lookup table in `CaseWhen` expression
            // creation when we don't know the schema, so we may receive dictionary encoded boolean arrays at execution time.
            DataType::Dictionary(_, value_type)
                if value_type.as_ref() == &DataType::Boolean =>
            {
                // Since it is not common to have dictionary encoded boolean arrays
                // at all than it is ok to do the cast here to simplify the implementation.
                let converted = arrow::compute::cast(array.as_ref(), &DataType::Boolean)?;
                self.map_boolean_array_to_when_indices(converted.as_boolean(), else_index)
            }
            _ => internal_err!(
                "Expected boolean array for BooleanIndexMap, got {:?}",
                array.data_type()
            ),
        }
    }
}
