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
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryViewArray, DictionaryArray,
    FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
    StringViewArray, downcast_integer,
};
use arrow::datatypes::{
    ArrowDictionaryKeyType, BinaryViewType, DataType, StringViewType,
};
use datafusion_common::{HashMap, ScalarValue, internal_err, plan_datafusion_err};
use std::fmt::Debug;

/// Map from byte-like literal values to their first occurrence index
///
/// This is a wrapper for handling different kinds of literal maps
#[derive(Clone, Debug)]
pub(super) struct BytesLikeIndexMap {
    /// Map from non-null literal value the first occurrence index in the literals
    map: HashMap<Vec<u8>, u32>,
}

impl BytesLikeIndexMap {
    /// Try creating a new lookup table from the given literals and else index
    /// The index of each literal in the vector is used as the mapped value in the lookup table.
    ///
    /// `literals` are guaranteed to be unique and non-nullable
    pub(super) fn try_new(
        unique_non_null_literals: Vec<ScalarValue>,
    ) -> datafusion_common::Result<Self> {
        let input = ScalarValue::iter_to_array(unique_non_null_literals)?;

        // Literals are guaranteed to not contain nulls
        if input.logical_null_count() > 0 {
            return internal_err!("Literal values for WHEN clauses cannot contain nulls");
        }

        let map: HashMap<Vec<u8>, u32> = try_get_bytes_iterator(&input)?
            // Flattening Option<&[u8]> to &[u8] as literals cannot contain nulls
            .flatten()
            .enumerate()
            .map(|(map_index, value)| (value.to_vec(), map_index as u32))
            // Because literals are unique we can collect directly, and we can avoid only inserting the first occurrence
            .collect();

        Ok(Self { map })
    }
}

impl WhenLiteralIndexMap for BytesLikeIndexMap {
    fn map_to_when_indices(
        &self,
        array: &ArrayRef,
        else_index: u32,
    ) -> datafusion_common::Result<Vec<u32>> {
        let indices = try_get_bytes_iterator(array)?
            .map(|value| match value {
                Some(value) => self.map.get(value).copied().unwrap_or(else_index),
                None => else_index,
            })
            .collect::<Vec<u32>>();

        Ok(indices)
    }
}

fn try_get_bytes_iterator(
    array: &ArrayRef,
) -> datafusion_common::Result<Box<dyn Iterator<Item = Option<&[u8]>> + '_>> {
    Ok(match array.data_type() {
        DataType::Utf8 => Box::new(array.as_string::<i32>().into_iter().map(|item| {
            item.map(|v| {
                let bytes: &[u8] = v.as_ref();

                bytes
            })
        })),

        DataType::LargeUtf8 => {
            Box::new(array.as_string::<i64>().into_iter().map(|item| {
                item.map(|v| {
                    let bytes: &[u8] = v.as_ref();

                    bytes
                })
            }))
        }

        DataType::Binary => Box::new(array.as_binary::<i32>().into_iter()),

        DataType::LargeBinary => Box::new(array.as_binary::<i64>().into_iter()),

        DataType::FixedSizeBinary(_) => Box::new(array.as_binary::<i64>().into_iter()),

        DataType::Utf8View => Box::new(
            array
                .as_byte_view::<StringViewType>()
                .into_iter()
                .map(|item| {
                    item.map(|v| {
                        let bytes: &[u8] = v.as_ref();

                        bytes
                    })
                }),
        ),
        DataType::BinaryView => {
            Box::new(array.as_byte_view::<BinaryViewType>().into_iter())
        }

        DataType::Dictionary(key, _) => {
            macro_rules! downcast_dictionary_array_helper {
                ($t:ty) => {{ get_bytes_iterator_for_dictionary(array.as_dictionary::<$t>())? }};
            }

            downcast_integer! {
                key.as_ref() => (downcast_dictionary_array_helper),
                k => unreachable!("unsupported dictionary key type: {}", k)
            }
        }
        t => {
            return Err(plan_datafusion_err!(
                "Unsupported data type for bytes lookup table: {}",
                t
            ));
        }
    })
}

fn get_bytes_iterator_for_dictionary<K: ArrowDictionaryKeyType + Send + Sync>(
    array: &DictionaryArray<K>,
) -> datafusion_common::Result<Box<dyn Iterator<Item = Option<&[u8]>> + '_>> {
    Ok(match array.values().data_type() {
        DataType::Utf8 => Box::new(
            array
                .downcast_dict::<StringArray>()
                .unwrap()
                .into_iter()
                .map(|item| {
                    item.map(|v| {
                        let bytes: &[u8] = v.as_ref();

                        bytes
                    })
                }),
        ),

        DataType::LargeUtf8 => Box::new(
            array
                .downcast_dict::<LargeStringArray>()
                .unwrap()
                .into_iter()
                .map(|item| {
                    item.map(|v| {
                        let bytes: &[u8] = v.as_ref();

                        bytes
                    })
                }),
        ),

        DataType::Binary => {
            Box::new(array.downcast_dict::<BinaryArray>().unwrap().into_iter())
        }

        DataType::LargeBinary => Box::new(
            array
                .downcast_dict::<LargeBinaryArray>()
                .unwrap()
                .into_iter(),
        ),

        DataType::FixedSizeBinary(_) => Box::new(
            array
                .downcast_dict::<FixedSizeBinaryArray>()
                .unwrap()
                .into_iter(),
        ),

        DataType::Utf8View => Box::new(
            array
                .downcast_dict::<StringViewArray>()
                .unwrap()
                .into_iter()
                .map(|item| {
                    item.map(|v| {
                        let bytes: &[u8] = v.as_ref();

                        bytes
                    })
                }),
        ),
        DataType::BinaryView => Box::new(
            array
                .downcast_dict::<BinaryViewArray>()
                .unwrap()
                .into_iter(),
        ),

        t => {
            return Err(plan_datafusion_err!(
                "Unsupported data type for lookup table dictionary value: {}",
                t
            ));
        }
    })
}
