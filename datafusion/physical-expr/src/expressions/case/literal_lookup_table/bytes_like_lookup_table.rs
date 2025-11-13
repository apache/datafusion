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
    ArrayIter, ArrayRef, AsArray, FixedSizeBinaryArray, FixedSizeBinaryIter,
    GenericByteArray, GenericByteViewArray, TypedDictionaryArray,
};
use arrow::datatypes::{ArrowDictionaryKeyType, ByteArrayType, ByteViewType};
use datafusion_common::{exec_datafusion_err, internal_err, HashMap, ScalarValue};
use std::fmt::Debug;
use std::iter::Map;
use std::marker::PhantomData;

/// Helper trait to convert various byte-like array types to iterator over byte slices
pub(super) trait BytesMapHelperWrapperTrait: Send + Sync {
    /// Iterator over byte slices that will return
    type IntoIter<'a>: Iterator<Item = Option<&'a [u8]>> + 'a;

    /// Convert the array to an iterator over byte slices
    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>>;
}

#[derive(Debug, Clone, Default)]
pub(super) struct GenericBytesHelper<T: ByteArrayType>(PhantomData<T>);

impl<T: ByteArrayType> BytesMapHelperWrapperTrait for GenericBytesHelper<T> {
    type IntoIter<'a> = Map<
        ArrayIter<&'a GenericByteArray<T>>,
        fn(Option<&'a <T as ByteArrayType>::Native>) -> Option<&[u8]>,
    >;

    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
        Ok(array.as_bytes::<T>().into_iter().map(|item| {
            item.map(|v| {
                let bytes: &[u8] = v.as_ref();

                bytes
            })
        }))
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct FixedBinaryHelper;

impl BytesMapHelperWrapperTrait for FixedBinaryHelper {
    type IntoIter<'a> = FixedSizeBinaryIter<'a>;

    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
        Ok(array.as_fixed_size_binary().into_iter())
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct GenericBytesViewHelper<T: ByteViewType>(PhantomData<T>);
impl<T: ByteViewType> BytesMapHelperWrapperTrait for GenericBytesViewHelper<T> {
    type IntoIter<'a> = Map<
        ArrayIter<&'a GenericByteViewArray<T>>,
        fn(Option<&'a <T as ByteViewType>::Native>) -> Option<&[u8]>,
    >;

    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
        Ok(array.as_byte_view::<T>().into_iter().map(|item| {
            item.map(|v| {
                let bytes: &[u8] = v.as_ref();

                bytes
            })
        }))
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct BytesDictionaryHelper<Key: ArrowDictionaryKeyType, Value: ByteArrayType>(
    PhantomData<(Key, Value)>,
);

impl<Key, Value> BytesMapHelperWrapperTrait for BytesDictionaryHelper<Key, Value>
where
    Key: ArrowDictionaryKeyType + Send + Sync,
    Value: ByteArrayType,
    for<'a> TypedDictionaryArray<'a, Key, GenericByteArray<Value>>:
        IntoIterator<Item = Option<&'a Value::Native>>,
{
    type IntoIter<'a> = Map<<TypedDictionaryArray<'a, Key, GenericByteArray<Value>> as IntoIterator>::IntoIter, fn(Option<&'a <Value as ByteArrayType>::Native>) -> Option<&[u8]>>;

    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
        let dict_array = array
            .as_dictionary::<Key>()
            .downcast_dict::<GenericByteArray<Value>>()
            .ok_or_else(|| {
                exec_datafusion_err!(
              "Failed to downcast dictionary array {} to expected dictionary value {}",
              array.data_type(),
              Value::DATA_TYPE
            )
            })?;

        Ok(dict_array.into_iter().map(|item| {
            item.map(|v| {
                let bytes: &[u8] = v.as_ref();

                bytes
            })
        }))
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct FixedBytesDictionaryHelper<Key: ArrowDictionaryKeyType>(
    PhantomData<Key>,
);

impl<Key> BytesMapHelperWrapperTrait for FixedBytesDictionaryHelper<Key>
where
    Key: ArrowDictionaryKeyType + Send + Sync,
    for<'a> TypedDictionaryArray<'a, Key, FixedSizeBinaryArray>:
        IntoIterator<Item = Option<&'a [u8]>>,
{
    type IntoIter<'a> =
        <TypedDictionaryArray<'a, Key, FixedSizeBinaryArray> as IntoIterator>::IntoIter;

    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
        let dict_array = array
          .as_dictionary::<Key>()
          .downcast_dict::<FixedSizeBinaryArray>()
          .ok_or_else(|| exec_datafusion_err!(
              "Failed to downcast dictionary array {} to expected dictionary fixed size binary values",
              array.data_type()
          ))?;

        Ok(dict_array.into_iter())
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct BytesViewDictionaryHelper<
    Key: ArrowDictionaryKeyType,
    Value: ByteViewType,
>(PhantomData<(Key, Value)>);

impl<Key, Value> BytesMapHelperWrapperTrait for BytesViewDictionaryHelper<Key, Value>
where
    Key: ArrowDictionaryKeyType + Send + Sync,
    Value: ByteViewType,
    for<'a> TypedDictionaryArray<'a, Key, GenericByteViewArray<Value>>:
        IntoIterator<Item = Option<&'a Value::Native>>,
{
    type IntoIter<'a> = Map<<TypedDictionaryArray<'a, Key, GenericByteViewArray<Value>> as IntoIterator>::IntoIter, fn(Option<&'a <Value as ByteViewType>::Native>) -> Option<&[u8]>>;

    fn array_to_iter(array: &ArrayRef) -> datafusion_common::Result<Self::IntoIter<'_>> {
        let dict_array = array
            .as_dictionary::<Key>()
            .downcast_dict::<GenericByteViewArray<Value>>()
            .ok_or_else(|| {
                exec_datafusion_err!(
                "Failed to downcast dictionary array {} to expected dictionary value {}",
                array.data_type(),
                Value::DATA_TYPE
            )
            })?;

        Ok(dict_array.into_iter().map(|item| {
            item.map(|v| {
                let bytes: &[u8] = v.as_ref();

                bytes
            })
        }))
    }
}

/// Map from byte-like literal values to their first occurrence index
///
/// This is a wrapper for handling different kinds of literal maps
#[derive(Clone)]
pub(super) struct BytesLikeIndexMap<Helper: BytesMapHelperWrapperTrait> {
    /// Map from non-null literal value the first occurrence index in the literals
    map: HashMap<Vec<u8>, u32>,

    /// The index to return when no match is found
    else_index: u32,

    _phantom_data: PhantomData<Helper>,
}

impl<T: BytesMapHelperWrapperTrait> Debug for BytesLikeIndexMap<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesMapHelper")
            .field("map", &self.map)
            .field("else_index", &self.else_index)
            .finish()
    }
}

impl<Helper: BytesMapHelperWrapperTrait> BytesLikeIndexMap<Helper> {
    /// Try creating a new lookup table from the given literals and else index
    /// The index of each literal in the vector is used as the mapped value in the lookup table.
    ///
    /// `literals` are guaranteed to be unique and non-nullable
    pub(super) fn try_new(
        unique_non_null_literals: Vec<ScalarValue>,
        else_index: u32,
    ) -> datafusion_common::Result<Self> {
        let input = ScalarValue::iter_to_array(unique_non_null_literals)?;

        // Literals are guaranteed to not contain nulls
        if input.null_count() > 0 {
            return internal_err!("Literal values for WHEN clauses cannot contain nulls");
        }

        let bytes_iter = Helper::array_to_iter(&input)?;

        let map: HashMap<Vec<u8>, u32> = bytes_iter
            // Flattening Option<&[u8]> to &[u8] as literals cannot contain nulls
            .flatten()
            .enumerate()
            .map(|(map_index, value): (usize, &[u8])| (value.to_vec(), map_index as u32))
            // Because literals are unique we can collect directly, and we can avoid only inserting the first occurrence
            .collect();

        Ok(Self {
            map,
            else_index,
            _phantom_data: Default::default(),
        })
    }
}

impl<Helper: BytesMapHelperWrapperTrait> WhenLiteralIndexMap
    for BytesLikeIndexMap<Helper>
{
    fn map_to_indices(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<u32>> {
        let bytes_iter = Helper::array_to_iter(array)?;
        let indices = bytes_iter
            .map(|value| match value {
                Some(value) => self.map.get(value).copied().unwrap_or(self.else_index),
                None => self.else_index,
            })
            .collect::<Vec<u32>>();

        Ok(indices)
    }
}
