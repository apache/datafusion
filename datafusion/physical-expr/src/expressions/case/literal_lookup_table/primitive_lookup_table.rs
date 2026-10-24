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
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray, PrimitiveArray,
};
use arrow::datatypes::{DataType, IntervalDayTime, IntervalMonthDayNano, i256};
use datafusion_common::{HashMap, ScalarValue, internal_err};
use half::f16;
use std::fmt::Debug;
use std::hash::Hash;

/// The maximum total size in bytes of the literals that are looked up by comparing each
/// value with all of them, rather than with a hash map.
///
/// The cost of the comparisons grows with the number of literals and with their size,
/// as fewer wider values fit in a vector register.
const MAX_LINEAR_LOOKUP_BYTES: usize = 128;

/// The maximum number of literals that are looked up by comparing each value with all
/// of them, for values narrow enough for many of them to fit [MAX_LINEAR_LOOKUP_BYTES].
const MAX_LINEAR_LOOKUP_LITERALS: usize = 32;

/// The maximum number of literals of type `K` that are looked up by comparing each
/// value with all of them, rather than with a hash map.
fn max_linear_lookup_literals<K>() -> usize {
    (MAX_LINEAR_LOOKUP_BYTES / size_of::<K>()).min(MAX_LINEAR_LOOKUP_LITERALS)
}

/// The number of values that are compared with all the literals at a time in a linear lookup.
const LINEAR_LOOKUP_CHUNK_SIZE: usize = 256;

#[derive(Clone)]
pub(super) struct PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    data_type: DataType,
    /// Literal value to map index
    lookup: PrimitiveLookup<<T::Native as ToHashableKey>::HashableKey>,
}

/// How literal values are mapped to their indices
#[derive(Clone, Debug)]
enum PrimitiveLookup<K> {
    /// The literals, each mapped to its position. Comparing a value with a few literals
    /// takes a fixed number of operations that vectorize well, while the cost of a hash
    /// map lookup depends on how the literals happen to be placed in it.
    Linear(Vec<K>),
    Hash(HashMap<K, u32>),
}

impl<T> Debug for PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveIndexMap")
            .field("lookup", &self.lookup)
            .finish()
    }
}

impl<T> PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    /// Try creating a new lookup table from the given literals and else index.
    /// The index of each literal in the vector is used as the mapped value in the lookup table.
    ///
    /// `literals` are guaranteed to be unique and non-nullable
    pub(super) fn try_new(
        unique_non_null_literals: Vec<ScalarValue>,
    ) -> datafusion_common::Result<Self> {
        let input = ScalarValue::iter_to_array(unique_non_null_literals)?;

        // Literals are guaranteed to not contain nulls
        if input.null_count() > 0 {
            return internal_err!("Literal values for WHEN clauses cannot contain nulls");
        }

        let values = input.as_primitive::<T>().values();
        let lookup = if values.len()
            <= max_linear_lookup_literals::<<T::Native as ToHashableKey>::HashableKey>()
        {
            PrimitiveLookup::Linear(
                values
                    .iter()
                    .map(|value| value.into_hashable_key())
                    .collect(),
            )
        } else {
            PrimitiveLookup::Hash(
                values
                    .iter()
                    .enumerate()
                    // Because literals are unique we can collect directly, and we can avoid only inserting the first occurrence
                    .map(|(map_index, value)| {
                        (value.into_hashable_key(), map_index as u32)
                    })
                    .collect(),
            )
        };

        Ok(Self {
            lookup,
            data_type: input.data_type().clone(),
        })
    }

    fn map_primitive_array_to_when_indices(
        &self,
        array: &PrimitiveArray<T>,
        else_index: u32,
    ) -> datafusion_common::Result<Vec<u32>> {
        let indices = match &self.lookup {
            PrimitiveLookup::Linear(literals) => {
                linear_lookup::<T>(literals, array, else_index)
            }
            PrimitiveLookup::Hash(map) => array
                .into_iter()
                .map(|value| match value {
                    Some(value) => map
                        .get(&value.into_hashable_key())
                        .copied()
                        .unwrap_or(else_index),

                    None => else_index,
                })
                .collect::<Vec<u32>>(),
        };

        Ok(indices)
    }
}

/// Maps the values of `array` to the positions of the matching `literals`, or to
/// `else_index` for values that match none and for nulls.
///
/// The values are compared with every literal a chunk at a time, selecting the position
/// without branching, so that the comparisons vectorize across the values. The literals
/// are unique, so a value matches at most one of them.
fn linear_lookup<T>(
    literals: &[<T::Native as ToHashableKey>::HashableKey],
    array: &PrimitiveArray<T>,
    else_index: u32,
) -> Vec<u32>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    let values = array.values();
    let mut indices = vec![else_index; values.len()];
    for (values, indices) in values
        .chunks(LINEAR_LOOKUP_CHUNK_SIZE)
        .zip(indices.chunks_mut(LINEAR_LOOKUP_CHUNK_SIZE))
    {
        for (literal_index, literal) in literals.iter().enumerate() {
            let literal_index = literal_index as u32;
            for (value, index) in values.iter().zip(indices.iter_mut()) {
                // Storing unconditionally vectorizes; storing only on a match branches per value
                *index = if value.into_hashable_key() == *literal {
                    literal_index
                } else {
                    *index
                };
            }
        }
    }
    if let Some(nulls) = array.nulls().filter(|nulls| nulls.null_count() > 0) {
        for (index, valid) in indices.iter_mut().zip(nulls.iter()) {
            if !valid {
                *index = else_index;
            }
        }
    }
    indices
}

impl<T> WhenLiteralIndexMap for PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    fn map_to_when_indices(
        &self,
        array: &ArrayRef,
        else_index: u32,
    ) -> datafusion_common::Result<Vec<u32>> {
        match array.data_type() {
            dt if dt == &self.data_type => {
                let primitive_array = array.as_primitive::<T>();

                self.map_primitive_array_to_when_indices(primitive_array, else_index)
            }
            // We support dictionary primitive array as we create the lookup table in `CaseWhen` expression
            // creation when we don't know the schema, so we may receive dictionary encoded primitive arrays at execution time.
            DataType::Dictionary(_, value_type)
                if value_type.as_ref() == &self.data_type =>
            {
                // Cast here to simplify the implementation.
                let converted = arrow::compute::cast(array.as_ref(), &self.data_type)?;
                self.map_primitive_array_to_when_indices(
                    converted.as_primitive::<T>(),
                    else_index,
                )
            }
            _ => internal_err!(
                "PrimitiveIndexMap expected array of type {:?} but got {:?}",
                self.data_type,
                array.data_type()
            ),
        }
    }
}

// TODO - We need to port it to arrow so that it can be reused in other places

/// Trait that help convert a value to a key that is hashable and equatable
/// This is needed as some types like f16/f32/f64 do not implement Hash/Eq directly
pub(super) trait ToHashableKey: ArrowNativeTypeOp {
    /// The type that is hashable and equatable
    /// It must be an Arrow native type but it NOT GUARANTEED to be the same as Self
    /// this is just a helper trait so you can reuse the same code for all arrow native types
    type HashableKey: Hash + Eq + Debug + Clone + Copy + Send + Sync;

    /// Converts self to a hashable key
    /// the result of this value can be used as the key in hash maps/sets
    fn into_hashable_key(self) -> Self::HashableKey;
}

macro_rules! impl_to_hashable_key {
    (@single_already_hashable | $t:ty) => {
        impl ToHashableKey for $t {
            type HashableKey = $t;

            #[inline]
            fn into_hashable_key(self) -> Self::HashableKey {
                self
            }
        }
    };
    (@already_hashable | $($t:ty),+ $(,)?) => {
        $(
            impl_to_hashable_key!(@single_already_hashable | $t);
        )+
    };
    (@float | $t:ty => $hashable:ty) => {
        impl ToHashableKey for $t {
            type HashableKey = $hashable;

            #[inline]
            fn into_hashable_key(self) -> Self::HashableKey {
                self.to_bits()
            }
        }
    };
}

impl_to_hashable_key!(@already_hashable | i8, i16, i32, i64, i128, i256, u8, u16, u32, u64, IntervalDayTime, IntervalMonthDayNano);
impl_to_hashable_key!(@float | f16 => u16);
impl_to_hashable_key!(@float | f32 => u32);
impl_to_hashable_key!(@float | f64 => u64);

#[cfg(test)]
mod tests {
    use super::{ToHashableKey, max_linear_lookup_literals};
    use arrow::datatypes::{IntervalMonthDayNano, i256};

    #[test]
    fn max_linear_lookup_literals_by_type() {
        assert_eq!(max_linear_lookup_literals::<i8>(), 32);
        assert_eq!(max_linear_lookup_literals::<u16>(), 32);
        assert_eq!(max_linear_lookup_literals::<i32>(), 32);
        assert_eq!(max_linear_lookup_literals::<i64>(), 16);
        assert_eq!(max_linear_lookup_literals::<i128>(), 8);
        assert_eq!(max_linear_lookup_literals::<IntervalMonthDayNano>(), 8);
        assert_eq!(max_linear_lookup_literals::<i256>(), 4);
    }
    use arrow::array::downcast_primitive;

    // This test ensure that all arrow primitive types implement ToHashableKey
    // otherwise the code will not compile
    #[test]
    fn should_implement_to_hashable_key_for_all_primitives() {
        #[derive(Debug, Default)]
        struct ExampleSet<T>
        where
            T: arrow::datatypes::ArrowPrimitiveType,
            T::Native: ToHashableKey,
        {
            _map: std::collections::HashSet<<T::Native as ToHashableKey>::HashableKey>,
        }

        macro_rules! create_matching_set {
            ($t:ty) => {{
                let _lookup_table = ExampleSet::<$t> {
                    _map: Default::default(),
                };

                return;
            }};
        }

        let data_type = arrow::datatypes::DataType::Float16;

        downcast_primitive! {
            data_type => (create_matching_set),
            _ => panic!("not implemented for {data_type}"),
        }
    }
}
