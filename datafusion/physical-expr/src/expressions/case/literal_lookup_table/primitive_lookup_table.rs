// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

/// [MAX_LINEAR_LOOKUP_BYTES] for values wider than 64 bits, which have no native
/// vector comparisons, making each comparison several times more costly.
const MAX_WIDE_LINEAR_LOOKUP_BYTES: usize = 64;

/// The maximum number of literals that are looked up by comparing each value with all
/// of them, for values narrow enough for many of them to fit [MAX_LINEAR_LOOKUP_BYTES].
const MAX_LINEAR_LOOKUP_LITERALS: usize = 32;

/// The maximum number of literals of type `K` that are looked up by comparing each
/// value with all of them, rather than with a hash map.
fn max_linear_lookup_literals<K>() -> usize {
    let max_bytes = if size_of::<K>() > size_of::<u64>() {
        MAX_WIDE_LINEAR_LOOKUP_BYTES
    } else {
        MAX_LINEAR_LOOKUP_BYTES
    };
    (max_bytes / size_of::<K>()).min(MAX_LINEAR_LOOKUP_LITERALS)
}

/// The maximum total size in bytes of the literals, each counted as at least 32 bits,
/// that are looked up by comparing each value with all of them even when they could be
/// looked up in a direct table. Comparing with this few literals is faster than indexing
/// the table, which takes a load per value.
const MAX_LINEAR_PREFERRED_BYTES: usize = 16;

/// Whether `num_literals` literals of type `K` are looked up faster by comparing each
/// value with all of them than in a direct table
fn prefer_linear_lookup<K>(num_literals: usize) -> bool {
    // The cost of comparing values narrower than the u32 positions they are mapped to
    // is dominated by selecting the positions
    num_literals * size_of::<K>().max(size_of::<u32>()) <= MAX_LINEAR_PREFERRED_BYTES
}

/// The number of values that are compared with all the literals at a time in a linear lookup.
const LINEAR_LOOKUP_CHUNK_SIZE: usize = 256;

/// The maximum length of the range of integer literals that are looked up by indexing a
/// table that covers the whole range. The table is small enough to stay in L1 cache.
const MAX_DIRECT_LOOKUP_RANGE: usize = 1024;

type Key<T> = <<T as ArrowPrimitiveType>::Native as ToLookupKey>::HashableKey;

#[derive(Clone)]
pub(super) struct PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToLookupKey,
{
    data_type: DataType,
    /// Literal value to map index
    lookup: PrimitiveLookup<<T::Native as ToLookupKey>::HashableKey>,
}

/// How literal values are mapped to their indices
#[derive(Clone, Debug)]
enum PrimitiveLookup<K> {
    /// The literals, each mapped to its position. Comparing a value with a few literals
    /// takes a fixed number of operations that vectorize well, while the cost of a hash
    /// map lookup depends on how the literals happen to be placed in it.
    Linear(Vec<K>),
    /// Integer literals spanning a small range: `table[value - min]` is the position of
    /// the literal equal to `value`, or the else index for the values in the range that
    /// match none. The last entry of `table` is the else index, looked up for all values
    /// outside the range, so that the lookup takes no branches.
    Direct {
        min: K,
        table: Vec<u32>,
    },
    Hash(HashMap<K, u32>),
}

impl<T> Debug for PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToLookupKey,
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
    T::Native: ToLookupKey,
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
        let linear = |values: &[T::Native]| {
            PrimitiveLookup::Linear(
                values
                    .iter()
                    .map(|value| value.into_hashable_key())
                    .collect(),
            )
        };
        let lookup = if prefer_linear_lookup::<Key<T>>(values.len()) {
            linear(values)
        } else if let Some(direct) = try_direct_lookup::<T>(values) {
            direct
        } else if values.len() <= max_linear_lookup_literals::<Key<T>>() {
            linear(values)
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
            PrimitiveLookup::Direct { min, table } => {
                debug_assert_eq!(table.last(), Some(&else_index));
                direct_lookup::<T>(*min, table, array, else_index)
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
    literals: &[<T::Native as ToLookupKey>::HashableKey],
    array: &PrimitiveArray<T>,
    else_index: u32,
) -> Vec<u32>
where
    T: ArrowPrimitiveType,
    T::Native: ToLookupKey,
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
                *index = if T::Native::keys_eq(value.into_hashable_key(), *literal) {
                    literal_index
                } else {
                    *index
                };
            }
        }
    }
    set_else_index_for_nulls(array, &mut indices, else_index);
    indices
}

/// Creates a [`PrimitiveLookup::Direct`] for unique `literals` if they are integers
/// spanning a range no longer than [MAX_DIRECT_LOOKUP_RANGE].
fn try_direct_lookup<T>(literals: &[T::Native]) -> Option<PrimitiveLookup<Key<T>>>
where
    T: ArrowPrimitiveType,
    T::Native: ToLookupKey,
{
    if !T::Native::supports_direct_lookup() {
        return None;
    }
    let min = literals.iter().copied().min_by(|a, b| a.compare(*b))?;
    let max = literals.iter().copied().max_by(|a, b| a.compare(*b))?;
    let min = min.into_hashable_key();
    let offset = |value: T::Native| {
        T::Native::direct_lookup_offset(value.into_hashable_key(), min)
    };
    if offset(max) >= MAX_DIRECT_LOOKUP_RANGE as u64 {
        return None;
    }
    let else_index = literals.len() as u32;
    let mut table = vec![else_index; offset(max) as usize + 2];
    for (literal_index, literal) in literals.iter().enumerate() {
        table[offset(*literal) as usize] = literal_index as u32;
    }
    Some(PrimitiveLookup::Direct { min, table })
}

/// Maps the values of `array` to the positions of the literals by looking them up in the
/// `table` of a [`PrimitiveLookup::Direct`].
fn direct_lookup<T>(
    min: Key<T>,
    table: &[u32],
    array: &PrimitiveArray<T>,
    else_index: u32,
) -> Vec<u32>
where
    T: ArrowPrimitiveType,
    T::Native: ToLookupKey,
{
    let last = (table.len() - 1) as u64;
    let mut indices = array
        .values()
        .iter()
        .map(|value| {
            let offset = T::Native::direct_lookup_offset(value.into_hashable_key(), min);
            table[offset.min(last) as usize]
        })
        .collect::<Vec<u32>>();
    set_else_index_for_nulls(array, &mut indices, else_index);
    indices
}

/// Sets `else_index` for the `indices` of the null values of `array`.
fn set_else_index_for_nulls<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    indices: &mut [u32],
    else_index: u32,
) {
    if let Some(nulls) = array.nulls().filter(|nulls| nulls.null_count() > 0) {
        let valid_chunks = nulls.inner().bit_chunks();
        let mut index_chunks = indices.chunks_mut(64);
        for (valid_bits, indices) in valid_chunks.iter().zip(&mut index_chunks) {
            if valid_bits != u64::MAX {
                set_else_index_for_null_bits(valid_bits, indices, else_index);
            }
        }
        if let Some(indices) = index_chunks.next() {
            set_else_index_for_null_bits(
                valid_chunks.remainder_bits(),
                indices,
                else_index,
            );
        }
    }
}

/// Sets `else_index` for each of up to 64 `indices` whose bit in `valid_bits` is unset.
#[inline]
fn set_else_index_for_null_bits(valid_bits: u64, indices: &mut [u32], else_index: u32) {
    for (i, index) in indices.iter_mut().enumerate() {
        *index = if valid_bits & (1 << i) != 0 {
            *index
        } else {
            else_index
        };
    }
}

impl<T> WhenLiteralIndexMap for PrimitiveIndexMap<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToLookupKey,
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

/// Converts a value to a key that the literals are looked up by, and provides the
/// operations on keys used by each kind of [`PrimitiveLookup`].
/// This is needed as some types like f16/f32/f64 do not implement Hash/Eq directly
pub(super) trait ToLookupKey: ArrowNativeTypeOp {
    /// The type that is hashable and equatable
    /// It must be an Arrow native type but it NOT GUARANTEED to be the same as Self
    /// this is just a helper trait so you can reuse the same code for all arrow native types
    type HashableKey: Hash + Eq + Debug + Clone + Copy + Send + Sync;

    /// Converts self to a hashable key
    /// the result of this value can be used as the key in hash maps/sets
    fn into_hashable_key(self) -> Self::HashableKey;

    /// Compares two keys, in a way that compiles to efficient code when comparing many
    /// values with a key
    #[inline]
    fn keys_eq(a: Self::HashableKey, b: Self::HashableKey) -> bool {
        a == b
    }

    /// Whether values of this type are integers that can be looked up in a
    /// [`PrimitiveLookup::Direct`] table
    fn supports_direct_lookup() -> bool {
        false
    }

    /// `key - min` as an unsigned number, which exceeds the range of the
    /// [`PrimitiveLookup::Direct`] table if `key < min`, and is `u64::MAX` if it does
    /// not fit in `u64`.
    ///
    /// Only called for types for which [`Self::supports_direct_lookup`] returns true.
    #[inline]
    fn direct_lookup_offset(_key: Self::HashableKey, _min: Self::HashableKey) -> u64 {
        u64::MAX
    }
}

macro_rules! impl_to_hashable_key {
    (@integer | $($t:ty => $unsigned:ty),+ $(,)?) => {
        $(
            impl ToLookupKey for $t {
                type HashableKey = $t;

                #[inline]
                fn into_hashable_key(self) -> Self::HashableKey {
                    self
                }

                fn supports_direct_lookup() -> bool {
                    true
                }

                #[inline]
                fn direct_lookup_offset(key: $t, min: $t) -> u64 {
                    key.wrapping_sub(min) as $unsigned as u64
                }
            }
        )+
    };
    (@single_already_hashable | $t:ty) => {
        impl ToLookupKey for $t {
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
        impl ToLookupKey for $t {
            type HashableKey = $hashable;

            #[inline]
            fn into_hashable_key(self) -> Self::HashableKey {
                self.to_bits()
            }
        }
    };
}

impl ToLookupKey for i128 {
    type HashableKey = i128;

    #[inline]
    fn into_hashable_key(self) -> Self::HashableKey {
        self
    }

    /// On x86-64-v3 and above, `a == b` is compiled to a per-value `sete`,
    /// which prevents the compiler from sharing one vector compare and select
    /// between several values. Comparing the 64-bit halves with XOR and OR
    /// allows it to, which is measurably faster for a few literals.
    #[inline]
    fn keys_eq(a: i128, b: i128) -> bool {
        let x = (a ^ b) as u128;
        (x as u64 | (x >> 64) as u64) == 0
    }

    fn supports_direct_lookup() -> bool {
        true
    }

    #[inline]
    fn direct_lookup_offset(key: i128, min: i128) -> u64 {
        u64::try_from(key.wrapping_sub(min) as u128).unwrap_or(u64::MAX)
    }
}

impl_to_hashable_key!(@integer | i8 => u8, i16 => u16, i32 => u32, i64 => u64, u8 => u8, u16 => u16, u32 => u32, u64 => u64);
impl_to_hashable_key!(@already_hashable | i256, IntervalDayTime, IntervalMonthDayNano);
impl_to_hashable_key!(@float | f16 => u16);
impl_to_hashable_key!(@float | f32 => u32);
impl_to_hashable_key!(@float | f64 => u64);

#[cfg(test)]
mod tests {
    use super::{
        MAX_DIRECT_LOOKUP_RANGE, PrimitiveIndexMap, PrimitiveLookup, ToLookupKey,
        max_linear_lookup_literals,
    };
    use arrow::array::downcast_primitive;
    use arrow::datatypes::{
        ArrowPrimitiveType, Decimal128Type, Float64Type, Int8Type, Int32Type, Int64Type,
        IntervalMonthDayNano, i256,
    };
    use datafusion_common::ScalarValue;

    #[test]
    fn max_linear_lookup_literals_by_type() {
        assert_eq!(max_linear_lookup_literals::<i8>(), 32);
        assert_eq!(max_linear_lookup_literals::<u16>(), 32);
        assert_eq!(max_linear_lookup_literals::<i32>(), 32);
        assert_eq!(max_linear_lookup_literals::<i64>(), 16);
        assert_eq!(max_linear_lookup_literals::<i128>(), 4);
        assert_eq!(max_linear_lookup_literals::<IntervalMonthDayNano>(), 4);
        assert_eq!(max_linear_lookup_literals::<i256>(), 2);
    }

    fn lookup_kind<T>(literals: impl IntoIterator<Item = T::Native>) -> &'static str
    where
        T: ArrowPrimitiveType,
        T::Native: ToLookupKey,
    {
        let literals = literals
            .into_iter()
            .map(|v| ScalarValue::new_primitive::<T>(Some(v), &T::DATA_TYPE).unwrap())
            .collect();
        match PrimitiveIndexMap::<T>::try_new(literals).unwrap().lookup {
            PrimitiveLookup::Linear(_) => "linear",
            PrimitiveLookup::Direct { .. } => "direct",
            PrimitiveLookup::Hash(_) => "hash",
        }
    }

    #[test]
    fn lookup_kind_by_literals() {
        let max_range = MAX_DIRECT_LOOKUP_RANGE as i32;

        assert_eq!(lookup_kind::<Int8Type>(0..4), "linear");
        assert_eq!(lookup_kind::<Int8Type>(0..5), "direct");
        assert_eq!(lookup_kind::<Int8Type>(i8::MIN..=i8::MAX), "direct");
        assert_eq!(lookup_kind::<Int32Type>(0..4), "linear");
        assert_eq!(lookup_kind::<Int32Type>(0..5), "direct");
        assert_eq!(
            lookup_kind::<Int32Type>([0, 1, 2, 3, max_range - 1]),
            "direct"
        );
        assert_eq!(lookup_kind::<Int32Type>([0, 1, 2, 3, max_range]), "linear");
        assert_eq!(lookup_kind::<Int32Type>(0..max_range), "direct");
        assert_eq!(lookup_kind::<Int32Type>((0..33).map(|i| i * 100)), "hash");
        assert_eq!(lookup_kind::<Int64Type>([i64::MIN, i64::MAX]), "linear");
        assert_eq!(lookup_kind::<Int64Type>([-1, 0, 1]), "direct");
        assert_eq!(lookup_kind::<Int64Type>([i64::MIN, 0, i64::MAX]), "linear");
        assert_eq!(lookup_kind::<Decimal128Type>([1, 2]), "direct");
        assert_eq!(
            lookup_kind::<Decimal128Type>([i128::MIN, i128::MAX]),
            "linear"
        );
        assert_eq!(lookup_kind::<Float64Type>([0.0, 1.0, 2.0, 3.0]), "linear");
        assert_eq!(lookup_kind::<Float64Type>((0..17).map(f64::from)), "hash");
    }

    // This test ensure that all arrow primitive types implement ToLookupKey
    // otherwise the code will not compile
    #[test]
    fn should_implement_to_hashable_key_for_all_primitives() {
        #[derive(Debug, Default)]
        struct ExampleSet<T>
        where
            T: ArrowPrimitiveType,
            T::Native: ToLookupKey,
        {
            _map: std::collections::HashSet<<T::Native as ToLookupKey>::HashableKey>,
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
