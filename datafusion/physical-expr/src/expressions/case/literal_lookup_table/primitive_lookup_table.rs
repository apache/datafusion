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
use arrow::array::{ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, AsArray};
use arrow::datatypes::{i256, IntervalDayTime, IntervalMonthDayNano};
use datafusion_common::{HashMap, ScalarValue};
use half::f16;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone)]
pub(super) struct PrimitiveArrayMapHolder<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    /// Literal value to map index
    ///
    /// If searching this map becomes a bottleneck consider using linear map implementations for small hashmaps
    map: HashMap<Option<<T::Native as ToHashableKey>::HashableKey>, i32>,
    else_index: i32,
}

impl<T> Debug for PrimitiveArrayMapHolder<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveArrayMapHolder")
            .field("map", &self.map)
            .field("else_index", &self.else_index)
            .finish()
    }
}

impl<T> WhenLiteralIndexMap for PrimitiveArrayMapHolder<T>
where
    T: ArrowPrimitiveType,
    T::Native: ToHashableKey,
{
    fn try_new(
        literals: Vec<ScalarValue>,
        else_index: i32,
    ) -> datafusion_common::Result<Self>
    where
        Self: Sized,
    {
        let input = ScalarValue::iter_to_array(literals)?;

        let map = input
            .as_primitive::<T>()
            .into_iter()
            .enumerate()
            .map(|(map_index, value)| {
                (value.map(|v| v.into_hashable_key()), map_index as i32)
            })
            .collect();

        Ok(Self { map, else_index })
    }

    fn match_values(&self, array: &ArrayRef) -> datafusion_common::Result<Vec<i32>> {
        let indices = array
            .as_primitive::<T>()
            .into_iter()
            .map(|value| {
                self.map
                    .get(&value.map(|item| item.into_hashable_key()))
                    .copied()
                    .unwrap_or(self.else_index)
            })
            .collect::<Vec<i32>>();

        Ok(indices)
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
    use super::ToHashableKey;
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
