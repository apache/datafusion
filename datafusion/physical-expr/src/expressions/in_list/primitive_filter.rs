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

//! Specialized primitive type filters for InList expressions

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, downcast_array, downcast_dictionary_array,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::compute::take;
use arrow::datatypes::*;
use datafusion_common::{HashSet, Result, exec_datafusion_err};
use std::hash::{Hash, Hasher};

use super::static_filter::StaticFilter;

/// Wrapper for f32 that implements Hash and Eq using bit comparison.
#[derive(Clone, Copy)]
pub(crate) struct OrderedFloat32(pub(crate) f32);

impl Hash for OrderedFloat32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat32 {}

impl From<f32> for OrderedFloat32 {
    fn from(v: f32) -> Self {
        Self(v)
    }
}

/// Wrapper for f64 that implements Hash and Eq using bit comparison.
#[derive(Clone, Copy)]
pub(crate) struct OrderedFloat64(pub(crate) f64);

impl Hash for OrderedFloat64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state);
    }
}

impl PartialEq for OrderedFloat64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat64 {}

impl From<f64> for OrderedFloat64 {
    fn from(v: f64) -> Self {
        Self(v)
    }
}

macro_rules! primitive_static_filter {
    ($Name:ident, $ArrowType:ty) => {
        pub(crate) struct $Name {
            null_count: usize,
            values: HashSet<<$ArrowType as ArrowPrimitiveType>::Native>,
        }

        impl $Name {
            pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array = in_array.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(v);
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                downcast_dictionary_array! {
                    v => {
                        let values_contains = self.contains(v.values().as_ref(), negated)?;
                        let result = take(&values_contains, v.keys(), None)?;
                        return Ok(downcast_array(result.as_ref()))
                    }
                    _ => {}
                }

                let v = v.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let haystack_has_nulls = self.null_count > 0;
                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&needle_values[i])
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&needle_values[i])
                    })
                };

                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => None,
                    (true, false) => needle_nulls.cloned(),
                    (false, true) => {
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        let needle_validity = needle_nulls
                            .map(|n| n.inner().clone())
                            .unwrap_or_else(|| {
                                BooleanBuffer::new_set(needle_values.len())
                            });
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

primitive_static_filter!(Int8StaticFilter, Int8Type);
primitive_static_filter!(Int16StaticFilter, Int16Type);
primitive_static_filter!(Int32StaticFilter, Int32Type);
primitive_static_filter!(Int64StaticFilter, Int64Type);
primitive_static_filter!(UInt8StaticFilter, UInt8Type);
primitive_static_filter!(UInt16StaticFilter, UInt16Type);
primitive_static_filter!(UInt32StaticFilter, UInt32Type);
primitive_static_filter!(UInt64StaticFilter, UInt64Type);

macro_rules! float_static_filter {
    ($Name:ident, $ArrowType:ty, $OrderedType:ty) => {
        pub(crate) struct $Name {
            null_count: usize,
            values: HashSet<$OrderedType>,
        }

        impl $Name {
            pub(crate) fn try_new(in_array: &ArrayRef) -> Result<Self> {
                let in_array = in_array.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let mut values = HashSet::with_capacity(in_array.len());
                let null_count = in_array.null_count();

                for v in in_array.iter().flatten() {
                    values.insert(<$OrderedType>::from(v));
                }

                Ok(Self { null_count, values })
            }
        }

        impl StaticFilter for $Name {
            fn null_count(&self) -> usize {
                self.null_count
            }

            fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
                downcast_dictionary_array! {
                    v => {
                        let values_contains = self.contains(v.values().as_ref(), negated)?;
                        let result = take(&values_contains, v.keys(), None)?;
                        return Ok(downcast_array(result.as_ref()))
                    }
                    _ => {}
                }

                let v = v.as_primitive_opt::<$ArrowType>().ok_or_else(|| {
                    exec_datafusion_err!(
                        "Failed to downcast an array to a '{}' array",
                        stringify!($ArrowType)
                    )
                })?;

                let haystack_has_nulls = self.null_count > 0;
                let needle_values = v.values();
                let needle_nulls = v.nulls();
                let needle_has_nulls = v.null_count() > 0;

                let contains_buffer = if negated {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        !self.values.contains(&<$OrderedType>::from(needle_values[i]))
                    })
                } else {
                    BooleanBuffer::collect_bool(needle_values.len(), |i| {
                        self.values.contains(&<$OrderedType>::from(needle_values[i]))
                    })
                };

                let result_nulls = match (needle_has_nulls, haystack_has_nulls) {
                    (false, false) => None,
                    (true, false) => needle_nulls.cloned(),
                    (false, true) => {
                        let validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        Some(NullBuffer::new(validity))
                    }
                    (true, true) => {
                        let needle_validity = needle_nulls
                            .map(|n| n.inner().clone())
                            .unwrap_or_else(|| {
                                BooleanBuffer::new_set(needle_values.len())
                            });
                        let haystack_validity = if negated {
                            !&contains_buffer
                        } else {
                            contains_buffer.clone()
                        };
                        let combined_validity = &needle_validity & &haystack_validity;
                        Some(NullBuffer::new(combined_validity))
                    }
                };

                Ok(BooleanArray::new(contains_buffer, result_nulls))
            }
        }
    };
}

float_static_filter!(Float32StaticFilter, Float32Type, OrderedFloat32);
float_static_filter!(Float64StaticFilter, Float64Type, OrderedFloat64);
