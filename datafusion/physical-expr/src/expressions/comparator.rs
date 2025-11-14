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

//! Enum-based comparator that eliminates dynamic dispatch for scalar types
//!
//! This module provides an optimized comparator implementation that uses an enum
//! with variants for each scalar Arrow type, eliminating the overhead of dynamic
//! dispatch for common comparison operations. Complex recursive types (List, Struct,
//! Map, Dictionary) fall back to dynamic dispatch.
//!
//! While we are implementing this in DataFusion for now we hope to upstream this into arrow-rs
//! and replace the existing completely dynamic comparator there with this more efficient one.

use arrow::array::types::*;
use arrow::array::{make_comparator as arrow_make_comparator, *};
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::{
    i256, DataType, IntervalDayTime, IntervalMonthDayNano, IntervalUnit, TimeUnit,
};
use arrow::error::ArrowError;
use std::cmp::Ordering;

// Type alias for dynamic comparator (same as arrow_ord::ord::DynComparator)
type DynComparator = Box<dyn Fn(usize, usize) -> Ordering + Send + Sync>;

/// Comparator that uses enum dispatch for scalar types and dynamic dispatch for complex types
pub(crate) enum Comparator {
    // Primitive integer types
    Int8 {
        left: ScalarBuffer<i8>,
        right: ScalarBuffer<i8>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Int16 {
        left: ScalarBuffer<i16>,
        right: ScalarBuffer<i16>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Int32 {
        left: ScalarBuffer<i32>,
        right: ScalarBuffer<i32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Int64 {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Unsigned integer types
    UInt8 {
        left: ScalarBuffer<u8>,
        right: ScalarBuffer<u8>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    UInt16 {
        left: ScalarBuffer<u16>,
        right: ScalarBuffer<u16>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    UInt32 {
        left: ScalarBuffer<u32>,
        right: ScalarBuffer<u32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    UInt64 {
        left: ScalarBuffer<u64>,
        right: ScalarBuffer<u64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Floating point types
    Float16 {
        left: ScalarBuffer<half::f16>,
        right: ScalarBuffer<half::f16>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Float32 {
        left: ScalarBuffer<f32>,
        right: ScalarBuffer<f32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Float64 {
        left: ScalarBuffer<f64>,
        right: ScalarBuffer<f64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Date and time types
    Date32 {
        left: ScalarBuffer<i32>,
        right: ScalarBuffer<i32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Date64 {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Time32Second {
        left: ScalarBuffer<i32>,
        right: ScalarBuffer<i32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Time32Millisecond {
        left: ScalarBuffer<i32>,
        right: ScalarBuffer<i32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Time64Microsecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Time64Nanosecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Timestamp types
    TimestampSecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    TimestampMillisecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    TimestampMicrosecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    TimestampNanosecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Duration types
    DurationSecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    DurationMillisecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    DurationMicrosecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    DurationNanosecond {
        left: ScalarBuffer<i64>,
        right: ScalarBuffer<i64>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Interval types
    IntervalYearMonth {
        left: ScalarBuffer<i32>,
        right: ScalarBuffer<i32>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    IntervalDayTime {
        left: ScalarBuffer<IntervalDayTime>,
        right: ScalarBuffer<IntervalDayTime>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    IntervalMonthDayNano {
        left: ScalarBuffer<IntervalMonthDayNano>,
        right: ScalarBuffer<IntervalMonthDayNano>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Decimal types
    Decimal128 {
        left: ScalarBuffer<i128>,
        right: ScalarBuffer<i128>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Decimal256 {
        left: ScalarBuffer<i256>,
        right: ScalarBuffer<i256>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Boolean type
    Boolean {
        left: BooleanBuffer,
        right: BooleanBuffer,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // String types
    Utf8 {
        left: GenericByteArray<Utf8Type>,
        right: GenericByteArray<Utf8Type>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    LargeUtf8 {
        left: GenericByteArray<LargeUtf8Type>,
        right: GenericByteArray<LargeUtf8Type>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    Utf8View {
        left: GenericByteViewArray<StringViewType>,
        right: GenericByteViewArray<StringViewType>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Binary types
    Binary {
        left: GenericByteArray<BinaryType>,
        right: GenericByteArray<BinaryType>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    LargeBinary {
        left: GenericByteArray<LargeBinaryType>,
        right: GenericByteArray<LargeBinaryType>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },
    BinaryView {
        left: GenericByteViewArray<BinaryViewType>,
        right: GenericByteViewArray<BinaryViewType>,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // FixedSizeBinary
    FixedSizeBinary {
        left: FixedSizeBinaryArray,
        right: FixedSizeBinaryArray,
        left_nulls: Option<NullBuffer>,
        right_nulls: Option<NullBuffer>,
        opts: SortOptions,
    },

    // Dynamic fallback for recursive/complex types:
    // - List, LargeList, FixedSizeList
    // - Struct
    // - Map
    // - Dictionary
    Dynamic(DynComparator),
}

/// Helper macro to reduce duplication for float comparisons using total_cmp
macro_rules! compare_float {
    ($left:expr, $right:expr, $left_nulls:expr, $right_nulls:expr, $opts:expr, $i:expr, $j:expr) => {{
        let ord = match (
            $left_nulls.as_ref().is_some_and(|n| n.is_null($i)),
            $right_nulls.as_ref().is_some_and(|n| n.is_null($j)),
        ) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if $opts.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, true) => {
                return if $opts.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, false) => {
                let left_slice = $left.as_ref();
                let right_slice = $right.as_ref();
                left_slice[$i].total_cmp(&right_slice[$j])
            }
        };

        if $opts.descending {
            ord.reverse()
        } else {
            ord
        }
    }};
}

impl Comparator {
    /// Compare elements at indices i (from left array) and j (from right array)
    #[inline]
    pub fn compare(&self, i: usize, j: usize) -> Ordering {
        match self {
            Self::Int8 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Int16 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Int32 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Int64 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::UInt8 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::UInt16 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::UInt32 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::UInt64 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Float16 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_float!(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Float32 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_float!(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Float64 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_float!(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Date32 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Date64 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Time32Second {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Time32Millisecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Time64Microsecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Time64Nanosecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::TimestampSecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::TimestampMillisecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::TimestampMicrosecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::TimestampNanosecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::DurationSecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::DurationMillisecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::DurationMicrosecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::DurationNanosecond {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::IntervalYearMonth {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::IntervalDayTime {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::IntervalMonthDayNano {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Decimal128 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Decimal256 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_ord_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Boolean {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_boolean_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Utf8 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_bytes_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::LargeUtf8 {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_bytes_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::Utf8View {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => {
                compare_byte_view_values(left, right, left_nulls, right_nulls, opts, i, j)
            }
            Self::Binary {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_bytes_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::LargeBinary {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_bytes_values(left, right, left_nulls, right_nulls, opts, i, j),
            Self::BinaryView {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => {
                compare_byte_view_values(left, right, left_nulls, right_nulls, opts, i, j)
            }
            Self::FixedSizeBinary {
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
            } => compare_fixed_binary_values(
                left,
                right,
                left_nulls,
                right_nulls,
                opts,
                i,
                j,
            ),
            Self::Dynamic(cmp) => cmp(i, j),
        }
    }
}

// Helper functions for comparing values with null handling
use arrow::datatypes::ArrowNativeType;

/// Compare values using Ord::cmp for types that implement Ord (integers, decimals, intervals, etc.)
#[inline]
fn compare_ord_values<T: ArrowNativeType + Ord>(
    left: &ScalarBuffer<T>,
    right: &ScalarBuffer<T>,
    left_nulls: &Option<NullBuffer>,
    right_nulls: &Option<NullBuffer>,
    opts: &SortOptions,
    i: usize,
    j: usize,
) -> Ordering {
    // Check nulls first
    let ord = match (
        left_nulls.as_ref().is_some_and(|n| n.is_null(i)),
        right_nulls.as_ref().is_some_and(|n| n.is_null(j)),
    ) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if opts.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if opts.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {
            let left_slice: &[T] = left.as_ref();
            let right_slice: &[T] = right.as_ref();
            left_slice[i].cmp(&right_slice[j])
        }
    };

    if opts.descending {
        ord.reverse()
    } else {
        ord
    }
}

#[inline]
fn compare_boolean_values(
    left: &BooleanBuffer,
    right: &BooleanBuffer,
    left_nulls: &Option<NullBuffer>,
    right_nulls: &Option<NullBuffer>,
    opts: &SortOptions,
    i: usize,
    j: usize,
) -> Ordering {
    // Check nulls first
    let ord = match (
        left_nulls.as_ref().is_some_and(|n| n.is_null(i)),
        right_nulls.as_ref().is_some_and(|n| n.is_null(j)),
    ) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if opts.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if opts.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => left.value(i).cmp(&right.value(j)),
    };

    if opts.descending {
        ord.reverse()
    } else {
        ord
    }
}

#[inline]
fn compare_bytes_values<T: ByteArrayType>(
    left: &GenericByteArray<T>,
    right: &GenericByteArray<T>,
    left_nulls: &Option<NullBuffer>,
    right_nulls: &Option<NullBuffer>,
    opts: &SortOptions,
    i: usize,
    j: usize,
) -> Ordering {
    // Check nulls first
    let ord = match (
        left_nulls.as_ref().is_some_and(|n| n.is_null(i)),
        right_nulls.as_ref().is_some_and(|n| n.is_null(j)),
    ) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if opts.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if opts.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {
            let l: &[u8] = left.value(i).as_ref();
            let r: &[u8] = right.value(j).as_ref();
            l.cmp(r)
        }
    };

    if opts.descending {
        ord.reverse()
    } else {
        ord
    }
}

#[inline]
fn compare_byte_view_values<T: ByteViewType>(
    left: &GenericByteViewArray<T>,
    right: &GenericByteViewArray<T>,
    left_nulls: &Option<NullBuffer>,
    right_nulls: &Option<NullBuffer>,
    opts: &SortOptions,
    i: usize,
    j: usize,
) -> Ordering {
    // Check nulls first
    let ord = match (
        left_nulls.as_ref().is_some_and(|n| n.is_null(i)),
        right_nulls.as_ref().is_some_and(|n| n.is_null(j)),
    ) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if opts.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if opts.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => {
            let l: &[u8] = left.value(i).as_ref();
            let r: &[u8] = right.value(j).as_ref();
            l.cmp(r)
        }
    };

    if opts.descending {
        ord.reverse()
    } else {
        ord
    }
}

#[inline]
fn compare_fixed_binary_values(
    left: &FixedSizeBinaryArray,
    right: &FixedSizeBinaryArray,
    left_nulls: &Option<NullBuffer>,
    right_nulls: &Option<NullBuffer>,
    opts: &SortOptions,
    i: usize,
    j: usize,
) -> Ordering {
    // Check nulls first
    let ord = match (
        left_nulls.as_ref().is_some_and(|n| n.is_null(i)),
        right_nulls.as_ref().is_some_and(|n| n.is_null(j)),
    ) {
        (true, true) => return Ordering::Equal,
        (true, false) => {
            return if opts.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        (false, true) => {
            return if opts.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        (false, false) => left.value(i).cmp(right.value(j)),
    };

    if opts.descending {
        ord.reverse()
    } else {
        ord
    }
}

/// Create a comparator for the given arrays and sort options.
///
/// This wraps Arrow's `make_comparator` but returns our enum-based `Comparator`
/// for scalar types, falling back to dynamic dispatch for complex types.
///
/// # Errors
/// If the data types of the arrays are not supported for comparison.
pub(crate) fn make_comparator(
    left: &dyn Array,
    right: &dyn Array,
    opts: SortOptions,
) -> Result<Comparator, ArrowError> {
    use DataType::*;

    let left_nulls = left.nulls().filter(|x| x.null_count() > 0).cloned();
    let right_nulls = right.nulls().filter(|x| x.null_count() > 0).cloned();

    Ok(match (left.data_type(), right.data_type()) {
        (Int8, Int8) => {
            let left = left.as_primitive::<Int8Type>();
            let right = right.as_primitive::<Int8Type>();
            Comparator::Int8 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Int16, Int16) => {
            let left = left.as_primitive::<Int16Type>();
            let right = right.as_primitive::<Int16Type>();
            Comparator::Int16 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Int32, Int32) => {
            let left = left.as_primitive::<Int32Type>();
            let right = right.as_primitive::<Int32Type>();
            Comparator::Int32 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Int64, Int64) => {
            let left = left.as_primitive::<Int64Type>();
            let right = right.as_primitive::<Int64Type>();
            Comparator::Int64 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (UInt8, UInt8) => {
            let left = left.as_primitive::<UInt8Type>();
            let right = right.as_primitive::<UInt8Type>();
            Comparator::UInt8 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (UInt16, UInt16) => {
            let left = left.as_primitive::<UInt16Type>();
            let right = right.as_primitive::<UInt16Type>();
            Comparator::UInt16 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (UInt32, UInt32) => {
            let left = left.as_primitive::<UInt32Type>();
            let right = right.as_primitive::<UInt32Type>();
            Comparator::UInt32 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (UInt64, UInt64) => {
            let left = left.as_primitive::<UInt64Type>();
            let right = right.as_primitive::<UInt64Type>();
            Comparator::UInt64 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Float16, Float16) => {
            let left = left.as_primitive::<Float16Type>();
            let right = right.as_primitive::<Float16Type>();
            Comparator::Float16 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Float32, Float32) => {
            let left = left.as_primitive::<Float32Type>();
            let right = right.as_primitive::<Float32Type>();
            Comparator::Float32 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Float64, Float64) => {
            let left = left.as_primitive::<Float64Type>();
            let right = right.as_primitive::<Float64Type>();
            Comparator::Float64 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Date32, Date32) => {
            let left = left.as_primitive::<Date32Type>();
            let right = right.as_primitive::<Date32Type>();
            Comparator::Date32 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Date64, Date64) => {
            let left = left.as_primitive::<Date64Type>();
            let right = right.as_primitive::<Date64Type>();
            Comparator::Date64 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Time32(TimeUnit::Second), Time32(TimeUnit::Second)) => {
            let left = left.as_primitive::<Time32SecondType>();
            let right = right.as_primitive::<Time32SecondType>();
            Comparator::Time32Second {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Time32(TimeUnit::Millisecond), Time32(TimeUnit::Millisecond)) => {
            let left = left.as_primitive::<Time32MillisecondType>();
            let right = right.as_primitive::<Time32MillisecondType>();
            Comparator::Time32Millisecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Time64(TimeUnit::Microsecond), Time64(TimeUnit::Microsecond)) => {
            let left = left.as_primitive::<Time64MicrosecondType>();
            let right = right.as_primitive::<Time64MicrosecondType>();
            Comparator::Time64Microsecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Time64(TimeUnit::Nanosecond), Time64(TimeUnit::Nanosecond)) => {
            let left = left.as_primitive::<Time64NanosecondType>();
            let right = right.as_primitive::<Time64NanosecondType>();
            Comparator::Time64Nanosecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Timestamp(TimeUnit::Second, _), Timestamp(TimeUnit::Second, _)) => {
            let left = left.as_primitive::<TimestampSecondType>();
            let right = right.as_primitive::<TimestampSecondType>();
            Comparator::TimestampSecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Timestamp(TimeUnit::Millisecond, _), Timestamp(TimeUnit::Millisecond, _)) => {
            let left = left.as_primitive::<TimestampMillisecondType>();
            let right = right.as_primitive::<TimestampMillisecondType>();
            Comparator::TimestampMillisecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Timestamp(TimeUnit::Microsecond, _), Timestamp(TimeUnit::Microsecond, _)) => {
            let left = left.as_primitive::<TimestampMicrosecondType>();
            let right = right.as_primitive::<TimestampMicrosecondType>();
            Comparator::TimestampMicrosecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Timestamp(TimeUnit::Nanosecond, _), Timestamp(TimeUnit::Nanosecond, _)) => {
            let left = left.as_primitive::<TimestampNanosecondType>();
            let right = right.as_primitive::<TimestampNanosecondType>();
            Comparator::TimestampNanosecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Duration(TimeUnit::Second), Duration(TimeUnit::Second)) => {
            let left = left.as_primitive::<DurationSecondType>();
            let right = right.as_primitive::<DurationSecondType>();
            Comparator::DurationSecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Duration(TimeUnit::Millisecond), Duration(TimeUnit::Millisecond)) => {
            let left = left.as_primitive::<DurationMillisecondType>();
            let right = right.as_primitive::<DurationMillisecondType>();
            Comparator::DurationMillisecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Duration(TimeUnit::Microsecond), Duration(TimeUnit::Microsecond)) => {
            let left = left.as_primitive::<DurationMicrosecondType>();
            let right = right.as_primitive::<DurationMicrosecondType>();
            Comparator::DurationMicrosecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Duration(TimeUnit::Nanosecond), Duration(TimeUnit::Nanosecond)) => {
            let left = left.as_primitive::<DurationNanosecondType>();
            let right = right.as_primitive::<DurationNanosecondType>();
            Comparator::DurationNanosecond {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Interval(IntervalUnit::YearMonth), Interval(IntervalUnit::YearMonth)) => {
            let left = left.as_primitive::<IntervalYearMonthType>();
            let right = right.as_primitive::<IntervalYearMonthType>();
            Comparator::IntervalYearMonth {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Interval(IntervalUnit::DayTime), Interval(IntervalUnit::DayTime)) => {
            let left = left.as_primitive::<IntervalDayTimeType>();
            let right = right.as_primitive::<IntervalDayTimeType>();
            Comparator::IntervalDayTime {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Interval(IntervalUnit::MonthDayNano), Interval(IntervalUnit::MonthDayNano)) => {
            let left = left.as_primitive::<IntervalMonthDayNanoType>();
            let right = right.as_primitive::<IntervalMonthDayNanoType>();
            Comparator::IntervalMonthDayNano {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Decimal128(_, _), Decimal128(_, _)) => {
            let left = left.as_primitive::<Decimal128Type>();
            let right = right.as_primitive::<Decimal128Type>();
            Comparator::Decimal128 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Decimal256(_, _), Decimal256(_, _)) => {
            let left = left.as_primitive::<Decimal256Type>();
            let right = right.as_primitive::<Decimal256Type>();
            Comparator::Decimal256 {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Boolean, Boolean) => {
            let left = left.as_boolean();
            let right = right.as_boolean();
            Comparator::Boolean {
                left: left.values().clone(),
                right: right.values().clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Utf8, Utf8) => {
            let left = left.as_string::<i32>();
            let right = right.as_string::<i32>();
            Comparator::Utf8 {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (LargeUtf8, LargeUtf8) => {
            let left = left.as_string::<i64>();
            let right = right.as_string::<i64>();
            Comparator::LargeUtf8 {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Utf8View, Utf8View) => {
            let left = left.as_string_view();
            let right = right.as_string_view();
            Comparator::Utf8View {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (Binary, Binary) => {
            let left = left.as_binary::<i32>();
            let right = right.as_binary::<i32>();
            Comparator::Binary {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (LargeBinary, LargeBinary) => {
            let left = left.as_binary::<i64>();
            let right = right.as_binary::<i64>();
            Comparator::LargeBinary {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (BinaryView, BinaryView) => {
            let left = left.as_binary_view();
            let right = right.as_binary_view();
            Comparator::BinaryView {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        (FixedSizeBinary(_), FixedSizeBinary(_)) => {
            let left = left.as_fixed_size_binary();
            let right = right.as_fixed_size_binary();
            Comparator::FixedSizeBinary {
                left: left.clone(),
                right: right.clone(),
                left_nulls,
                right_nulls,
                opts,
            }
        }
        // Fall back to dynamic dispatch for complex types
        _ => {
            let cmp = arrow_make_comparator(left, right, opts)?;
            Comparator::Dynamic(cmp)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Float64Array, Int32Array, StringArray,
    };

    #[test]
    fn test_int32_compare() {
        let left = Int32Array::from(vec![1, 2, 3]);
        let right = Int32Array::from(vec![2, 2, 1]);

        let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // 1 < 2
        assert_eq!(cmp.compare(1, 1), Ordering::Equal); // 2 == 2
        assert_eq!(cmp.compare(2, 2), Ordering::Greater); // 3 > 1
    }

    #[test]
    fn test_int32_compare_with_nulls() {
        let left = Int32Array::from(vec![Some(1), None, Some(3)]);
        let right = Int32Array::from(vec![Some(2), Some(2), None]);

        let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // 1 < 2
        assert_eq!(cmp.compare(1, 1), Ordering::Less); // null < 2 (nulls_first=true)
        assert_eq!(cmp.compare(2, 2), Ordering::Greater); // 3 > null
    }

    #[test]
    fn test_int32_descending() {
        let left = Int32Array::from(vec![1, 2, 3]);
        let right = Int32Array::from(vec![2, 2, 1]);

        let cmp = make_comparator(
            &left,
            &right,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        )
        .unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Greater); // 1 > 2 (descending)
        assert_eq!(cmp.compare(1, 1), Ordering::Equal); // 2 == 2
        assert_eq!(cmp.compare(2, 2), Ordering::Less); // 3 < 1 (descending)
    }

    #[test]
    fn test_float64_compare() {
        let left = Float64Array::from(vec![1.5, 2.5, f64::NAN]);
        let right = Float64Array::from(vec![2.5, 2.5, 1.5]);

        let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // 1.5 < 2.5
        assert_eq!(cmp.compare(1, 1), Ordering::Equal); // 2.5 == 2.5
        assert_eq!(cmp.compare(2, 2), Ordering::Greater); // NaN > 1.5 (using total_cmp)
    }

    #[test]
    fn test_string_compare() {
        let left = StringArray::from(vec!["a", "b", "c"]);
        let right = StringArray::from(vec!["b", "b", "a"]);

        let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // "a" < "b"
        assert_eq!(cmp.compare(1, 1), Ordering::Equal); // "b" == "b"
        assert_eq!(cmp.compare(2, 2), Ordering::Greater); // "c" > "a"
    }

    #[test]
    fn test_boolean_compare() {
        let left = BooleanArray::from(vec![false, true, false]);
        let right = BooleanArray::from(vec![true, true, false]);

        let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // false < true
        assert_eq!(cmp.compare(1, 1), Ordering::Equal); // true == true
        assert_eq!(cmp.compare(2, 2), Ordering::Equal); // false == false
    }

    #[test]
    fn test_date32_compare() {
        let left = Date32Array::from(vec![100, 200, 300]);
        let right = Date32Array::from(vec![200, 200, 100]);

        let cmp = make_comparator(&left, &right, SortOptions::default()).unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // 100 < 200
        assert_eq!(cmp.compare(1, 1), Ordering::Equal); // 200 == 200
        assert_eq!(cmp.compare(2, 2), Ordering::Greater); // 300 > 100
    }

    #[test]
    fn test_nulls_last() {
        let left = Int32Array::from(vec![Some(1), None, Some(3)]);
        let right = Int32Array::from(vec![Some(2), Some(2), None]);

        let cmp = make_comparator(
            &left,
            &right,
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        )
        .unwrap();

        assert_eq!(cmp.compare(0, 0), Ordering::Less); // 1 < 2
        assert_eq!(cmp.compare(1, 1), Ordering::Greater); // null > 2 (nulls_first=false)
        assert_eq!(cmp.compare(2, 2), Ordering::Less); // 3 < null
    }
}
