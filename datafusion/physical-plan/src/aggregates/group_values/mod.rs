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

//! [`GroupValues`] trait for storing and interning group keys

use arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::array::{ArrayRef, ArrowPrimitiveType, downcast_primitive};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_common::stats::{ColumnStatistics, Precision};

use datafusion_expr::EmitTo;

pub mod multi_group_by;

mod row;
mod single_group_by;
use datafusion_physical_expr::binary_map::OutputType;
use multi_group_by::GroupValuesColumn;
use row::GroupValuesRows;
use single_group_by::primitive_flat::{FlatIndex, GroupValuesPrimitiveFlat};

pub(crate) use single_group_by::primitive::HashValue;

use crate::aggregates::{
    group_values::single_group_by::{
        boolean::GroupValuesBoolean, bytes::GroupValuesBytes,
        bytes_view::GroupValuesBytesView, primitive::GroupValuesPrimitive,
    },
    order::GroupOrdering,
};

mod metrics;
mod null_builder;

pub(crate) use metrics::GroupByMetrics;

/// Stores the group values during hash aggregation.
///
/// # Background
///
/// In a query such as `SELECT a, b, count(*) FROM t GROUP BY a, b`, the group values
/// identify each group, and correspond to all the distinct values of `(a,b)`.
///
/// ```sql
/// -- Input has 4 rows with 3 distinct combinations of (a,b) ("groups")
/// create table t(a int, b varchar)
/// as values (1, 'a'), (2, 'b'), (1, 'a'), (3, 'c');
///
/// select a, b, count(*) from t group by a, b;
/// ----
/// 1 a 2
/// 2 b 1
/// 3 c 1
/// ```
///
/// # Design
///
/// Managing group values is a performance critical operation in hash
/// aggregation. The major operations are:
///
/// 1. Intern: Quickly finding existing and adding new group values
/// 2. Emit: Returning the group values as an array
///
/// There are multiple specialized implementations of this trait optimized for
/// different data types and number of columns, optimized for these operations.
/// See [`new_group_values`] for details.
///
/// # Group Ids
///
/// Each distinct group in a hash aggregation is identified by a unique group id
/// (usize) which is assigned by instances of this trait. Group ids are
/// continuous without gaps, starting from 0.
pub trait GroupValues: Send {
    /// Calculates the group id for each input row of `cols`, assigning new
    /// group ids as necessary.
    ///
    /// When the function returns, `groups`  must contain the group id for each
    /// row in `cols`.
    ///
    /// If a row has the same value as a previous row, the same group id is
    /// assigned. If a row has a new value, the next available group id is
    /// assigned.
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()>;

    /// Returns the number of bytes of memory used by this [`GroupValues`]
    fn size(&self) -> usize;

    /// Returns true if this [`GroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values (distinct group values) stored in this [`GroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, num_rows: usize);
}

/// Maximum value range for which flat (direct-indexed) group values are
/// unconditionally used instead of a hash table, regardless of fill rate.
/// At 8 bytes per slot, 65536 slots = 512KB.
const MAX_FLAT_GROUP_RANGE: usize = 65_536;

/// Maximum fill rate (range / num_rows) for which flat group values are used
/// with larger ranges. A fill rate below this threshold means the flat map
/// is at most this many times larger than a perfectly dense map, which is
/// still worthwhile to avoid hashing overhead.
const MAX_FLAT_FILL_RATE: usize = 4;

/// Absolute cap on the range when using fill-rate-based flat indexing.
/// Limits memory usage to approximately 128 MB per flat array
/// (128 MB / 8 bytes per `usize` slot = 16,777,216 slots).
const MAX_FLAT_RANGE_ABSOLUTE: usize = 16_777_216;

/// Return a specialized implementation of [`GroupValues`] for the given schema.
///
/// [`GroupValues`] implementations choosing logic:
///
///   - If group by single column with a small integer type (i8/u8/i16/u16),
///     or a larger integer type with statistics showing a small range
///     or a low fill rate (range / num_rows < 4),
///     [`GroupValuesPrimitiveFlat`] will be chosen for O(1) direct-indexed
///     lookups.
///
///   - If group by single column, and type of this column has
///     the specific [`GroupValues`] implementation, such implementation
///     will be chosen.
///
///   - If group by multiple columns, and all column types have the specific
///     `GroupColumn` implementations, `GroupValuesColumn` will be chosen.
///
///   - Otherwise, the general implementation `GroupValuesRows` will be chosen.
///
/// `GroupColumn`:  crate::aggregates::group_values::multi_group_by::GroupColumn
/// `GroupValuesColumn`: crate::aggregates::group_values::multi_group_by::GroupValuesColumn
/// `GroupValuesRows`: crate::aggregates::group_values::row::GroupValuesRows
pub fn new_group_values(
    schema: SchemaRef,
    group_ordering: &GroupOrdering,
    column_statistics: &[ColumnStatistics],
    num_rows: Option<usize>,
) -> Result<Box<dyn GroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

        // Try flat (direct-indexed) implementation first
        if let Some(flat) =
            try_create_flat_group_values(d, column_statistics.first(), num_rows)
        {
            return Ok(flat);
        }

        macro_rules! downcast_helper {
            ($t:ty, $d:ident) => {
                return Ok(Box::new(GroupValuesPrimitive::<$t>::new($d.clone())))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d),
            _ => {}
        }

        match d {
            DataType::Date32 => {
                downcast_helper!(Date32Type, d);
            }
            DataType::Date64 => {
                downcast_helper!(Date64Type, d);
            }
            DataType::Time32(t) => match t {
                TimeUnit::Second => downcast_helper!(Time32SecondType, d),
                TimeUnit::Millisecond => downcast_helper!(Time32MillisecondType, d),
                _ => {}
            },
            DataType::Time64(t) => match t {
                TimeUnit::Microsecond => downcast_helper!(Time64MicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(Time64NanosecondType, d),
                _ => {}
            },
            DataType::Timestamp(t, _tz) => match t {
                TimeUnit::Second => downcast_helper!(TimestampSecondType, d),
                TimeUnit::Millisecond => downcast_helper!(TimestampMillisecondType, d),
                TimeUnit::Microsecond => downcast_helper!(TimestampMicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(TimestampNanosecondType, d),
            },
            DataType::Decimal128(_, _) => {
                downcast_helper!(Decimal128Type, d);
            }
            DataType::Utf8 => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Utf8)));
            }
            DataType::LargeUtf8 => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Utf8)));
            }
            DataType::Utf8View => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::Utf8View)));
            }
            DataType::Binary => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Binary)));
            }
            DataType::LargeBinary => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Binary)));
            }
            DataType::BinaryView => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::BinaryView)));
            }
            DataType::Boolean => {
                return Ok(Box::new(GroupValuesBoolean::new()));
            }
            _ => {}
        }
    }

    if multi_group_by::supported_schema(schema.as_ref()) {
        let stats = column_statistics.to_vec();
        if matches!(group_ordering, GroupOrdering::None) {
            Ok(Box::new(GroupValuesColumn::<false>::try_new(
                schema, stats, num_rows,
            )?))
        } else {
            Ok(Box::new(GroupValuesColumn::<true>::try_new(
                schema, stats, num_rows,
            )?))
        }
    } else {
        Ok(Box::new(GroupValuesRows::try_new(schema)?))
    }
}

/// Try to create a flat (direct-indexed) [`GroupValues`] implementation for a
/// single primitive column. Returns `None` if the type doesn't support flat
/// indexing or the statistics don't indicate a small enough value range.
fn try_create_flat_group_values(
    data_type: &DataType,
    stats: Option<&ColumnStatistics>,
    num_rows: Option<usize>,
) -> Option<Box<dyn GroupValues>> {
    match data_type {
        // Small integer types: always use flat indexing (full type range)
        DataType::Int8 => Some(Box::new(GroupValuesPrimitiveFlat::<Int8Type>::new(
            data_type.clone(),
            i8::MIN,
            256,
        ))),
        DataType::UInt8 => Some(Box::new(GroupValuesPrimitiveFlat::<UInt8Type>::new(
            data_type.clone(),
            0u8,
            256,
        ))),
        DataType::Int16 => Some(Box::new(GroupValuesPrimitiveFlat::<Int16Type>::new(
            data_type.clone(),
            i16::MIN,
            65536,
        ))),
        DataType::UInt16 => Some(Box::new(GroupValuesPrimitiveFlat::<UInt16Type>::new(
            data_type.clone(),
            0u16,
            65536,
        ))),

        // Larger integer types: use flat indexing when exact statistics show
        // either a small value range or a low fill rate (range / num_rows)
        DataType::Int32 => {
            try_flat_from_stats::<Int32Type>(data_type, stats, num_rows, |sv| match sv {
                ScalarValue::Int32(Some(v)) => Some(*v),
                _ => None,
            })
        }
        DataType::UInt32 => {
            try_flat_from_stats::<UInt32Type>(data_type, stats, num_rows, |sv| match sv {
                ScalarValue::UInt32(Some(v)) => Some(*v),
                _ => None,
            })
        }
        DataType::Int64 => {
            try_flat_from_stats::<Int64Type>(data_type, stats, num_rows, |sv| match sv {
                ScalarValue::Int64(Some(v)) => Some(*v),
                _ => None,
            })
        }
        DataType::UInt64 => {
            try_flat_from_stats::<UInt64Type>(data_type, stats, num_rows, |sv| match sv {
                ScalarValue::UInt64(Some(v)) => Some(*v),
                _ => None,
            })
        }
        DataType::Date32 => {
            try_flat_from_stats::<Date32Type>(data_type, stats, num_rows, |sv| match sv {
                ScalarValue::Date32(Some(v)) => Some(*v),
                _ => None,
            })
        }
        DataType::Date64 => {
            try_flat_from_stats::<Date64Type>(data_type, stats, num_rows, |sv| match sv {
                ScalarValue::Date64(Some(v)) => Some(*v),
                _ => None,
            })
        }

        _ => None,
    }
}

/// Try to create a flat [`GroupValues`] from column statistics.
///
/// Uses flat indexing when either:
/// - The value range is ≤ [`MAX_FLAT_GROUP_RANGE`] (always worth it), or
/// - The fill rate (`range / num_rows`) is < [`MAX_FLAT_FILL_RATE`] and the
///   range does not exceed [`MAX_FLAT_RANGE_ABSOLUTE`], meaning the flat map
///   is only a small constant factor larger than the number of input rows,
///   making it still cheaper than hashing.
///
/// Both `Exact` and `Inexact` statistics are accepted because inexact bounds
/// (e.g. after filter narrowing) are always conservative — the actual values
/// are guaranteed to fall within the reported range.
fn try_flat_from_stats<T: ArrowPrimitiveType>(
    data_type: &DataType,
    stats: Option<&ColumnStatistics>,
    num_rows: Option<usize>,
    extract: impl Fn(&ScalarValue) -> Option<T::Native>,
) -> Option<Box<dyn GroupValues>>
where
    T::Native: FlatIndex,
{
    let stats = stats?;
    let min = match &stats.min_value {
        Precision::Exact(sv) | Precision::Inexact(sv) => extract(sv)?,
        _ => return None,
    };
    let max = match &stats.max_value {
        Precision::Exact(sv) | Precision::Inexact(sv) => extract(sv)?,
        _ => return None,
    };
    let range = max.index_from(min).checked_add(1)?;

    let use_flat = range <= MAX_FLAT_GROUP_RANGE
        || num_rows.is_some_and(|n| {
            n > 0 && range <= n * MAX_FLAT_FILL_RATE && range <= MAX_FLAT_RANGE_ABSOLUTE
        });

    if use_flat {
        Some(Box::new(GroupValuesPrimitiveFlat::<T>::new(
            data_type.clone(),
            min,
            range,
        )))
    } else {
        None
    }
}
