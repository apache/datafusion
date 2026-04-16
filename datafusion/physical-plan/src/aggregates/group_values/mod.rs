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
    Date32Type, Date64Type, Decimal128Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{ArrayRef, downcast_primitive};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion_common::Result;

use datafusion_expr::EmitTo;

pub mod multi_group_by;

mod row;
mod single_group_by;
use datafusion_physical_expr::binary_map::OutputType;
use multi_group_by::GroupValuesColumn;
use row::GroupValuesRows;

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

    /// Interns the rows from `cols` without materializing a group id per input
    /// row.
    ///
    /// This is useful for hash aggregate operators that only need the set of
    /// distinct group keys and have no per-row accumulator updates to perform.
    fn intern_no_group_ids(&mut self, cols: &[ArrayRef]) -> Result<()> {
        let mut groups = Vec::new();
        self.intern(cols, &mut groups)
    }

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

/// Return a specialized implementation of [`GroupValues`] for the given schema.
///
/// [`GroupValues`] implementations choosing logic:
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
    require_group_indices: bool,
) -> Result<Box<dyn GroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();
        let track_group_ids =
            require_group_indices || !matches!(group_ordering, GroupOrdering::None);

        macro_rules! downcast_helper {
            ($t:ty, $d:ident, $track_group_ids:expr) => {
                return Ok(Box::new(GroupValuesPrimitive::<$t>::new(
                    $d.clone(),
                    $track_group_ids,
                )))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d, track_group_ids),
            _ => {}
        }

        match d {
            DataType::Date32 => {
                downcast_helper!(Date32Type, d, track_group_ids);
            }
            DataType::Date64 => {
                downcast_helper!(Date64Type, d, track_group_ids);
            }
            DataType::Time32(t) => match t {
                TimeUnit::Second => {
                    downcast_helper!(Time32SecondType, d, track_group_ids)
                }
                TimeUnit::Millisecond => {
                    downcast_helper!(Time32MillisecondType, d, track_group_ids)
                }
                _ => {}
            },
            DataType::Time64(t) => match t {
                TimeUnit::Microsecond => {
                    downcast_helper!(Time64MicrosecondType, d, track_group_ids)
                }
                TimeUnit::Nanosecond => {
                    downcast_helper!(Time64NanosecondType, d, track_group_ids)
                }
                _ => {}
            },
            DataType::Timestamp(t, _tz) => match t {
                TimeUnit::Second => {
                    downcast_helper!(TimestampSecondType, d, track_group_ids)
                }
                TimeUnit::Millisecond => {
                    downcast_helper!(TimestampMillisecondType, d, track_group_ids)
                }
                TimeUnit::Microsecond => {
                    downcast_helper!(TimestampMicrosecondType, d, track_group_ids)
                }
                TimeUnit::Nanosecond => {
                    downcast_helper!(TimestampNanosecondType, d, track_group_ids)
                }
            },
            DataType::Decimal128(_, _) => {
                downcast_helper!(Decimal128Type, d, track_group_ids);
            }
            DataType::Utf8 => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(
                    OutputType::Utf8,
                    track_group_ids,
                )));
            }
            DataType::LargeUtf8 => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(
                    OutputType::Utf8,
                    track_group_ids,
                )));
            }
            DataType::Utf8View => {
                return Ok(Box::new(GroupValuesBytesView::new(
                    OutputType::Utf8View,
                    track_group_ids,
                )));
            }
            DataType::Binary => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(
                    OutputType::Binary,
                    track_group_ids,
                )));
            }
            DataType::LargeBinary => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(
                    OutputType::Binary,
                    track_group_ids,
                )));
            }
            DataType::BinaryView => {
                return Ok(Box::new(GroupValuesBytesView::new(
                    OutputType::BinaryView,
                    track_group_ids,
                )));
            }
            DataType::Boolean => {
                return Ok(Box::new(GroupValuesBoolean::new()));
            }
            _ => {}
        }
    }

    if multi_group_by::supported_schema(schema.as_ref()) {
        if matches!(group_ordering, GroupOrdering::None) {
            Ok(Box::new(GroupValuesColumn::<false>::try_new(schema)?))
        } else {
            Ok(Box::new(GroupValuesColumn::<true>::try_new(schema)?))
        }
    } else {
        Ok(Box::new(GroupValuesRows::try_new(schema)?))
    }
}
