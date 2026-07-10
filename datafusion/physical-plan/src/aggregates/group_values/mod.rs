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
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion_common::Result;
use std::sync::Arc;

use datafusion_expr::EmitTo;

/// Returns `schema` with its leading group fields updated to the data types
/// actually emitted by [`GroupValues`]. Dictionary key types may grow at
/// runtime, while all field names, nullability, and metadata remain unchanged.
pub(crate) fn schema_with_group_values(
    schema: &SchemaRef,
    group_values: &[ArrayRef],
) -> SchemaRef {
    if group_values
        .iter()
        .zip(schema.fields())
        .all(|(array, field)| array.data_type() == field.data_type())
    {
        return Arc::clone(schema);
    }

    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| match group_values.get(index) {
            Some(array) if array.data_type() != field.data_type() => Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(array.data_type().clone()),
            ),
            _ => Arc::clone(field),
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// Returns the largest partial aggregate batch that can be emitted without
/// changing any top-level dictionary group key type in `schema`.
///
/// Partial aggregate batches cross fixed-schema transport boundaries such as
/// repartition coalescing. Splitting them at the dictionary key capacity keeps
/// that transport schema stable; the final aggregate can still combine all
/// groups and promote its output key type dynamically.
pub(crate) fn group_value_emit_batch_size(
    schema: &SchemaRef,
    num_group_fields: usize,
    batch_size: usize,
) -> usize {
    schema
        .fields()
        .iter()
        .take(num_group_fields)
        .filter_map(|field| match field.data_type() {
            DataType::Dictionary(key_type, _) => dictionary_key_capacity(key_type),
            _ => None,
        })
        .fold(batch_size, usize::min)
        .max(1)
}

fn dictionary_key_capacity(key_type: &DataType) -> Option<usize> {
    let capacity = match key_type {
        DataType::Int8 => 1u64 << 7,
        DataType::Int16 => 1u64 << 15,
        DataType::Int32 => 1u64 << 31,
        DataType::UInt8 => 1u64 << 8,
        DataType::UInt16 => 1u64 << 16,
        DataType::UInt32 => 1u64 << 32,
        DataType::Int64 | DataType::UInt64 => return Some(usize::MAX),
        _ => return None,
    };
    Some(usize::try_from(capacity).unwrap_or(usize::MAX))
}

/// Casts the leading group-value arrays to the corresponding schema fields.
/// Dictionary arrays are decoded and re-encoded when their key type changes so
/// sliced batches receive compact keys starting at zero.
pub(crate) fn cast_group_values_to_schema(
    group_values: &mut [ArrayRef],
    schema: &SchemaRef,
) -> Result<()> {
    for (array, field) in group_values.iter_mut().zip(schema.fields()) {
        if array.data_type() == field.data_type() {
            continue;
        }

        *array = match (array.data_type(), field.data_type()) {
            (DataType::Dictionary(_, _), DataType::Dictionary(_, value_type)) => {
                let values = cast(array.as_ref(), value_type.as_ref())?;
                cast(values.as_ref(), field.data_type())?
            }
            _ => cast(array.as_ref(), field.data_type())?,
        };
    }
    Ok(())
}

/// Returns a stable spill schema whose top-level dictionary group keys use
/// their widest key type. This widening is internal to spill IPC and does not
/// change planned or non-spill aggregate output schemas.
pub(crate) fn group_value_spill_schema(
    schema: &SchemaRef,
    num_group_fields: usize,
) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if index < num_group_fields {
                group_value_spill_field(field.as_ref())
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn group_value_spill_field(field: &Field) -> FieldRef {
    let data_type = match field.data_type() {
        DataType::Dictionary(key_type, value_type) => {
            let key_type = match key_type.as_ref() {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    DataType::Int64
                }
                DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => DataType::UInt64,
                _ => key_type.as_ref().clone(),
            };
            DataType::Dictionary(Box::new(key_type), Box::new(value_type.as_ref().clone()))
        }
        _ => field.data_type().clone(),
    };
    Arc::new(field.clone().with_data_type(data_type))
}

pub mod multi_group_by;

mod row;
pub use row::GroupValuesRows;
mod single_group_by;
use datafusion_physical_expr::binary_map::OutputType;
use multi_group_by::GroupValuesColumn;

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
/// `GroupValuesRows`: crate::aggregates::group_values::GroupValuesRows
pub fn new_group_values(
    schema: SchemaRef,
    group_ordering: &GroupOrdering,
) -> Result<Box<dyn GroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

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
        if matches!(group_ordering, GroupOrdering::None) {
            Ok(Box::new(GroupValuesColumn::<false>::try_new(schema)?))
        } else {
            Ok(Box::new(GroupValuesColumn::<true>::try_new(schema)?))
        }
    } else {
        Ok(Box::new(GroupValuesRows::try_new(schema)?))
    }
}
