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

//! [`BlockedGroupValues`]: group keys stored in fixed size blocks

use arrow::array::ArrayRef;
use arrow::array::downcast_primitive;
use arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion_common::Result;
use datafusion_expr::{BlockedEmitTo, BlocksIndex};

mod primitive;
use primitive::BlockedGroupValuesPrimitive;

/// Like [`GroupValues`], but the group keys are stored in blocks of
/// `block_size` groups, matching the blocks of the
/// [`BlockedGroupsAccumulator`]s of the same hash table, so each block of
/// groups can be emitted (and its memory freed) on its own.
///
/// [`GroupValues`]: super::GroupValues
/// [`BlockedGroupsAccumulator`]: datafusion_expr::BlockedGroupsAccumulator
pub(crate) trait BlockedGroupValues: Send {
    /// Calculates the group index of each row of `cols`, adding new groups
    /// as needed. Same contract as [`GroupValues::intern`].
    ///
    /// [`GroupValues::intern`]: super::GroupValues::intern
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()>;

    /// Returns the number of bytes of memory used.
    fn size(&self) -> usize;

    /// Returns true if there are no groups.
    fn is_empty(&self) -> bool;

    /// The number of groups.
    fn len(&self) -> usize;

    /// Emits the group values, indexed `[block][group column]`; same block
    /// rules as `BlockedGroupsAccumulator::evaluate`.
    fn emit(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>>;

    /// Clear the contents and shrink the capacity to the size of the batch
    /// (free up memory usage).
    fn clear_shrink(&mut self, num_rows: usize);
}

/// Returns a [`BlockedGroupValues`] for `schema`, or `None` if blocked group
/// values are not supported for it.
///
/// Only a single group column of a type that `GroupValuesPrimitive` handles
/// in [`new_group_values`] is supported.
///
/// [`new_group_values`]: super::new_group_values
pub(crate) fn new_blocked_group_values(
    schema: &SchemaRef,
    block_size: usize,
) -> Option<Box<dyn BlockedGroupValues>> {
    if schema.fields.len() != 1 {
        return None;
    }
    let d = schema.fields[0].data_type();

    macro_rules! downcast_helper {
        ($t:ty, $d:ident) => {
            return Some(Box::new(BlockedGroupValuesPrimitive::<$t>::new(
                $d.clone(),
                block_size,
            )))
        };
    }

    downcast_primitive! {
        d => (downcast_helper, d),
        _ => {}
    }

    match d {
        DataType::Date32 => downcast_helper!(Date32Type, d),
        DataType::Date64 => downcast_helper!(Date64Type, d),
        DataType::Time32(t) => match t {
            TimeUnit::Second => downcast_helper!(Time32SecondType, d),
            TimeUnit::Millisecond => downcast_helper!(Time32MillisecondType, d),
            _ => None,
        },
        DataType::Time64(t) => match t {
            TimeUnit::Microsecond => downcast_helper!(Time64MicrosecondType, d),
            TimeUnit::Nanosecond => downcast_helper!(Time64NanosecondType, d),
            _ => None,
        },
        DataType::Timestamp(t, _tz) => match t {
            TimeUnit::Second => downcast_helper!(TimestampSecondType, d),
            TimeUnit::Millisecond => downcast_helper!(TimestampMillisecondType, d),
            TimeUnit::Microsecond => downcast_helper!(TimestampMicrosecondType, d),
            TimeUnit::Nanosecond => downcast_helper!(TimestampNanosecondType, d),
        },
        DataType::Decimal128(_, _) => downcast_helper!(Decimal128Type, d),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};

    use super::*;

    fn supported(data_type: DataType) -> bool {
        let schema = Arc::new(Schema::new(vec![Field::new("a", data_type, true)]));
        new_blocked_group_values(&schema, 4).is_some()
    }

    #[test]
    fn supports_same_types_as_group_values_primitive() {
        assert!(supported(DataType::Int32));
        assert!(supported(DataType::UInt8));
        assert!(supported(DataType::Float64));
        assert!(supported(DataType::Date32));
        assert!(supported(DataType::Time32(TimeUnit::Second)));
        assert!(supported(DataType::Time64(TimeUnit::Nanosecond)));
        assert!(supported(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into())
        )));
        assert!(supported(DataType::Decimal128(10, 2)));

        assert!(!supported(DataType::Utf8));
        assert!(!supported(DataType::Boolean));
        let two_columns = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]));
        assert!(new_blocked_group_values(&two_columns, 4).is_none());
    }
}
