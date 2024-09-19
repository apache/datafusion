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

use arrow::record_batch::RecordBatch;
use arrow_array::{downcast_primitive, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use bytes_view::GroupValuesBytesView;
use datafusion_common::Result;

pub(crate) mod primitive;
use datafusion_expr::EmitTo;
use primitive::GroupValuesPrimitive;

mod row;
use row::GroupValuesRows;

mod bytes;
mod bytes_view;
use bytes::GroupValuesByes;
use datafusion_physical_expr::binary_map::OutputType;

use crate::aggregates::group_values::row::PartitionedGroupValuesRows;

pub enum GroupValuesLike {
    Single(Box<dyn GroupValues>),
    Partitioned(Box<dyn PartitionedGroupValues>),
}

impl GroupValuesLike {
    #[inline]
    pub fn is_partitioned(&self) -> bool {
        matches!(&self, GroupValuesLike::Partitioned(_))
    }

    #[inline]
    pub fn num_partitions(&self) -> usize {
        if let Self::Partitioned(group_values) = self {
            group_values.num_partitions()
        } else {
            1
        }
    }

    #[inline]
    pub fn as_single(&self) -> &Box<dyn GroupValues> {
        match self {
            GroupValuesLike::Single(v) => v,
            GroupValuesLike::Partitioned(_) => unreachable!(),
        }
    }

    #[inline]
    pub fn as_partitioned(&self) -> &Box<dyn PartitionedGroupValues> {
        match self {
            GroupValuesLike::Partitioned(v) => v,
            GroupValuesLike::Single(_) => unreachable!(),
        }
    }

    #[inline]
    pub fn as_single_mut(&mut self) -> &mut Box<dyn GroupValues> {
        match self {
            GroupValuesLike::Single(v) => v,
            GroupValuesLike::Partitioned(_) => unreachable!(),
        }
    }

    #[inline]
    pub fn as_partitioned_mut(&mut self) -> &mut Box<dyn PartitionedGroupValues> {
        match self {
            GroupValuesLike::Partitioned(v) => v,
            GroupValuesLike::Single(_) => unreachable!(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            GroupValuesLike::Single(v) => v.len(),
            GroupValuesLike::Partitioned(v) => v.len(),
        }
    }

    #[inline]
    pub fn size(&self) -> usize {
        match self {
            GroupValuesLike::Single(v) => v.size(),
            GroupValuesLike::Partitioned(v) => v.size(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self {
            GroupValuesLike::Single(v) => v.is_empty(),
            GroupValuesLike::Partitioned(v) => v.is_empty(),
        }
    }
}

pub trait PartitionedGroupValues: Send {
    /// Calculates the `groups` for each input row of `cols`
    fn intern(
        &mut self,
        cols: &[ArrayRef],
        part_groups: &mut Vec<Vec<usize>>,
        part_row_indices: &mut Vec<Vec<u32>>,
    ) -> Result<()>;

    fn num_partitions(&self) -> usize;

    /// Returns the number of bytes used by this [`GroupValues`]
    fn size(&self) -> usize;

    /// Returns true if this [`GroupValues`] is empty
    fn is_empty(&self) -> bool;

    fn partition_len(&self, partition_index: usize) -> usize;

    /// The number of values stored in this [`GroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<Vec<ArrayRef>>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear(&mut self);
}

/// An interning store for group keys
pub trait GroupValues: Send {
    /// Calculates the `groups` for each input row of `cols`
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()>;

    /// Returns the number of bytes used by this [`GroupValues`]
    fn size(&self) -> usize;

    /// Returns true if this [`GroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values stored in this [`GroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, batch: &RecordBatch);
}

pub fn new_group_values(
    schema: SchemaRef,
    partitioning_group_values: bool,
    num_partitions: usize,
) -> Result<GroupValuesLike> {
    let group_values = if partitioning_group_values && schema.fields.len() > 1 {
        dbg!("partitioned");
        GroupValuesLike::Partitioned(Box::new(PartitionedGroupValuesRows::try_new(
            schema,
            num_partitions,
        )?))
    } else {
        GroupValuesLike::Single(new_single_group_values(schema)?)
    };

    Ok(group_values)
}

pub fn new_single_group_values(schema: SchemaRef) -> Result<Box<dyn GroupValues>> {
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
            DataType::Utf8 => {
                return Ok(Box::new(GroupValuesByes::<i32>::new(OutputType::Utf8)));
            }
            DataType::LargeUtf8 => {
                return Ok(Box::new(GroupValuesByes::<i64>::new(OutputType::Utf8)));
            }
            DataType::Utf8View => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::Utf8View)));
            }
            DataType::Binary => {
                return Ok(Box::new(GroupValuesByes::<i32>::new(OutputType::Binary)));
            }
            DataType::LargeBinary => {
                return Ok(Box::new(GroupValuesByes::<i64>::new(OutputType::Binary)));
            }
            DataType::BinaryView => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::BinaryView)));
            }
            _ => {}
        }
    }

    Ok(Box::new(GroupValuesRows::try_new(schema)?))
}
