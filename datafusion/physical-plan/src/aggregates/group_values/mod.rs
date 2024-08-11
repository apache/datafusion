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

const GROUP_IDX_HIGH_16_BITS_MASK: u64 = 0xffff000000000000;
const GROUP_IDX_LOW_48_BITS_MASK: u64 = 0x0000ffffffffffff;

#[derive(Debug, Clone, Copy)]
pub struct GroupIdx(u64);

impl GroupIdx {
    pub fn new(block_id: u16, block_offset: u64) -> Self {
        let group_idx_high_part = ((block_id as u64) << 48) & GROUP_IDX_HIGH_16_BITS_MASK;
        let group_idx_low_part = block_offset & GROUP_IDX_LOW_48_BITS_MASK;

        Self(group_idx_high_part | group_idx_low_part)
    }

    #[inline]
    pub fn block_id(&self) -> usize {
        ((self.0 & GROUP_IDX_HIGH_16_BITS_MASK) >> 48) as usize
    }

    #[inline]
    pub fn block_offset(&self) -> usize {
        (self.0 & GROUP_IDX_LOW_48_BITS_MASK) as usize
    }

    pub fn as_flat_group_idx(&self, max_block_size: usize) -> usize {
        let block_id = self.block_id();
        let block_offset = self.block_offset();

        block_id * max_block_size + block_offset
    }
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
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<Vec<ArrayRef>>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, batch: &RecordBatch);
}

pub fn new_group_values(schema: SchemaRef, batch_size: usize) -> Result<Box<dyn GroupValues>> {
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

    Ok(Box::new(GroupValuesRows::try_new(schema, batch_size)?))
}
