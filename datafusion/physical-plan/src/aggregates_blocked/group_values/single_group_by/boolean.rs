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

use crate::aggregates::group_values::GroupValues;

use arrow::array::{
    ArrayRef, AsArray as _, BooleanArray, BooleanBufferBuilder, NullBufferBuilder,
};
use datafusion_common::Result;
use datafusion_expr::EmitTo;
use std::{mem::size_of, sync::Arc};
use datafusion_expr_common::groups_accumulator::BlocksIndex;
use crate::aggregates_blocked::group_values::BlockedGroupValues;

#[derive(Debug)]
pub struct GroupValuesBoolean {
    false_group: Option<usize>,
    true_group: Option<usize>,
    null_group: Option<usize>,
    block_size: usize,
}

impl GroupValuesBoolean {
    pub fn new(block_size: usize) -> Self {
        assert_ne!(block_size, 0);
        Self {
            block_size,
            false_group: None,
            true_group: None,
            null_group: None,
        }
    }
}

impl BlockedGroupValues for GroupValuesBoolean {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<BlocksIndex>) -> Result<()> {
        let array = cols[0].as_boolean();
        groups.clear();

        for value in array.iter() {
            let index = match value {
                Some(false) => {
                    if let Some(index) = self.false_group {
                        index
                    } else {
                        let index = self.len();
                        self.false_group = Some(index);
                        index
                    }
                }
                Some(true) => {
                    if let Some(index) = self.true_group {
                        index
                    } else {
                        let index = self.len();
                        self.true_group = Some(index);
                        index
                    }
                }
                None => {
                    if let Some(index) = self.null_group {
                        index
                    } else {
                        let index = self.len();
                        self.null_group = Some(index);
                        index
                    }
                }
            };

            groups.push(BlocksIndex::from_index_in_fixed_block_size(index, self.block_size));
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.false_group.is_some() as usize
            + self.true_group.is_some() as usize
            + self.null_group.is_some() as usize
    }

    fn emit_all(&mut self) -> Result<Vec<Vec<ArrayRef>>> {
        let mut blocks = vec![];

        while let Some(block) = self.emit_block()? {
            blocks.push(block);
        }

        Ok(blocks)
    }

    fn emit_block(&mut self) -> Result<Option<Vec<ArrayRef>>> {
        let len = self.len();
        if len == 0 {
            return Ok(None);
        }
        // Group indices are always contiguous from 0, so the last block holds the remaining groups
        let emit_count = self.block_size.min(len);
        let mut builder = BooleanBufferBuilder::new(emit_count);
        builder.append_n(emit_count, false);
        if let Some(idx) = self.true_group.as_mut() {
            if *idx < emit_count {
                builder.set_bit(*idx, true);
                self.true_group = None;
            } else {
                *idx -= emit_count;
            }
        }

        if let Some(idx) = self.false_group.as_mut() {
            if *idx < emit_count {
                // already false, no need to set
                self.false_group = None;
            } else {
                *idx -= emit_count;
            }
        }

        let values = builder.finish();

        let nulls = if let Some(idx) = self.null_group.as_mut() {
            if *idx < emit_count {
                let mut buffer = NullBufferBuilder::new(len);
                buffer.append_n_non_nulls(*idx);
                buffer.append_null();
                buffer.append_n_non_nulls(emit_count - *idx - 1);

                self.null_group = None;
                Some(buffer.finish().unwrap())
            } else {
                *idx -= emit_count;
                None
            }
        } else {
            None
        };

        Ok(Some(vec![Arc::new(BooleanArray::new(values, nulls)) as _]))
    }

    fn emit_first_n(&mut self, n: usize) -> Result<Vec<ArrayRef>> {
        assert!(n < self.block_size, "{n} must be less than block_size {}", self.block_size);

        {
            let len = self.len();
            assert!(n < len, "{n} must be less than length {len}");
        }

        let mut builder = BooleanBufferBuilder::new(n);
        builder.append_n(n, false);
        if let Some(idx) = self.true_group.as_mut() {
            if *idx < n {
                builder.set_bit(*idx, true);
                self.true_group = None;
            } else {
                *idx -= n;
            }
        }

        if let Some(idx) = self.false_group.as_mut() {
            if *idx < n {
                // already false, no need to set
                self.false_group = None;
            } else {
                *idx -= n;
            }
        }

        let values = builder.finish();

        let nulls = if let Some(idx) = self.null_group.as_mut() {
            if *idx < n {
                let mut buffer = NullBufferBuilder::new(builder.len());
                buffer.append_n_non_nulls(*idx);
                buffer.append_null();
                buffer.append_n_non_nulls(n - *idx - 1);

                self.null_group = None;
                Some(buffer.finish().unwrap())
            } else {
                *idx -= n;
                None
            }
        } else {
            None
        };

        Ok(vec![Arc::new(BooleanArray::new(values, nulls)) as _])
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        self.false_group = None;
        self.true_group = None;
        self.null_group = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intern(group_values: &mut GroupValuesBoolean, values: &[Option<bool>]) -> Vec<BlocksIndex> {
        let mut groups = vec![];
        let array: ArrayRef = Arc::new(BooleanArray::from(values.to_vec()));
        group_values.intern(&[array], &mut groups).unwrap();
        groups
    }

    fn emitted(blocks: Vec<Vec<ArrayRef>>) -> Vec<Vec<Option<bool>>> {
        blocks
            .into_iter()
            .map(|block| block[0].as_boolean().iter().collect())
            .collect()
    }

    #[test]
    fn emit_all_only_emits_existing_groups() {
        let mut group_values = GroupValuesBoolean::new(531);
        let groups = intern(&mut group_values, &[Some(true), None, Some(true), Some(false)]);
        assert_eq!(
            groups,
            [0, 1, 0, 2].map(BlocksIndex::new_in_first_block).to_vec()
        );
        assert_eq!(group_values.len(), 3);

        let blocks = emitted(group_values.emit_all().unwrap());
        assert_eq!(blocks, vec![vec![Some(true), None, Some(false)]]);
        assert!(group_values.is_empty());
        assert!(group_values.emit_block().unwrap().is_none());
    }

    #[test]
    fn emit_all_splits_into_blocks() {
        let mut group_values = GroupValuesBoolean::new(2);
        intern(&mut group_values, &[Some(false), Some(true), None]);

        let blocks = emitted(group_values.emit_all().unwrap());
        assert_eq!(blocks, vec![vec![Some(false), Some(true)], vec![None]]);
        assert!(group_values.is_empty());
    }

    #[test]
    fn emit_first_n_shifts_remaining() {
        let mut group_values = GroupValuesBoolean::new(4);
        intern(&mut group_values, &[None, Some(false), Some(true)]);

        let first = group_values.emit_first_n(1).unwrap();
        assert_eq!(first[0].as_boolean().iter().collect::<Vec<_>>(), vec![None]);
        assert_eq!(group_values.len(), 2);

        let blocks = emitted(group_values.emit_all().unwrap());
        assert_eq!(blocks, vec![vec![Some(false), Some(true)]]);
    }
}
