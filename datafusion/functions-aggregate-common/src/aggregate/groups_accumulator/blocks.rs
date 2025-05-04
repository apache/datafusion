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

//! Aggregation intermediate results blocks in blocked approach

/// Structure used to store aggregation intermediate results in `blocked approach`
///
/// Aggregation intermediate results will be stored as multiple [`Block`]s
/// (simply you can think a [`Block`] as a `Vec`). And `Blocks` is the structure
/// to represent such multiple [`Block`]s.
///
/// More details about `blocked approach` can see in: [`GroupsAccumulator::supports_blocked_groups`].
///
/// [`GroupsAccumulator::supports_blocked_groups`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator::supports_blocked_groups
///
#[derive(Debug)]
pub struct Blocks<B: Block> {
    inner: VecDeque<B>,
    block_size: Option<usize>,
}

impl<B: Block> Blocks<B> {
    pub fn new(block_size: Option<usize>) -> Self {
        Self {
            inner: VecDeque::new(),
            block_size,
        }
    }

    pub fn resize<F>(
        &mut self,
        total_num_groups: usize,
        new_block: F,
        default_value: B::T,
    ) where
        F: Fn(Option<usize>) -> B,
    {
        let block_size = self.block_size.unwrap_or(usize::MAX);
        // For resize, we need to:
        //   1. Ensure the blks are enough first
        //   2. and then ensure slots in blks are enough
        let (mut cur_blk_idx, exist_slots) = if !self.inner.is_empty() {
            let cur_blk_idx = self.inner.len() - 1;
            let exist_slots =
                (self.inner.len() - 1) * block_size + self.inner.back().unwrap().len();

            (cur_blk_idx, exist_slots)
        } else {
            (0, 0)
        };

        // No new groups, don't need to expand, just return
        if exist_slots >= total_num_groups {
            return;
        }

        // 1. Ensure blks are enough
        let exist_blks = self.inner.len();
        let new_blks = total_num_groups.div_ceil(block_size) - exist_blks;
        if new_blks > 0 {
            for _ in 0..new_blks {
                let block = new_block(self.block_size);
                self.inner.push_back(block);
            }
        }

        // 2. Ensure slots are enough
        let mut new_slots = total_num_groups - exist_slots;

        // 2.1 Only fill current blk if it may be already enough
        let cur_blk_rest_slots = block_size - self.inner[cur_blk_idx].len();
        if cur_blk_rest_slots >= new_slots {
            self.inner[cur_blk_idx].fill_default_value(new_slots, default_value.clone());
            return;
        }

        // 2.2 Fill current blk to full
        self.inner[cur_blk_idx]
            .fill_default_value(cur_blk_rest_slots, default_value.clone());
        new_slots -= cur_blk_rest_slots;

        // 2.3 Fill complete blks
        let complete_blks = new_slots / block_size;
        for _ in 0..complete_blks {
            cur_blk_idx += 1;
            self.inner[cur_blk_idx].fill_default_value(block_size, default_value.clone());
        }

        // 2.4 Fill last blk if needed
        let rest_slots = new_slots % block_size;
        if rest_slots > 0 {
            self.inner
                .back_mut()
                .unwrap()
                .fill_default_value(rest_slots, default_value);
        }
    }

    pub fn pop_block(&mut self) -> Option<B> {
        self.inner.pop_front()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn size(&self) -> usize {
        self.inner.iter().map(|b| b.size()).sum::<usize>()
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl<B: Block> Index<usize> for Blocks<B> {
    type Output = B;

    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<B: Block> IndexMut<usize> for Blocks<B> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.inner[index]
    }
}

/// The abstraction to represent one aggregation intermediate result block
/// in `blocked approach`, multiple blocks compose a [`Blocks`]
///
/// Many types of aggregation intermediate result exist, and we define an interface
/// to abstract the necessary behaviors of various intermediate result types.
///
pub trait Block: Debug {
    type T: Clone;

    fn fill_default_value(&mut self, fill_len: usize, default_value: Self::T);

    fn len(&self) -> usize;

    fn size(&self) -> usize;
}
