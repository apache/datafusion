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

use datafusion_expr::EmitTo;
use std::mem::size_of;

/// Tracks grouping state when group keys are disjoint across output partitions,
/// but rows within each partition are not ordered by the group keys.
///
/// Groups can only be emitted once the partition input is complete, because
/// intra-partition row order does not reveal completed groups early.
#[derive(Debug)]
pub struct GroupOrderingPartitionDisjoint {
    state: State,
}

#[derive(Debug)]
enum State {
    InProgress,
    Complete,
}

impl GroupOrderingPartitionDisjoint {
    pub fn new() -> Self {
        Self {
            state: State::InProgress,
        }
    }

    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::InProgress => None,
            State::Complete => Some(EmitTo::All),
        }
    }

    pub fn remove_groups(&mut self, _n: usize) {
        // No tracked group indexes to shift; emission bookkeeping lives in
        // `GroupValues` / accumulators only.
    }

    pub fn input_done(&mut self) {
        self.state = State::Complete;
    }

    pub fn reset(&mut self) {
        self.state = State::InProgress;
    }

    pub fn new_groups(&mut self, _total_num_groups: usize) {}

    pub(crate) fn size(&self) -> usize {
        size_of::<Self>()
    }
}

impl Default for GroupOrderingPartitionDisjoint {
    fn default() -> Self {
        Self::new()
    }
}
