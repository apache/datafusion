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

//! Applying [`EmitTo`] to blocked per-group state.

pub use datafusion_common::utils::blocked_vec::{
    BLOCK_LEN, BlockedVec, StorageMut, StorageRef, THRESHOLD_LEN, block_offset,
};

use datafusion_expr_common::groups_accumulator::EmitTo;

/// [`EmitTo`] applied to a [`BlockedVec`], returning one contiguous allocation.
pub fn take_blocked<T: Clone>(values: &mut BlockedVec<T>, emit_to: EmitTo) -> Vec<T> {
    match emit_to {
        EmitTo::All => values.take_contiguous(),
        EmitTo::First(n) => values.take_first(n),
    }
}
