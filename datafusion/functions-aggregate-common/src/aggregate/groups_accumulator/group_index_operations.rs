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

//! Useful tools for operating group index

use std::fmt::Debug;

/// Operations about group index parsing
///
/// There are mainly 2 `group index` needing parsing: `flat` and `blocked`.
///
/// # Flat group index
/// `flat group index` format is like:
///
/// ```text
///   | block_offset(64bit) |
/// ```
///
/// It is used in `flat GroupValues/GroupAccumulator`, only a single block
/// exists, so its `block_id` is always 0, and use all 64 bits to store the
/// `block offset`.
///
/// # Blocked group index
/// `blocked group index` format is like:
///
/// ```text
///   | block_id(32bit) | block_offset(32bit)
/// ```
///
/// It is used in `blocked GroupValues/GroupAccumulator`, multiple blocks
/// exist, and we use high 32 bits to store `block_id`, and low 32 bit to
/// store `block_offset`.
///
/// The `get_block_offset` method requires to return `block_offset` as u64,
/// that is for compatible for `flat group index`'s parsing.
///
pub trait GroupIndexOperations: Debug {
    fn get_block_id(group_index: usize, block_size: usize) -> usize;

    fn get_block_offset(group_index: usize, block_size: usize) -> usize;
}

#[derive(Debug)]
pub struct BlockedGroupIndexOperations;

impl GroupIndexOperations for BlockedGroupIndexOperations {
    fn get_block_id(group_index: usize, block_size: usize) -> usize {
        group_index / block_size
    }

    fn get_block_offset(group_index: usize, block_size: usize) -> usize {
        group_index % block_size
    }
}

#[derive(Debug)]
pub struct FlatGroupIndexOperations;

impl GroupIndexOperations for FlatGroupIndexOperations {
    fn get_block_id(_group_index: usize, _block_size: usize) -> usize {
        0
    }

    fn get_block_offset(group_index: usize, _block_size: usize) -> usize {
        group_index
    }
}
