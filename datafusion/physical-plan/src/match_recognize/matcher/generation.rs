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

//! Lightweight wrapper around a `u32` generation counter used by the
//! matcher to mark visited bitmap entries without having to clear the
//! entire buffer on every row.
//!
//! Placing the wrap-around logic behind this newtype centralises the tricky
//! corner-cases and removes repeated boiler-plate from the hot path.

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Generation {
    value: u32,
}

impl Generation {
    /// Start a new counter at generation 1 (0 is reserved as the sentinel
    /// meaning "unvisited").
    #[inline]
    pub fn new() -> Self {
        Self { value: 1 }
    }

    /// The current generation value.
    #[inline]
    pub fn current(&self) -> u32 {
        self.value
    }

    /// Advance to the next generation, resetting `visited` to all zeros if
    /// the counter wraps around.
    #[inline]
    pub fn advance(&mut self, visited: &mut [u32]) -> u32 {
        self.value = self.value.wrapping_add(1);
        if self.value == 0 {
            visited.fill(0);
            self.value = 1;
        }
        self.value
    }
}
