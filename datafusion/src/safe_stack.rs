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

use crate::error::{DataFusionError, Result};
use std::cell::RefCell;

#[derive(Default, Debug, Clone)]
/// Record recursion depth
pub struct ProtectRecursion {
    /// the depth of recursion
    depth: RefCell<usize>,
    /// the limit of recursion
    limit: usize,
}

impl ProtectRecursion {
    /// Make a protect recursion with specific limit
    pub fn new_with_limit(limit: usize) -> ProtectRecursion {
        ProtectRecursion {
            depth: RefCell::new(0),
            limit,
        }
    }

    fn depth_ascend(&self) {
        *self.depth.borrow_mut() -= 1;
    }

    fn depth_descend(&self) -> Result<()> {
        let mut depth = self.depth.borrow_mut();
        if *depth >= self.limit {
            return Err(DataFusionError::RecursionLimitErr(self.limit));
        }
        *depth += 1;
        Ok(())
    }
}

/// Bytes available in the current stack
pub const STACKER_RED_ZONE: usize = {
    64 << 10 // 64KB
};

/// Allocate a new stack of at least stack_size bytes.
pub const STACKER_SIZE: usize = {
    4 << 20 // 4MB
};

/// The trait is used to prevent stack overflow panic
pub trait SafeRecursion {
    fn protect_recursion(&self) -> &ProtectRecursion;

    fn safe_recursion<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        self.protect_recursion().depth_descend().unwrap();
        let result = maybe_grow(f);
        self.protect_recursion().depth_ascend();
        result
    }
}

/// Wrap stacker::maybe_grow with STACKER_RED_ZONE and STACKER_SIZE
pub fn maybe_grow<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    stacker::maybe_grow(STACKER_RED_ZONE, STACKER_SIZE, f)
}
