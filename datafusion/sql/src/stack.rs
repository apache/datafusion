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

pub use inner::StackGuard;

/// A guard that sets the minimum stack size for the current thread to `min_stack_size` bytes.
#[cfg(feature = "recursive_protection")]
mod inner {
    /// Sets the stack size to `min_stack_size` bytes on call to `new()` and
    /// resets to the previous value when this structure is dropped.
    pub struct StackGuard {
        previous_stack_size: usize,
    }

    impl StackGuard {
        /// Sets the stack size to `min_stack_size` bytes on call to `new()` and
        /// resets to the previous value when this structure is dropped.
        pub fn new(min_stack_size: usize) -> Self {
            let previous_stack_size = recursive::get_minimum_stack_size();
            recursive::set_minimum_stack_size(min_stack_size);
            Self {
                previous_stack_size,
            }
        }
    }

    impl Drop for StackGuard {
        fn drop(&mut self) {
            recursive::set_minimum_stack_size(self.previous_stack_size);
        }
    }
}

/// A stub implementation of the stack guard when the recursive protection
/// feature is not enabled
#[cfg(not(feature = "recursive_protection"))]
mod inner {
    /// A stub implementation of the stack guard when the recursive protection
    /// feature is not enabled that does nothing
    pub struct StackGuard;

    impl StackGuard {
        /// A stub implementation of the stack guard when the recursive protection
        /// feature is not enabled
        pub fn new(_min_stack_size: usize) -> Self {
            Self
        }
    }
}
