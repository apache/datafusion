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

use std::sync::atomic::{AtomicUsize, Ordering};

/// A utility struct that can be used to generate unique aliases when optimizing queries
pub struct AliasGenerator {
    next_id: AtomicUsize,
}

impl Default for AliasGenerator {
    fn default() -> Self {
        Self {
            next_id: AtomicUsize::new(1),
        }
    }
}

impl AliasGenerator {
    /// Create a new [`AliasGenerator`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a unique alias with the provided prefix
    pub fn next(&self, prefix: &str) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("{}_{}", prefix, id)
    }
}
