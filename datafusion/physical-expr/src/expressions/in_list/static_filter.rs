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

use std::sync::Arc;

use arrow::array::{Array, BooleanArray};
use datafusion_common::Result;

pub(super) type StaticFilterRef = Arc<dyn StaticFilter>;

/// Trait for InList static filters.
///
/// Static filters store a pre-computed set of values (the haystack) and check
/// whether needle values are contained in that set. The haystack is always
/// represented in its non-dictionary (value) type. Dictionary haystacks are
/// flattened before construction.
///
/// Dictionary-encoded needles are unwrapped before the concrete filter is called.
pub(super) trait StaticFilter: Send + Sync {
    fn null_count(&self) -> usize;

    /// Checks if non-dictionary needles are contained in this filter's haystack.
    fn contains(&self, needles: &dyn Array, negated: bool) -> Result<BooleanArray>;
}
