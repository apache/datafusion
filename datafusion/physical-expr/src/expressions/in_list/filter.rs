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

//! Static filter trait for InList expressions

use arrow::array::{Array, BooleanArray};
use datafusion_common::Result;

/// Trait for InList static filters
///
/// Static filters pre-compute a lookup structure from the IN list values,
/// allowing O(1) or O(log n) membership tests during query execution.
pub(crate) trait StaticFilter {
    /// Returns the number of null values in the filter's haystack
    fn null_count(&self) -> usize;

    /// Checks if values in `v` are contained in the filter
    ///
    /// Returns a `BooleanArray` where:
    /// - `true` means the value is in the set (or not in the set if `negated`)
    /// - `false` means the value is not in the set (or in the set if `negated`)
    /// - `null` follows SQL three-valued logic for NULL handling
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray>;
}
