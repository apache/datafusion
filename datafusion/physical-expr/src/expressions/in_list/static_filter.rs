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

//! Core trait for InList static filters

use arrow::array::{Array, BooleanArray};
use datafusion_common::Result;

/// Trait for InList static filters.
///
/// Static filters are pre-computed lookup structures that enable efficient
/// membership testing for IN list expressions. Different implementations
/// optimize for different data types:
///
/// - [`super::primitive_filter::BitmapFilter`]: O(1) bit test for u8/u16
/// - [`super::primitive_filter::BranchlessFilter`]: Unrolled OR-chain for small lists
/// - [`super::primitive_filter::DirectProbeFilter`]: O(1) hash lookups for larger primitive types
/// - [`super::transform::Utf8TwoStageFilter`]: Two-stage filter for Utf8/LargeUtf8
/// - [`super::nested_filter::NestedTypeFilter`]: Dynamic comparator for complex types
pub(crate) trait StaticFilter {
    /// Returns the number of null values in the filter's haystack.
    fn null_count(&self) -> usize;

    /// Checks if values in `v` are contained in the filter.
    ///
    /// Returns a `BooleanArray` with the same length as `v`, where each element
    /// indicates whether the corresponding value is in the filter (or NOT in,
    /// if `negated` is true).
    ///
    /// Follows SQL three-valued logic:
    /// - If the needle value is null, the result is null
    /// - If the needle is not found AND the haystack contains nulls, the result is null
    /// - Otherwise, the result is true/false based on membership
    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray>;
}
