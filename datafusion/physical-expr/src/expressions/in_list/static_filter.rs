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

use arrow::array::{ArrayRef, BooleanArray};
use datafusion_common::Result;

/// Trait for InList static filters.
///
/// Static filters store a pre-computed set of values (the haystack) and check
/// whether needle values are contained in that set. The haystack is always
/// represented in its non-dictionary (value) type. Dictionary haystacks are
/// flattened via `cast()` before construction.
///
/// Dictionary-encoded needles are unwrapped inside `contains()` and
/// evaluated against the dictionary's values.
pub(super) trait StaticFilter {
    fn null_count(&self) -> usize;

    /// Checks if values in `v` (needle) are contained in this filter's
    /// haystack. `v` may be dictionary-encoded, in which case the
    /// implementation unwraps the dictionary and operates on its values.
    fn contains(&self, v: ArrayRef, negated: bool) -> Result<BooleanArray>;
}

/// Evaluate dictionary-encoded needles by applying a filter to dictionary
/// values and remapping the result through the keys.
macro_rules! handle_dictionary {
    ($self:ident, $v:ident, $negated:ident) => {
        let array = $v.as_ref();
        arrow::array::downcast_dictionary_array! {
            array => {
                let values_contains =
                    $self.contains(std::sync::Arc::clone(array.values()), $negated)?;
                let result = arrow::compute::take(&values_contains, array.keys(), None)?;
                return Ok(arrow::array::downcast_array(result.as_ref()))
            }
            _ => {}
        }
    };
}

pub(super) use handle_dictionary;
