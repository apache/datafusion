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

use std::cmp::Ordering;

use datafusion_common::ScalarValue;

/// Which `IN` form a sorted domain is pruning for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SetMembership {
    /// `col IN (...)`. A row matches only where the domain intersects the
    /// interval, so a disjoint interval excludes every row.
    In,
    /// `col NOT IN (...)`. Overlap proves nothing here: values outside the
    /// domain still satisfy the predicate. An interval excludes every row only
    /// when it holds a single value that the domain contains.
    NotIn,
}

impl SetMembership {
    pub(crate) fn display_name(self) -> &'static str {
        match self {
            Self::In => "IN_SET_INTERSECTS",
            Self::NotIn => "NOT_IN_SET_MAY_MATCH",
        }
    }

    pub(crate) fn compare_bytes(self, left: &[u8], right: &[u8]) -> Ordering {
        match self {
            // IN uses this order for interval searches.
            Self::In => left.cmp(right),
            // NOT IN only needs exact membership. Reject impossible lengths
            // before comparing bytes that may have a long common prefix.
            Self::NotIn => left.len().cmp(&right.len()).then_with(|| left.cmp(right)),
        }
    }
}

/// Evaluates `NOT IN` against an inclusive statistics interval.
///
/// A container can be excluded only when both bounds identify one value in the
/// domain. A known bound outside the domain proves the container may match and
/// lets an enclosing Boolean expression short-circuit.
///
/// This remains safe when Parquet truncates byte-array statistics. A truncated
/// minimum is no greater than the true minimum, and a truncated maximum is no
/// less than the true maximum. Therefore, equal stored bounds prove that the
/// true minimum and maximum are also equal.
#[inline(always)]
pub(crate) fn not_in_may_match<T: ?Sized + PartialEq>(
    min: Option<&T>,
    max: Option<&T>,
    contains: impl Fn(&T) -> bool,
) -> Option<bool> {
    match (min, max) {
        (Some(min), Some(max)) if contains(min) => Some(min != max),
        (Some(_), Some(_)) => Some(true),
        (Some(bound), None) | (None, Some(bound)) if !contains(bound) => Some(true),
        _ => None,
    }
}

/// Removes scalar wrappers that do not change the represented value.
pub(crate) fn unwrap_scalar(value: &ScalarValue) -> &ScalarValue {
    match value {
        ScalarValue::Dictionary(_, value) | ScalarValue::RunEndEncoded(_, _, value) => {
            unwrap_scalar(value)
        }
        value => value,
    }
}
