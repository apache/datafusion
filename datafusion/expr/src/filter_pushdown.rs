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

/// Result of attempting to push down a filter/predicate expression.
///
/// This is used by [`FilterPushdownResult`] to indicate whether the filter was
/// "absorbed" by the child ([`FilterPushdownSupport::Exact`]) or not
/// ([`FilterPushdownSupport::Unsupported`] or [`FilterPushdownSupport::Inexact`]).
///
/// If the filter was not absorbed, the parent plan must apply the filter
/// itself, or return to the caller that it was not pushed down.
///
/// If the filter was absorbed, the parent plan can drop the filter or
/// tell the caller that it was pushed down by forwarding on the [`FilterPushdownSupport::Exact`]
/// information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterPushdown {
    /// No child plan was able to absorb the filter.
    /// In this case the parent **must** behave as if the filter was not pushed down
    /// and must apply the filter itself.
    Unsupported,
    /// A child plan may be able to partially apply the filter or a less selective version of it,
    /// but it might return false positives (but no false negatives).
    /// In this case the parent **must** behave as if the filter was not pushed down
    /// and must apply the filter itself.
    Inexact,
    /// Filter was pushed down to the child plan and the child plan promises that
    /// it will apply the filter correctly with no false positives or false negatives.
    /// The parent can safely drop the filter.
    Exact,
}

impl FilterPushdown {
    /// Create a new [`FilterPushdownSupport`].
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact)
    }
}
