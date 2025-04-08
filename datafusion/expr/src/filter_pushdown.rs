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
/// This is used by:
/// * `FilterPushdownResult` in `ExecutionPlan` to do predicate pushdown at the physical plan level
///   (e.g. pushing down dynamic fitlers from a hash join into scans).
/// * `TableProvider` to do predicate pushdown at planning time (e.g. pruning partitions).
///
/// There are three possible outcomes of a filter pushdown:
/// * [`FilterPushdown::Unsupported`] - the filter could not be applied / is not understood.
/// * [`FilterPushdown::Inexact`] - the filter could be applied, but it may not be exact.
///   The caller should treat this the same as [`FilterPushdown::Unsupported`] for the most part
///   and must not assume that any pruning / filter was done since there may be false positives.
/// * [`FilterPushdown::Exact`] - the filter was absorbed by the child plan and it promises
///   to apply the filter correctly.
///   The parent plan can drop the filter and assume that the child plan will apply it correctly.
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
    /// Create a new [`FilterPushdown`].
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact)
    }
}
