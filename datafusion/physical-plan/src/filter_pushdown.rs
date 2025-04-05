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

/// Result of trying to push down fitlers to a child plan.
/// This is used by [`FilterPushdownResult`] to indicate whether the filter was
/// "absorbed" by the child ([`FilterPushdownSupport::Exact`]) or not
/// ([`FilterPushdownSupport::Unsupported`]).
/// If the filter was not absorbed, the parent plan must apply the filter
/// itself, or return to the caller that it was not pushed down.
/// If the filter was absorbed, the parent plan can drop the filter or
/// tell the caller that it was pushed down by forwarding on the [`FilterPushdownSupport::Exact`]
/// information.
#[derive(Debug, Clone, Copy)]
pub enum FilterPushdownSupport {
    /// Filter may not have been pushed down to the child plan, or the child plan
    /// can only partially apply the filter but may have false positives (but not false negatives).
    /// In this case the parent **must** behave as if the filter was not pushed down
    /// and must apply the filter itself.
    Unsupported,
    /// Filter was pushed down to the child plan and the child plan promises that
    /// it will apply the filter correctly with no false positives or false negatives.
    /// The parent can safely drop the filter.
    Exact,
}

/// The combined result of a filter pushdown operation.
/// This includes:
/// * The inner plan that was produced by the pushdown operation.
/// * The support for each filter that was pushed down.
pub enum FilterPushdownResult<T> {
    /// No pushdown was possible, keep this node as is in the tree.
    NotPushed,
    /// Pushed some or all filters into this node.
    /// The caller should replace the node in the tree with the new one provided
    /// and should transmit to parents the support for each filter.
    Pushed {
        /// The inner node that was produced by the pushdown operation.
        inner: T,
        /// The support for each filter that was pushed down.
        support: Vec<FilterPushdownSupport>,
    },
}

impl<T> FilterPushdownResult<T> {
    /// Create a new [`FilterPushdownResult`].
    pub fn new(inner: T, support: Vec<FilterPushdownSupport>) -> Self {
        Self::Pushed { inner, support }
    }
}
