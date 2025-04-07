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

pub use datafusion_expr::FilterPushdown;
use datafusion_physical_expr::PhysicalExprRef;

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
        support: Vec<FilterPushdown>,
    },
}

impl<T> FilterPushdownResult<T> {
    /// Create a new [`FilterPushdownResult`] with the given inner plan and marking
    /// all filters as pushed down with `Exact` support.
    pub fn new_exact(inner: T, filters: &[PhysicalExprRef]) -> Self {
        Self::Pushed {
            inner,
            support: vec![FilterPushdown::Exact; filters.len()],
        }
    }

    /// Create a new [`FilterPushdownResult`] with the given inner plan and support
    /// for each filter that was pushed down as `Inexact`.
    pub fn new_inexact(inner: T, filters: &[PhysicalExprRef]) -> Self {
        Self::Pushed {
            inner,
            support: vec![FilterPushdown::Inexact; filters.len()],
        }
    }

    /// Create a new [`FilterPushdownResult`] with no pushdown.
    pub fn new_not_pushed() -> Self {
        Self::NotPushed
    }
}
