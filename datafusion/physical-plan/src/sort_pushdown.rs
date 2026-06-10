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

//! Sort pushdown types for physical execution plans.
//!
//! This module provides types used for pushing sort ordering requirements
//! down through the execution plan tree to data sources.

/// Result of attempting to push down sort ordering to a node.
///
/// Used by [`ExecutionPlan::try_pushdown_sort`] to communicate
/// whether and how sort ordering was successfully pushed down.
///
/// [`ExecutionPlan::try_pushdown_sort`]: crate::ExecutionPlan::try_pushdown_sort
#[derive(Debug, Clone)]
pub enum SortOrderPushdownResult<T> {
    /// The source can guarantee exact ordering (data is perfectly sorted).
    ///
    /// When this is returned, the optimizer can safely remove the Sort operator
    /// entirely since the data source guarantees the requested ordering.
    Exact {
        /// The optimized node that provides exact ordering
        inner: T,
    },
    /// The source has optimized for the ordering but cannot guarantee perfect sorting.
    ///
    /// This indicates the data source has been optimized (e.g., reordered files/row groups
    /// based on statistics, enabled reverse scanning) but the data may not be perfectly
    /// sorted. The optimizer should keep the Sort operator but benefits from the
    /// optimization (e.g., faster TopK queries due to early termination).
    Inexact {
        /// The optimized node that provides approximate ordering
        inner: T,
    },
    /// The source cannot optimize for this ordering.
    ///
    /// The data source does not support the requested sort ordering and no
    /// optimization was applied.
    Unsupported,
}

impl<T> SortOrderPushdownResult<T> {
    /// Extract the inner value if present
    pub fn into_inner(self) -> Option<T> {
        match self {
            Self::Exact { inner } | Self::Inexact { inner } => Some(inner),
            Self::Unsupported => None,
        }
    }

    /// Map the inner value to a different type while preserving the variant.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> SortOrderPushdownResult<U> {
        match self {
            Self::Exact { inner } => SortOrderPushdownResult::Exact { inner: f(inner) },
            Self::Inexact { inner } => {
                SortOrderPushdownResult::Inexact { inner: f(inner) }
            }
            Self::Unsupported => SortOrderPushdownResult::Unsupported,
        }
    }

    /// Try to map the inner value, returning an error if the function fails.
    pub fn try_map<U, E, F: FnOnce(T) -> Result<U, E>>(
        self,
        f: F,
    ) -> Result<SortOrderPushdownResult<U>, E> {
        match self {
            Self::Exact { inner } => {
                Ok(SortOrderPushdownResult::Exact { inner: f(inner)? })
            }
            Self::Inexact { inner } => {
                Ok(SortOrderPushdownResult::Inexact { inner: f(inner)? })
            }
            Self::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
        }
    }

    /// Convert this result to `Inexact`, downgrading `Exact` if present.
    ///
    /// This is useful when an operation (like merging multiple partitions)
    /// cannot guarantee exact ordering even if the input provides it.
    ///
    /// # Examples
    ///
    /// ```
    /// # use datafusion_physical_plan::SortOrderPushdownResult;
    /// let exact = SortOrderPushdownResult::Exact { inner: 42 };
    /// let inexact = exact.into_inexact();
    /// assert!(matches!(inexact, SortOrderPushdownResult::Inexact { inner: 42 }));
    ///
    /// let already_inexact = SortOrderPushdownResult::Inexact { inner: 42 };
    /// let still_inexact = already_inexact.into_inexact();
    /// assert!(matches!(still_inexact, SortOrderPushdownResult::Inexact { inner: 42 }));
    ///
    /// let unsupported = SortOrderPushdownResult::<i32>::Unsupported;
    /// let still_unsupported = unsupported.into_inexact();
    /// assert!(matches!(still_unsupported, SortOrderPushdownResult::Unsupported));
    /// ```
    pub fn into_inexact(self) -> Self {
        match self {
            Self::Exact { inner } => Self::Inexact { inner },
            Self::Inexact { inner } => Self::Inexact { inner },
            Self::Unsupported => Self::Unsupported,
        }
    }
}
