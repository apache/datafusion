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

//! Expression placement information for optimization decisions.

/// Describes where an expression should be placed in the query plan for
/// optimal execution. This is used by optimizers to make decisions about
/// expression placement, such as whether to push expressions down through
/// projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExpressionPlacement {
    /// A constant literal value.
    Literal,
    /// A simple column reference.
    Column,
    /// A cheap expression that can be pushed to leaf nodes in the plan.
    /// Examples include `get_field` for struct field access.
    /// Pushing these expressions down in the plan can reduce data early
    /// at low compute cost.
    /// See [`ExpressionPlacement::should_push_to_leaves`] for details.
    MoveTowardsLeafNodes,
    /// An expensive expression that should stay where it is in the plan.
    /// Examples include complex scalar functions or UDFs.
    KeepInPlace,
}

impl ExpressionPlacement {
    /// Returns true if the expression can be pushed down to leaf nodes
    /// in the query plan.
    ///
    /// This returns true for:
    /// - [`ExpressionPlacement::Column`]: Simple column references can be pushed down. They do no compute and do not increase or
    ///   decrease the amount of data being processed.
    ///   A projection that reduces the number of columns can eliminate unnecessary data early,
    ///   but this method only considers one expression at a time, not a projection as a whole.
    /// - [`ExpressionPlacement::MoveTowardsLeafNodes`]: Cheap expressions can be pushed down to leaves to take advantage of
    ///   early computation and potential optimizations at the data source level.
    ///   For example `struct_col['field']` is cheap to compute (just an Arc clone of the nested array for `'field'`)
    ///   and thus can reduce data early in the plan at very low compute cost.
    ///   It may even be possible to eliminate the expression entirely if the data source can project only the needed field
    ///   (as e.g. Parquet can).
    pub fn should_push_to_leaves(&self) -> bool {
        matches!(
            self,
            ExpressionPlacement::Column | ExpressionPlacement::MoveTowardsLeafNodes
        )
    }
}
