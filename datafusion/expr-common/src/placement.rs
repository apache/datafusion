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
    PlaceAtLeaves,
    /// An expensive expression that should stay at the root of the plan.
    /// This is the default for most expressions.
    PlaceAtRoot,
}
