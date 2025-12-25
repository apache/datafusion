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

//! Utilities for working with dynamic filters in both stateful and stateless plan modes.
//!
//! Each dynamic filter can exist in two states: planning and executable.The difference
//! between these two states is that a planned filter does not assume concurrent modifications,
//! while an executable filter does. During the planning or optimization stage, a filter is created
//! as a planned filter and then converted into an executable form when [`execute`] is called.
//!
//! For stateful (default) plan mode, there is no need to distinguish between planned
//! and executable filters, as they are stored directly within the owner's [`ExecutionPlan`]
//! and shared with children during the planning stage via filter push-down optimization.
//! In this mode, both states are represented by the same type: [`DynamicFilterPhysicalExpr`].
//!
//! For stateless plan mode, filters are similarly pushed from the owner's [`ExecutionPlan`]
//! to a child during filter push-down. However, because the [`ExecutionPlan`] is stateless,
//! it cannot store a shared version of the filter. Instead, the executable filter is created
//! when [`execute`] is called. In this mode, the two states are represented by different types:
//! the planned version is [`PlannedDynamicFilterPhysicalExpr`] from the physical-expr crate,
//! and the executable version remains [`DynamicFilterPhysicalExpr`].
//!
//! [`ExecutionPlan`]: crate::ExecutionPlan
//! [`execute`]: crate::ExecutionPlan::execute
//!

use std::sync::Arc;

use datafusion_physical_expr::PhysicalExpr;

pub use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;

#[cfg(feature = "stateless_plan")]
pub use datafusion_physical_expr::expressions::PlannedDynamicFilterPhysicalExpr;

/// For stateful plans planned and executable filters are the same.
#[cfg(not(feature = "stateless_plan"))]
pub type PlannedDynamicFilterPhysicalExpr = DynamicFilterPhysicalExpr;

/// Helper to make a new planning stage dynamic filter.
#[cfg(feature = "stateless_plan")]
pub fn make_planned_dynamic_filter(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> PlannedDynamicFilterPhysicalExpr {
    PlannedDynamicFilterPhysicalExpr::new(expr, children)
}

#[cfg(not(feature = "stateless_plan"))]
pub fn make_planned_dynamic_filter(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> PlannedDynamicFilterPhysicalExpr {
    // For stateful plans executable planned and executable filters are the same.
    make_executable_dynamic_filter(expr, children)
}

/// Helper to make a new execution stage [`DynamicFilterPhysicalExpr`].
#[cfg(feature = "stateless_plan")]
#[cfg(test)]
pub fn make_executable_dynamic_filter(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> DynamicFilterPhysicalExpr {
    PlannedDynamicFilterPhysicalExpr::new(expr, children).to_executable()
}

#[cfg(not(feature = "stateless_plan"))]
pub fn make_executable_dynamic_filter(
    expr: Arc<dyn PhysicalExpr>,
    children: Vec<Arc<dyn PhysicalExpr>>,
) -> DynamicFilterPhysicalExpr {
    DynamicFilterPhysicalExpr::new(expr, children)
}
