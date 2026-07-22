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

//! Physical optimizer interfaces.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::operator_statistics::StatisticsRegistry;

/// Context available to physical optimizer rules.
///
/// This trait provides access to configuration options and an optional statistics
/// registry for enhanced statistics lookup.
pub trait PhysicalOptimizerContext: Send + Sync {
    /// Returns the configuration options.
    fn config_options(&self) -> &ConfigOptions;

    /// Returns the statistics registry for enhanced statistics lookup.
    ///
    /// Returns `None` if no registry is configured, in which case rules
    /// should fall back to using [`ExecutionPlan::partition_statistics`].
    fn statistics_registry(&self) -> Option<&StatisticsRegistry> {
        None
    }
}

/// Transforms one [`ExecutionPlan`] into another that computes the same results,
/// but may do so more efficiently.
pub trait PhysicalOptimizerRule: Debug + std::any::Any {
    /// Rewrite `plan` to an optimized form.
    ///
    /// Rules that need the statistics registry should override
    /// [`Self::optimize_with_context`] instead.
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Rewrite `plan` with access to extended context.
    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize(plan, context.config_options())
    }

    /// A human-readable name for this optimizer rule.
    fn name(&self) -> &str;

    /// Whether the physical planner should validate that this rule preserves
    /// the plan schema.
    fn schema_check(&self) -> bool;
}
