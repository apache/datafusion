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

//! Query optimizer traits

use chrono::{DateTime, Utc};
use std::sync::Arc;

use log::{debug, trace};

use crate::error::Result;
use crate::logical_plan::LogicalPlan;

/// `OptimizerRule` transforms one ['LogicalPlan'] into another which
/// computes the same results, but in a potentially more efficient
/// way.
pub trait OptimizerRule {
    /// Rewrite `plan` to an optimized form
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;
}

/// Placeholder for optimizer configuration options
#[derive(Debug)]
pub struct OptimizerConfig {
    /// Query execution start time that can be used to rewrite expressions such as `now()`
    /// to use a literal value instead
    pub query_execution_start_time: DateTime<Utc>,
}

impl OptimizerConfig {
    /// Create optimizer config
    pub fn new() -> Self {
        Self {
            query_execution_start_time: chrono::Utc::now(),
        }
    }
}

impl Default for OptimizerConfig {
    /// Create optimizer config
    fn default() -> Self {
        Self::new()
    }
}

/// A rule-based optimizer.
#[derive(Clone)]
pub struct Optimizer {
    /// All rules to apply
    pub rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

impl Optimizer {
    /// Create a new optimizer with the given rules
    pub fn new(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }

    /// Optimizes the logical plan by applying optimizer rules, and
    /// invoking observer function after each call
    pub fn optimize<F>(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        let mut new_plan = plan.clone();
        debug!("Input logical plan:\n{}\n", plan.display_indent());
        trace!("Full input logical plan:\n{:?}", plan);
        for rule in &self.rules {
            new_plan = rule.optimize(&new_plan, optimizer_config)?;
            observer(&new_plan, rule.as_ref());
        }
        debug!("Optimized logical plan:\n{}\n", new_plan.display_indent());
        trace!("Full Optimized logical plan:\n {:?}", plan);
        Ok(new_plan)
    }
}
