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

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::LogicalPlan;

use crate::analyzer::AsyncAnalyzerRule;
use crate::optimizer::AsyncOptimizerRule;

use super::Strategy;

/// A named, activatable phase whose rules are applied asynchronously.
///
/// Use [`AsyncAnalysisPhase`] for [`AsyncAnalyzerRule`]s and
/// [`AsyncOptimizationPhase`] for [`AsyncOptimizerRule`]s.
pub struct AsyncPhase<T: ?Sized> {
    pub(super) name: String,
    pub enabled: bool,
    pub strategy: Strategy,
    pub rules: Vec<Arc<T>>,
}

impl<T: ?Sized> Clone for AsyncPhase<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            enabled: self.enabled,
            strategy: self.strategy.clone(),
            rules: self.rules.clone(),
        }
    }
}

impl<T: ?Sized + Debug> Debug for AsyncPhase<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncPhase")
            .field("name", &self.name)
            .field("enabled", &self.enabled)
            .field("strategy", &self.strategy)
            .field("rules", &self.rules)
            .finish()
    }
}

impl<T: ?Sized> AsyncPhase<T> {
    pub fn new(name: impl Into<String>, strategy: Strategy) -> Self {
        Self {
            name: name.into(),
            enabled: true,
            strategy,
            rules: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl AsyncPhase<dyn AsyncAnalyzerRule + Send + Sync> {
    pub async fn apply(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        if !self.enabled || self.rules.is_empty() {
            return Ok(plan);
        }
        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => {
                max_passes.unwrap_or_else(|| config.optimizer.max_passes)
            }
        };
        let mut plan = plan;
        for _ in 0..passes {
            for rule in &self.rules {
                plan = rule.analyze(plan, config).await?;
            }
        }
        Ok(plan)
    }

    /// Like [`Self::apply`] but calls `observer` after each rule for EXPLAIN output.
    pub async fn apply_observed(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
        observer: &mut (dyn FnMut(&LogicalPlan, &str) + Send),
    ) -> Result<LogicalPlan> {
        if !self.enabled || self.rules.is_empty() {
            return Ok(plan);
        }
        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => {
                max_passes.unwrap_or_else(|| config.optimizer.max_passes)
            }
        };
        let mut plan = plan;
        for _ in 0..passes {
            for rule in &self.rules {
                plan = rule.analyze(plan, config).await?;
                observer(&plan, rule.name());
            }
        }
        Ok(plan)
    }
}

impl AsyncPhase<dyn AsyncOptimizerRule + Send + Sync> {
    pub async fn apply(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        if !self.enabled || self.rules.is_empty() {
            return Ok(plan);
        }
        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => {
                max_passes.unwrap_or_else(|| config.optimizer.max_passes)
            }
        };
        let mut plan = plan;
        for _ in 0..passes {
            for rule in &self.rules {
                plan = rule.rewrite(plan, config).await?;
            }
        }
        Ok(plan)
    }

    /// Like [`Self::apply`] but calls `observer` after each rule for EXPLAIN output.
    pub async fn apply_observed(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
        observer: &mut (dyn FnMut(&LogicalPlan, &str) + Send),
    ) -> Result<LogicalPlan> {
        if !self.enabled || self.rules.is_empty() {
            return Ok(plan);
        }
        let passes = match &self.strategy {
            Strategy::Once => 1,
            Strategy::FixedPoint { max_passes } => {
                max_passes.unwrap_or_else(|| config.optimizer.max_passes)
            }
        };
        let mut plan = plan;
        for _ in 0..passes {
            for rule in &self.rules {
                plan = rule.rewrite(plan, config).await?;
                observer(&plan, rule.name());
            }
        }
        Ok(plan)
    }
}

pub type AsyncAnalysisPhase = AsyncPhase<dyn AsyncAnalyzerRule + Send + Sync>;
pub type AsyncOptimizationPhase = AsyncPhase<dyn AsyncOptimizerRule + Send + Sync>;
