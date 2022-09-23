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
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use log::{debug, trace, warn};
use std::sync::Arc;

/// `OptimizerRule` transforms one ['LogicalPlan'] into another which
/// computes the same results, but in a potentially more efficient
/// way.
pub trait OptimizerRule {
    /// Rewrite `plan` to an optimized form
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;
}

/// Options to control the DataFusion Optimizer.
#[derive(Debug)]
pub struct OptimizerConfig {
    /// Query execution start time that can be used to rewrite
    /// expressions such as `now()` to use a literal value instead
    query_execution_start_time: DateTime<Utc>,
    /// id generator for optimizer passes
    // TODO this should not be on the config,
    // it should be its own 'OptimizerState' or something)
    next_id: usize,
    /// Option to skip rules that produce errors
    skip_failing_rules: bool,
}

impl OptimizerConfig {
    /// Create optimizer config
    pub fn new() -> Self {
        Self {
            query_execution_start_time: chrono::Utc::now(),
            next_id: 0, // useful for generating things like unique subquery aliases
            skip_failing_rules: true,
        }
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_tart_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = query_execution_tart_time;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_skip_failing_rules(mut self, b: bool) -> Self {
        self.skip_failing_rules = b;
        self
    }

    /// Generate the next ID needed
    pub fn next_id(&mut self) -> usize {
        self.next_id += 1;
        self.next_id
    }

    /// Return the time at which the query execution started. This
    /// time is used as the value for now()
    pub fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.query_execution_start_time
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
        optimizer_config: &mut OptimizerConfig,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        let mut new_plan = plan.clone();
        debug!("Input logical plan:\n{}\n", plan.display_indent());
        trace!("Full input logical plan:\n{:?}", plan);
        for rule in &self.rules {
            let result = rule.optimize(&new_plan, optimizer_config);
            match result {
                Ok(plan) => {
                    new_plan = plan;
                    observer(&new_plan, rule.as_ref());
                    debug!("After apply {} rule:\n", rule.name());
                    debug!("Optimized logical plan:\n{}\n", new_plan.display_indent());
                }
                Err(ref e) => {
                    if optimizer_config.skip_failing_rules {
                        // Note to future readers: if you see this warning it signals a
                        // bug in the DataFusion optimizer. Please consider filing a ticket
                        // https://github.com/apache/arrow-datafusion
                        warn!(
                            "Skipping optimizer rule '{}' due to unexpected error: {}",
                            rule.name(),
                            e
                        );
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Optimizer rule '{}' failed due to unexpected error: {}",
                            rule.name(),
                            e
                        )));
                    }
                }
            }
        }
        debug!("Optimized logical plan:\n{}\n", new_plan.display_indent());
        trace!("Full Optimized logical plan:\n {:?}", new_plan);
        Ok(new_plan)
    }
}

#[cfg(test)]
mod tests {
    use crate::optimizer::Optimizer;
    use crate::{OptimizerConfig, OptimizerRule};
    use datafusion_common::{DFSchema, DataFusionError};
    use datafusion_expr::logical_plan::EmptyRelation;
    use datafusion_expr::LogicalPlan;
    use std::sync::Arc;

    #[test]
    fn skip_failing_rule() -> Result<(), DataFusionError> {
        let opt = Optimizer::new(vec![Arc::new(BadRule {})]);
        let mut config = OptimizerConfig::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(&plan, &mut config, &observe)?;
        Ok(())
    }

    #[test]
    fn no_skip_failing_rule() -> Result<(), DataFusionError> {
        let opt = Optimizer::new(vec![Arc::new(BadRule {})]);
        let mut config = OptimizerConfig::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let result = opt.optimize(&plan, &mut config, &observe);
        assert_eq!(
            "Internal error: Optimizer rule 'bad rule' failed due to unexpected error: \
            Error during planning: rule failed. This was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker",
            format!("{}", result.err().unwrap())
        );
        Ok(())
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    struct BadRule {}

    impl OptimizerRule for BadRule {
        fn optimize(
            &self,
            _plan: &LogicalPlan,
            _optimizer_config: &mut OptimizerConfig,
        ) -> datafusion_common::Result<LogicalPlan> {
            Err(DataFusionError::Plan("rule failed".to_string()))
        }

        fn name(&self) -> &str {
            "bad rule"
        }
    }
}
