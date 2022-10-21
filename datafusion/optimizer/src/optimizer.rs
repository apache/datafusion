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

use crate::common_subexpr_eliminate::CommonSubexprEliminate;
use crate::decorrelate_where_exists::DecorrelateWhereExists;
use crate::decorrelate_where_in::DecorrelateWhereIn;
use crate::eliminate_filter::EliminateFilter;
use crate::eliminate_limit::EliminateLimit;
use crate::filter_null_join_keys::FilterNullJoinKeys;
use crate::filter_push_down::FilterPushDown;
use crate::inline_table_scan::InlineTableScan;
use crate::limit_push_down::LimitPushDown;
use crate::projection_push_down::ProjectionPushDown;
use crate::reduce_cross_join::ReduceCrossJoin;
use crate::reduce_outer_join::ReduceOuterJoin;
use crate::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use crate::scalar_subquery_to_join::ScalarSubqueryToJoin;
use crate::simplify_expressions::SimplifyExpressions;
use crate::single_distinct_to_groupby::SingleDistinctToGroupBy;
use crate::subquery_filter_to_join::SubqueryFilterToJoin;
use crate::type_coercion::TypeCoercion;
use crate::unwrap_cast_in_comparison::UnwrapCastInComparison;
use chrono::{DateTime, Utc};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::logical_plan::LogicalPlan;
use log::{debug, trace, warn};
use std::sync::Arc;
use std::time::Instant;

/// `OptimizerRule` transforms one ['LogicalPlan'] into another which
/// computes the same results, but in a potentially more efficient
/// way. If there are no suitable transformations for the input plan,
/// the optimizer can simply return it as is.
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
    /// Specify whether to enable the filter_null_keys rule
    filter_null_keys: bool,
    /// Maximum number of times to run optimizer against a plan
    max_passes: u8,
}

impl OptimizerConfig {
    /// Create optimizer config
    pub fn new() -> Self {
        Self {
            query_execution_start_time: chrono::Utc::now(),
            next_id: 0, // useful for generating things like unique subquery aliases
            skip_failing_rules: true,
            filter_null_keys: true,
            max_passes: 3,
        }
    }

    /// Specify whether to enable the filter_null_keys rule
    pub fn filter_null_keys(mut self, filter_null_keys: bool) -> Self {
        self.filter_null_keys = filter_null_keys;
        self
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

    /// Specify how many times to attempt to optimize the plan
    pub fn with_max_passes(mut self, v: u8) -> Self {
        self.max_passes = v;
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
    /// Create a new optimizer using the recommended list of rules
    pub fn new(config: &OptimizerConfig) -> Self {
        let mut rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(InlineTableScan::new()),
            Arc::new(TypeCoercion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(UnwrapCastInComparison::new()),
            Arc::new(DecorrelateWhereExists::new()),
            Arc::new(DecorrelateWhereIn::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(SubqueryFilterToJoin::new()),
            // simplify expressions does not simplify expressions in subqueries, so we
            // run it again after running the optimizations that potentially converted
            // subqueries to joins
            Arc::new(SimplifyExpressions::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(ReduceCrossJoin::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(RewriteDisjunctivePredicate::new()),
        ];
        if config.filter_null_keys {
            rules.push(Arc::new(FilterNullJoinKeys::default()));
        }
        rules.push(Arc::new(ReduceOuterJoin::new()));
        rules.push(Arc::new(FilterPushDown::new()));
        rules.push(Arc::new(LimitPushDown::new()));
        rules.push(Arc::new(SingleDistinctToGroupBy::new()));

        // The previous optimizations added expressions and projections,
        // that might benefit from the following rules
        rules.push(Arc::new(SimplifyExpressions::new()));
        rules.push(Arc::new(UnwrapCastInComparison::new()));
        rules.push(Arc::new(CommonSubexprEliminate::new()));
        rules.push(Arc::new(ProjectionPushDown::new()));

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
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
        let start_time = Instant::now();
        let mut plan_str = format!("{}", plan.display_indent());
        let mut new_plan = plan.clone();
        let mut i = 0;
        while i < optimizer_config.max_passes {
            log_plan(&format!("Optimizer input (pass {})", i), &new_plan);

            for rule in &self.rules {
                let result = rule.optimize(&new_plan, optimizer_config);
                match result {
                    Ok(plan) => {
                        new_plan = plan;
                        observer(&new_plan, rule.as_ref());
                        log_plan(rule.name(), &new_plan);
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
            log_plan(&format!("Optimized plan (pass {})", i), &new_plan);

            // TODO this is an expensive way to see if the optimizer did anything and
            // it would be better to change the OptimizerRule trait to return an Option
            // instead
            let new_plan_str = format!("{}", new_plan.display_indent());
            if plan_str == new_plan_str {
                // plan did not change, so no need to continue trying to optimize
                debug!("optimizer pass {} did not make changes", i);
                break;
            }
            plan_str = new_plan_str;
            i += 1;
        }
        log_plan("Final optimized plan", &new_plan);
        debug!("Optimizer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }
}

/// Log the plan in debug/tracing mode after some part of the optimizer runs
fn log_plan(description: &str, plan: &LogicalPlan) {
    debug!("{description}:\n{}\n", plan.display_indent());
    trace!("{description}::\n{}\n", plan.display_indent_schema());
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
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
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
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
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
