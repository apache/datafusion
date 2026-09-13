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

//! Running SQL under a fixed memory budget, and saying what happened.
//!
//! [`TestCase`] in this module's parent covers queries over one of the built-in
//! [`Scenario`] tables, and asserts on the error text. These helpers cover the
//! other shape: arbitrary SQL against a context the test set up itself, where
//! *whether* the query completed — and what it had to spill to get there — is
//! the thing under test, not the message it failed with.
//!
//! ```text
//! let ctx = BudgetedEnv::new(16 * 1024 * 1024).build_ctx();
//! let outcome = run_under_budget(&ctx, "SELECT ...").await;
//! println!("{}", outcome.summary());
//! outcome.assert_completed().assert_spilled("SortExec");
//! ```
//!
//! [`Scenario`]: super::Scenario

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::{ExecutionPlan, displayable, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::DataFusionError;
use datafusion_common::human_readable_size;
use datafusion_common::instant::Instant;
use datafusion_execution::disk_manager::DiskManagerBuilder;
use datafusion_execution::memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool};
use datafusion_execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use futures::StreamExt;

/// How many consumers an exhaustion error lists. Higher than the pool's default
/// of 3, so that a failure names every partition of the operator that hit the
/// budget rather than cutting off at the largest few.
const TRACKED_CONSUMERS: usize = 8;

/// A [`RuntimeEnv`] with a fixed memory budget and spilling available.
///
/// The budget is enforced by a [`FairSpillPool`], the pool
/// `datafusion-cli --mem-pool-type fair` installs: spilling consumers get an
/// equal share of what is left after the unspillable ones.
#[derive(Debug, Clone)]
pub struct BudgetedEnv {
    budget: usize,
    config: SessionConfig,
}

impl BudgetedEnv {
    /// A budget of `budget` bytes.
    pub fn new(budget: usize) -> Self {
        Self {
            budget,
            config: SessionConfig::new(),
        }
    }

    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = config;
        self
    }

    /// The memory pool this budget is enforced by.
    pub fn build_pool(&self) -> Arc<dyn MemoryPool> {
        let tracked =
            NonZeroUsize::new(TRACKED_CONSUMERS).expect("non-zero tracked consumers");
        Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(self.budget),
            tracked,
        ))
    }

    pub fn build_runtime(&self) -> Arc<RuntimeEnv> {
        RuntimeEnvBuilder::new()
            .with_memory_pool(self.build_pool())
            // Operators that can spill, may: whether a query needs the disk to
            // finish is part of what these tests report.
            .with_disk_manager_builder(DiskManagerBuilder::default())
            .build_arc()
            .expect("building a budgeted runtime")
    }

    /// A context whose queries run against this budget.
    pub fn build_ctx(&self) -> SessionContext {
        SessionContext::new_with_config_rt(self.config.clone(), self.build_runtime())
    }
}

/// Spill metrics of one operator of an executed plan, summed over its
/// partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSpill {
    pub operator: String,
    pub spill_count: usize,
    pub spilled_bytes: usize,
}

/// What one query did under a budget.
pub struct QueryOutcome {
    /// The executed plan, for metrics and for asserting on operators
    pub plan: Arc<dyn ExecutionPlan>,
    pub elapsed: Duration,
    /// Rows returned, or rows returned before the failure
    pub row_count: usize,
    /// Why the query stopped, if it did not finish
    pub error: Option<DataFusionError>,
    /// Every operator that spilled, in plan order
    pub spills: Vec<OperatorSpill>,
}

impl QueryOutcome {
    pub fn completed(&self) -> bool {
        self.error.is_none()
    }

    /// Whether the query failed because it ran out of budget, as opposed to any
    /// other error.
    pub fn exhausted_budget(&self) -> bool {
        matches!(
            self.error.as_ref().map(DataFusionError::find_root),
            Some(DataFusionError::ResourcesExhausted(_))
        )
    }

    /// Spills recorded by every operator named `operator`.
    pub fn spill_count_of(&self, operator: &str) -> usize {
        self.spills
            .iter()
            .filter(|spill| spill.operator == operator)
            .map(|spill| spill.spill_count)
            .sum()
    }

    /// One line saying what the query did: worth printing from a test, since
    /// what a budget does to a query is a report as much as an assertion.
    pub fn summary(&self) -> String {
        let outcome = match &self.error {
            None => format!("completed, {} row(s)", self.row_count),
            Some(error) => format!("failed: {}", error.find_root()),
        };
        let spills = if self.spills.is_empty() {
            "nothing spilled".to_string()
        } else {
            self.spills
                .iter()
                .map(|spill| {
                    format!(
                        "{} spilled {} in {} event(s)",
                        spill.operator,
                        human_readable_size(spill.spilled_bytes),
                        spill.spill_count
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!("{outcome} in {:?}; {spills}", self.elapsed)
    }

    /// The plan, as it would be displayed. Handy in assertion messages.
    pub fn plan_display(&self) -> String {
        displayable(self.plan.as_ref()).indent(true).to_string()
    }

    /// Assert the query ran to completion under the budget.
    pub fn assert_completed(&self) -> &Self {
        assert!(
            self.completed(),
            "expected the query to complete under the budget, but it failed with: {}\n{}",
            self.error.as_ref().expect("checked above"),
            self.plan_display(),
        );
        self
    }

    /// Assert the query failed because the budget ran out.
    pub fn assert_exhausted_budget(&self) -> &Self {
        match &self.error {
            None => panic!(
                "expected the query to run out of budget, but it completed\n{}",
                self.plan_display()
            ),
            Some(error) => assert!(
                self.exhausted_budget(),
                "expected a ResourcesExhausted failure, got: {error}"
            ),
        }
        self
    }

    /// Assert the plan contains `operator`, so the test is measuring the
    /// operator it means to.
    pub fn assert_operator(&self, operator: &str) -> &Self {
        let plan = self.plan_display();
        assert!(
            plan.contains(operator),
            "expected the plan to use {operator}, got:\n{plan}"
        );
        self
    }

    /// Assert `operator` spilled at least once.
    pub fn assert_spilled(&self, operator: &str) -> &Self {
        self.assert_operator(operator);
        assert!(
            self.spill_count_of(operator) > 0,
            "expected {operator} to spill, but it did not. Spills: {:?}",
            self.spills
        );
        self
    }

    /// Assert `operator` did not spill.
    pub fn assert_did_not_spill(&self, operator: &str) -> &Self {
        self.assert_operator(operator);
        assert_eq!(
            self.spill_count_of(operator),
            0,
            "expected {operator} not to spill. Spills: {:?}",
            self.spills
        );
        self
    }
}

/// Run `sql` on `ctx` and report what it did.
///
/// Batches are dropped as they arrive, so the budget is spent on the query
/// rather than on holding its output. An execution failure — running out of
/// budget, most of all — is part of the outcome; only a planning failure
/// panics, since that means the test asked for something it cannot run.
pub async fn run_under_budget(ctx: &SessionContext, sql: &str) -> QueryOutcome {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"))
        .create_physical_plan()
        .await
        .unwrap_or_else(|e| panic!("planning `{sql}`: {e}"));

    let start = Instant::now();
    let mut row_count = 0;
    let mut error = None;
    match execute_stream(Arc::clone(&plan), ctx.task_ctx()) {
        Err(e) => error = Some(e),
        Ok(mut stream) => {
            while let Some(batch) = stream.next().await {
                match batch {
                    Ok(batch) => row_count += batch.num_rows(),
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
        }
    }
    let elapsed = start.elapsed();

    let mut spills = vec![];
    collect_spills(plan.as_ref(), &mut spills);

    QueryOutcome {
        plan,
        elapsed,
        row_count,
        error,
        spills,
    }
}

/// Collect the spill metrics of every operator in `plan` that spilled, merging
/// repeats of the same operator name.
fn collect_spills(plan: &dyn ExecutionPlan, spills: &mut Vec<OperatorSpill>) {
    let metrics = plan.metrics();
    let spill_count = metrics.as_ref().and_then(|m| m.spill_count()).unwrap_or(0);
    let spilled_bytes = metrics
        .as_ref()
        .and_then(|m| m.spilled_bytes())
        .unwrap_or(0);

    if spill_count > 0 || spilled_bytes > 0 {
        let name = plan.name();
        match spills.iter_mut().find(|spill| spill.operator == name) {
            Some(spill) => {
                spill.spill_count += spill_count;
                spill.spilled_bytes += spilled_bytes;
            }
            None => spills.push(OperatorSpill {
                operator: name.to_string(),
                spill_count,
                spilled_bytes,
            }),
        }
    }

    for child in plan.children() {
        collect_spills(child.as_ref(), spills);
    }
}
