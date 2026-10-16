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

//! [`PlanCheck`] and the [`PlanChecker`] that runs checks over a plan.

use std::sync::Arc;
use std::time::Duration;

use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;

use super::checks;
use crate::physical_plan::context::{CheckContext, Gather, Timeouts};
use crate::physical_plan::exec::runtime;
use crate::report::{Finding, Report, Violation};

/// What a check reads, which decides what the [`PlanChecker`] gathers in the
/// [`CheckContext`] before any check runs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckKind {
    /// Reads only what the node reports about itself, without executing it
    Static,
    /// Reads the output of executing each node, [`CheckContext::output`]
    Execution,
    /// Also reads the runs of rewritten copies of each node, or of copies run
    /// under other settings, [`CheckContext::variant_runs`]
    Variant,
    /// Also reads the stream experiments on each node,
    /// [`CheckContext::stream_runs`]
    Stream,
}

/// A property that every [`ExecutionPlan`] node should satisfy.
///
/// Checks run on one node at a time. A check may inspect the node's children
/// (for example to compare statistics), but must only report problems with
/// the node itself: the [`PlanChecker`] visits every node of the tree, so each
/// child is checked separately. Checks do not execute plans themselves: they
/// read what the [`PlanChecker`] gathered for their [`CheckKind`].
///
/// See `docs/physical_plan/CHECKS.md` in this crate for the catalog of checks.
///
/// # Example
/// ```
/// use datafusion_property_tests::Finding;
/// use datafusion_property_tests::physical_plan::{CheckKind, PlanCheck, PlanChecker};
///
/// let no_empty_names = PlanCheck {
///     name: "no_empty_names",
///     kind: CheckKind::Static,
///     check: |node, _context| {
///         Ok(if node.name().is_empty() {
///             vec![Finding::invariant("name() is empty")]
///         } else {
///             vec![]
///         })
///     },
/// };
/// let checker = PlanChecker::new().with_check(no_empty_names);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct PlanCheck {
    /// Stable, unique name, such as `equal_cardinality_num_rows`, used in
    /// reports and to skip the check with [`PlanChecker::allow`]
    pub name: &'static str,
    /// What the check reads
    pub kind: CheckKind,
    /// Check one node and return the problems found.
    ///
    /// Returning an `Err` means the check itself could not run, and aborts the
    /// whole [`PlanChecker::check`] call. Problems with the plan, including
    /// errors returned by the plan's own methods, are [`Finding`]s.
    pub check: fn(&Arc<dyn ExecutionPlan>, &CheckContext) -> Result<Vec<Finding>>,
}

/// Runs a set of [`PlanCheck`]s over every node of an [`ExecutionPlan`] tree.
///
/// Unless every check is [`CheckKind::Static`], every node of the plan is
/// executed first, each on its own, and so are the variant runs and stream
/// experiments that the checks need (see [`CheckContext`]). Plans under test
/// should therefore be built on inputs that can be executed repeatedly, such
/// as [`MockSourceExec`]. Variants and experiments that change how inputs
/// behave only change `MockSourceExec` leaves.
///
/// Plans are executed on a current-thread Tokio runtime, one run after
/// another, so that reports are deterministic.
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_physical_plan::ExecutionPlan;
/// # use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
/// use datafusion_property_tests::physical_plan::PlanChecker;
/// use datafusion_property_tests::physical_plan::fixtures::SourceSpec;
///
/// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
/// let source = SourceSpec::new(schema).with_partition_rows(&[10, 20]).build_arc()?;
/// let plan: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(source));
///
/// let report = PlanChecker::new().check(&plan)?;
/// report.assert_no_invariant_violations();
/// # Ok::<(), datafusion_common::DataFusionError>(())
/// ```
///
/// [`MockSourceExec`]: crate::physical_plan::fixtures::MockSourceExec
#[derive(Debug, Clone)]
pub struct PlanChecker {
    checks: Vec<PlanCheck>,
    timeouts: Timeouts,
}

impl Default for PlanChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl PlanChecker {
    /// Create a checker with all built-in checks
    pub fn new() -> Self {
        Self::with_checks(checks::all_checks())
    }

    /// Create a checker that runs `checks`
    pub fn with_checks(checks: Vec<PlanCheck>) -> Self {
        Self {
            checks,
            timeouts: Timeouts {
                execution: Duration::from_secs(30),
                stream: Duration::from_secs(2),
            },
        }
    }

    /// Add a check, such as a check specific to a user defined plan
    pub fn with_check(mut self, check: PlanCheck) -> Self {
        self.checks.push(check);
        self
    }

    /// Skip the check named `name`.
    ///
    /// # Panics
    ///
    /// If the checker has no check named `name`, which is usually a typo.
    pub fn allow(mut self, name: &str) -> Self {
        assert!(
            self.checks.iter().any(|check| check.name == name),
            "the checker has no check named '{name}'"
        );
        self.checks.retain(|check| check.name != name);
        self
    }

    /// Set the time allowed for executing a single node. A node that takes
    /// longer is reported as failing to execute. Variant runs, and stream
    /// experiments on finite inputs, which should end like a normal execution,
    /// use it too. Defaults to 30 seconds.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.execution = timeout;
        self
    }

    /// Set the time a stream experiment on inputs that never end may take, and
    /// the time allowed for a node to release streams and memory it no longer
    /// needs. A node that is expected to end or produce output on such inputs,
    /// but does not, is reported after this long. A wait for streams or memory
    /// to be released ends early once no task is alive on the Tokio runtime,
    /// since nothing is left that could release them. Defaults to 2 seconds.
    pub fn with_stream_timeout(mut self, timeout: Duration) -> Self {
        self.timeouts.stream = timeout;
        self
    }

    /// Keep only the checks for which `keep` returns true
    pub(crate) fn retain(mut self, keep: impl Fn(&PlanCheck) -> bool) -> Self {
        self.checks.retain(keep);
        self
    }

    /// Run every check against every node in `plan`.
    ///
    /// Starts a Tokio runtime to execute the plan, and so panics if called
    /// from within a Tokio runtime. Use [`Self::check_async`] in async code.
    pub fn check(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<Report> {
        runtime()?.block_on(self.check_async(plan))
    }

    /// Run every check against every node in `plan`, executing the plan on
    /// the current Tokio runtime if needed
    pub async fn check_async(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<Report> {
        let needs = |kind| self.checks.iter().any(|check| check.kind == kind);
        let gather = Gather {
            outputs: self
                .checks
                .iter()
                .any(|check| check.kind != CheckKind::Static),
            variants: needs(CheckKind::Variant),
            experiments: needs(CheckKind::Stream),
        };
        let context = CheckContext::gather(plan, gather, self.timeouts).await;
        let mut violations = vec![];
        self.check_recursive(plan, &context, &mut vec![], &mut violations)?;
        Ok(Report { violations })
    }

    fn check_recursive(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        context: &CheckContext,
        path: &mut Vec<usize>,
        violations: &mut Vec<Violation>,
    ) -> Result<()> {
        for check in &self.checks {
            for finding in (check.check)(plan, context)? {
                violations.push(Violation {
                    check: check.name,
                    severity: finding.severity,
                    node: plan.name().to_string(),
                    path: path.clone(),
                    message: finding.message,
                });
            }
        }
        for (i, child) in plan.children().into_iter().enumerate() {
            path.push(i);
            self.check_recursive(child, context, path, violations)?;
            path.pop();
        }
        Ok(())
    }
}
