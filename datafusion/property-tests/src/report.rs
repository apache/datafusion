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

//! Types describing the outcome of running checks against a plan.

use std::fmt;

/// How serious a [`Violation`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Severity {
    /// The plan breaks a guarantee that DataFusion relies on. Optimizer rules
    /// or parent operators that trust the reported property can produce wrong
    /// results.
    Invariant,
    /// The plan is correct but reports weaker properties than it could, so
    /// DataFusion misses optimizations.
    Lint,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Invariant => write!(f, "invariant"),
            Severity::Lint => write!(f, "lint"),
        }
    }
}

/// A single problem found by a check, before it is attributed to a plan node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    /// How serious the problem is
    pub severity: Severity,
    /// Human readable description of the problem
    pub message: String,
}

impl Finding {
    /// Create a [`Severity::Invariant`] finding
    pub fn invariant(message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Invariant,
            message: message.into(),
        }
    }

    /// Create a [`Severity::Lint`] finding
    pub fn lint(message: impl Into<String>) -> Self {
        Self {
            severity: Severity::Lint,
            message: message.into(),
        }
    }
}

/// A [`Finding`] attributed to a check and a node in the plan tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// Name of the check, such as `equal_cardinality_num_rows`
    pub check: &'static str,
    /// How serious the problem is
    pub severity: Severity,
    /// The name of the offending node, such as [`ExecutionPlan::name`]
    ///
    /// [`ExecutionPlan::name`]: datafusion_physical_plan::ExecutionPlan::name
    pub node: String,
    /// Child indexes from the root to the offending node. Empty for the root.
    pub path: Vec<usize>,
    /// Human readable description of the problem
    pub message: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {} at {} (root",
            self.severity, self.check, self.node
        )?;
        for child in &self.path {
            write!(f, "/{child}")?;
        }
        write!(f, "): {}", self.message)
    }
}

/// The violations found by a checker, such as a [`PlanChecker`], in plan
/// traversal order.
///
/// [`PlanChecker`]: crate::physical_plan::PlanChecker
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub violations: Vec<Violation>,
}

impl Report {
    /// Returns true if the report contains a [`Severity::Invariant`] violation
    pub fn has_invariant_violations(&self) -> bool {
        self.violations
            .iter()
            .any(|v| v.severity == Severity::Invariant)
    }

    /// Panics if the report contains any [`Severity::Invariant`] violation
    pub fn assert_no_invariant_violations(&self) {
        assert!(
            !self.has_invariant_violations(),
            "invariant violations found:\n{self}"
        );
    }

    /// Panics if the report contains any violation, including lints
    pub fn assert_clean(&self) {
        assert!(
            self.violations.is_empty(),
            "check violations found:\n{self}"
        );
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.violations.is_empty() {
            return write!(f, "no violations");
        }
        for (i, violation) in self.violations.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "{violation}")?;
        }
        Ok(())
    }
}
