// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Reports the column id issues that analyzer and optimizer rules introduce
//! (see [`LogicalPlan::column_id_issues`]).
//!
//! Off by default, and free when off. Two switches turn it on:
//!
//! - logging at `debug` level under the target `datafusion::column_id`: one
//!   line per rule that changes the counts, and the issues of the kinds it
//!   introduced;
//! - the environment variable `DATAFUSION_COLUMN_ID_STATS=<path>`: appends to
//!   `<path>` one JSON line per (rule, issue kind) whose count the rule
//!   changed, `{"rule":"<name>","kind":"<kind>","count":<n>}`, where `n` is
//!   the number of issues after the rule minus the number before (negative
//!   when the rule removed issues). The analyzer is recorded once, after all
//!   its rules, as `analyzer`.
//!
//! It never fails a query.

use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::sync::{LazyLock, Mutex};

use datafusion_expr::{ColumnIdIssue, ColumnIdIssueKind, LogicalPlan};
use log::{Level, debug, log_enabled};

/// The log target of the reports
const LOG_TARGET: &str = "datafusion::column_id";

/// The environment variable that names the file the statistics go to
const STATS_ENV: &str = "DATAFUSION_COLUMN_ID_STATS";

/// The file named by [`STATS_ENV`], opened for appending, if set
static STATS_FILE: LazyLock<Option<Mutex<File>>> = LazyLock::new(|| {
    let path = std::env::var_os(STATS_ENV)?;
    match File::options().create(true).append(true).open(&path) {
        Ok(file) => Some(Mutex::new(file)),
        Err(e) => {
            log::warn!("Cannot open {STATS_ENV}={}: {e}", path.display());
            None
        }
    }
});

/// The number of issues of each kind, indexed like [`ColumnIdIssueKind::ALL`]
type Counts = [i64; ColumnIdIssueKind::ALL.len()];

fn counts(issues: &[ColumnIdIssue]) -> Counts {
    let mut counts = Counts::default();
    for issue in issues {
        counts[issue.kind as usize] += 1;
    }
    counts
}

/// Compares the column id issues of a plan before and after each rule.
/// Holds nothing and does nothing when no switch is on.
pub(crate) struct ColumnIdCheck {
    /// The counts of the last plan checked, `None` when off
    before: Option<Counts>,
}

impl ColumnIdCheck {
    /// Starts checking from `plan`, if a switch is on
    pub(crate) fn new(plan: &LogicalPlan) -> Self {
        let enabled =
            log_enabled!(target: LOG_TARGET, Level::Debug) || STATS_FILE.is_some();
        Self {
            before: enabled.then(|| counts(&plan.column_id_issues())),
        }
    }

    /// Records the issues that `rule` introduced, `plan` being its output
    pub(crate) fn after_rule(&mut self, rule: &str, plan: &LogicalPlan) {
        let Some(before) = &mut self.before else {
            return;
        };
        let issues = plan.column_id_issues();
        let after = counts(&issues);
        let mut lines = String::new();
        for kind in ColumnIdIssueKind::ALL {
            let change = after[kind as usize] - before[kind as usize];
            if change == 0 {
                continue;
            }
            // writing to a `String` cannot fail
            let _ = writeln!(
                lines,
                "{{\"rule\":\"{}\",\"kind\":\"{kind}\",\"count\":{change}}}",
                rule.replace('\\', "\\\\").replace('"', "\\\"")
            );
            if log_enabled!(target: LOG_TARGET, Level::Debug) {
                debug!(target: LOG_TARGET, "{rule}: {change:+} {kind} column id issues");
                if change > 0 {
                    issues
                        .iter()
                        .filter(|issue| issue.kind == kind)
                        .for_each(|issue| debug!(target: LOG_TARGET, "  {issue}"));
                }
            }
        }
        if !lines.is_empty()
            && let Some(file) = STATS_FILE.as_ref()
        {
            // one write per rule, so that concurrent queries do not interleave
            // lines; a poisoned lock or a failed write only loses statistics
            if let Ok(mut file) = file.lock() {
                let _ = file.write_all(lines.as_bytes());
            }
        }
        *before = after;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::logical_plan::builder::table_scan;
    use datafusion_expr::{col, lit};

    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn off_by_default() -> datafusion_common::Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let plan = table_scan(Some("t"), &schema, None)?
            .filter(col("a").eq(lit(1)))?
            .build()?;
        if std::env::var_os(STATS_ENV).is_none()
            && !log_enabled!(target: LOG_TARGET, Level::Debug)
        {
            assert!(ColumnIdCheck::new(&plan).before.is_none());
        }
        Ok(())
    }

    #[test]
    fn counts_by_kind() {
        let issue = |kind| ColumnIdIssue {
            kind,
            column: datafusion_common::Column::from_name("a"),
            node: String::new(),
        };
        let issues = vec![
            issue(ColumnIdIssueKind::Missing),
            issue(ColumnIdIssueKind::Missing),
            issue(ColumnIdIssueKind::DuplicateDefinition),
        ];
        assert_eq!(counts(&issues), [2, 0, 0, 0, 1]);
    }
}
