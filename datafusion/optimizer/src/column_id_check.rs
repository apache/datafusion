// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Reports and checks the column id issues that analyzer and optimizer rules
//! introduce (see [`LogicalPlan::column_id_issues`]).
//!
//! Off by default, and free when off. Three switches turn it on:
//!
//! - logging at `debug` level under the target `datafusion::column_id`: one
//!   line per rule that changes the counts, and the issues of the kinds it
//!   introduced;
//! - the environment variable `DATAFUSION_COLUMN_ID_STATS=<path>`: appends to
//!   `<path>` one JSON line per (rule, issue kind) whose count the rule
//!   changed, `{"rule":"<name>","kind":"<kind>","count":<n>}`, where `n` is
//!   the number of issues after the rule minus the number before (negative
//!   when the rule removed issues). The analyzer is recorded once, after all
//!   its rules, as `analyzer`;
//! - the environment variable `DATAFUSION_COLUMN_ID_VERIFY=1` (always on in
//!   the tests of this crate, and set by the sqllogictest runner): fails the
//!   query with an internal error
//!   - after the analyzer, if the plan has any
//!     [`DuplicateDefinition`](ColumnIdIssueKind::DuplicateDefinition) or
//!     [`Disagree`](ColumnIdIssueKind::Disagree) issue, or more
//!     [`Missing`](ColumnIdIssueKind::Missing),
//!     [`Stale`](ColumnIdIssueKind::Stale) or
//!     [`Misresolved`](ColumnIdIssueKind::Misresolved) references than its
//!     input (which is the output of the planner, or a plan optimized before
//!     for a prepared statement);
//!   - after an optimizer rule, if it introduced issues of a kind it may not
//!     introduce: a rule in [`UNMIGRATED_RULES`] may introduce issues of the
//!     kind listed with it (a `Missing` or `Stale` reference), any other
//!     rule nothing.
//!
//!   A plan without any id (built without a generator) is not checked: all
//!   its references are `Missing`, as before ids. Neither is the output of
//!   the analyzer for such a plan.
//!
//! Only the verification switch fails queries; the others only report.

use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::sync::{LazyLock, Mutex};

use datafusion_common::{Result, internal_err};
use datafusion_expr::{ColumnIdIssueKind, LogicalPlan};
use log::{Level, debug, log_enabled};

/// The rules that may still introduce column id issues, each with the one
/// kind of issue it may introduce. Every other rule may introduce none;
/// remove a rule once it keeps its ids, CI then fails if it regresses.
///
/// The rules listed are migrated: the issues they may introduce come from
/// plans that are wrong without ids (on `main`), which the decorrelation
/// rules build and the unnesting rule of #5492 replaces, and from a node
/// that does not define the columns it reads (MERGE INTO). See the
/// [module documentation](self).
///
/// The analyzer is checked as a whole, after all its rules, and may not
/// introduce any issue.
pub(crate) const UNMIGRATED_RULES: &[(&str, ColumnIdIssueKind)] = &[
    // pushes into a join filter the reference that
    // DecorrelatePredicateSubquery leaves out of scope when it decorrelates
    // a subquery that reads a column two levels up (merge_into.slt; the
    // SELECT form fails on `main` with "No field named t.id"): the
    // reference has the right id, which is stale there
    ("push_down_filter", ColumnIdIssueKind::Stale),
    // copies the subquery of a set comparison, and with it the outer
    // references to the target columns of a MERGE INTO, which have no ids:
    // no node defines them (the target is not an input of the `Dml` node)
    ("rewrite_set_comparison", ColumnIdIssueKind::Missing),
    // a subquery nested in a correlated scalar subquery that reads a column
    // two levels up (an `EXISTS` inside a scalar subquery) ends up reading
    // it as a plain column in a join filter where it is not in scope, as on
    // `main` (explain-only tests in subquery.slt): the reference has the
    // right id, which is stale there
    ("scalar_subquery_to_join", ColumnIdIssueKind::Stale),
];

/// The log target of the reports
const LOG_TARGET: &str = "datafusion::column_id";

/// The environment variable that names the file the statistics go to
const STATS_ENV: &str = "DATAFUSION_COLUMN_ID_STATS";

/// The environment variable that turns verification on (`1` or `true`)
pub const VERIFY_ENV: &str = "DATAFUSION_COLUMN_ID_VERIFY";

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

/// Whether verification is on: always in the tests of this crate, otherwise
/// if [`VERIFY_ENV`] is set to `1` or `true`
static VERIFY: LazyLock<bool> = LazyLock::new(|| {
    cfg!(test)
        || std::env::var(VERIFY_ENV)
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
});

/// The number of issues of each kind, indexed like [`ColumnIdIssueKind::ALL`]
type Counts = [usize; ColumnIdIssueKind::ALL.len()];

/// Whether `rule` may introduce issues of `kind` (see [`UNMIGRATED_RULES`])
fn may_introduce(rule: &str, kind: ColumnIdIssueKind) -> bool {
    UNMIGRATED_RULES.contains(&(rule, kind))
}

/// The kinds of issues that a plan may not have after the analyzer at all
const AFTER_ANALYZER_FORBIDDEN: [ColumnIdIssueKind; 2] = [
    ColumnIdIssueKind::Disagree,
    ColumnIdIssueKind::DuplicateDefinition,
];

/// The kinds of issues that the analyzer may not introduce: a plan may have
/// them after the analyzer only if its input had them (the output of the
/// planner, or a plan optimized before for a prepared statement)
const ANALYZER_MAY_NOT_INTRODUCE: [ColumnIdIssueKind; 3] = [
    ColumnIdIssueKind::Missing,
    ColumnIdIssueKind::Stale,
    ColumnIdIssueKind::Misresolved,
];

/// Compares the column id issues of a plan before and after each rule.
/// Holds nothing and does nothing when no switch is on.
pub(crate) struct ColumnIdCheck {
    /// The counts of the last plan checked, `None` when off
    before: Option<Counts>,
    /// Whether the plan checking started from has ids
    input_has_ids: bool,
}

impl ColumnIdCheck {
    /// Starts checking from `plan`, if a switch is on
    pub(crate) fn new(plan: &LogicalPlan) -> Self {
        let enabled = *VERIFY
            || log_enabled!(target: LOG_TARGET, Level::Debug)
            || STATS_FILE.is_some();
        Self {
            before: enabled.then(|| plan.column_id_issue_counts()),
            input_has_ids: enabled && plan.max_column_id().is_some(),
        }
    }

    /// Records the issues that the analyzer introduced, `plan` being its
    /// output, and returns an error if verification is on and `plan` has
    /// issues it may not have after the analyzer
    pub(crate) fn after_analyzer(&mut self, plan: &LogicalPlan) -> Result<()> {
        let before = match &self.before {
            Some(before) => *before,
            None => return Ok(()),
        };
        let Some(after) = self.record("analyzer", plan) else {
            return Ok(());
        };
        // a plan built without ids gets some where the analyzer defines
        // columns (casting a subquery), and its references are all missing,
        // as before ids
        if !*VERIFY || !self.input_has_ids {
            return Ok(());
        }
        let failed: Vec<_> = ColumnIdIssueKind::ALL
            .into_iter()
            .filter(|kind| {
                if ANALYZER_MAY_NOT_INTRODUCE.contains(kind) {
                    after[*kind as usize] > before[*kind as usize]
                } else {
                    AFTER_ANALYZER_FORBIDDEN.contains(kind) && after[*kind as usize] > 0
                }
            })
            .collect();
        fail_if_any("the analyzer produced", &failed, &after, plan)
    }

    /// Records the issues that `rule` introduced, `plan` being its output,
    /// and returns an error if verification is on and `rule` introduced
    /// issues of a kind it may not introduce (see [`UNMIGRATED_RULES`])
    pub(crate) fn after_rule(&mut self, rule: &str, plan: &LogicalPlan) -> Result<()> {
        let before = match &self.before {
            Some(before) => *before,
            None => return Ok(()),
        };
        let Some(after) = self.record(rule, plan) else {
            return Ok(());
        };
        if !*VERIFY {
            return Ok(());
        }
        let failed: Vec<_> = ColumnIdIssueKind::ALL
            .into_iter()
            .filter(|kind| {
                after[*kind as usize] > before[*kind as usize]
                    && !may_introduce(rule, *kind)
            })
            .collect();
        fail_if_any(
            &format!("optimizer rule '{rule}' introduced"),
            &failed,
            &after,
            plan,
        )
    }

    /// Updates the counts with `plan`, the output of `rule`, and reports the
    /// changes. Returns the new counts, `None` when off.
    fn record(&mut self, rule: &str, plan: &LogicalPlan) -> Option<Counts> {
        let before = self.before.as_mut()?;
        let after = plan.column_id_issue_counts();
        let mut lines = String::new();
        let mut issues = None;
        for kind in ColumnIdIssueKind::ALL {
            let change = after[kind as usize] as i64 - before[kind as usize] as i64;
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
                        .get_or_insert_with(|| plan.column_id_issues())
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
        Some(after)
    }
}

/// The number of issues listed in an error, per kind
const MAX_LISTED_ISSUES: usize = 10;

/// Returns an error listing the issues of the `failed` kinds of `plan`, if
/// any. A plan without ids is never checked: its references are all
/// `Missing`, as before ids.
fn fail_if_any(
    what: &str,
    failed: &[ColumnIdIssueKind],
    counts: &Counts,
    plan: &LogicalPlan,
) -> Result<()> {
    if failed.is_empty() || plan.max_column_id().is_none() {
        return Ok(());
    }
    let issues = plan.column_id_issues();
    let mut message = String::new();
    for kind in failed {
        let _ = write!(
            message,
            "\n{} {kind} column id issue(s) in the plan:",
            counts[*kind as usize]
        );
        for issue in issues
            .iter()
            .filter(|issue| issue.kind == *kind)
            .take(MAX_LISTED_ISSUES)
        {
            let _ = write!(message, "\n  {issue}");
        }
    }
    internal_err!(
        "Column id verification ({VERIFY_ENV}) failed: {what} column id issues of kinds {}:{message}",
        failed
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{Column, ColumnId, DFSchema};
    use datafusion_expr::logical_plan::builder::table_scan;
    use datafusion_expr::{Expr, Filter, lit};

    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn id(n: u64) -> ColumnId {
        ColumnId::new(std::num::NonZeroU64::new(n).unwrap())
    }

    /// A scan of `t` with an Int32 column `a` with id #1 (if `with_id`)
    fn scan(with_id: bool) -> Result<LogicalPlan> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let LogicalPlan::TableScan(mut scan) =
            table_scan(Some("t"), &schema, None)?.build()?
        else {
            unreachable!()
        };
        // the builder allocates ids: set them
        // the ids are set by hand: the scan defines its projected columns
        scan.source_column_ids = None;
        scan.projected_schema = Arc::new(
            DFSchema::clone(&scan.projected_schema)
                .with_field_ids(vec![with_id.then(|| id(1))])?,
        );
        Ok(LogicalPlan::TableScan(scan))
    }

    /// A filter `t.a = 1` over `input`, where `t.a` has the id `n`
    fn filter(input: LogicalPlan, n: Option<u64>) -> Result<LogicalPlan> {
        let column = Expr::Column(Column::new(Some("t"), "a").with_id(n.map(id)));
        Ok(LogicalPlan::Filter(Filter::try_new(
            column.eq(lit(1)),
            Arc::new(input),
        )?))
    }

    #[test]
    fn on_in_the_tests_of_this_crate() {
        assert!(*VERIFY);
        let plan = scan(false).unwrap();
        assert!(ColumnIdCheck::new(&plan).before.is_some());
    }

    #[test]
    fn listed_rules_may_lose_ids() -> Result<()> {
        let plan = scan(true)?;
        let mut check = ColumnIdCheck::new(&plan);
        // a listed rule adds a stale reference (`t.a#9` resolves by name to
        // a field without an id), the kind listed with it
        check.after_rule("scalar_subquery_to_join", &filter(scan(false)?, Some(9))?)?;
        // but not a missing one
        let err = check
            .after_rule(
                "scalar_subquery_to_join",
                &filter(filter(scan(false)?, Some(9))?, None)?,
            )
            .unwrap_err();
        assert!(err.to_string().contains("kinds missing"), "{err}");

        // another rule may not
        let mut check = ColumnIdCheck::new(&plan);
        let err = check
            .after_rule("my_rule", &filter(scan(true)?, None)?)
            .unwrap_err();
        assert!(
            err.to_string().contains(
                "optimizer rule 'my_rule' introduced column id issues of kinds missing"
            ),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn no_rule_may_disagree() -> Result<()> {
        // the reference finds #1 by id, but its name resolves to `t.b`
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let LogicalPlan::TableScan(mut table_scan) =
            table_scan(Some("t"), &schema, None)?.build()?
        else {
            unreachable!()
        };
        // the ids are set by hand: the scan defines its projected columns
        table_scan.source_column_ids = None;
        table_scan.projected_schema = Arc::new(
            DFSchema::clone(&table_scan.projected_schema)
                .with_field_ids(vec![Some(id(1)), Some(id(2))])?,
        );
        let input = LogicalPlan::TableScan(table_scan);
        let mut check = ColumnIdCheck::new(&input);
        let column = Expr::Column(Column::new(Some("t"), "b").with_id(Some(id(1))));
        let a = Expr::Column(Column::new(Some("t"), "a").with_id(Some(id(1))));
        let plan = Filter::try_new(a.eq(column), Arc::new(input))?;
        let err = check
            .after_rule("scalar_subquery_to_join", &LogicalPlan::Filter(plan))
            .unwrap_err();
        assert!(err.to_string().contains("kinds disagree"), "{err}");
        Ok(())
    }

    #[test]
    fn analyzer_may_not_introduce_issues() -> Result<()> {
        let plan = scan(true)?;
        let mut check = ColumnIdCheck::new(&plan);
        // `t.a#9` is not in the input, and `t.a` resolves to `t.a#1`
        let err = check
            .after_analyzer(&filter(scan(true)?, Some(9))?)
            .unwrap_err();
        assert!(err.to_string().contains("misresolved: t.a#9"), "{err}");

        // nor missing references
        let mut check = ColumnIdCheck::new(&plan);
        let err = check
            .after_analyzer(&filter(scan(true)?, None)?)
            .unwrap_err();
        assert!(err.to_string().contains("missing: t.a"), "{err}");

        // unless its input had them
        let input = filter(scan(true)?, None)?;
        let mut check = ColumnIdCheck::new(&input);
        check.after_analyzer(&filter(scan(true)?, None)?)
    }

    #[test]
    fn plans_without_ids_are_not_checked() -> Result<()> {
        let plan = scan(false)?;
        let mut check = ColumnIdCheck::new(&plan);
        check.after_rule("my_rule", &filter(scan(false)?, None)?)
    }

    #[test]
    fn unmigrated_rules_exist() {
        let optimizer = crate::Optimizer::new();
        let analyzer = crate::Analyzer::new();
        let names: Vec<&str> = optimizer
            .rules
            .iter()
            .map(|rule| rule.name())
            .chain(analyzer.rules.iter().map(|rule| rule.name()))
            .collect();
        for (rule, _) in UNMIGRATED_RULES {
            assert!(names.contains(rule), "{rule} is not a rule");
        }
    }
}
