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

//! `CheckKind::Variant` checks: they compare the normal output of a node with
//! the output of rewritten copies of it, which must produce what the node
//! promises, and with its output under settings and input layouts that should
//! not change it.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

use super::{
    moves_rows_between_partitions, partition_count, passes_rows_through,
    reports_sorted_output,
};
use crate::Finding;
use crate::physical_plan::{
    CheckContext, NodeOutput, RunError, Variant, VariantRun, oracle,
};

/// What the output of a node must satisfy when at most `limit` rows per
/// partition are wanted from it: the output of a node with a fetch, or of a
/// node whose inputs were limited.
///
/// A fetch does not have to keep exactly `limit` rows per partition: the
/// partitions of a TopK `SortExec` share one threshold and can keep fewer rows,
/// and the `LimitPushdown` rule only relies on the total.
struct FetchExpectation<'a> {
    /// Largest number of rows allowed in each partition
    limit: usize,
    /// The whole output must have at least `min(limit, available_rows)` rows
    available_rows: usize,
    /// The output without the limit, if known. Every row must appear in it,
    /// and where `ordering` is set, the output must keep its first rows (see
    /// [`order_problem`]).
    unlimited: Option<&'a NodeOutput>,
    /// The ordering the node reports, if any
    ordering: Option<&'a LexOrdering>,
}

impl FetchExpectation<'_> {
    /// The problems of `output`. Comparisons that cannot be computed, for
    /// example because the output does not match the schema (reported by
    /// `batch_schema`), are skipped.
    fn problems(&self, output: &NodeOutput) -> Vec<String> {
        let mut problems = vec![];
        let rows = output.partition_num_rows();
        if let Some((p, n)) = rows.iter().enumerate().find(|(_, n)| **n > self.limit) {
            problems.push(format!(
                "partition {p} has {n} rows, more than the limit of {}",
                self.limit
            ));
        }
        let total = output.num_rows();
        let min_rows = self.limit.min(self.available_rows);
        if total < min_rows {
            problems.push(format!(
                "the output has {total} rows, but limiting {} rows to {} gives {min_rows}",
                self.available_rows, self.limit
            ));
        }
        let Some(unlimited) = self.unlimited else {
            return problems;
        };
        if let Ok(unmatched) =
            oracle::unmatched_rows(&output.batches(), &unlimited.batches())
            && unmatched > 0
        {
            problems.push(format!(
                "{unmatched} of its {total} rows do not appear in the output without the \
                 limit"
            ));
        }
        if let Some(ordering) = self.ordering {
            problems.extend(order_problem(output, unlimited, self.limit, ordering));
        }
        problems
    }
}

/// Whether `output` keeps the first rows of `unlimited` in sort order.
///
/// With one partition, the output must be a prefix of the unlimited output,
/// apart from rows that tie on the ordering. With several, each partition must
/// be sorted, and the partitions together must contain the first
/// `min(limit, rows)` rows of the whole unlimited output in the same sense,
/// which is what a `SortPreservingMergeExec` with the same fetch above them
/// needs. Each partition does not have to be a prefix of its own partition:
/// the partitions of a TopK `SortExec` share a threshold and reject rows that
/// tie with it, so a partition can keep rows it saw before another partition
/// set the threshold instead of its own first rows.
fn order_problem(
    output: &NodeOutput,
    unlimited: &NodeOutput,
    limit: usize,
    ordering: &LexOrdering,
) -> Option<String> {
    if let ([batches], [unlimited]) = (output.partitions(), unlimited.partitions()) {
        let row = oracle::first_non_prefix_row(unlimited, batches, ordering).ok()??;
        return Some(format!(
            "the output is not a prefix of the output without the limit sorted by \
             [{ordering}], apart from the order of rows that tie on it, from row {row}"
        ));
    }
    for (p, batches) in output.partitions().iter().enumerate() {
        if let Ok(Some(row)) = oracle::first_unsorted_row(batches, ordering) {
            return Some(format!(
                "partition {p} is not sorted by [{ordering}] at row {row}"
            ));
        }
    }
    // The first `n` rows of all partitions of `output`, in sort order
    let sorted_first_rows = |output: &NodeOutput, n: usize| {
        let sorted = oracle::sort_rows(&output.batches(), ordering)?;
        Ok::<_, datafusion_common::DataFusionError>(
            first_rows(&NodeOutput::new(vec![sorted]), n).batches(),
        )
    };
    let n = limit.min(unlimited.num_rows());
    let expected = sorted_first_rows(unlimited, unlimited.num_rows()).ok()?;
    let kept = sorted_first_rows(output, n).ok()?;
    let row = oracle::first_non_prefix_row(&expected, &kept, ordering).ok()??;
    Some(format!(
        "the first {n} rows of all partitions together, in sort order, are not the \
         first rows of the output without the limit sorted by [{ordering}], apart from \
         the order of rows that tie on it, from row {row}"
    ))
}

/// The first `n` rows of each partition of `output`
fn first_rows(output: &NodeOutput, n: usize) -> NodeOutput {
    let partitions = output
        .partitions()
        .iter()
        .map(|batches| {
            let mut remaining = n;
            let mut kept = vec![];
            for batch in batches {
                if remaining == 0 {
                    break;
                }
                let rows = batch.num_rows().min(remaining);
                kept.push(batch.slice(0, rows));
                remaining -= rows;
            }
            kept
        })
        .collect();
    NodeOutput::new(partitions)
}

/// D11: a node that does not support limit pushdown, but produces the first
/// rows of each output partition from the first rows of its inputs.
pub(super) fn limit_pushdown_missed_at_runtime(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    if node.supports_limit_pushdown() || node.children().is_empty() {
        return Ok(vec![]);
    }
    // Reported by `limit_pushdown_missed` from the node's properties
    if passes_rows_through(node.as_ref()) {
        return Ok(vec![]);
    }
    // Only a node that produces a row for each input row, without its fetch,
    // can produce its first rows from the first rows of its inputs whatever
    // their values. A node that can drop or merge rows, or a join, which
    // combines rows of its inputs, needs later rows in general, and only looks
    // prefix-closed when the generated data does not show it: a final
    // aggregate whose input has each group once, or a mark join whose first
    // input rows already have every key.
    let effect = node.with_fetch(None).map_or_else(
        || node.cardinality_effect(),
        |plan| plan.cardinality_effect(),
    );
    if !matches!(
        effect,
        CardinalityEffect::Equal | CardinalityEffect::GreaterEqual
    ) {
        return Ok(vec![]);
    }
    // Input that is sorted by an ordering the node does not maintain, or has a
    // constant, can be in the order the node sorts it into: a sort passes such
    // input through, as if it only needed its first rows
    let maintains = node.maintains_input_order();
    let sorted_input = node.children().iter().enumerate().any(|(i, child)| {
        maintains.get(i) != Some(&true) && reports_sorted_output(child.as_ref())
    });
    if sorted_input {
        return Ok(vec![]);
    }
    // A node that places rows in partitions by their values can leave the rows
    // of generated input where they are, as if it only needed the first rows
    // of its input
    if (0..node.children().len()).any(|i| moves_rows_between_partitions(node.as_ref(), i))
    {
        return Ok(vec![]);
    }
    let Some(normal) = context.output(node) else {
        return Ok(vec![]);
    };
    let ordering = node.properties().output_ordering();
    let mut limits = vec![];
    let mut limited = false;
    for run in context.variant_runs(node) {
        let Variant::LimitedInputs(n) = run.variant else {
            continue;
        };
        // A run that failed or produced other rows shows that the node needs
        // more than the first rows of its inputs
        let Ok(output) = &run.output else {
            return Ok(vec![]);
        };
        if !keeps_first_rows(normal, output, n, ordering) {
            return Ok(vec![]);
        }
        limited |= output.num_rows() < normal.num_rows();
        limits.push(n);
    }
    // Only runs in which the limits removed output rows show anything
    if !limited {
        return Ok(vec![]);
    }
    Ok(vec![Finding::lint(format!(
        "supports_limit_pushdown() is false, but with every child limited to its first \
         n rows per partition, for n in {limits:?}, every output partition has the \
         first n rows of the same partition of the normal output, so a limit above the \
         node could be pushed to its children; return true from \
         supports_limit_pushdown if this holds for every input"
    ))])
}

/// Returns true if every partition of `output` has the first `n` rows of the
/// same partition of `normal`, in the same order, apart from the order of rows
/// that tie on `ordering`
fn keeps_first_rows(
    normal: &NodeOutput,
    output: &NodeOutput,
    n: usize,
    ordering: Option<&LexOrdering>,
) -> bool {
    let first = first_rows(normal, n);
    output.partitions().len() == normal.partitions().len()
        && output.partition_num_rows() == first.partition_num_rows()
        && output
            .partitions()
            .iter()
            .zip(normal.partitions().iter().zip(first.partitions()))
            .all(|(output, (normal, first))| match ordering {
                Some(ordering) => matches!(
                    oracle::first_non_prefix_row(normal, output, ordering),
                    Ok(None)
                ),
                None => matches!(oracle::same_rows_in_order(output, first), Ok(true)),
            })
}

/// D1: the plan returned by `with_fetch` produces a valid limit of the node's
/// output, and the node's own fetch does too.
pub(super) fn with_fetch_equivalent(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let runs: Vec<_> = context
        .variant_runs(node)
        .iter()
        .filter(|run| {
            matches!(run.variant, Variant::WithoutFetch | Variant::WithFetch(_))
        })
        .collect();
    let mut findings: Vec<Finding> = runs
        .iter()
        .flat_map(|run| plan_problems(node, run.variant))
        .collect();
    // A failing node is reported by `execution_succeeds`
    let Some(normal) = context.output(node) else {
        return Ok(findings);
    };
    let unfetched = context.unfetched_output(node);
    let expectation = |limit| match unfetched {
        Some(unfetched) => FetchExpectation {
            limit,
            available_rows: unfetched.num_rows(),
            unlimited: Some(unfetched),
            ordering: node.properties().output_ordering(),
        },
        // A node with a fetch whose `with_fetch(None)` returns `None`. Its
        // output without the fetch has at least as many rows as its normal
        // output, but which rows it has is unknown.
        None => FetchExpectation {
            limit,
            available_rows: normal.num_rows(),
            unlimited: None,
            ordering: None,
        },
    };

    // The normal output of a node with a fetch is the output of that fetch
    if let Some(fetch) = node.fetch()
        && !runs.is_empty()
    {
        for problem in expectation(fetch).problems(normal) {
            findings.push(Finding::invariant(format!(
                "the node's own fetch of {fetch}: {problem}"
            )));
        }
    }

    for run in runs {
        let output = match &run.output {
            Ok(output) => output,
            Err(error) => {
                findings.push(Finding::invariant(format!(
                    "{}: executing the plan failed: {error}",
                    run.variant
                )));
                continue;
            }
        };
        match run.variant {
            Variant::WithoutFetch => {
                let same = oracle::same_rows(&output.batches(), &normal.batches());
                if node.fetch().is_none() && matches!(same, Ok(false)) {
                    findings.push(Finding::invariant(format!(
                        "{}: the node has no fetch, but the plan produced {} rows that \
                         differ from the {} rows of the normal output",
                        run.variant,
                        output.num_rows(),
                        normal.num_rows()
                    )));
                }
            }
            Variant::WithFetch(n) => {
                for problem in expectation(n).problems(output) {
                    findings
                        .push(Finding::invariant(format!("{}: {problem}", run.variant)));
                }
            }
            _ => {}
        }
    }
    Ok(findings)
}

/// Problems with what the plan that `variant` executes reports about itself,
/// prefixed with the variant
fn plan_problems(node: &Arc<dyn ExecutionPlan>, variant: Variant) -> Vec<Finding> {
    let (plan, fetch) = match variant {
        Variant::WithoutFetch => (node.with_fetch(None), None),
        Variant::WithFetch(n) => (node.with_fetch(Some(n)), Some(n)),
        _ => return vec![],
    };
    let Some(plan) = plan else {
        return vec![];
    };
    let mut problems = vec![];
    if plan.fetch() != fetch {
        problems.push(Finding::invariant(format!(
            "the plan reports fetch() {:?} instead of {fetch:?}",
            plan.fetch()
        )));
    }
    if plan.schema() != node.schema() {
        let fields = |plan: &Arc<dyn ExecutionPlan>| {
            plan.schema()
                .fields()
                .iter()
                .map(|f| format!("{}: {}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
                .join(", ")
        };
        problems.push(Finding::invariant(format!(
            "the plan has the schema [{}], but the node has [{}]",
            fields(&plan),
            fields(node)
        )));
    }
    let (partitions, expected) = (
        partition_count(plan.as_ref()),
        partition_count(node.as_ref()),
    );
    if partitions != expected {
        problems.push(Finding::invariant(format!(
            "the plan has {partitions} output partitions, but the node has {expected}"
        )));
    }
    let eq_properties = plan.properties().equivalence_properties();
    for ordering in node
        .properties()
        .equivalence_properties()
        .oeq_class()
        .iter()
    {
        if matches!(eq_properties.ordering_satisfy(ordering.clone()), Ok(false)) {
            problems.push(Finding::lint(format!(
                "the plan does not report the ordering [{ordering}] that the node \
                 reports"
            )));
        }
    }
    problems
        .into_iter()
        .map(|finding| Finding {
            message: format!("{variant}: {}", finding.message),
            ..finding
        })
        .collect()
}

/// D2: a node that supports limit pushdown produces a valid limit of its
/// output from inputs limited the way `LimitPushdown` limits them.
pub(super) fn limit_pushdown_equivalent(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    if !node.supports_limit_pushdown() || node.children().is_empty() {
        return Ok(vec![]);
    }
    // `LimitPushdown` merges limit nodes into the limit it pushes down before
    // it asks any node whether it supports limit pushdown, so it never relies
    // on their answer
    if node.is::<GlobalLimitExec>() || node.is::<LocalLimitExec>() {
        return Ok(vec![]);
    }
    let Some(normal) = context.output(node) else {
        return Ok(vec![]);
    };
    // For these nodes `LimitPushdown` does not remove the limit above them: it
    // gives them a fetch, or keeps the limit, so only their first `n` rows
    // count. It removes the limit above any other node that supports limit
    // pushdown and limits each child instead.
    let combines_partitions =
        node.is::<CoalescePartitionsExec>() || node.is::<SortPreservingMergeExec>();

    let mut findings = vec![];
    for run in context.variant_runs(node) {
        let Variant::LimitedInputs(n) = run.variant else {
            continue;
        };
        let output = match &run.output {
            Ok(output) => output,
            Err(error) => {
                findings.push(Finding::invariant(format!(
                    "with {}: executing the node failed: {error}",
                    run.variant
                )));
                continue;
            }
        };
        let candidate = if combines_partitions {
            first_rows(output, n)
        } else {
            output.clone()
        };
        let expectation = FetchExpectation {
            limit: n,
            available_rows: normal.num_rows(),
            unlimited: context.unfetched_output(node),
            ordering: node.properties().output_ordering(),
        };
        for problem in expectation.problems(&candidate) {
            findings.push(Finding::invariant(format!(
                "with {}: {problem}, so a limit above the node cannot be pushed to its \
                 children; return false from supports_limit_pushdown",
                run.variant
            )));
        }
    }
    Ok(findings)
}

/// D9: executing the node again after `reset_plan_states` gives the same
/// results, and executing it again without a reset gives the same results or
/// an error.
pub(super) fn reset_state_reexecution(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let mut findings =
        check_invariance(node, context, |variant| matches!(variant, Variant::Reset));
    // A failing node is reported by `execution_succeeds`
    let Some(normal) = context.output(node) else {
        return Ok(findings);
    };
    let Some(run) = context.variant_run(node, Variant::Rerun) else {
        return Ok(findings);
    };
    if !inputs_unchanged(node, context, Variant::Rerun) {
        return Ok(findings);
    }
    match &run.output {
        Ok(output) => {
            for difference in differences(node, context, normal, output) {
                findings.push(Finding::invariant(format!(
                    "with {}: {difference}; a plan executed again without \
                     reset_state must produce the same rows or return an error",
                    run.variant
                )));
            }
        }
        // Executing a plan a second time without a reset may fail
        Err(RunError::Failed(_)) => {}
        Err(error @ (RunError::Panicked(_) | RunError::TimedOut(_))) => {
            findings.push(Finding::invariant(format!(
                "with {}: executing the node {error}; return an error instead",
                run.variant
            )));
        }
    }
    Ok(findings)
}

/// E1: results do not depend on the session batch size.
pub(super) fn batch_size_invariance(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    Ok(check_invariance(node, context, |variant| {
        matches!(variant, Variant::BatchSize(_))
    }))
}

/// E2: results do not depend on how input rows are split into batches.
pub(super) fn batch_boundary_invariance(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    Ok(check_invariance(node, context, |variant| {
        matches!(variant, Variant::BatchLayout(_))
    }))
}

/// Compare the output of `node` under each variant for which `compared`
/// returns true with its normal output
fn check_invariance(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
    compared: fn(&Variant) -> bool,
) -> Vec<Finding> {
    // A failing node is reported by `execution_succeeds`
    let Some(normal) = context.output(node) else {
        return vec![];
    };
    let mut findings = vec![];
    for run in context.variant_runs(node) {
        if !compared(&run.variant) || !inputs_unchanged(node, context, run.variant) {
            continue;
        }
        match &run.output {
            Ok(output) => {
                for difference in differences(node, context, normal, output) {
                    findings.push(Finding::invariant(format!(
                        "with {}: {difference}",
                        run.variant
                    )));
                }
            }
            Err(error) => findings.push(Finding::invariant(format!(
                "with {}: executing the node failed: {error}",
                run.variant
            ))),
        }
    }
    findings
}

/// Returns true if every child of `node` produced the same rows, in the same
/// order and partitions, under `variant` as normally. The node then saw the
/// same input, only split into batches differently.
///
/// Batch sizes and leaf layouts apply to the whole subtree, so a child can
/// produce different results under a variant, either because it is broken
/// (reported on the child) or legitimately, for example rows that tie on a
/// sort key in another order, or rows interleaved differently by a
/// repartition. A node whose output depends on the order of its input rows
/// can then legitimately differ too, so it is not compared under that variant.
fn inputs_unchanged(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
    variant: Variant,
) -> bool {
    node.children().into_iter().all(|child| {
        let Some(normal) = context.output(child) else {
            return false;
        };
        match context.variant_run(child, variant) {
            // The variant does not apply to the child, which then runs as
            // it does normally, such as a child with no `MockSourceExec` leaf
            // to split differently
            None => true,
            Some(VariantRun {
                output: Ok(output), ..
            }) => {
                output.partitions().len() == normal.partitions().len()
                    && output.partitions().iter().zip(normal.partitions()).all(
                        |(a, b)| matches!(oracle::same_rows_in_order(a, b), Ok(true)),
                    )
            }
            Some(_) => false,
        }
    })
}

/// How the output of `node` under a variant differs from its normal output
/// in ways the variant should not change
fn differences(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
    normal: &NodeOutput,
    output: &NodeOutput,
) -> Vec<String> {
    let mut differences = vec![];
    if let Some(fetch) = node.fetch() {
        // Which rows a fetch keeps can depend on timing and batch boundaries,
        // such as how the partitions below a `CoalescePartitionsExec`
        // interleave, so only compare what the fetch guarantees. With one
        // output partition this includes the exact number of rows.
        let unfetched = context.unfetched_output(node);
        let expectation = FetchExpectation {
            limit: fetch,
            available_rows: unfetched.unwrap_or(normal).num_rows(),
            unlimited: unfetched,
            ordering: node.properties().output_ordering(),
        };
        differences.extend(expectation.problems(output));
    } else if matches!(
        oracle::same_rows(&output.batches(), &normal.batches()),
        Ok(false)
    ) {
        let unmatched =
            oracle::unmatched_rows(&output.batches(), &normal.batches()).unwrap_or(0);
        differences.push(format!(
            "the node produced {} rows, {unmatched} of which do not appear in the \
             normal output, instead of the {} rows it produces normally",
            output.num_rows(),
            normal.num_rows()
        ));
    }
    for ordering in node
        .properties()
        .equivalence_properties()
        .oeq_class()
        .iter()
    {
        for (p, batches) in output.partitions().iter().enumerate() {
            // An ordering that does not hold normally is reported by
            // `orderings_hold`
            let sorted_normally = normal.partitions().get(p).is_some_and(|normal| {
                matches!(oracle::first_unsorted_row(normal, ordering), Ok(None))
            });
            if sorted_normally
                && let Ok(Some(row)) = oracle::first_unsorted_row(batches, ordering)
            {
                differences.push(format!(
                    "partition {p} is not sorted by [{ordering}] at row {row}, although \
                     the node reports that ordering and its normal output is sorted by it"
                ));
                break;
            }
        }
    }
    differences
}
