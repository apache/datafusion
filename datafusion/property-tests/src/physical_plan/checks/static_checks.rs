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

//! `CheckKind::Static` checks: they compare the statistics, cardinality effect,
//! fetch, limit pushdown support, per-child metadata, schema, expressions and
//! dynamic filters a node reports with each other and with what its children
//! report, without executing it.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, Statistics};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateInputMode};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::{ChildSatisfactionOptions, ChildStats, ExecutionPlan};

use super::{
    overall_statistics, partition_count, partition_statistics, passes_rows_through,
};
use crate::Finding;
use crate::physical_plan::CheckContext;

/// The input rows that one output partition of a node is made of, which its
/// statistics describe
#[derive(Debug, Clone, Copy)]
enum InputRows {
    /// Every row of the input, for a node with one output partition
    All,
    /// The rows of one partition of the input: the partition whose
    /// statistics the node computes the output partition's statistics from
    Partition(usize),
}

impl InputRows {
    /// The statistics of these rows of `child`
    fn statistics(self, child: &dyn ExecutionPlan) -> Result<Arc<Statistics>> {
        match self {
            InputRows::All => overall_statistics(child),
            InputRows::Partition(p) => partition_statistics(child, p),
        }
    }

    /// These rows, of input `child` if the node has several inputs
    fn describe(self, child: Option<usize>) -> String {
        match (self, child) {
            (InputRows::All, None) => "the input".to_string(),
            (InputRows::All, Some(i)) => format!("input {i}"),
            (InputRows::Partition(p), None) => format!("input partition {p}"),
            (InputRows::Partition(p), Some(i)) => format!("partition {p} of input {i}"),
        }
    }
}

/// The rows of each child that output partition `partition` of `node` is made
/// of, where known: every row of each child if the node has one output
/// partition, and otherwise the child partition that `child_stats_requests`
/// requests for the partition. A node that requests the overall statistics of
/// a child, such as a repartition, can spread its rows over every partition.
fn partition_inputs(
    node: &dyn ExecutionPlan,
    partition: usize,
) -> Vec<Option<InputRows>> {
    if partition_count(node) == 1 {
        return vec![Some(InputRows::All); node.children().len()];
    }
    node.child_stats_requests(Some(partition))
        .into_iter()
        .map(|request| match request {
            ChildStats::At(Some(p)) => Some(InputRows::Partition(p)),
            ChildStats::At(None) | ChildStats::Skip => None,
        })
        .collect()
}

/// A1: a single-child node with `CardinalityEffect::Equal` and no fetch reports
/// the same `num_rows` as its input, with the same precision, overall and for
/// each partition whose input rows are known.
pub(super) fn equal_cardinality_num_rows(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    if !matches!(node.cardinality_effect(), CardinalityEffect::Equal)
        || node.fetch().is_some()
    {
        return Ok(vec![]);
    }
    let children = node.children();
    let [child] = children.as_slice() else {
        return Ok(vec![]);
    };
    let mut findings = vec![];
    // Errors are reported by `statistics_shape`
    if let (Ok(input), Ok(output)) = (
        overall_statistics(child.as_ref()),
        overall_statistics(node.as_ref()),
    ) {
        findings.extend(equal_row_counts(
            input.num_rows,
            output.num_rows,
            "overall",
            "the input",
        ));
    }
    for partition in 0..partition_count(node.as_ref()) {
        let [Some(rows)] = partition_inputs(node.as_ref(), partition)[..] else {
            continue;
        };
        if let (Ok(input), Ok(output)) = (
            rows.statistics(child.as_ref()),
            partition_statistics(node.as_ref(), partition),
        ) {
            findings.extend(equal_row_counts(
                input.num_rows,
                output.num_rows,
                &format!("partition {partition}"),
                &rows.describe(None),
            ));
        }
    }
    Ok(findings)
}

/// How the `num_rows` that an `Equal` node reports for `target` disagrees with
/// the `num_rows` of `inputs`, the input rows it is made of
fn equal_row_counts(
    input_rows: Precision<usize>,
    output_rows: Precision<usize>,
    target: &str,
    inputs: &str,
) -> Option<Finding> {
    match (input_rows, output_rows) {
        (Precision::Exact(n), Precision::Exact(m)) if n != m => {
            Some(Finding::invariant(format!(
                "cardinality_effect() is Equal, but the {target} num_rows is \
                 {output_rows} for {inputs} with num_rows {input_rows}"
            )))
        }
        (Precision::Inexact(_) | Precision::Absent, Precision::Exact(_)) => {
            Some(Finding::invariant(format!(
                "cardinality_effect() is Equal, but the {target} num_rows is \
                 {output_rows} for {inputs} with num_rows {input_rows}; an Equal node \
                 cannot know its row count more precisely than its input does"
            )))
        }
        (Precision::Exact(_), Precision::Inexact(_)) => Some(Finding::lint(format!(
            "cardinality_effect() is Equal and {inputs} has num_rows {input_rows}, but \
             the {target} num_rows is {output_rows}; it could be {input_rows}"
        ))),
        (Precision::Inexact(n), Precision::Inexact(m)) if n != m => {
            Some(Finding::lint(format!(
                "cardinality_effect() is Equal, but the {target} num_rows estimate \
                 {output_rows} differs from the estimate {input_rows} of {inputs}"
            )))
        }
        // Absent output for a known input is reported by
        // `statistics_ignore_inputs`
        _ => None,
    }
}

/// A2: a node with a fetch does not claim `CardinalityEffect::Equal`.
pub(super) fn fetch_not_equal_cardinality(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    match (node.fetch(), node.cardinality_effect()) {
        (Some(fetch), CardinalityEffect::Equal) => Ok(vec![Finding::invariant(format!(
            "fetch() is Some({fetch}), but cardinality_effect() is Equal; a fetch \
             can drop input rows, so the effect should be LowerEqual while a fetch \
             is set"
        ))]),
        _ => Ok(vec![]),
    }
}

/// A3: a node with `fetch() == Some(n)` reports at most `n` rows per partition,
/// and at most `n * partition_count` rows overall.
pub(super) fn fetch_bounds_num_rows(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(fetch) = node.fetch() else {
        return Ok(vec![]);
    };
    let check_bound = |target: &str, num_rows: Precision<usize>, bound: usize| {
        match num_rows {
            Precision::Exact(n) if n > bound => Some(Finding::invariant(format!(
                "fetch() is Some({fetch}), but {target} num_rows is {num_rows}, which is \
             more than the fetch allows ({bound})"
            ))),
            Precision::Inexact(n) if n > bound => Some(Finding::lint(format!(
                "fetch() is Some({fetch}), but the {target} num_rows estimate {num_rows} is \
             more than the fetch allows ({bound})"
            ))),
            _ => None,
        }
    };
    let partitions = partition_count(node.as_ref());
    let mut findings = vec![];
    // A fetch limits each partition, so the overall bound grows with the
    // partition count. Nodes with a single output partition are bounded by
    // `fetch` exactly.
    if let Ok(stats) = overall_statistics(node.as_ref()) {
        let bound = fetch.saturating_mul(partitions.max(1));
        findings.extend(check_bound("overall", stats.num_rows, bound));
    }
    if partitions > 1 {
        for partition in 0..partitions {
            if let Ok(stats) = partition_statistics(node.as_ref(), partition) {
                let target = format!("partition {partition}");
                findings.extend(check_bound(&target, stats.num_rows, fetch));
            }
        }
    }
    Ok(findings)
}

/// A4: `num_rows` is consistent with `CardinalityEffect::LowerEqual` and
/// `CardinalityEffect::GreaterEqual`, overall and for each partition.
pub(super) fn cardinality_effect_bounds_num_rows(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let effect = node.cardinality_effect();
    if !matches!(
        effect,
        CardinalityEffect::LowerEqual | CardinalityEffect::GreaterEqual
    ) {
        return Ok(vec![]);
    }
    let children = node.children();
    let partitions = (0..partition_count(node.as_ref())).map(Some);
    let mut findings = vec![];
    for partition in std::iter::once(None).chain(partitions) {
        let (target, output, inputs) = match partition {
            None => (
                "overall".to_string(),
                overall_statistics(node.as_ref()),
                vec![Some(InputRows::All); children.len()],
            ),
            Some(p) => (
                format!("partition {p}"),
                partition_statistics(node.as_ref(), p),
                partition_inputs(node.as_ref(), p),
            ),
        };
        // Errors are reported by `statistics_shape`
        let Ok(output) = output else {
            continue;
        };
        let output = output.num_rows;
        match effect {
            // Only single-child nodes: which input a multi-child node's
            // LowerEqual refers to is not defined
            CardinalityEffect::LowerEqual => {
                let [child] = children.as_slice() else {
                    break;
                };
                // A partition of any node that drops rows has at most every
                // input row
                let rows = inputs.first().copied().flatten().unwrap_or(InputRows::All);
                let Ok(input) = rows.statistics(child.as_ref()) else {
                    continue;
                };
                let input_rows = input.num_rows;
                let inputs = rows.describe(None);
                findings.extend(match (input_rows, output) {
                    (Precision::Exact(n), Precision::Exact(m)) if m > n => {
                        Some(Finding::invariant(format!(
                            "cardinality_effect() is LowerEqual, but the {target} \
                             num_rows is {output} for {inputs} with num_rows {input_rows}"
                        )))
                    }
                    (
                        Precision::Exact(n) | Precision::Inexact(n),
                        Precision::Inexact(m),
                    ) if m > n => Some(Finding::lint(format!(
                        "cardinality_effect() is LowerEqual, but the {target} num_rows \
                         estimate {output} is larger than the num_rows {input_rows} of \
                         {inputs}"
                    ))),
                    _ => None,
                });
            }
            // Producing at least as many rows as all inputs together implies
            // producing at least as many rows as each input
            CardinalityEffect::GreaterEqual => {
                for (i, (child, rows)) in children.iter().zip(inputs).enumerate() {
                    let Some(rows) = rows else {
                        continue;
                    };
                    let Ok(input) = rows.statistics(child.as_ref()) else {
                        continue;
                    };
                    let input_rows = input.num_rows;
                    let inputs = rows.describe(Some(i));
                    findings.extend(match (input_rows, output) {
                        (Precision::Exact(n), Precision::Exact(m)) if m < n => {
                            Some(Finding::invariant(format!(
                                "cardinality_effect() is GreaterEqual, but the {target} \
                                 num_rows is {output} while {inputs} has num_rows \
                                 {input_rows}"
                            )))
                        }
                        (Precision::Exact(n), Precision::Inexact(m)) if m < n => {
                            Some(Finding::lint(format!(
                                "cardinality_effect() is GreaterEqual, but the {target} \
                                 num_rows estimate {output} is smaller than the num_rows \
                                 {input_rows} of {inputs}"
                            )))
                        }
                        _ => None,
                    });
                }
            }
            CardinalityEffect::Equal | CardinalityEffect::Unknown => {}
        }
    }
    Ok(findings)
}

/// A5: a node whose properties show that it passes the rows of its child
/// through supports limit pushdown.
pub(super) fn limit_pushdown_missed(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    if node.supports_limit_pushdown() || !passes_rows_through(node.as_ref()) {
        return Ok(vec![]);
    }
    Ok(vec![Finding::lint(
        "supports_limit_pushdown() is false, but the cardinality effect without a \
         fetch is Equal, maintains_input_order() is true, and each output partition \
         is made of at most one partition of the child, so a limit above the node \
         could be pushed to its child; return true from supports_limit_pushdown, \
         unless an output row depends on later input rows, as in a window function, \
         or the node has side effects on the rows it reads"
            .to_string(),
    )])
}

/// A6: a node that supports limit pushdown and has one output partition does
/// not read a child with several partitions, unless `LimitPushdown` keeps the
/// limit above it.
pub(super) fn limit_pushdown_merges_partitions(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    if !node.supports_limit_pushdown() || partition_count(node.as_ref()) != 1 {
        return Ok(vec![]);
    }
    // `LimitPushdown` merges limit nodes into the limit it pushes down before
    // it asks whether a node supports limit pushdown, and gives these nodes the
    // limit as a fetch, or keeps a limit above them
    // (`combines_input_partitions`)
    if node.is::<GlobalLimitExec>()
        || node.is::<LocalLimitExec>()
        || node.is::<CoalescePartitionsExec>()
        || node.is::<SortPreservingMergeExec>()
    {
        return Ok(vec![]);
    }
    Ok(node
        .children()
        .iter()
        .enumerate()
        .filter_map(|(i, child)| {
            let partitions = partition_count(child.as_ref());
            (partitions > 1).then(|| {
                Finding::invariant(format!(
                    "supports_limit_pushdown() is true and the node has one output \
                     partition, but child {i} has {partitions} partitions; \
                     LimitPushdown removes a limit of n rows above the node and \
                     limits each partition of the child to n rows instead, so the \
                     node can produce up to {partitions} * n rows; return false from \
                     supports_limit_pushdown when a child has several partitions"
                ))
            })
        })
        .collect())
}

/// A7: every method that returns one entry per child returns exactly
/// `children().len()` entries.
pub(super) fn per_child_lengths(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let children = node.children();
    let wrong_len = |method: &str, actual: usize| {
        (actual != children.len()).then(|| {
            Finding::invariant(format!(
                "{method} returned {actual} entries, but the node has {} children",
                children.len()
            ))
        })
    };
    let distributions = node.input_distribution_requirements();
    let mut findings: Vec<Finding> = [
        (
            "maintains_input_order()",
            node.maintains_input_order().len(),
        ),
        (
            "required_input_ordering()",
            node.required_input_ordering().len(),
        ),
        (
            "benefits_from_input_partitioning()",
            node.benefits_from_input_partitioning().len(),
        ),
        (
            "input_distribution_requirements()",
            distributions.per_child_distributions().len(),
        ),
    ]
    .into_iter()
    .filter_map(|(method, len)| wrong_len(method, len))
    .collect();
    let partitions = (0..partition_count(node.as_ref())).map(Some);
    for partition in std::iter::once(None).chain(partitions) {
        let method = format!("child_stats_requests({partition:?})");
        let requests = node.child_stats_requests(partition);
        findings.extend(wrong_len(&method, requests.len()));
        for (i, (child, request)) in children.iter().zip(&requests).enumerate() {
            if let ChildStats::At(Some(p)) = request {
                let child_partitions = partition_count(child.as_ref());
                if *p >= child_partitions {
                    findings.push(Finding::invariant(format!(
                        "{method} requested partition {p} of child {i}, which has \
                         {child_partitions} partitions"
                    )));
                }
            }
        }
    }
    Ok(findings)
}

/// A7: the node's own [`ExecutionPlan::check_invariants`] passes at
/// [`InvariantLevel::Always`], and at [`InvariantLevel::Executable`] when its
/// children meet its input requirements.
pub(super) fn check_invariants(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    if let Err(e) = node.check_invariants(InvariantLevel::Always) {
        return Ok(vec![Finding::invariant(format!(
            "check_invariants(Always) failed: {}",
            e.strip_backtrace()
        ))]);
    }
    // A plan whose requirements are not met cannot be executed, so it can
    // fail the executable invariants for a reason that is not the node's
    if !input_requirements_met(node.as_ref()) {
        return Ok(vec![]);
    }
    match node.check_invariants(InvariantLevel::Executable) {
        Ok(()) => Ok(vec![]),
        Err(e) => Ok(vec![Finding::invariant(format!(
            "check_invariants(Executable) failed, although the children meet the \
             node's ordering, distribution and co-partitioning requirements: {}",
            e.strip_backtrace()
        ))]),
    }
}

/// Returns true if the children of `node` meet its ordering and distribution
/// requirements, as `SanityCheckPlan` checks them before it calls
/// `check_invariants(Executable)`, and are co-partitioned where required
fn input_requirements_met(node: &dyn ExecutionPlan) -> bool {
    let children = node.children();
    let orderings = node.required_input_ordering();
    let distributions = node.input_distribution_requirements();
    // Wrong lengths are reported by `per_child_lengths`
    if orderings.len() != children.len()
        || distributions.per_child_distributions().len() != children.len()
    {
        return false;
    }
    let met = children
        .iter()
        .zip(orderings)
        .enumerate()
        .all(|(i, (child, ordering))| {
            let ordering_met = ordering.is_none_or(|ordering| {
                child
                    .properties()
                    .equivalence_properties()
                    .ordering_satisfy_requirement(ordering.into_single())
                    .unwrap_or(false)
            });
            let options = ChildSatisfactionOptions::new().with_allow_subset(true);
            let distribution_met = distributions
                .child_satisfaction(i, child.as_ref(), options)
                .is_ok_and(|satisfaction| satisfaction.is_satisfied());
            ordering_met && distribution_met
        });
    let children: Vec<&dyn ExecutionPlan> =
        children.iter().map(|child| child.as_ref()).collect();
    met && distributions
        .unsatisfied_co_partitioned_children(node.name(), &children)
        .is_ok_and(|unsatisfied| unsatisfied.is_empty())
}

/// A8: statistics can be computed, overall and for every partition, and have
/// one column statistics entry per field.
pub(super) fn statistics_shape(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    // Errors from a child are reported on the child, not again on its parent
    let child_fails = || {
        node.children().into_iter().any(|child| {
            overall_statistics(child.as_ref()).is_err()
                || (0..partition_count(child.as_ref()))
                    .any(|p| partition_statistics(child.as_ref(), p).is_err())
        })
    };
    let fields = node.schema().fields().len();
    let partitions = (0..partition_count(node.as_ref())).map(Some);
    let mut findings = vec![];
    for partition in std::iter::once(None).chain(partitions) {
        let (target, result) = match partition {
            None => ("overall".to_string(), overall_statistics(node.as_ref())),
            Some(p) => (
                format!("partition {p}"),
                partition_statistics(node.as_ref(), p),
            ),
        };
        match result {
            Ok(stats) => {
                let columns = stats.column_statistics.len();
                if columns != fields {
                    findings.push(Finding::invariant(format!(
                        "{target} statistics have {columns} column statistics \
                         entries, but the schema has {fields} fields"
                    )));
                }
            }
            Err(e) if !child_fails() => findings.push(Finding::invariant(format!(
                "computing {target} statistics returned an error: {}; return \
                 Statistics::new_unknown when statistics are not available",
                e.strip_backtrace()
            ))),
            Err(_) => {}
        }
    }
    Ok(findings)
}

/// A8: per-partition row counts are consistent with the overall row count.
pub(super) fn partition_statistics_sum(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let partitions = partition_count(node.as_ref());
    if partitions == 0 {
        return Ok(vec![]);
    }
    // Errors are reported by `statistics_shape`
    let Ok(overall) = overall_statistics(node.as_ref()) else {
        return Ok(vec![]);
    };
    let Ok(per_partition) = (0..partitions)
        .map(|p| partition_statistics(node.as_ref(), p).map(|s| s.num_rows))
        .collect::<Result<Vec<_>>>()
    else {
        return Ok(vec![]);
    };

    let overall_rows = overall.num_rows;
    let exact_rows = per_partition
        .iter()
        .map(|rows| match rows {
            Precision::Exact(n) => Some(*n),
            _ => None,
        })
        .collect::<Option<Vec<_>>>();

    let mut findings = vec![];
    if let Some(exact_rows) = exact_rows {
        let sum: usize = exact_rows.iter().sum();
        match overall_rows {
            Precision::Exact(n) if n != sum => {
                findings.push(Finding::invariant(format!(
                    "overall num_rows is {overall_rows}, but the exact per-partition \
                     num_rows {exact_rows:?} sum to {sum}"
                )));
            }
            Precision::Exact(_) => {}
            Precision::Inexact(_) | Precision::Absent => {
                findings.push(Finding::lint(format!(
                    "every partition has an exact num_rows ({exact_rows:?}), but \
                     overall num_rows is {overall_rows}; it could be Exact({sum})"
                )));
            }
        }
    } else if let Precision::Exact(n) = overall_rows {
        for (p, rows) in per_partition.iter().enumerate() {
            if let Precision::Exact(m) = rows
                && *m > n
            {
                findings.push(Finding::invariant(format!(
                    "partition {p} num_rows is {rows}, which is more than the overall \
                     num_rows {overall_rows}"
                )));
            }
        }
    }
    Ok(findings)
}

/// A9: a single-child `CardinalityEffect::Equal` node without a fetch whose
/// input has a known row count also reports a row count.
pub(super) fn statistics_ignore_inputs(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    if !matches!(node.cardinality_effect(), CardinalityEffect::Equal)
        || node.fetch().is_some()
    {
        return Ok(vec![]);
    }
    let children = node.children();
    let [child] = children.as_slice() else {
        return Ok(vec![]);
    };
    let (Ok(input), Ok(output)) = (
        overall_statistics(child.as_ref()),
        overall_statistics(node.as_ref()),
    ) else {
        return Ok(vec![]);
    };
    if input.num_rows == Precision::Absent || output.num_rows != Precision::Absent {
        return Ok(vec![]);
    }

    let hint = if node
        .child_stats_requests(None)
        .iter()
        .all(|r| *r == ChildStats::Skip)
    {
        "; child_stats_requests() skips the input, so statistics_from_inputs only \
         receives unknown input statistics"
    } else {
        ""
    };
    Ok(vec![Finding::lint(format!(
        "cardinality_effect() is Equal and the input has num_rows {}, but the node \
         reports Absent{hint}",
        input.num_rows
    ))])
}

/// A10: `schema()` is the schema of the node's equivalence properties.
pub(super) fn schema_consistency(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let findings = schema_differences(node.as_ref());
    // A node builds its schema and its equivalence properties from its
    // children's, so a child whose two schemas differ can make the node's
    // differ too; report it on the child
    if node
        .children()
        .iter()
        .any(|child| !schema_differences(child.as_ref()).is_empty())
    {
        return Ok(vec![]);
    }
    Ok(findings)
}

/// How `schema()` of `plan` differs from the schema of its equivalence
/// properties: one finding per field that differs, and one for the schema
/// metadata
fn schema_differences(plan: &dyn ExecutionPlan) -> Vec<Finding> {
    let schema = plan.schema();
    let properties = plan.properties().equivalence_properties().schema();
    if schema.fields().len() != properties.fields().len() {
        return vec![Finding::invariant(format!(
            "schema() has {} fields, but the schema of the equivalence properties has \
             {}",
            schema.fields().len(),
            properties.fields().len()
        ))];
    }
    let describe = |field: &Field| {
        let nullability = if field.is_nullable() { "" } else { " NOT NULL" };
        format!("'{}' {}{nullability}", field.name(), field.data_type())
    };
    let mut findings = vec![];
    for (i, (field, other)) in schema.fields().iter().zip(properties.fields()).enumerate()
    {
        if field.name() != other.name()
            || field.data_type() != other.data_type()
            || field.is_nullable() != other.is_nullable()
        {
            findings.push(Finding::invariant(format!(
                "field {i} of schema() is {}, but the equivalence properties have {}",
                describe(field),
                describe(other)
            )));
        } else if field.metadata() != other.metadata() {
            findings.push(Finding::lint(format!(
                "field '{}' has metadata {:?} in schema(), but {:?} in the equivalence \
                 properties",
                field.name(),
                field.metadata(),
                other.metadata()
            )));
        }
    }
    if schema.metadata() != properties.metadata() {
        findings.push(Finding::lint(format!(
            "schema() has metadata {:?}, but the schema of the equivalence properties \
             has {:?}",
            schema.metadata(),
            properties.metadata()
        )));
    }
    findings
}

/// A10: every column in the output orderings, equivalence classes, constants
/// and output partitioning refers to a field of `schema()`, by index and name,
/// and for a single-child node, every column in the expressions visited by
/// `apply_expressions` refers to a field of the input in the same way.
pub(super) fn expression_column_refs(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let schema = node.schema();
    let properties = node.properties();
    let eq_properties = properties.equivalence_properties();
    let mut findings = vec![];
    for ordering in eq_properties.oeq_class().iter() {
        let exprs = ordering.iter().map(|sort_expr| &sort_expr.expr);
        let what = format!("the ordering [{ordering}]");
        findings.extend(stale_columns(&what, exprs, &schema, "schema()")?);
    }
    // Constants are the equivalence classes with a constant value
    for class in eq_properties.eq_group().iter() {
        let what = format!("the equivalence class {class}");
        findings.extend(stale_columns(&what, class.iter(), &schema, "schema()")?);
    }
    let partitioning = properties.output_partitioning();
    let exprs: Vec<&Arc<dyn PhysicalExpr>> = match partitioning {
        Partitioning::Hash(exprs, _) => exprs.iter().collect(),
        Partitioning::Range(range) => range
            .ordering()
            .iter()
            .map(|sort_expr| &sort_expr.expr)
            .collect(),
        Partitioning::RoundRobinBatch(_) | Partitioning::UnknownPartitioning(_) => {
            vec![]
        }
    };
    let what = format!("the output partitioning {partitioning}");
    findings.extend(stale_columns(&what, exprs, &schema, "schema()")?);

    let children = node.children();
    let [child] = children.as_slice() else {
        return Ok(findings);
    };
    let roots = match visited_expressions(node.as_ref()) {
        Ok(roots) => roots,
        Err(e) => {
            findings.push(Finding::invariant(format!(
                "apply_expressions returned an error: {}",
                e.strip_backtrace()
            )));
            return Ok(findings);
        }
    };
    let input = child.schema();
    // An aggregate that merges partial states, such as a final aggregate,
    // binds its aggregate expressions to the input of the partial aggregate,
    // `input_schema()`, rather than to its own input
    let partial_input = node
        .downcast_ref::<AggregateExec>()
        .filter(|aggregate| aggregate.mode().input_mode() == AggregateInputMode::Partial)
        .map(AggregateExec::input_schema);
    for root in &roots {
        if let Some(partial_input) = &partial_input
            && stale_columns("", [root], partial_input, "")?.is_empty()
        {
            continue;
        }
        let what = format!("the expression {root} visited by apply_expressions");
        findings.extend(stale_columns(&what, [root], &input, "the input schema")?);
    }
    Ok(findings)
}

/// An invariant finding for each column in `exprs` that does not refer to a
/// field of `schema`, called `name` in the message, by index and name, such as
/// `{what} refers to column 'a'@3, but schema() has 2 fields`
pub(super) fn stale_columns<'a>(
    what: &str,
    exprs: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
    schema: &Schema,
    name: &str,
) -> Result<Vec<Finding>> {
    let mut findings = vec![];
    for expr in exprs {
        expr.apply(|expr| {
            if let Some(column) = expr.downcast_ref::<Column>() {
                let (column_name, index) = (column.name(), column.index());
                let fields = schema.fields();
                if index >= fields.len() {
                    findings.push(Finding::invariant(format!(
                        "{what} refers to column '{column_name}'@{index}, but {name} has \
                         {} fields",
                        fields.len()
                    )));
                } else if fields[index].name() != column_name {
                    findings.push(Finding::invariant(format!(
                        "{what} refers to column '{column_name}'@{index}, but field \
                         {index} of {name} is '{}'",
                        fields[index].name()
                    )));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(findings)
}

/// The root expressions that `apply_expressions` visits
fn visited_expressions(node: &dyn ExecutionPlan) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let mut roots = vec![];
    node.apply_expressions(&mut |root| {
        roots.push(Arc::clone(root));
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(roots)
}

/// A11: every dynamic expression the node produces has an expression id, and
/// `apply_expressions` visits an expression with that id.
pub(super) fn dynamic_expressions_visited(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let produced = node.dynamic_expressions_produced();
    if produced.is_empty() {
        return Ok(vec![]);
    }
    let roots = match visited_expressions(node.as_ref()) {
        Ok(roots) => roots,
        Err(e) => {
            return Ok(vec![Finding::invariant(format!(
                "apply_expressions returned an error: {}",
                e.strip_backtrace()
            ))]);
        }
    };
    // The ids of the visited expressions and of their children
    let mut visited = HashSet::new();
    for root in &roots {
        root.apply(|expr| {
            visited.extend(expr.expression_id());
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    let mut findings = vec![];
    for expr in produced {
        match expr.expression_id() {
            None => findings.push(Finding::invariant(format!(
                "dynamic_expressions_produced() returned {expr}, which has no \
                 expression id"
            ))),
            Some(id) if !visited.contains(&id) => {
                findings.push(Finding::invariant(format!(
                    "dynamic_expressions_produced() returned {expr}, but no expression \
                     visited by apply_expressions has its expression id; include \
                     produced dynamic filters in apply_expressions"
                )))
            }
            Some(_) => {}
        }
    }
    Ok(findings)
}

/// A11: after `reset_state`, the node produces none of the dynamic
/// expressions it produced before, which share the state that executing the
/// node updates.
pub(super) fn dynamic_expressions_reset(
    node: &Arc<dyn ExecutionPlan>,
    _context: &CheckContext,
) -> Result<Vec<Finding>> {
    let ids: HashSet<u64> = node
        .dynamic_expressions_produced()
        .iter()
        .filter_map(|expr| expr.expression_id())
        .collect();
    if ids.is_empty() {
        return Ok(vec![]);
    }
    let reset = match Arc::clone(node).reset_state() {
        Ok(reset) => reset,
        Err(e) => {
            return Ok(vec![Finding::invariant(format!(
                "reset_state failed: {}, so the dynamic expressions the node produces \
                 cannot be reset",
                e.strip_backtrace()
            ))]);
        }
    };
    Ok(reset
        .dynamic_expressions_produced()
        .into_iter()
        .filter(|expr| expr.expression_id().is_some_and(|id| ids.contains(&id)))
        .map(|expr| {
            Finding::invariant(format!(
                "after reset_state, dynamic_expressions_produced() still returns {expr}, \
                 with the expression id of a dynamic expression the node produced \
                 before the reset, so it shares the state that executing the node \
                 updates; create new dynamic filters in reset_state"
            ))
        })
        .collect())
}
