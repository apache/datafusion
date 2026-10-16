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

//! `CheckKind::Execution` checks: they compare what a node reports with the
//! output it produces, and check that it releases the memory it reserves.

use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{Result, ScalarValue, Statistics, internal_err};
use datafusion_physical_expr::expressions::{Literal, UnKnownColumn};
use datafusion_physical_expr::{Partitioning, PhysicalExpr};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::{ChildStats, ExecutionPlan, StatisticsArgs};

use super::static_checks::stale_columns;
use super::{
    moves_rows_between_partitions, overall_statistics, partition_count,
    partition_statistics, reports_sorted_output,
};
use crate::Finding;
use crate::physical_plan::fixtures::ROW_ID_COLUMN;
use crate::physical_plan::oracle::{InputOrder, RowPosition};
use crate::physical_plan::{CheckContext, NodeOutput, oracle};

/// C4: the rows of a child for which `maintains_input_order()` is false keep
/// their relative order in the output, as tracked by their row ids.
pub(super) fn maintains_input_order_missed(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    // A node that reports an ordering of its own orders its output, for
    // example by sorting it, which keeps input rows that tie on the sort key
    // in their input order without maintaining the order of every input
    if node.properties().output_ordering().is_some() {
        return Ok(vec![]);
    }
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let schema = node.schema();
    let maintains = node.maintains_input_order();
    let mut findings = vec![];
    for (i, child) in node.children().into_iter().enumerate() {
        // Input that is sorted, or has a constant, is not in a random order:
        // its order can be the order the node sorts it into. A node that
        // places rows in partitions by their values can leave the rows of
        // generated input where they are.
        if maintains.get(i) != Some(&false)
            || reports_sorted_output(child.as_ref())
            || moves_rows_between_partitions(node.as_ref(), i)
        {
            continue;
        }
        let Some(child_output) = context.output(child) else {
            continue;
        };
        let Some(positions) =
            tracked_positions(&schema, output, &child.schema(), child_output)
        else {
            continue;
        };
        let InputOrder::Kept {
            input_partitions,
            max_partition_rows,
        } = oracle::input_order_of(&positions)
        else {
            continue;
        };
        // The order of a single row cannot be seen, and a node over a child
        // with several partitions must be seen to keep them apart
        if max_partition_rows < 2
            || (partition_count(child.as_ref()) > 1 && input_partitions < 2)
        {
            continue;
        }
        findings.push(Finding::lint(format!(
            "maintains_input_order()[{i}] is false, but every output partition only has \
             rows of one partition of child {i}, in the order of that partition; return \
             true if the node never reorders the rows of this child, and keep the \
             child's orderings in the output equivalence properties"
        )));
    }
    Ok(findings)
}

/// For each row of each output partition of a node, the position of the row
/// of a child that it copies, if any (see [`oracle::input_positions`]),
/// tracked by the [`ROW_ID_COLUMN`] of the child and the one row id column of
/// the output that has its ids. `None` if the child does not have exactly one
/// row id column, or its ids are not unique, the output has no row id column
/// or several with its ids, or the batches do not match the schemas, which
/// `batch_schema` reports.
fn tracked_positions(
    schema: &Schema,
    output: &NodeOutput,
    child_schema: &Schema,
    child_output: &NodeOutput,
) -> Option<Vec<Vec<Option<RowPosition>>>> {
    let row_id_columns = |schema: &Schema| -> Vec<usize> {
        schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.name() == ROW_ID_COLUMN && field.data_type() == &DataType::UInt64
            })
            .map(|(i, _)| i)
            .collect()
    };
    let [child_column] = row_id_columns(child_schema)[..] else {
        return None;
    };
    let mut tracked = vec![];
    for column in row_id_columns(schema) {
        let positions = oracle::input_positions(
            child_output.partitions(),
            child_column,
            output.partitions(),
            column,
        )
        .ok()??;
        if positions.iter().flatten().any(Option::is_some) {
            tracked.push(positions);
        }
    }
    let [positions] = <[_; 1]>::try_from(tracked).ok()?;
    Some(positions)
}

/// C1: the rows of a child for which `maintains_input_order()` is true keep
/// their order in the output, as tracked by their row ids: within each output
/// partition, the rows of each partition of the child appear in the order of
/// that partition. How rows of several partitions are merged is checked by
/// `orderings_hold`, against the ordering the node reports.
pub(super) fn maintains_input_order_holds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let schema = node.schema();
    let maintains = node.maintains_input_order();
    let mut findings = vec![];
    for (i, child) in node.children().into_iter().enumerate() {
        if maintains.get(i) != Some(&true) {
            continue;
        }
        let Some(child_output) = context.output(child) else {
            continue;
        };
        let child_schema = child.schema();
        let Some(positions) =
            tracked_positions(&schema, output, &child_schema, child_output)
        else {
            continue;
        };
        let describe = |position: RowPosition| {
            format!(
                "row {} of partition {} of child {i}",
                position.row, position.partition
            )
        };
        for order_break in oracle::order_breaks(&positions) {
            findings.push(Finding::invariant(format!(
                "maintains_input_order()[{i}] is true, but row {} of output partition \
                 {} is {}, which comes before {}, at row {} of the output partition",
                order_break.output_row,
                order_break.output_partition,
                describe(order_break.input_row),
                describe(order_break.earlier_input_row),
                order_break.earlier_output_row,
            )));
        }
    }
    Ok(findings)
}

/// B0: a node executes without errors, panics or timeouts.
pub(super) fn execution_succeeds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(error) = context.execution_error(node) else {
        return Ok(vec![]);
    };
    // A failing child makes its parents fail too; report it on the child
    if node
        .children()
        .into_iter()
        .any(|child| context.execution_error(child).is_some())
    {
        return Ok(vec![]);
    }
    Ok(vec![Finding::invariant(format!(
        "executing the node failed: {error}"
    ))])
}

/// B1: every output batch has the node's schema, and non-nullable fields
/// contain no nulls.
pub(super) fn batch_schema(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let expected = node.schema();
    let mut findings = vec![];
    // Each problem is reported for the first batch that has it
    let mut reported = HashSet::new();
    for (p, batches) in output.partitions().iter().enumerate() {
        for (b, batch) in batches.iter().enumerate() {
            for finding in batch_problems(&expected, batch) {
                if reported.insert(finding.message.clone()) {
                    findings.push(Finding {
                        message: format!(
                            "batch {b} of partition {p}: {}",
                            finding.message
                        ),
                        ..finding
                    });
                }
            }
        }
    }
    Ok(findings)
}

/// How `batch` differs from `expected`
fn batch_problems(expected: &Schema, batch: &RecordBatch) -> Vec<Finding> {
    let actual = batch.schema();
    if actual.fields().len() != expected.fields().len() {
        return vec![Finding::invariant(format!(
            "the batch has {} columns, but schema() has {} fields",
            actual.fields().len(),
            expected.fields().len()
        ))];
    }
    let mut findings = vec![];
    for (i, (actual_field, field)) in
        actual.fields().iter().zip(expected.fields()).enumerate()
    {
        let name = field.name();
        if actual_field.name() != name || actual_field.data_type() != field.data_type() {
            findings.push(Finding::invariant(format!(
                "column {i} is '{}' {}, but schema() declares '{name}' {}",
                actual_field.name(),
                actual_field.data_type(),
                field.data_type()
            )));
            continue;
        }
        if !field.is_nullable() && batch.column(i).logical_null_count() > 0 {
            findings.push(Finding::invariant(format!(
                "column '{name}' has nulls, but schema() declares it non-nullable"
            )));
        } else if actual_field.is_nullable() != field.is_nullable() {
            findings.push(Finding::lint(format!(
                "column '{name}' has nullable={}, but schema() declares nullable={}",
                actual_field.is_nullable(),
                field.is_nullable()
            )));
        }
        if actual_field.metadata() != field.metadata() {
            findings.push(Finding::lint(format!(
                "column '{name}' has field metadata {:?}, but schema() declares {:?}",
                actual_field.metadata(),
                field.metadata()
            )));
        }
    }
    if actual.metadata() != expected.metadata() {
        findings.push(Finding::lint(format!(
            "the batch has schema metadata {:?}, but schema() declares {:?}",
            actual.metadata(),
            expected.metadata()
        )));
    }
    findings
}

/// B2: every exact statistic the node reports is true of its output.
pub(super) fn exact_statistics_hold(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let schema = node.schema();
    let mut findings = vec![];
    let targets = std::iter::once(None).chain((0..output.partitions().len()).map(Some));
    for partition in targets {
        // Output that does not match the schema is reported by `batch_schema`
        let Some(actual) = actual_statistics(&schema, output, partition)? else {
            return Ok(vec![]);
        };
        // Errors computing statistics are reported by `statistics_shape`
        let Ok((claimed, corrected_inputs)) =
            statistics_from_true_inputs(node, partition, context)
        else {
            continue;
        };
        let mismatches = correct(&schema, &mut claimed.as_ref().clone(), &actual);
        if mismatches.is_empty() {
            continue;
        }
        let target = match partition {
            None => "overall".to_string(),
            Some(p) => format!("partition {p}"),
        };
        // The statistics of the child are false too, and reported on the
        // child; these are false because of the node
        let prefix = if corrected_inputs {
            "with the false exact statistics of its children replaced by the true \
             values, "
        } else {
            ""
        };
        findings.push(Finding::invariant(format!(
            "{prefix}the {target} statistics are false: {}",
            mismatches.join("; ")
        )));
    }
    Ok(findings)
}

/// The statistics `node` reports overall (`partition` is `None`) or for one
/// partition, computed with `statistics_from_inputs` from the statistics of
/// its children with every exact statistic that the output of a child
/// contradicts replaced by the true value, and whether any was replaced. A
/// node computes its statistics from its children's, so a child with a false
/// exact statistic can make the node's false too, even for a node that passes
/// statistics through unchanged, such as a repartition; with true inputs, a
/// false statistic is the node's own.
fn statistics_from_true_inputs(
    node: &Arc<dyn ExecutionPlan>,
    partition: Option<usize>,
    context: &CheckContext,
) -> Result<(Arc<Statistics>, bool)> {
    let children = node.children();
    let requests = node.child_stats_requests(partition);
    if requests.len() != children.len() {
        return internal_err!(
            "child_stats_requests returned {} entries for {} children",
            requests.len(),
            children.len()
        );
    }
    let mut corrected = false;
    let mut inputs = vec![];
    for (child, request) in children.into_iter().zip(requests) {
        let ChildStats::At(p) = request else {
            inputs.push(Arc::new(Statistics::new_unknown(&child.schema())));
            continue;
        };
        let claimed = match p {
            None => overall_statistics(child.as_ref())?,
            Some(p) => partition_statistics(child.as_ref(), p)?,
        };
        let schema = child.schema();
        let actual = match context.output(child) {
            Some(output) => actual_statistics(&schema, output, p)?,
            None => None,
        };
        let mut statistics = claimed.as_ref().clone();
        match actual {
            Some(actual) if !correct(&schema, &mut statistics, &actual).is_empty() => {
                corrected = true;
                inputs.push(Arc::new(statistics));
            }
            _ => inputs.push(claimed),
        }
    }
    let args = StatisticsArgs::new().with_partition(partition);
    Ok((node.statistics_from_inputs(&inputs, &args)?, corrected))
}

/// The exact statistics of the output of a node with `schema`, overall
/// (`partition` is `None`) or of one partition. `None` if the partition does
/// not exist, or a batch does not have a column for each field.
fn actual_statistics(
    schema: &Schema,
    output: &NodeOutput,
    partition: Option<usize>,
) -> Result<Option<Statistics>> {
    let batches = match partition {
        None => output.batches(),
        Some(p) => match output.partitions().get(p) {
            Some(batches) => batches.clone(),
            None => return Ok(None),
        },
    };
    if batches
        .iter()
        .any(|batch| batch.num_columns() != schema.fields().len())
    {
        return Ok(None);
    }
    oracle::exact_statistics(schema, &batches).map(Some)
}

/// Replaces each exact statistic in `claimed` that `actual` contradicts with
/// the actual value, and describes each, such as `null_count of l_k@0 is
/// Exact(9), but the output has Exact(0)`. An exact minimum, maximum or sum
/// cannot be contradicted by a column without non-null values, which has
/// none, and only the sum of an integer or decimal column is known (see
/// [`oracle::exact_statistics`]).
fn correct(
    schema: &Schema,
    claimed: &mut Statistics,
    actual: &Statistics,
) -> Vec<String> {
    let mut mismatches = vec![];
    if let (Precision::Exact(claimed_rows), Precision::Exact(actual_rows)) =
        (claimed.num_rows, actual.num_rows)
        && claimed_rows != actual_rows
    {
        mismatches.push(format!(
            "num_rows is Exact({claimed_rows}), but the output has {actual_rows} rows"
        ));
        claimed.num_rows = actual.num_rows;
    }
    let columns = claimed
        .column_statistics
        .iter_mut()
        .zip(&actual.column_statistics);
    for (i, (claimed, actual)) in columns.enumerate() {
        let column = format!("{}@{i}", schema.field(i).name());
        let mut describe = |statistic, claimed: &dyn Display, actual: &dyn Display| {
            mismatches.push(format!(
                "{statistic} of {column} is {claimed}, but the output has {actual}"
            ));
        };
        if claimed.null_count.is_exact() == Some(true)
            && claimed.null_count != actual.null_count
        {
            describe("null_count", &claimed.null_count, &actual.null_count);
            claimed.null_count = actual.null_count;
        }
        if claimed.distinct_count.is_exact() == Some(true)
            && claimed.distinct_count != actual.distinct_count
        {
            describe(
                "distinct_count",
                &claimed.distinct_count,
                &actual.distinct_count,
            );
            claimed.distinct_count = actual.distinct_count;
        }
        // A sum is compared by value: it may be kept in the type of the column
        // rather than the wider type of SQL `SUM`, and adding decimals
        // increases their precision, so its type depends on how it was added
        let values = [
            (
                "min_value",
                false,
                &mut claimed.min_value,
                &actual.min_value,
            ),
            (
                "max_value",
                false,
                &mut claimed.max_value,
                &actual.max_value,
            ),
            ("sum_value", true, &mut claimed.sum_value, &actual.sum_value),
        ];
        for (statistic, by_value, claimed, actual) in values {
            let (Precision::Exact(claimed_value), Precision::Exact(actual_value)) =
                (&*claimed, actual)
            else {
                continue;
            };
            let equal = if by_value {
                claimed_value
                    .cast_to(&actual_value.data_type())
                    .is_ok_and(|value| value == *actual_value)
            } else {
                claimed_value == actual_value
            };
            if !equal {
                describe(statistic, &*claimed, actual);
                *claimed = actual.clone();
            }
        }
    }
    mismatches
}

/// B3: every output partition is sorted by every ordering in the node's
/// equivalence properties.
pub(super) fn orderings_hold(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let mut findings = vec![];
    let eq_properties = node.properties().equivalence_properties();
    for ordering in eq_properties.oeq_class().iter() {
        for (p, batches) in output.partitions().iter().enumerate() {
            // An ordering that cannot be evaluated on the output points to a
            // schema mismatch, which `batch_schema` reports
            if let Ok(Some(row)) = oracle::first_unsorted_row(batches, ordering) {
                findings.push(Finding::invariant(format!(
                    "the node reports the ordering [{ordering}], but partition {p} is \
                     not sorted by it at row {row}"
                )));
            }
        }
    }
    Ok(findings)
}

/// B4: every constant in the node's equivalence properties has a single value
/// within each output partition, and, if it is uniform across partitions, the
/// same value in every partition: the given value, if there is one.
pub(super) fn constants_hold(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let schema = node.schema();
    let mut findings = vec![];
    for constant in node.properties().equivalence_properties().constants() {
        let expr = &constant.expr;
        // A constant that refers to a column by a wrong index or name is
        // reported by `expression_column_refs`
        if !stale_columns("", [expr], &schema, "")?.is_empty() {
            continue;
        }
        // An expression that cannot be evaluated on the output points to a
        // schema mismatch, which `batch_schema` reports
        let Ok(violations) = oracle::constant_violations(output.partitions(), &constant)
        else {
            continue;
        };
        findings.extend(violations.into_iter().map(|violation| {
            Finding::invariant(format!("the node reports that {violation}"))
        }));
    }
    Ok(findings)
}

/// B5: the expressions of each equivalence class of the node are equal on
/// every output row.
pub(super) fn equivalence_classes_hold(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let Some(output) = context.output(node) else {
        return Ok(vec![]);
    };
    let schema = node.schema();
    let mut findings = vec![];
    let eq_properties = node.properties().equivalence_properties();
    for class in eq_properties.eq_group().iter() {
        // A literal makes the class constant, which `constants_hold` checks,
        // and an expression that refers to a column by a wrong index or name
        // is reported by `expression_column_refs`
        let mut exprs = vec![];
        for expr in class.iter() {
            if expr.downcast_ref::<Literal>().is_none()
                && stale_columns("", [expr], &schema, "")?.is_empty()
            {
                exprs.push(expr);
            }
        }
        let Some((first, others)) = exprs.split_first() else {
            continue;
        };
        for other in others {
            for (p, batches) in output.partitions().iter().enumerate() {
                // An expression that cannot be evaluated on the output points
                // to a schema mismatch, which `batch_schema` reports
                if let Ok(Some((row, left, right))) =
                    oracle::first_unequal_row(batches, first, other)
                {
                    findings.push(Finding::invariant(format!(
                        "the node reports that {first} and {other} are equal, but row \
                         {row} of partition {p} has {left} and {right}"
                    )));
                }
            }
        }
    }
    Ok(findings)
}

/// B7: every output row of a node that reports `Partitioning::Hash` is in the
/// partition that the hash of its key assigns it to, as `RepartitionExec`
/// hashes it.
pub(super) fn hash_partitioning_holds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    // A node that passes a false hash partitioning of its child through, as
    // a filter does, has a false one too; it is reported on the child
    for child in node.children() {
        if !misplaced_rows(child, context)?.is_empty() {
            return Ok(vec![]);
        }
    }
    misplaced_rows(node, context)
}

/// A finding for each output partition of `plan` with rows that do not
/// belong to it by the hash partitioning `plan` reports, if it reports one
fn misplaced_rows(
    plan: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let partitioning = plan.properties().output_partitioning();
    let Partitioning::Hash(exprs, partitions) = partitioning else {
        return Ok(vec![]);
    };
    let Some(output) = context.output(plan) else {
        return Ok(vec![]);
    };
    // An expression that refers to a column by a wrong index or name is
    // reported by `expression_column_refs`. A key that is not in the output,
    // which projecting a partitioning marks with an `UnKnownColumn`, matches
    // no requirement and cannot be evaluated.
    let unknown_column = |expr: &Arc<dyn PhysicalExpr>| {
        expr.exists(|expr| Ok(expr.downcast_ref::<UnKnownColumn>().is_some()))
            .unwrap_or(true)
    };
    if !stale_columns("", exprs, &plan.schema(), "")?.is_empty()
        || exprs.iter().any(unknown_column)
    {
        return Ok(vec![]);
    }
    let mut findings = vec![];
    for (p, batches) in output.partitions().iter().enumerate() {
        // Keys that cannot be evaluated on the output point to a schema
        // mismatch, which `batch_schema` reports
        let Ok(split) = oracle::hash_partition(batches, exprs, *partitions) else {
            continue;
        };
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        let kept: usize = split
            .get(p)
            .map_or(0, |batches| batches.iter().map(RecordBatch::num_rows).sum());
        if kept == rows {
            continue;
        }
        // The first row that belongs to the first other partition with rows
        let example =
            split
                .iter()
                .enumerate()
                .filter(|(q, _)| *q != p)
                .find_map(|(q, batches)| {
                    let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
                    Some((q, batch))
                });
        let example = match example {
            Some((q, batch)) => {
                let key = exprs
                    .iter()
                    .map(|expr| {
                        let values =
                            expr.evaluate(batch)?.into_array(batch.num_rows())?;
                        Ok(ScalarValue::try_from_array(&values, 0)?.to_string())
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(|key| key.join(", "))
                    .unwrap_or_default();
                format!(": one has the key ({key}), which belongs to partition {q}")
            }
            None => String::new(),
        };
        findings.push(Finding::invariant(format!(
            "the node reports the partitioning {partitioning}, but {} of the {rows} rows \
             of partition {p} belong to other partitions by the hash of their \
             key{example}",
            rows - kept,
        )));
    }
    Ok(findings)
}

/// B7: executing partition `partition_count`, which does not exist, returns
/// an error, from `execute` or from the first poll of the stream it returns,
/// rather than panicking, producing a batch, ending or hanging.
pub(super) fn invalid_partition_errors(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    // An error, from `execute` or from the stream, is what is expected
    let Some(problem) = context.invalid_partition_problem(node) else {
        return Ok(vec![]);
    };
    let partitions = partition_count(node.as_ref());
    // A node that executes the same partition of a child with as many
    // partitions, as a filter does, does what the child does; it is reported
    // on the child
    let child_fails = node.children().into_iter().any(|child| {
        partition_count(child.as_ref()) == partitions
            && context.invalid_partition_problem(child).is_some()
    });
    if child_fails {
        return Ok(vec![]);
    }
    Ok(vec![Finding::invariant(format!(
        "the node has {partitions} output partitions, but execute({partitions}) \
         {problem}; return an error for a partition at or past the partition count"
    ))])
}

/// B8: the number of output rows agrees with `cardinality_effect()`.
pub(super) fn cardinality_effect_holds(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let (Some(output), Some(inputs)) =
        (context.output(node), context.child_outputs(node))
    else {
        return Ok(vec![]);
    };
    let out = output.num_rows();
    let findings = match (node.cardinality_effect(), inputs.as_slice()) {
        (CardinalityEffect::Equal, [input]) if out != input.num_rows() => {
            vec![Finding::invariant(format!(
                "cardinality_effect() is Equal, but the node produced {out} rows from \
                 {} input rows",
                input.num_rows()
            ))]
        }
        (CardinalityEffect::LowerEqual, [input]) if out > input.num_rows() => {
            vec![Finding::invariant(format!(
                "cardinality_effect() is LowerEqual, but the node produced {out} rows \
                 from {} input rows",
                input.num_rows()
            ))]
        }
        (CardinalityEffect::GreaterEqual, inputs) => inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| out < input.num_rows())
            .map(|(i, input)| {
                Finding::invariant(format!(
                    "cardinality_effect() is GreaterEqual, but the node produced {out} \
                     rows while input {i} has {} rows",
                    input.num_rows()
                ))
            })
            .collect(),
        _ => vec![],
    };
    Ok(findings)
}

/// F1: memory reserved while executing a node to completion is released once
/// its streams and the plan are dropped.
pub(super) fn memory_released(
    node: &Arc<dyn ExecutionPlan>,
    context: &CheckContext,
) -> Result<Vec<Finding>> {
    let mut findings = vec![];
    // A child that leaks makes its parents leak too; report it on the child
    let child_leaks = node.children().into_iter().any(|child| {
        context
            .memory_reserved_after_execution(child)
            .is_some_and(|bytes| bytes > 0)
    });
    if let Some(reserved) = context.memory_reserved_after_execution(node)
        && reserved > 0
        && !child_leaks
    {
        findings.push(Finding::invariant(format!(
            "after executing the node to completion and dropping its streams and the \
             plan, {reserved} bytes are still reserved in the memory pool"
        )));
    }
    Ok(findings)
}
