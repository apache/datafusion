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

//! Built-in [`PlanCheck`]s. See `docs/physical_plan/CHECKS.md` for what each
//! check verifies. Each check is a function named after the check, documented
//! with its code in the catalog, and lives in the module for its
//! [`CheckKind`].

use std::sync::Arc;

use datafusion_common::{Result, Statistics};
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::{
    ChildStats, ExecutionPlan, StatisticsArgs, StatisticsContext,
};

use super::{CheckContext, CheckKind, PlanCheck};
use crate::Finding;

mod execution_checks;
mod static_checks;
mod stream_checks;
mod variant_checks;

use execution_checks::{
    batch_schema, cardinality_effect_holds, constants_hold, equivalence_classes_hold,
    exact_statistics_hold, execution_succeeds, hash_partitioning_holds,
    invalid_partition_errors, maintains_input_order_holds, maintains_input_order_missed,
    memory_released, orderings_hold,
};
use static_checks::{
    cardinality_effect_bounds_num_rows, check_invariants, dynamic_expressions_reset,
    dynamic_expressions_visited, equal_cardinality_num_rows, expression_column_refs,
    fetch_bounds_num_rows, fetch_not_equal_cardinality, limit_pushdown_merges_partitions,
    limit_pushdown_missed, partition_statistics_sum, per_child_lengths,
    schema_consistency, statistics_ignore_inputs, statistics_shape,
};
use stream_checks::{
    boundedness_holds, emission_type_holds, errors_propagate, lazy_evaluation_holds,
    streams_released,
};
use variant_checks::{
    batch_boundary_invariance, batch_size_invariance, limit_pushdown_equivalent,
    limit_pushdown_missed_at_runtime, reset_state_reexecution, with_fetch_equivalent,
};

type CheckFn = fn(&Arc<dyn ExecutionPlan>, &CheckContext) -> Result<Vec<Finding>>;

const fn check(name: &'static str, kind: CheckKind, check: CheckFn) -> PlanCheck {
    PlanCheck { name, kind, check }
}

/// All built-in checks, in catalog order
pub fn all_checks() -> Vec<PlanCheck> {
    use CheckKind::*;
    vec![
        check(
            "equal_cardinality_num_rows",
            Static,
            equal_cardinality_num_rows,
        ),
        check(
            "fetch_not_equal_cardinality",
            Static,
            fetch_not_equal_cardinality,
        ),
        check("fetch_bounds_num_rows", Static, fetch_bounds_num_rows),
        check(
            "cardinality_effect_bounds_num_rows",
            Static,
            cardinality_effect_bounds_num_rows,
        ),
        check("limit_pushdown_missed", Static, limit_pushdown_missed),
        check(
            "limit_pushdown_merges_partitions",
            Static,
            limit_pushdown_merges_partitions,
        ),
        check("per_child_lengths", Static, per_child_lengths),
        check("check_invariants", Static, check_invariants),
        check("statistics_shape", Static, statistics_shape),
        check("partition_statistics_sum", Static, partition_statistics_sum),
        check("statistics_ignore_inputs", Static, statistics_ignore_inputs),
        check("schema_consistency", Static, schema_consistency),
        check("expression_column_refs", Static, expression_column_refs),
        check(
            "dynamic_expressions_visited",
            Static,
            dynamic_expressions_visited,
        ),
        check(
            "dynamic_expressions_reset",
            Static,
            dynamic_expressions_reset,
        ),
        check("execution_succeeds", Execution, execution_succeeds),
        check("batch_schema", Execution, batch_schema),
        check("exact_statistics_hold", Execution, exact_statistics_hold),
        check("orderings_hold", Execution, orderings_hold),
        check("constants_hold", Execution, constants_hold),
        check(
            "equivalence_classes_hold",
            Execution,
            equivalence_classes_hold,
        ),
        check(
            "hash_partitioning_holds",
            Execution,
            hash_partitioning_holds,
        ),
        check(
            "invalid_partition_errors",
            Execution,
            invalid_partition_errors,
        ),
        check(
            "cardinality_effect_holds",
            Execution,
            cardinality_effect_holds,
        ),
        check("boundedness_holds", Stream, boundedness_holds),
        check("emission_type_holds", Stream, emission_type_holds),
        check("lazy_evaluation_holds", Stream, lazy_evaluation_holds),
        check(
            "maintains_input_order_holds",
            Execution,
            maintains_input_order_holds,
        ),
        check(
            "maintains_input_order_missed",
            Execution,
            maintains_input_order_missed,
        ),
        check("with_fetch_equivalent", Variant, with_fetch_equivalent),
        check(
            "limit_pushdown_equivalent",
            Variant,
            limit_pushdown_equivalent,
        ),
        check("reset_state_reexecution", Variant, reset_state_reexecution),
        check(
            "limit_pushdown_missed_at_runtime",
            Variant,
            limit_pushdown_missed_at_runtime,
        ),
        check("batch_size_invariance", Variant, batch_size_invariance),
        check(
            "batch_boundary_invariance",
            Variant,
            batch_boundary_invariance,
        ),
        check("memory_released", Execution, memory_released),
        check("streams_released", Stream, streams_released),
        check("errors_propagate", Stream, errors_propagate),
    ]
}

/// Statistics of `plan` for all partitions, computed with a fresh
/// [`StatisticsContext`] and no statistics providers
fn overall_statistics(plan: &dyn ExecutionPlan) -> Result<Arc<Statistics>> {
    StatisticsContext::new().compute(plan, &StatisticsArgs::new())
}

/// Statistics of `plan` for a single partition, computed with a fresh
/// [`StatisticsContext`] and no statistics providers
fn partition_statistics(
    plan: &dyn ExecutionPlan,
    partition: usize,
) -> Result<Arc<Statistics>> {
    StatisticsContext::new()
        .compute(plan, &StatisticsArgs::new().with_partition(Some(partition)))
}

/// Whether `plan` reports that its partitions can already be in the order a
/// node sorts them into: it reports an ordering, or a constant. Sorting on a
/// constant keeps rows in their order, and the equivalence properties of a
/// sort on a constant report no ordering.
fn reports_sorted_output(plan: &dyn ExecutionPlan) -> bool {
    let properties = plan.properties();
    properties.output_ordering().is_some()
        || !properties.equivalence_properties().constants().is_empty()
}

/// Number of output partitions of `plan`
fn partition_count(plan: &dyn ExecutionPlan) -> usize {
    plan.properties().output_partitioning().partition_count()
}

/// Whether `node` can move the rows of its child `child`, which has several
/// partitions, between partitions: it computes the statistics of an output
/// partition from the overall statistics of the child, as `RepartitionExec`
/// does. Which output partition a row goes to then depends on the data, and
/// on generated data, such as input already hash partitioned the same way or
/// with one constant per partition, every output partition can happen to get
/// the rows of one child partition only, in order, so that the node looks as
/// if it passed its input through, which it does not do for other input.
fn moves_rows_between_partitions(node: &dyn ExecutionPlan, child: usize) -> bool {
    let children = node.children();
    let Some(child_plan) = children.get(child) else {
        return false;
    };
    partition_count(child_plan.as_ref()) > 1
        && (0..partition_count(node)).any(|p| {
            matches!(
                node.child_stats_requests(Some(p)).get(child),
                Some(ChildStats::At(None))
            )
        })
}

/// Whether the properties of `node` show that it passes the rows of its only
/// child through, which is what limit pushdown needs: its cardinality effect
/// without its fetch (that of the plan returned by `with_fetch(None)`) is
/// `Equal`, it maintains the order of the child, and each of its output
/// partitions is made of the rows of at most one partition of the child. A
/// child with one partition qualifies however the node spreads its rows, and
/// a child with several only if the node has as many partitions and does not
/// move rows between them.
fn passes_rows_through(node: &dyn ExecutionPlan) -> bool {
    let children = node.children();
    let [child] = children.as_slice() else {
        return false;
    };
    let effect = node.with_fetch(None).map_or_else(
        || node.cardinality_effect(),
        |plan| plan.cardinality_effect(),
    );
    let child_partitions = partition_count(child.as_ref());
    matches!(effect, CardinalityEffect::Equal)
        && node.maintains_input_order().first() == Some(&true)
        && (child_partitions <= 1
            || (child_partitions == partition_count(node)
                && !moves_rows_between_partitions(node, 0)))
}
