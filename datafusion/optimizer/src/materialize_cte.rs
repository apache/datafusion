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

//! [`InlineCte`] optimizer rule — inlines materialized CTEs where
//! materialization is not beneficial (DuckDB-style CTE inlining).

use std::collections::HashSet;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::logical_plan::{Extension, LogicalPlan, MaterializedCteProducer};
use datafusion_expr::{Expr, Operator, SubqueryAlias};

/// Optimizer rule that selectively inlines materialized CTEs where
/// materialization is not beneficial.
///
/// The SQL planner materializes ALL multi-referenced CTEs upfront
/// (wrapping them in `MaterializedCteProducer`/`Reader` nodes).
/// This rule then removes materialization for CTEs that are better
/// off inlined, following DuckDB's approach.
///
/// A CTE is inlined (materialization removed) when:
/// - It is cheap to recompute (e.g., literal projections over EmptyRelation)
/// - It is consumed under a top-level LIMIT (benefits from early termination)
/// - It has few base table references (recomputation is inexpensive)
///
/// A CTE is KEPT materialized when:
/// - It contains volatile functions (preserve "evaluate once" semantics)
/// - It ends in Aggregate/Distinct/Window (expensive to recompute)
/// - It has many base table references (expensive joins)
/// - It was explicitly marked MATERIALIZED (indicated by `force_materialized` field)
///
/// Consumers applying disjoint group-key filters (Q39 pattern) are also
/// inlined, since predicate pushdown can specialize each copy.
#[derive(Debug, Default)]
pub struct InlineCte {}

impl InlineCte {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for InlineCte {
    fn name(&self) -> &str {
        "inline_cte"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().execution.enable_materialized_ctes {
            return Ok(Transformed::no(plan));
        }

        // Find MaterializedCteProducer nodes and decide which to inline
        plan.transform_down(|node| {
            let LogicalPlan::Extension(Extension { node: ext }) = &node else {
                return Ok(Transformed::no(node));
            };

            let Some(producer) = ext.as_any().downcast_ref::<MaterializedCteProducer>()
            else {
                return Ok(Transformed::no(node));
            };

            // Never inline explicitly MATERIALIZED CTEs
            if producer.force_materialized {
                return Ok(Transformed::no(node));
            }

            let cte_plan = &producer.cte_plan;
            let continuation = &producer.continuation;

            // Count how many readers reference this CTE in the continuation
            let ref_count = count_readers_in_plan(continuation, &producer.name);

            if should_inline(cte_plan, &producer.name, continuation, ref_count) {
                // Inline: replace readers with CTE body copies, return continuation
                let inlined = inline_cte_readers(
                    continuation.as_ref().clone(),
                    &producer.name,
                    cte_plan,
                )?;
                Ok(Transformed::yes(inlined))
            } else {
                Ok(Transformed::no(node))
            }
        })
    }
}

/// Decide whether a materialized CTE should be inlined.
/// Returns `true` if inlining is preferred over materialization.
fn should_inline(
    cte_plan: &LogicalPlan,
    cte_name: &str,
    continuation: &LogicalPlan,
    ref_count: usize,
) -> bool {
    // Single-ref or dead CTEs: always inline
    if ref_count <= 1 {
        return true;
    }

    // Volatile CTEs: never inline (preserve "evaluate once" semantics)
    if plan_contains_volatile_functions(cte_plan) {
        return false;
    }

    // Cheap CTEs: always inline (recomputation is trivial)
    if is_cheap_to_inline(cte_plan) {
        return true;
    }

    // Aggregate/Distinct/Window CTEs: keep materialized unless
    // consumers apply disjoint group-key filters (Q39 pattern)
    if ends_in_aggregate_distinct_or_window(cte_plan) {
        return consumers_apply_disjoint_group_key_filters(
            cte_name,
            continuation,
            ref_count,
        );
    }

    // Cost-based: inline if the CTE is cheap to recompute
    let base_table_references = count_base_table_references(cte_plan);
    if base_table_references > 2 && base_table_references * ref_count > 10 {
        return false; // expensive — keep materialized
    }

    // If continuation has a top-level LIMIT, inline (benefits from early termination)
    contains_limit_on_single_child_path(continuation)
}

/// Count MaterializedCteReader nodes for a given CTE name in the plan.
fn count_readers_in_plan(plan: &LogicalPlan, cte_name: &str) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::Extension(Extension { node: ext }) = node
            && let Some(reader) = ext
                .as_any()
                .downcast_ref::<datafusion_expr::logical_plan::MaterializedCteReader>(
            )
            && reader.name == cte_name
        {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

/// Replace MaterializedCteReader nodes with inline copies of the CTE body.
fn inline_cte_readers(
    plan: LogicalPlan,
    cte_name: &str,
    cte_plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    plan.transform_down(|node| {
        if let LogicalPlan::Extension(Extension { node: ext }) = &node
            && let Some(reader) = ext
                .as_any()
                .downcast_ref::<datafusion_expr::logical_plan::MaterializedCteReader>(
            )
            && reader.name == cte_name
        {
            // Replace reader with a SubqueryAlias wrapping the CTE body
            let alias = SubqueryAlias::try_new(Arc::new(cte_plan.clone()), cte_name)?;
            return Ok(Transformed::yes(LogicalPlan::SubqueryAlias(alias)));
        }
        Ok(Transformed::no(node))
    })
    .map(|t| t.data)
}

fn plan_contains_volatile_functions(plan: &LogicalPlan) -> bool {
    let mut has_volatile = false;
    plan.apply(|node| {
        for expr in node.expressions() {
            if expr.is_volatile() {
                has_volatile = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    has_volatile
}

fn ends_in_aggregate_distinct_or_window(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Distinct(_) => true,
        LogicalPlan::Window(_) => true,
        _ => {
            let inputs = plan.inputs();
            inputs.len() == 1 && ends_in_aggregate_distinct_or_window(inputs[0])
        }
    }
}

/// Detects Q39-style patterns where each CTE reader is filtered on a different
/// literal value of a group-by key. In this case inlining is better because
/// predicate pushdown can specialize each copy.
fn consumers_apply_disjoint_group_key_filters(
    cte_name: &str,
    continuation: &LogicalPlan,
    ref_count: usize,
) -> bool {
    let per_ref_filters = collect_per_reference_filters(continuation, cte_name);
    if per_ref_filters.len() != ref_count || per_ref_filters.is_empty() {
        return false;
    }

    let all_col_names: HashSet<&str> = per_ref_filters
        .iter()
        .flat_map(|filters| filters.iter().map(|(col, _)| col.as_str()))
        .collect();

    for col_name in all_col_names {
        let mut seen_values: HashSet<&str> = HashSet::new();
        let mut all_have_filter = true;
        for filters in &per_ref_filters {
            let mut found = false;
            for (filter_col, filter_val) in filters {
                if filter_col == col_name {
                    seen_values.insert(filter_val.as_str());
                    found = true;
                    break;
                }
            }
            if !found {
                all_have_filter = false;
                break;
            }
        }
        if all_have_filter && seen_values.len() == ref_count {
            return true;
        }
    }

    false
}

fn collect_per_reference_filters(
    plan: &LogicalPlan,
    cte_name: &str,
) -> Vec<Vec<(String, String)>> {
    let mut ref_aliases: Vec<String> = Vec::new();
    collect_cte_ref_aliases(plan, cte_name, &mut ref_aliases);
    if ref_aliases.is_empty() {
        return Vec::new();
    }
    let mut all_filters: Vec<(Option<String>, String, String)> = Vec::new();
    collect_all_equality_filters(plan, cte_name, &mut all_filters);
    ref_aliases
        .iter()
        .map(|alias| {
            all_filters
                .iter()
                .filter(|(qualifier, _, _)| qualifier.as_deref() == Some(alias.as_str()))
                .map(|(_, col, val)| (col.clone(), val.clone()))
                .collect()
        })
        .collect()
}

fn collect_cte_ref_aliases(
    plan: &LogicalPlan,
    cte_name: &str,
    aliases: &mut Vec<String>,
) {
    if let LogicalPlan::SubqueryAlias(outer_alias) = plan
        && outer_alias.alias.table() != cte_name
    {
        // Check if the subtree below the alias contains a reader for this CTE
        if subtree_contains_reader(outer_alias.input.as_ref(), cte_name) {
            aliases.push(outer_alias.alias.table().to_string());
            return;
        }
    }
    for input in plan.inputs() {
        collect_cte_ref_aliases(input, cte_name, aliases);
    }
}

fn subtree_contains_reader(plan: &LogicalPlan, cte_name: &str) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if let LogicalPlan::Extension(Extension { node: ext }) = node
            && let Some(reader) = ext
                .as_any()
                .downcast_ref::<datafusion_expr::logical_plan::MaterializedCteReader>(
            )
            && reader.name == cte_name
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

fn collect_all_equality_filters(
    plan: &LogicalPlan,
    cte_name: &str,
    out: &mut Vec<(Option<String>, String, String)>,
) {
    // Stop at readers for this CTE
    if let LogicalPlan::Extension(Extension { node: ext }) = plan
        && let Some(reader) =
            ext.as_any()
                .downcast_ref::<datafusion_expr::logical_plan::MaterializedCteReader>()
        && reader.name == cte_name
    {
        return;
    }

    if let LogicalPlan::Filter(filter) = plan {
        extract_qualified_equality_conditions(&filter.predicate, out);
    }

    if let LogicalPlan::Join(join) = plan
        && let Some(filter) = &join.filter
    {
        extract_qualified_equality_conditions(filter, out);
    }

    for input in plan.inputs() {
        collect_all_equality_filters(input, cte_name, out);
    }
}

fn extract_qualified_equality_conditions(
    expr: &Expr,
    out: &mut Vec<(Option<String>, String, String)>,
) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), rhs) => {
                    if let Some(val) = try_eval_constant(rhs) {
                        out.push((
                            col.relation.as_ref().map(|r| r.table().to_string()),
                            col.name().to_string(),
                            val,
                        ));
                    }
                }
                (lhs, Expr::Column(col)) => {
                    if let Some(val) = try_eval_constant(lhs) {
                        out.push((
                            col.relation.as_ref().map(|r| r.table().to_string()),
                            col.name().to_string(),
                            val,
                        ));
                    }
                }
                _ => {}
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            extract_qualified_equality_conditions(&binary.left, out);
            extract_qualified_equality_conditions(&binary.right, out);
        }
        _ => {}
    }
}

fn try_eval_constant(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(val, _) => Some(val.to_string()),
        Expr::BinaryExpr(binary) => {
            let left = try_eval_constant_i64(&binary.left)?;
            let right = try_eval_constant_i64(&binary.right)?;
            let result = match binary.op {
                Operator::Plus => left.checked_add(right)?,
                Operator::Minus => left.checked_sub(right)?,
                Operator::Multiply => left.checked_mul(right)?,
                _ => return None,
            };
            Some(result.to_string())
        }
        _ => None,
    }
}

fn try_eval_constant_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(val, _) => match val {
            ScalarValue::Int8(Some(v)) => Some(*v as i64),
            ScalarValue::Int16(Some(v)) => Some(*v as i64),
            ScalarValue::Int32(Some(v)) => Some(*v as i64),
            ScalarValue::Int64(Some(v)) => Some(*v),
            ScalarValue::UInt8(Some(v)) => Some(*v as i64),
            ScalarValue::UInt16(Some(v)) => Some(*v as i64),
            ScalarValue::UInt32(Some(v)) => Some(*v as i64),
            _ => None,
        },
        _ => None,
    }
}

fn is_cheap_to_inline(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::EmptyRelation(_) => true,
        _ => {
            let inputs = plan.inputs();
            inputs.len() == 1 && is_cheap_to_inline(inputs[0])
        }
    }
}

fn count_base_table_references(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(_) = node {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    count
}

fn contains_limit_on_single_child_path(plan: &LogicalPlan) -> bool {
    if matches!(plan, LogicalPlan::Limit(_)) {
        return true;
    }
    let inputs = plan.inputs();
    inputs.len() == 1 && contains_limit_on_single_child_path(inputs[0])
}
