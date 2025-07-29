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

//! [`EliminateUniqueKeyedSelfJoin`] eliminates self joins on unique constraint columns

use super::{
    is_table_scan_same, merge_table_scans, unique_indexes, OptimizationResult,
    RenamedAlias,
};
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    DFSchema, Result, TableReference,
};
use datafusion_expr::{
    Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
    TableScan,
};

use indexmap::IndexSet;

#[derive(Default, Debug)]
pub struct EliminateUniqueKeyedSelfJoin;

impl EliminateUniqueKeyedSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Collect joins that should not be eliminated because they are under
    /// projections that reference columns from both sides
    fn collect_protected_joins(
        plan: &LogicalPlan,
        protected_joins: &mut std::collections::HashSet<String>,
    ) {
        plan.apply(|node| {
            if let LogicalPlan::Projection(projection) = node {
                if let LogicalPlan::Join(join) = projection.input.as_ref() {
                    if join.join_type == JoinType::Inner
                        && projection_references_both_sides(&projection.expr, join)
                    {
                        // Create a unique identifier for this join based on its structure
                        let join_id = Self::create_join_identifier(join);
                        protected_joins.insert(join_id);
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .ok();
    }

    /// Check if a join is protected (should not be eliminated)
    fn is_join_protected(
        join: &Join,
        protected_joins: &std::collections::HashSet<String>,
    ) -> bool {
        let join_id = Self::create_join_identifier(join);
        protected_joins.contains(&join_id)
    }

    /// Create a unique identifier for a join based on its structure
    fn create_join_identifier(join: &Join) -> String {
        let on_str = join
            .on
            .iter()
            .map(|(l, r)| format!("{l:?}={r:?}"))
            .collect::<Vec<_>>()
            .join(",");

        let filter_str = join
            .filter
            .as_ref()
            .map(|f| format!("{f:?}"))
            .unwrap_or_default();

        format!(
            "{}|{}|{}|{}",
            join.left.display_indent(),
            join.right.display_indent(),
            on_str,
            filter_str
        )
    }
}

impl OptimizerRule for EliminateUniqueKeyedSelfJoin {
    fn name(&self) -> &str {
        "eliminate_unique_keyed_self_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // First, collect information about which joins should not be eliminated
        // because they are under projections that reference both sides
        let mut protected_joins = std::collections::HashSet::new();
        Self::collect_protected_joins(&plan, &mut protected_joins);

        let mut renamed = None;
        let Transformed {
            data: plan,
            transformed,
            ..
        } = plan
            .transform_up(|plan| {
                match &plan {
                    LogicalPlan::Projection(projection) => {
                        // Handle Projection -> Join pattern
                        match projection.input.as_ref() {
                            LogicalPlan::Join(join)
                                if join.join_type == JoinType::Inner =>
                            {
                                // Check if projection references columns from both sides of the join
                                // If it does, we cannot eliminate the join
                                if projection_references_both_sides(
                                    &projection.expr,
                                    join,
                                ) {
                                    return Ok(Transformed::no(plan));
                                }

                                if let Some(OptimizationResult {
                                    plan: optimized,
                                    renamed_alias,
                                }) = try_eliminate_unique_keyed_self_join(join)
                                {
                                    let projection_expr = projection
                                        .expr
                                        .iter()
                                        .cloned()
                                        .map(|expr| {
                                            if let Some(renamed_alias) = &renamed_alias {
                                                renamed_alias
                                                    .rewrite_expression(expr)
                                                    .unwrap()
                                                    .data
                                            } else {
                                                expr
                                            }
                                        })
                                        .collect::<Vec<_>>();
                                    renamed = renamed_alias;

                                    let plan = Projection::try_new(
                                        projection_expr,
                                        optimized.into(),
                                    )
                                    .unwrap();
                                    let plan = LogicalPlan::Projection(plan);
                                    Ok(Transformed::yes(plan))
                                } else {
                                    Ok(Transformed::no(plan))
                                }
                            }
                            _ => Ok(Transformed::no(plan)),
                        }
                    }
                    LogicalPlan::Join(join) if join.join_type == JoinType::Inner => {
                        // Handle bare Join (no projection directly above)
                        // Check if this join is protected
                        if Self::is_join_protected(join, &protected_joins) {
                            return Ok(Transformed::no(plan));
                        }

                        if let Some(OptimizationResult {
                            plan: optimized,
                            renamed_alias,
                        }) = try_eliminate_unique_keyed_self_join(join)
                        {
                            renamed = renamed_alias;
                            Ok(Transformed::yes(optimized))
                        } else {
                            Ok(Transformed::no(plan))
                        }
                    }
                    _ => Ok(Transformed::no(plan)),
                }
            })
            .unwrap();

        if transformed {
            if let Some(renamed) = renamed {
                let plan = renamed.rewrite_logical_plan(plan)?;
                Ok(Transformed::yes(plan))
            } else {
                Ok(Transformed::yes(plan))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

/// Optimize self-join query by combining LHS and RHS of the join. Current implementation is
/// very conservative. It only merges nodes if one of them is `TableScan`. It should be possible
/// to merge projections and filters together as well.
///
/// TLDR; of current implementation is
/// - If LHS and RHS is `TableScan`, then merge table scans,
/// - If LHS is `TableScan` and RHS isn't `TableScan`, then find `TableScan` on RHS and merge them
/// - If LHS isn't `TableScan` and RHS is `TableScan` recursively call `optimize` with children swapped
/// - If LHS and RHS is `SubqueryAlias`, recursively call `optimize` with their input
fn optimize(left: &LogicalPlan, right: &LogicalPlan) -> Option<OptimizationResult> {
    match (left, right) {
        (LogicalPlan::TableScan(left_scan), LogicalPlan::TableScan(right_scan)) => {
            let table_scan = merge_table_scans(left_scan, right_scan);
            let plan = LogicalPlan::TableScan(table_scan)
                .recompute_schema()
                .unwrap();
            Some(OptimizationResult {
                plan,
                renamed_alias: None,
            })
        }
        (
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: left_input,
                alias: left_alias,
                ..
            }),
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input: right_input,
                alias: right_alias,
                ..
            }),
        ) => {
            let OptimizationResult {
                plan,
                renamed_alias,
            } = optimize(left_input, right_input)?;
            assert!(renamed_alias.is_none(), "Assert `renamed_alias` is `None` because nested `SubqueryAlias` shouldn't be possible");

            let plan = LogicalPlanBuilder::new(plan)
                .alias(left_alias.clone())
                .unwrap()
                .build()
                .unwrap();
            let plan = plan.recompute_schema().unwrap();
            Some(OptimizationResult {
                plan,
                renamed_alias: Some(RenamedAlias {
                    from: right_alias.clone(),
                    to: left_alias.clone(),
                }),
            })
        }
        (LogicalPlan::TableScan(left_scan), _) => {
            let transformed = right
                .clone()
                .transform_up(|plan| match &plan {
                    LogicalPlan::TableScan(right_scan) => {
                        let merged = merge_table_scans(left_scan, right_scan);
                        Ok(Transformed::yes(LogicalPlan::TableScan(merged)))
                    }
                    _ => Ok(Transformed::no(plan)),
                })
                .unwrap();
            assert!(
                transformed.transformed,
                "Called `transform_up` and no merged `TableScan`"
            );
            if transformed.transformed {
                Some(OptimizationResult {
                    plan: transformed.data,
                    renamed_alias: None,
                })
            } else {
                None
            }
        }
        (_, LogicalPlan::TableScan(_)) => optimize(right, left),
        _ => None,
    }
}

fn try_resolve_join_on_columns_to_indexes(
    join: &Join,
    schema: &DFSchema,
    source: &TableReference,
    left_alias: Option<&TableReference>,
    right_alias: Option<&TableReference>,
) -> Option<IndexSet<usize>> {
    let length = join.on.len();
    let mut on_idx = IndexSet::with_capacity(length);

    for on in &join.on {
        let (left_col, right_col) = match on {
            (Expr::Column(left_col), Expr::Column(right_col)) => (left_col, right_col),
            _ => return None,
        };
        let source_ref = Some(source);

        // If LHS column's alias isn't LHS's alias or table name then bail
        let left_ref = left_col.relation.as_ref();
        if left_ref != left_alias && left_ref != source_ref {
            return None;
        }
        // If RHS column's alias isn't RHS's alias or table name then bail
        let right_ref = right_col.relation.as_ref();
        if right_ref != right_alias && right_ref != source_ref {
            return None;
        }

        // It's safe to resolve column's without their qualifiers as we know source `TableSource` are the same.
        let left_idx = schema.index_of_column_by_name(None, left_col.name())?;
        let right_idx = schema.index_of_column_by_name(None, right_col.name())?;

        // If LHS and RHS are different then optimization is impossible
        if left_idx != right_idx {
            return None;
        }
        on_idx.insert(left_idx);
    }
    Some(on_idx)
}

#[derive(Debug)]
struct Resolution {
    /// Source `TableScan`
    table_scan: TableScan,
    /// Column indexes into `TableScan` that form a unique index
    alias: Option<TableReference>,
}

fn try_resolve_to_table_scan_alias(branch: &LogicalPlan) -> Option<Resolution> {
    let mut maybe_alias = None;
    let mut table_scan = None;
    branch
        .apply_with_subqueries(|plan| match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
                maybe_alias = Some(alias.clone());
                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::TableScan(source) => {
                table_scan = Some(source.clone());
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })
        .unwrap();

    let table_scan = table_scan?;
    Some(Resolution {
        table_scan,
        alias: maybe_alias,
    })
}

/// Extract the top-level alias from a logical plan branch
fn extract_top_alias(plan: &LogicalPlan) -> Option<TableReference> {
    match plan {
        LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => Some(alias.clone()),
        LogicalPlan::TableScan(scan) => Some(scan.table_name.clone()),
        _ => None,
    }
}

/// Check if projection references columns from both sides of the join
/// Returns true if columns from both left and right sides are referenced
/// Special handling: If only join columns are selected from both sides and at least one is aliased,
/// we can still optimize
fn projection_references_both_sides(exprs: &[Expr], join: &Join) -> bool {
    // Extract top-level aliases from join sides
    let left_alias = extract_top_alias(&join.left);
    let right_alias = extract_top_alias(&join.right);

    // If we can't determine aliases, be conservative and don't optimize
    let (left_alias, right_alias) = match (left_alias, right_alias) {
        (Some(l), Some(r)) => (l, r),
        _ => return false,
    };

    // Extract join column names
    let mut join_columns = std::collections::HashSet::new();
    for (left_expr, right_expr) in &join.on {
        if let (Expr::Column(left_col), Expr::Column(right_col)) = (left_expr, right_expr)
        {
            // Join columns should have the same name in a self-join
            if left_col.name == right_col.name {
                join_columns.insert(left_col.name.clone());
            }
        }
    }

    let mut left_cols = Vec::new();
    let mut right_cols = Vec::new();
    let mut has_aliased_join_col = false;

    for expr in exprs {
        match expr {
            // Check for aliased expressions
            Expr::Alias(alias) => {
                if let Expr::Column(col) = alias.expr.as_ref() {
                    if let Some(relation) = &col.relation {
                        if relation == &left_alias {
                            left_cols.push((col.name.clone(), true)); // true = aliased
                            if join_columns.contains(&col.name) {
                                has_aliased_join_col = true;
                            }
                        } else if relation == &right_alias {
                            right_cols.push((col.name.clone(), true)); // true = aliased
                            if join_columns.contains(&col.name) {
                                has_aliased_join_col = true;
                            }
                        }
                    }
                }
            }
            // Check for non-aliased columns
            Expr::Column(col) => {
                if let Some(relation) = &col.relation {
                    if relation == &left_alias {
                        left_cols.push((col.name.clone(), false)); // false = not aliased
                    } else if relation == &right_alias {
                        right_cols.push((col.name.clone(), false)); // false = not aliased
                    }
                }
            }
            _ => {
                // For other expressions, check recursively
                expr.apply(|e| {
                    if let Expr::Column(col) = e {
                        if let Some(relation) = &col.relation {
                            if relation == &left_alias {
                                left_cols.push((col.name.clone(), false));
                            } else if relation == &right_alias {
                                right_cols.push((col.name.clone(), false));
                            }
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .ok();
            }
        }
    }

    // If no columns from both sides, we can optimize
    if left_cols.is_empty() || right_cols.is_empty() {
        return false;
    }

    // Check if we're only selecting join columns from both sides
    let left_col_names: std::collections::HashSet<_> =
        left_cols.iter().map(|(name, _)| name.clone()).collect();
    let right_col_names: std::collections::HashSet<_> =
        right_cols.iter().map(|(name, _)| name.clone()).collect();

    // Find columns that are selected from both sides
    let both_sides_cols: std::collections::HashSet<_> = left_col_names
        .intersection(&right_col_names)
        .cloned()
        .collect();

    // If all columns selected from both sides are join columns
    let all_are_join_cols = both_sides_cols.iter().all(|col| join_columns.contains(col));

    if all_are_join_cols && !both_sides_cols.is_empty() {
        // Special case: if we're selecting the same join columns from both sides
        // and at least one is aliased, we can optimize
        if has_aliased_join_col {
            return false; // Allow optimization
        }
        // If none are aliased (e.g., SELECT a.id, b.id), don't optimize
        return true;
    }

    // Default case: if selecting from both sides, don't optimize
    true
}

fn try_eliminate_unique_keyed_self_join(join: &Join) -> Option<OptimizationResult> {
    // Cannot eliminate joins with additional filter conditions
    // These filters change the semantics of the join beyond simple equality
    if join.filter.is_some() {
        return None;
    }

    let left_schema = join.left.schema().as_ref();
    let right_schema = join.right.schema().as_ref();

    // No need for strict schema equality - we only need join columns to match
    // This allows optimization even when one side has been projected to fewer columns

    let left_unique = unique_indexes(left_schema);

    // For the left side, we need unique constraints
    if left_unique.is_empty() {
        return None;
    }

    let Resolution {
        table_scan: left_scan,
        alias: left_alias,
    } = try_resolve_to_table_scan_alias(&join.left)?;
    let Resolution {
        table_scan: right_scan,
        alias: right_alias,
    } = try_resolve_to_table_scan_alias(&join.right)?;

    // Verify both sides reference the same base table
    if !is_table_scan_same(&left_scan, &right_scan) {
        return None;
    }

    // Verify join columns exist in both schemas and resolve their indexes
    // This also handles the case where right side might have fewer columns due to projection
    for on in &join.on {
        let (left_col, right_col) = match on {
            (Expr::Column(left_col), Expr::Column(right_col)) => (left_col, right_col),
            _ => return None,
        };

        // Verify the columns exist in their respective schemas
        left_schema
            .index_of_column_by_name(left_col.relation.as_ref(), left_col.name())?;
        right_schema
            .index_of_column_by_name(right_col.relation.as_ref(), right_col.name())?;
    }

    let column_index = try_resolve_join_on_columns_to_indexes(
        join,
        left_schema,
        &left_scan.table_name,
        left_alias.as_ref(),
        right_alias.as_ref(),
    )?;

    // Check if the join columns form a unique constraint
    let forms_unique_constraint = left_unique
        .iter()
        .any(|unique_constraint| column_index.is_superset(unique_constraint));
    if !forms_unique_constraint {
        return None;
    }

    optimize(&join.left, &join.right)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::OptimizerContext;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Constraint, Constraints, TableReference};
    use datafusion_expr::{
        col, logical_plan::builder::LogicalTableSource, Expr, JoinType, LogicalPlan,
        LogicalPlanBuilder,
    };

    fn create_table_scan(alias: Option<&str>) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("department", DataType::Utf8, false),
        ]));

        // Add unique constraint on 'id' column (index 0)
        let constraints = Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]), // id is primary key
        ]);

        let table_source =
            Arc::new(LogicalTableSource::new(schema).with_constraints(constraints));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            TableReference::bare("employees"),
            table_source,
            None,
            vec![],
        )
        .unwrap();

        if let Some(alias) = alias {
            builder = builder.alias(alias).unwrap();
        }

        builder.build().unwrap()
    }

    fn optimize_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
        let optimizer = EliminateUniqueKeyedSelfJoin::new();
        let config = OptimizerContext::default();
        let result = optimizer.rewrite(plan, &config)?;
        Ok(result.data)
    }

    #[test]
    fn test_eliminate_self_join_on_unique_key() -> Result<()> {
        // Create self join on unique key (id)
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a.id"], vec!["b.id"]), None)?
            .project(vec![col("a.id"), col("a.name")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::SubqueryAlias(_) => {
                        // Success - join was eliminated
                    }
                    _ => panic!(
                        "Expected SubqueryAlias after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_on_non_unique_column() -> Result<()> {
        // Create self join on non-unique column (name)
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.name"], vec!["b.name"]),
                None,
            )?
            .project(vec![col("a.id"), col("a.name")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Join(_) => {
                        // Success - join was not eliminated as expected
                    }
                    _ => panic!(
                        "Expected Join to remain after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_with_filter_on_right() -> Result<()> {
        // Create self join with filter on right side
        let left = create_table_scan(Some("a"));

        // Build right side with filter
        let right_base = create_table_scan(None);
        let right = LogicalPlanBuilder::from(right_base)
            .filter(col("department").eq(Expr::Literal(
                datafusion_common::ScalarValue::Utf8(Some("HR".to_string())),
                None,
            )))?
            .alias("b")?
            .build()?;

        let join_plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a.id"], vec!["b.id"]), None)?
            .project(vec![col("a.id")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and filter was preserved
        match &optimized {
            LogicalPlan::Projection(_) => {
                // Check that we have a filter somewhere in the plan
                let mut has_filter = false;
                optimized.apply(|node| {
                    if matches!(node, LogicalPlan::Filter(_)) {
                        has_filter = true;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert!(
                    has_filter,
                    "Expected filter to be preserved after optimization"
                );
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_different_columns_selected() -> Result<()> {
        // Create self join where we select columns from both sides with different qualifiers
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(right, JoinType::Inner, (vec!["a.id"], vec!["b.id"]), None)?
            .project(vec![col("a.id"), col("b.id")])? // Selecting both a.id and b.id
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated (because we're selecting from both sides)
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Join(_) => {
                        // Success - join was not eliminated as expected
                    }
                    _ => panic!(
                        "Expected Join to remain after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_multiple_self_joins() -> Result<()> {
        // Create multiple self joins on unique key
        let a = create_table_scan(Some("a"));
        let b = create_table_scan(Some("b"));
        let c = create_table_scan(Some("c"));

        let join_plan = LogicalPlanBuilder::from(a)
            .join(b, JoinType::Inner, (vec!["a.id"], vec!["b.id"]), None)?
            .join(c, JoinType::Inner, (vec!["a.id"], vec!["c.id"]), None)?
            .project(vec![col("a.id")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify all joins were eliminated
        let mut join_count = 0;
        optimized.apply(|node| {
            if matches!(node, LogicalPlan::Join(_)) {
                join_count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        assert_eq!(
            join_count, 0,
            "Expected all joins to be eliminated, but found {join_count} joins",
        );

        Ok(())
    }

    #[test]
    fn test_no_elimination_for_outer_join() -> Result<()> {
        // Create left outer join on unique key
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Left, // Left outer join
                (vec!["a.id"], vec!["b.id"]),
                None,
            )?
            .project(vec![col("a.id")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated (only inner joins are optimized)
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Join(_) => {
                        // Success - join was not eliminated as expected
                    }
                    _ => panic!(
                        "Expected Join to remain after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_with_subquery() -> Result<()> {
        // Create self join where right side is a subquery
        let left = create_table_scan(Some("a"));

        let subquery_base = create_table_scan(None);
        let subquery = LogicalPlanBuilder::from(subquery_base)
            .filter(col("department").eq(Expr::Literal(
                datafusion_common::ScalarValue::Utf8(Some("HR".to_string())),
                None,
            )))?
            .project(vec![col("id")])?
            .alias("b")?
            .build()?;

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                subquery,
                JoinType::Inner,
                (vec!["a.id"], vec!["b.id"]),
                None,
            )?
            .project(vec![col("a.id")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                let mut join_count = 0;
                proj.input.apply(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        join_count += 1;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert_eq!(join_count, 0, "Expected join to be eliminated");
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_join_filter() -> Result<()> {
        // Create self join with additional join filter beyond key equality
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_filter = Some(col("a.department").not_eq(col("b.department")));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.id"], vec!["b.id"]),
                join_filter,
            )?
            .project(vec![col("a.id")])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated (has additional join filter)
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Join(_) => {
                        // Success - join was not eliminated as expected
                    }
                    _ => panic!(
                        "Expected Join to remain after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}",),
        }

        Ok(())
    }
}
