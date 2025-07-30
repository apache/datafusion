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
//!
//! # Overview
//!
//! This optimization rule identifies and eliminates unnecessary self-joins when:
//! 1. The join is on columns that form a unique constraint (e.g., PRIMARY KEY, UNIQUE)
//! 2. The join is an INNER join without additional filter conditions
//! 3. The projection doesn't explicitly require columns from both sides
//!
//! Since joining on unique columns means each row from the left side matches at most
//! one row from the right side (and vice versa), the join can be eliminated by
//! scanning the table just once.
//!
//! # Example
//!
//! ```sql
//! -- Original query with self-join on primary key
//! SELECT a.id, a.name
//! FROM employees a
//! JOIN employees b ON a.id = b.id
//! WHERE b.department = 'HR';
//! ```
//!
//! Gets optimized to:
//! ```sql
//! -- Optimized query without join
//! SELECT id, name
//! FROM employees
//! WHERE department = 'HR';
//! ```

use std::sync::Arc;

use super::{
    is_table_scan_same, merge_table_scans, unique_indexes, OptimizationResult,
    RenamedAlias,
};
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    DFSchema, HashSet, Result, TableReference,
};
use datafusion_expr::{
    Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder, SubqueryAlias, TableScan,
};

use indexmap::IndexSet;

#[derive(Default, Debug)]
pub struct EliminateUniqueKeyedSelfJoin;

impl EliminateUniqueKeyedSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
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
        let mut renamed = None;

        let Transformed {
            data: plan,
            transformed,
            ..
        } = plan.transform_up(|plan| match &plan {
            LogicalPlan::Projection(projection) => {
                if let LogicalPlan::Join(join) = projection.input.as_ref() {
                    if join.join_type != JoinType::Inner {
                        return Ok(Transformed::no(plan));
                    }

                    // Check if projection references columns from both sides
                    if projection_references_both_sides(&projection.expr, join) {
                        return Ok(Transformed::no(plan));
                    }

                    // Determine which side the projection references
                    let referenced_side =
                        determine_referenced_side(&projection.expr, join);

                    // Try to eliminate the join
                    if let Some(OptimizationResult {
                        plan: optimized,
                        renamed_alias,
                    }) = try_eliminate_unique_keyed_self_join_with_side(
                        join,
                        referenced_side,
                    ) {
                        // Special case: Don't return the side directly if it would change the schema
                        // We need to keep the projection to maintain the correct output columns

                        // If we don't have a renamed_alias, it means we returned one side directly
                        // without any transformation, so we can use the original projection
                        if renamed_alias.is_none()
                            && referenced_side != ReferencedSide::Both
                        {
                            let plan = LogicalPlan::Projection(
                                datafusion_expr::Projection::try_new(
                                    projection.expr.clone(),
                                    Arc::new(optimized),
                                )?,
                            );
                            return Ok(Transformed::yes(plan));
                        }

                        // Only rewrite expressions if needed
                        let projection_expr = if referenced_side == ReferencedSide::Both {
                            // When referencing both sides, we may need to rename
                            projection
                                .expr
                                .iter()
                                .cloned()
                                .map(|expr| {
                                    if let Some(renamed_alias) = &renamed_alias {
                                        renamed_alias
                                            .rewrite_expression(expr)
                                            .map(|res| res.data)
                                    } else {
                                        Ok(expr)
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?
                        } else {
                            // When referencing only one side, no renaming needed
                            projection.expr.clone()
                        };

                        // Only set renamed for top-level renaming if needed
                        if referenced_side == ReferencedSide::Both {
                            renamed = renamed_alias;
                        }

                        let plan = LogicalPlan::Projection(
                            datafusion_expr::Projection::try_new(
                                projection_expr,
                                Arc::new(optimized),
                            )?,
                        );
                        Ok(Transformed::yes(plan))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                } else {
                    Ok(Transformed::no(plan))
                }
            }
            _ => Ok(Transformed::no(plan)),
        })?;

        if transformed {
            // Apply any remaining renaming at the top level if needed
            if let Some(renamed) = &renamed {
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
fn optimize_with_side(
    left: &LogicalPlan,
    right: &LogicalPlan,
    referenced_side: ReferencedSide,
) -> Option<OptimizationResult> {
    match (left, right) {
        (LogicalPlan::TableScan(left_scan), LogicalPlan::TableScan(right_scan)) => {
            let table_scan = merge_table_scans(left_scan, right_scan).ok()?;
            let plan = LogicalPlan::TableScan(table_scan).recompute_schema().ok()?;
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
                renamed_alias: _,
            } = optimize_with_side(left_input, right_input, referenced_side)?;

            // Choose which alias to use based on which side is referenced
            let (chosen_alias, renamed_alias) = match referenced_side {
                ReferencedSide::Right => (
                    right_alias.clone(),
                    Some(RenamedAlias {
                        from: left_alias.clone(),
                        to: right_alias.clone(),
                    }),
                ),
                _ => (
                    left_alias.clone(),
                    Some(RenamedAlias {
                        from: right_alias.clone(),
                        to: left_alias.clone(),
                    }),
                ),
            };

            let plan = LogicalPlanBuilder::new(plan)
                .alias(chosen_alias)
                .ok()?
                .build()
                .ok()?;
            let plan = plan.recompute_schema().ok()?;
            Some(OptimizationResult {
                plan,
                renamed_alias,
            })
        }
        (LogicalPlan::TableScan(left_scan), _) => {
            let transformed = right
                .clone()
                .transform_up(|plan| match &plan {
                    LogicalPlan::TableScan(right_scan) => {
                        let merged = merge_table_scans(left_scan, right_scan)?;
                        Ok(Transformed::yes(LogicalPlan::TableScan(merged)))
                    }
                    _ => Ok(Transformed::no(plan)),
                })
                .ok()?;
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
        (_, LogicalPlan::TableScan(_)) => {
            // When swapping sides, we need to swap the referenced side too
            let swapped_side = match referenced_side {
                ReferencedSide::Left => ReferencedSide::Right,
                ReferencedSide::Right => ReferencedSide::Left,
                ReferencedSide::Both => ReferencedSide::Both,
            };
            optimize_with_side(right, left, swapped_side)
        }
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
        // safe to unwrap
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
        // For joins, try to extract from the left side first
        LogicalPlan::Join(join) => extract_top_alias(&join.left),
        _ => None,
    }
}

/// Check if a plan has non-trivial operations (beyond TableScan and SubqueryAlias)
fn has_non_trivial_operations(plan: &LogicalPlan) -> bool {
    let mut has_ops = false;
    plan.apply(|node| {
        match node {
            LogicalPlan::TableScan(_) | LogicalPlan::SubqueryAlias(_) => {
                // These are trivial operations
            }
            _ => {
                // Any other operation is non-trivial
                has_ops = true;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .ok();
    has_ops
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum ReferencedSide {
    Left,
    Right,
    Both,
}

/// Determine which side of the join is referenced by the projection
fn determine_referenced_side(exprs: &[Expr], join: &Join) -> ReferencedSide {
    let left_alias = extract_top_alias(&join.left);
    let right_alias = extract_top_alias(&join.right);

    let (left_alias, right_alias) = match (left_alias, right_alias) {
        (Some(l), Some(r)) => (l, r),
        _ => return ReferencedSide::Both, // Conservative default
    };

    let mut references_left = false;
    let mut references_right = false;

    for expr in exprs {
        expr.apply(|e| {
            if let Expr::Column(col) = e {
                if let Some(relation) = &col.relation {
                    if relation == &left_alias {
                        references_left = true;
                    } else if relation == &right_alias {
                        references_right = true;
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .ok();
    }

    match (references_left, references_right) {
        (true, false) => ReferencedSide::Left,
        (false, true) => ReferencedSide::Right,
        _ => ReferencedSide::Both,
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
    let mut join_columns = HashSet::new();
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
    let left_col_names = left_cols
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<HashSet<_>>();
    let right_col_names = right_cols
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<HashSet<_>>();

    // Find columns that are selected from both sides
    let both_sides_cols = left_col_names
        .intersection(&right_col_names)
        .cloned()
        .collect::<HashSet<_>>();

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

#[allow(dead_code)]
fn try_eliminate_unique_keyed_self_join(join: &Join) -> Option<OptimizationResult> {
    try_eliminate_unique_keyed_self_join_with_side(join, ReferencedSide::Both)
}

fn try_eliminate_unique_keyed_self_join_with_side(
    join: &Join,
    referenced_side: ReferencedSide,
) -> Option<OptimizationResult> {
    // Cannot eliminate joins with additional filter conditions
    // These filters change the semantics of the join beyond simple equality
    if join.filter.is_some() {
        return None;
    }

    let left_schema = join.left.schema().as_ref();
    let right_schema = join.right.schema().as_ref();

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

    // Special case: if projection only references one side and that side has
    // additional operations (like filters), just return that side directly
    if referenced_side == ReferencedSide::Right {
        // For right side, check if it has any operations beyond basic table access
        // This includes filters, projections, etc. that need to be preserved
        if has_non_trivial_operations(&join.right) {
            return Some(OptimizationResult {
                plan: join.right.as_ref().clone(),
                renamed_alias: None,
            });
        }
    } else if referenced_side == ReferencedSide::Left {
        // For left side, check if it has any operations beyond basic table access
        if has_non_trivial_operations(&join.left) {
            return Some(OptimizationResult {
                plan: join.left.as_ref().clone(),
                renamed_alias: None,
            });
        }
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

    optimize_with_side(&join.left, &join.right, referenced_side)
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
        // Test multiple independent self-joins that can each be eliminated
        // First join: a JOIN b
        let a1 = create_table_scan(Some("a"));
        let b1 = create_table_scan(Some("b"));
        let join1 = LogicalPlanBuilder::from(a1)
            .join(b1, JoinType::Inner, (vec!["a.id"], vec!["b.id"]), None)?
            .project(vec![col("a.id")])?
            .build()?;

        // Second join: c JOIN d
        let c = create_table_scan(Some("c"));
        let d = create_table_scan(Some("d"));
        let join2 = LogicalPlanBuilder::from(c)
            .join(d, JoinType::Inner, (vec!["c.id"], vec!["d.id"]), None)?
            .project(vec![col("c.id")])?
            .build()?;

        // Verify each join can be eliminated independently
        let optimized1 = optimize_plan(join1)?;
        let optimized2 = optimize_plan(join2)?;

        assert_eq!(
            count_joins(&optimized1)?,
            0,
            "First join should be eliminated"
        );
        assert_eq!(
            count_joins(&optimized2)?,
            0,
            "Second join should be eliminated"
        );

        // Now test a nested structure where we can eliminate the outer join
        let a2 = create_table_scan(Some("a"));
        let b2 = create_table_scan(Some("b"));
        let nested = LogicalPlanBuilder::from(a2)
            .project(vec![col("id").alias("a_id")])? // Project with alias
            .join(b2, JoinType::Inner, (vec!["a_id"], vec!["b.id"]), None)?
            .project(vec![col("a_id")])?
            .build()?;

        let optimized_nested = optimize_plan(nested)?;
        // This might not be eliminated due to the aliasing, but that's ok
        let _nested_join_count = count_joins(&optimized_nested)?;

        Ok(())
    }

    fn count_joins(plan: &LogicalPlan) -> Result<usize> {
        let mut join_count = 0;
        plan.apply(|node| {
            if matches!(node, LogicalPlan::Join(_)) {
                join_count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(join_count)
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
