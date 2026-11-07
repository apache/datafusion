// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! This file implements the `ProjectionPushdown` physical optimization rule.
//! The function [`remove_unnecessary_projections`] tries to push down all
//! projections one by one if the operator below is amenable to this. If a
//! projection reaches a source, it can even disappear from the plan entirely.

use crate::PhysicalOptimizerRule;
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::alias::AliasGenerator;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{JoinSide, JoinType, Result, internal_datafusion_err};
use datafusion_expr::ExpressionPlacement;
use datafusion_physical_expr::expressions::{Column, LambdaExpr, LambdaVariable};
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, is_volatile};
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::projection::{
    ProjectionExec, remove_unnecessary_projections,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, Partitioning, ReplaceChildrenOptions,
};

/// This rule inspects `ProjectionExec`'s in the given physical plan and tries to
/// remove or swap with its child.
///
/// Furthermore, tries to push down projections from nested loop join filters that only depend on
/// one side of the join. By pushing these projections down, functions that only depend on one side
/// of the join must be evaluated for the cartesian product of the two sides.
#[derive(Default, Debug)]
pub struct ProjectionPushdown {}

impl ProjectionPushdown {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for ProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let alias_generator = AliasGenerator::new();
        let plan = plan
            .transform_up(|plan| match plan.downcast_ref::<NestedLoopJoinExec>() {
                None => Ok(Transformed::no(plan)),
                Some(hash_join) => try_push_down_join_filter(
                    Arc::clone(&plan),
                    hash_join,
                    &alias_generator,
                ),
            })
            .map(|t| t.data)?;

        // Push what can already descend, then split the projections that are
        // stuck above a `RepartitionExec`, then clean up once more: the split
        // can leave an outer projection that is now only column references.
        let plan = plan.transform_down(remove_unnecessary_projections).data()?;
        let plan = plan
            .transform_down(|plan| {
                Ok(match try_split_leaf_projection(&plan)? {
                    Some(new_plan) => Transformed::yes(new_plan),
                    None => Transformed::no(plan),
                })
            })
            .data()?;
        plan.transform_down(remove_unnecessary_projections).data()
    }

    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Tries to push down parts of the filter.
///
/// See [JoinFilterRewriter] for details.
fn try_push_down_join_filter(
    original_plan: Arc<dyn ExecutionPlan>,
    join: &NestedLoopJoinExec,
    alias_generator: &AliasGenerator,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    // Mark joins are currently not supported.
    if matches!(join.join_type(), JoinType::LeftMark | JoinType::RightMark) {
        return Ok(Transformed::no(original_plan));
    }

    let projections = join.projection();
    let Some(filter) = join.filter() else {
        return Ok(Transformed::no(original_plan));
    };

    let original_lhs_length = join.left().schema().fields().len();
    let original_rhs_length = join.right().schema().fields().len();

    let lhs_rewrite = try_push_down_projection(
        Arc::clone(&join.right().schema()),
        Arc::clone(join.left()),
        JoinSide::Left,
        filter.clone(),
        alias_generator,
    )?;
    let rhs_rewrite = try_push_down_projection(
        Arc::clone(&lhs_rewrite.data.0.schema()),
        Arc::clone(join.right()),
        JoinSide::Right,
        lhs_rewrite.data.1,
        alias_generator,
    )?;
    if !lhs_rewrite.transformed && !rhs_rewrite.transformed {
        return Ok(Transformed::no(original_plan));
    }

    let join_filter = minimize_join_filter(
        Arc::clone(rhs_rewrite.data.1.expression()),
        rhs_rewrite.data.1.column_indices(),
        lhs_rewrite.data.0.schema().as_ref(),
        rhs_rewrite.data.0.schema().as_ref(),
    )?;

    let new_lhs_length = lhs_rewrite.data.0.schema().fields.len();
    let projections = match projections.as_ref() {
        None => match join.join_type() {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                // Build projections that ignore the newly projected columns.
                let mut projections =
                    Vec::with_capacity(original_lhs_length + original_rhs_length);
                projections.extend(0..original_lhs_length);
                projections.extend(new_lhs_length..new_lhs_length + original_rhs_length);
                projections
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // Only return original left columns
                let mut projections = Vec::new();
                projections.extend(0..original_lhs_length);
                projections
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                // Only return original right columns
                let mut projections = Vec::new();
                projections.extend(0..original_rhs_length);
                projections
            }
            _ => unreachable!("Unsupported join type"),
        },
        Some(projections) => {
            let rhs_offset = new_lhs_length - original_lhs_length;
            projections
                .iter()
                .map(|idx| {
                    if *idx >= original_lhs_length {
                        idx + rhs_offset
                    } else {
                        *idx
                    }
                })
                .collect()
        }
    };

    Ok(Transformed::yes(Arc::new(NestedLoopJoinExec::try_new(
        lhs_rewrite.data.0,
        rhs_rewrite.data.0,
        Some(join_filter),
        join.join_type(),
        Some(projections),
    )?)))
}

/// Tries to push down parts of `expr` into the `join_side`.
fn try_push_down_projection(
    other_schema: SchemaRef,
    plan: Arc<dyn ExecutionPlan>,
    join_side: JoinSide,
    join_filter: JoinFilter,
    alias_generator: &AliasGenerator,
) -> Result<Transformed<(Arc<dyn ExecutionPlan>, JoinFilter)>> {
    let expr = Arc::clone(join_filter.expression());
    let original_plan_schema = plan.schema();
    let mut rewriter = JoinFilterRewriter::new(
        join_side,
        original_plan_schema.as_ref(),
        join_filter.column_indices().to_vec(),
        alias_generator,
    );
    let new_expr = rewriter.rewrite(expr)?;

    if new_expr.transformed {
        let new_join_side =
            ProjectionExec::try_new(rewriter.join_side_projections, plan)?;
        let new_schema = Arc::clone(&new_join_side.schema());

        let (lhs_schema, rhs_schema) = match join_side {
            JoinSide::Left => (new_schema, other_schema),
            JoinSide::Right => (other_schema, new_schema),
            JoinSide::None => unreachable!("Mark join not supported"),
        };
        let intermediate_schema = rewriter
            .intermediate_column_indices
            .iter()
            .map(|ci| match ci.side {
                JoinSide::Left => Arc::clone(&lhs_schema.fields[ci.index]),
                JoinSide::Right => Arc::clone(&rhs_schema.fields[ci.index]),
                JoinSide::None => unreachable!("Mark join not supported"),
            })
            .collect::<Fields>();

        let expression = remap_filter_expr(
            new_expr.data,
            join_filter.schema().fields().len(),
            rewriter.intermediate_column_indices.len(),
            |column| Ok(column.clone()),
        )?;
        let join_filter = JoinFilter::new(
            expression,
            rewriter.intermediate_column_indices,
            Arc::new(Schema::new(intermediate_schema)),
        );
        Ok(Transformed::yes((Arc::new(new_join_side), join_filter)))
    } else {
        Ok(Transformed::no((plan, join_filter)))
    }
}

/// Creates a new [JoinFilter] and tries to minimize the internal schema.
///
/// This could eliminate some columns that were only part of a computation that has been pushed
/// down. As this computation is now materialized on one side of the join, the original input
/// columns are not needed anymore.
fn minimize_join_filter(
    expr: Arc<dyn PhysicalExpr>,
    old_column_indices: &[ColumnIndex],
    lhs_schema: &Schema,
    rhs_schema: &Schema,
) -> Result<JoinFilter> {
    let mut used_columns = HashSet::with_capacity(old_column_indices.len());
    expr.apply(|expr| {
        if let Some(col) = expr.downcast_ref::<Column>() {
            used_columns.insert(col.index());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let new_column_indices = old_column_indices
        .iter()
        .enumerate()
        .filter(|(idx, _)| used_columns.contains(idx))
        .map(|(_, ci)| ci.clone())
        .collect::<Vec<_>>();
    let fields = new_column_indices
        .iter()
        .map(|ci| match ci.side {
            JoinSide::Left => lhs_schema.field(ci.index).clone(),
            JoinSide::Right => rhs_schema.field(ci.index).clone(),
            JoinSide::None => unreachable!("Mark join not supported"),
        })
        .collect::<Fields>();

    let final_expr = remap_filter_expr(
        expr,
        old_column_indices.len(),
        new_column_indices.len(),
        |column| {
            let new_idx = used_columns
                .iter()
                .filter(|idx| **idx < column.index())
                .count();
            Ok(Column::new(column.name(), new_idx))
        },
    )?;

    Ok(JoinFilter::new(
        final_expr,
        new_column_indices,
        Arc::new(Schema::new(fields)),
    ))
}

/// Implements the push-down machinery.
///
/// The rewriter starts at the top of the filter expression and traverses the expression tree. For
/// each (sub-)expression, the rewriter checks whether it only refers to one side of the join. If
/// this is never the case, no subexpressions of the filter can be pushed down. If there is a
/// subexpression that can be computed using only one side of the join, the entire subexpression is
/// pushed down to the join side.
struct JoinFilterRewriter<'a> {
    join_side: JoinSide,
    join_side_schema: &'a Schema,
    join_side_projections: Vec<(Arc<dyn PhysicalExpr>, String)>,
    intermediate_column_indices: Vec<ColumnIndex>,
    original_filter_column_count: usize,
    alias_generator: &'a AliasGenerator,
}

impl<'a> JoinFilterRewriter<'a> {
    /// Creates a new [JoinFilterRewriter].
    fn new(
        join_side: JoinSide,
        join_side_schema: &'a Schema,
        column_indices: Vec<ColumnIndex>,
        alias_generator: &'a AliasGenerator,
    ) -> Self {
        let projections = join_side_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                (
                    Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                    field.name().to_string(),
                )
            })
            .collect();

        Self {
            join_side,
            join_side_schema,
            join_side_projections: projections,
            original_filter_column_count: column_indices.len(),
            intermediate_column_indices: column_indices,
            alias_generator,
        }
    }

    /// Executes the push-down machinery on `expr`.
    ///
    /// See the [JoinFilterRewriter] for further information.
    fn rewrite(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        // A lambda is evaluated by its enclosing high-order function. Keep its
        // body in that scope; a complete high-order function may still be moved.
        if expr.is::<LambdaExpr>() {
            return Ok(Transformed::no(expr));
        }
        let depends_on_this_side = self.depends_on_join_side(&expr, self.join_side)?;
        // We don't push down things that do not depend on this side (other side or no side).
        if !depends_on_this_side {
            return Ok(Transformed::no(expr));
        }

        // Recurse if there is a dependency to both sides or if the entire expression is volatile.
        let depends_on_other_side =
            self.depends_on_join_side(&expr, self.join_side.negate())?;
        if depends_on_other_side || is_volatile(&expr) {
            return expr.map_children(|expr| self.rewrite(expr));
        }

        // There is only a dependency on this side.

        // If this expression has no children, we do not push down, as it should already be a column
        // reference.
        if expr.children().is_empty() {
            return Ok(Transformed::no(expr));
        }

        // Otherwise, we push down a projection.
        let alias = self.alias_generator.next("join_proj_push_down");
        let idx = self.create_new_column(alias.clone(), expr)?;

        Ok(Transformed::yes(
            Arc::new(Column::new(&alias, idx)) as Arc<dyn PhysicalExpr>
        ))
    }

    /// Creates a new column in the current join side.
    fn create_new_column(
        &mut self,
        name: String,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<usize> {
        // First, add a new projection. The expression must be rewritten, as it is no longer
        // executed against the filter schema.
        let new_idx = self.join_side_projections.len();
        let rewritten_expr = remap_filter_expr(
            expr,
            self.original_filter_column_count,
            self.join_side_schema.fields().len(),
            |column| {
                let intermediate_column =
                    &self.intermediate_column_indices[column.index()];
                assert_eq!(intermediate_column.side, self.join_side);

                let join_side_index = intermediate_column.index;
                let field = self.join_side_schema.field(join_side_index);
                Ok(Column::new(field.name(), join_side_index))
            },
        )?;
        self.join_side_projections.push((rewritten_expr, name));

        // Then, update the column indices
        let new_intermediate_idx = self.intermediate_column_indices.len();
        let idx = ColumnIndex {
            index: new_idx,
            side: self.join_side,
        };
        self.intermediate_column_indices.push(idx);

        Ok(new_intermediate_idx)
    }

    /// Checks whether the expression references any columns from `join_side`.
    fn depends_on_join_side(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        join_side: JoinSide,
    ) -> Result<bool> {
        expr.exists(|expr| {
            Ok(expr.downcast_ref::<Column>().is_some_and(|c| {
                self.intermediate_column_indices[c.index()].side == join_side
            }))
        })
    }
}

/// Remaps captured columns and rebases lambda parameters when the input changes.
/// Parameters are appended after the input columns, including at nested lambda
/// scopes. Moving a complete high-order expression preserves their relative
/// offsets, so every parameter shifts by the change in the root input width.
fn remap_filter_expr(
    expr: Arc<dyn PhysicalExpr>,
    old_input_len: usize,
    new_input_len: usize,
    map_column: impl Fn(&Column) -> Result<Column>,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_up(|expr| {
        if let Some(column) = expr.downcast_ref::<Column>() {
            let new_column = map_column(column)?;
            if new_column != *column {
                return Ok(Transformed::yes(Arc::new(new_column) as _));
            }
        } else if let Some(variable) = expr.downcast_ref::<LambdaVariable>()
            && old_input_len != new_input_len
        {
            let rebased = rebase_lambda_variable(variable, old_input_len, new_input_len)?;
            return Ok(Transformed::yes(rebased));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

/// Shifts a lambda parameter by the change in input width, for the reason
/// [`remap_filter_expr`] documents.
fn rebase_lambda_variable(
    variable: &LambdaVariable,
    old_input_len: usize,
    new_input_len: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    let offset = variable.index().checked_sub(old_input_len).ok_or_else(|| {
        internal_datafusion_err!(
            "Lambda parameter {variable} precedes the {old_input_len} input columns"
        )
    })?;
    Ok(Arc::new(LambdaVariable::new(
        new_input_len + offset,
        Arc::clone(variable.field()),
    )))
}

/// Prefix for the columns this rule adds when it splits a projection.
const LEAF_COLUMN_PREFIX: &str = "__datafusion_leaf";

/// Name of the column the split adds for the leaf expression at `index`.
///
/// Single source of truth: the lower projection's alias and the reference the
/// outer projection is rewritten to must agree, or the rewrite silently points
/// at nothing.
fn leaf_column_name(index: usize) -> String {
    format!("{LEAF_COLUMN_PREFIX}_{index}")
}

/// Splits a `ProjectionExec` that mixes leaf expressions with ones that must
/// stay in place, moving the leaves into a projection the source absorbs.
///
/// `benefits_from_input_partitioning` is true as soon as any expression is
/// `KeepInPlace`, which both inserts a `RepartitionExec` below the projection
/// and stops that projection descending past it, so a `MoveTowardsLeafNodes`
/// expression sharing it never reaches the source. Splitting leaves the
/// computation above the repartition, still parallelised.
fn try_split_leaf_projection(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.downcast_ref::<ProjectionExec>() else {
        return Ok(None);
    };
    let Some(repartition) = projection.input().downcast_ref::<RepartitionExec>() else {
        return Ok(None);
    };
    // Hash and Range partitioning reference columns by index, which would have
    // to be remapped against the rewritten child schema. Round robin carries no
    // column references, and is what `EnsureRequirements` inserts here.
    if !matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_)) {
        return Ok(None);
    }

    let leaves = collect_leaf_exprs(projection.expr())?;
    if leaves.is_empty() {
        return Ok(None);
    }

    // Pass through only the child columns the outer expressions still need. A
    // column used *only* inside an extracted leaf must be dropped, otherwise the
    // source keeps reading it: for `s['a'] + 1` that would read the whole struct
    // alongside the extracted field, which is worse than not splitting at all.
    let child = repartition.input();
    let child_schema = child.schema();
    let passthrough_indices = collect_outer_columns(projection.expr(), &leaves)?;
    let mut lower_exprs = passthrough_indices
        .iter()
        .map(|&idx| {
            let field = child_schema.field(idx);
            ProjectionExpr {
                expr: Arc::new(Column::new(field.name(), idx)),
                alias: field.name().to_string(),
            }
        })
        .collect::<Vec<_>>();
    for (index, leaf) in leaves.iter().enumerate() {
        lower_exprs.push(ProjectionExpr {
            expr: Arc::clone(leaf),
            alias: leaf_column_name(index),
        });
    }
    let lower_projection = ProjectionExec::try_new(lower_exprs, Arc::clone(child))?;

    // Only commit the split if the child really absorbs the leaf projection.
    // Otherwise this would just add a projection that helps nobody.
    let Some(new_child) = child.try_swapping_with_projection(&lower_projection)? else {
        return Ok(None);
    };

    let new_exprs = projection
        .expr()
        .iter()
        .map(|proj_expr| {
            Ok(ProjectionExpr {
                expr: rewrite_outer_expr(
                    Arc::clone(&proj_expr.expr),
                    &leaves,
                    &passthrough_indices,
                    child_schema.fields().len(),
                )?,
                alias: proj_expr.alias.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // The child gained the leaf columns, so the repartition's properties have to
    // be recomputed rather than carried over.
    let new_repartition = Arc::clone(projection.input()).replace_children(
        vec![new_child],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )?;
    // `try_new` would derive the output schema from the expressions alone and
    // drop the metadata the planner set, which `schema_check` then rejects.
    Ok(Some(Arc::new(
        ProjectionExec::try_new_with_schema_metadata(
            new_exprs,
            new_repartition,
            projection.schema().as_ref(),
        )?,
    )))
}

/// Collects the distinct outermost [`ExpressionPlacement::MoveTowardsLeafNodes`]
/// sub-expressions of `exprs`.
///
/// Nested leaf expressions are not collected separately: `s['a']['b']` is one
/// unit to hand to the source, not two.
fn collect_leaf_exprs(exprs: &[ProjectionExpr]) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let mut leaves: Vec<Arc<dyn PhysicalExpr>> = vec![];
    for proj_expr in exprs {
        proj_expr.expr.apply(|node| {
            if node.placement() == ExpressionPlacement::MoveTowardsLeafNodes {
                if !leaves.iter().any(|leaf| leaf == node) {
                    leaves.push(Arc::clone(node));
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(leaves)
}

/// Collects the child column indices that `exprs` still reference *outside* the
/// extracted leaf sub-expressions, sorted and deduplicated.
///
/// Recursion stops at a leaf: columns used only inside one are supplied by the
/// lower projection itself and must not be passed through.
fn collect_outer_columns(
    exprs: &[ProjectionExpr],
    leaves: &[Arc<dyn PhysicalExpr>],
) -> Result<Vec<usize>> {
    let mut indices: Vec<usize> = vec![];
    for proj_expr in exprs {
        proj_expr.expr.apply(|node| {
            if leaves.iter().any(|leaf| leaf == node) {
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Some(column) = node.downcast_ref::<Column>() {
                indices.push(column.index());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    indices.sort_unstable();
    indices.dedup();
    Ok(indices)
}

/// Rewrites an outer projection expression against the split child schema:
/// every leaf sub-expression becomes a reference to the column the split added
/// for it, and every surviving column or lambda parameter is remapped to its
/// new index. `old_input_len` is the child's width before the split.
fn rewrite_outer_expr(
    expr: Arc<dyn PhysicalExpr>,
    leaves: &[Arc<dyn PhysicalExpr>],
    passthrough_indices: &[usize],
    old_input_len: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    // The leaf columns were appended after the passthrough ones.
    let first_leaf = passthrough_indices.len();
    let new_input_len = first_leaf + leaves.len();
    expr.transform_down(|node| {
        if let Some(pos) = leaves.iter().position(|leaf| leaf == &node) {
            let column = Column::new(&leaf_column_name(pos), first_leaf + pos);
            return Ok(Transformed::new(
                Arc::new(column) as Arc<dyn PhysicalExpr>,
                true,
                TreeNodeRecursion::Jump,
            ));
        }
        if let Some(column) = node.downcast_ref::<Column>() {
            let new_index = passthrough_indices
                .iter()
                .position(|&idx| idx == column.index())
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "projection split dropped column {} that is still referenced",
                        column.index()
                    )
                })?;
            if new_index != column.index() {
                let remapped = Column::new(column.name(), new_index);
                return Ok(Transformed::yes(Arc::new(remapped) as Arc<dyn PhysicalExpr>));
            }
        }
        // The split widens the input, which moves the lambda parameters indexed
        // after it; see `remap_filter_expr`.
        if let Some(variable) = node.downcast_ref::<LambdaVariable>()
            && old_input_len != new_input_len
        {
            let rebased = rebase_lambda_variable(variable, old_input_len, new_input_len)?;
            return Ok(Transformed::yes(rebased));
        }
        Ok(Transformed::no(node))
    })
    .data()
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use datafusion_expr_common::operator::Operator;
    use datafusion_functions::math::random;
    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_expr::expressions::{binary, lit};
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_plan::displayable;
    use datafusion_physical_plan::empty::EmptyExec;
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[tokio::test]
    async fn no_computation_does_not_project() -> Result<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            a_x(),
            None,
            a_greater_than_x,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=a@0 > x@1
          EmptyExec
          EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn simple_push_down() -> Result<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            a_x(),
            None,
            a_plus_one_greater_than_x_plus_one,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[a@0, x@2]
          ProjectionExec: expr=[a@0 as a, a@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, x@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn does_not_push_down_short_circuiting_expressions() -> Result<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            a_x(),
            None,
            |schema| {
                binary(
                    lit(false),
                    Operator::And,
                    a_plus_one_greater_than_x_plus_one(schema)?,
                    schema,
                )
            },
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=false AND join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[a@0, x@2]
          ProjectionExec: expr=[a@0 as a, a@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, x@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn does_not_push_down_volatile_functions() -> Result<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            a_x(),
            None,
            a_plus_rand_greater_than_x,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=a@0 + rand() > x@1
          EmptyExec
          EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn complex_schema_push_down() -> Result<()> {
        let (left_schema, right_schema) = create_complex_schemas();

        let optimized_plan = run_test(
            left_schema,
            right_schema,
            a_b_x_z(),
            None,
            a_plus_b_greater_than_x_plus_z,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[a@0, b@1, c@2, x@4, y@5, z@6]
          ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c, a@0 + b@1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, y@1 as y, z@2 as z, x@0 + z@2 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn push_down_with_existing_projections() -> Result<()> {
        let (left_schema, right_schema) = create_complex_schemas();

        let optimized_plan = run_test(
            left_schema,
            right_schema,
            a_b_x_z(),
            Some(vec![1, 3, 5]), // ("b", "x", "z")
            a_plus_b_greater_than_x_plus_z,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[b@1, x@4, z@6]
          ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c, a@0 + b@1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, y@1 as y, z@2 as z, x@0 + z@2 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn left_semi_join_projection() -> Result<()> {
        let (left_schema, right_schema) = create_simple_schemas();

        let left_semi_join_plan = run_test(
            left_schema.clone(),
            right_schema.clone(),
            a_x(),
            None,
            a_plus_one_greater_than_x_plus_one,
            JoinType::LeftSemi,
        )?;

        assert_snapshot!(left_semi_join_plan, @r"
        NestedLoopJoinExec: join_type=LeftSemi, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[a@0]
          ProjectionExec: expr=[a@0 as a, a@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, x@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn right_semi_join_projection() -> Result<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let right_semi_join_plan = run_test(
            left_schema,
            right_schema,
            a_x(),
            None,
            a_plus_one_greater_than_x_plus_one,
            JoinType::RightSemi,
        )?;
        assert_snapshot!(right_semi_join_plan, @r"
        NestedLoopJoinExec: join_type=RightSemi, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[x@0]
          ProjectionExec: expr=[a@0 as a, a@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, x@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    fn run_test(
        left_schema: Schema,
        right_schema: Schema,
        column_indices: Vec<ColumnIndex>,
        existing_projections: Option<Vec<usize>>,
        filter_expr_builder: impl FnOnce(&Schema) -> Result<Arc<dyn PhysicalExpr>>,
        join_type: JoinType,
    ) -> Result<String> {
        let left = Arc::new(EmptyExec::new(Arc::new(left_schema.clone())));
        let right = Arc::new(EmptyExec::new(Arc::new(right_schema.clone())));

        let join_fields: Vec<_> = column_indices
            .iter()
            .map(|ci| match ci.side {
                JoinSide::Left => left_schema.field(ci.index).clone(),
                JoinSide::Right => right_schema.field(ci.index).clone(),
                JoinSide::None => unreachable!(),
            })
            .collect();
        let join_schema = Arc::new(Schema::new(join_fields));

        let filter_expr = filter_expr_builder(join_schema.as_ref())?;

        let join_filter = JoinFilter::new(filter_expr, column_indices, join_schema);

        let join = NestedLoopJoinExec::try_new(
            left,
            right,
            Some(join_filter),
            &join_type,
            existing_projections,
        )?;

        let optimizer = ProjectionPushdown::new();
        let optimized_plan = optimizer.optimize(Arc::new(join), &Default::default())?;

        let displayable_plan = displayable(optimized_plan.as_ref()).indent(false);
        Ok(displayable_plan.to_string())
    }

    fn create_simple_schemas() -> (Schema, Schema) {
        let left_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let right_schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);

        (left_schema, right_schema)
    }

    fn create_complex_schemas() -> (Schema, Schema) {
        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("z", DataType::Int32, false),
        ]);

        (left_schema, right_schema)
    }

    fn a_x() -> Vec<ColumnIndex> {
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ]
    }

    fn a_b_x_z() -> Vec<ColumnIndex> {
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ]
    }

    fn a_plus_one_greater_than_x_plus_one(
        join_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let left_expr = binary(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            lit(1),
            join_schema,
        )?;
        let right_expr = binary(
            Arc::new(Column::new("x", 1)),
            Operator::Plus,
            lit(1),
            join_schema,
        )?;
        binary(left_expr, Operator::Gt, right_expr, join_schema)
    }

    fn a_plus_rand_greater_than_x(join_schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        let left_expr = binary(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(ScalarFunctionExpr::new(
                "rand",
                random(),
                vec![],
                FieldRef::new(Field::new("out", DataType::Float64, false)),
                Arc::new(ConfigOptions::default()),
            )),
            join_schema,
        )?;
        let right_expr = Arc::new(Column::new("x", 1));
        binary(left_expr, Operator::Gt, right_expr, join_schema)
    }

    fn a_greater_than_x(join_schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        binary(
            Arc::new(Column::new("a", 0)),
            Operator::Gt,
            Arc::new(Column::new("x", 1)),
            join_schema,
        )
    }

    fn a_plus_b_greater_than_x_plus_z(
        join_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let lhs = binary(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("b", 1)),
            join_schema,
        )?;
        let rhs = binary(
            Arc::new(Column::new("x", 2)),
            Operator::Plus,
            Arc::new(Column::new("z", 3)),
            join_schema,
        )?;
        binary(lhs, Operator::Gt, rhs, join_schema)
    }
}
