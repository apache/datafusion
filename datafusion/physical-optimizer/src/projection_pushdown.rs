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

//! This file implements the `ProjectionPushdown` physical optimization rule.
//! The function [`remove_unnecessary_projections`] tries to push down all
//! projections one by one if the operator below is amenable to this. If a
//! projection reaches a source, it can even disappear from the plan entirely.

use crate::PhysicalOptimizerRule;
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::alias::AliasGenerator;
use indexmap::IndexMap;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{JoinSide, JoinType, Result};
use datafusion_expr::ExpressionPlacement;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::projection::{
    ProjectionExec, remove_unnecessary_projections,
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
            .transform_up(|plan| {
                match plan.as_any().downcast_ref::<NestedLoopJoinExec>() {
                    None => Ok(Transformed::no(plan)),
                    Some(hash_join) => try_push_down_join_filter(
                        Arc::clone(&plan),
                        hash_join,
                        &alias_generator,
                    ),
                }
            })
            .map(|t| t.data)?;

        // First, try to split mixed projections (beneficial + non-beneficial expressions)
        // This allows the beneficial parts to be pushed down while keeping non-beneficial parts above.
        let plan = plan
            .transform_down(|plan| try_split_projection(plan, &alias_generator))
            .map(|t| t.data)?;

        // Then apply the normal projection pushdown logic
        plan.transform_down(remove_unnecessary_projections).data()
    }

    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Tries to split a projection to extract beneficial sub-expressions for pushdown.
///
/// This function walks each expression in the projection and extracts beneficial
/// sub-expressions (like `get_field`) from within larger non-beneficial expressions.
/// For example:
/// - Input: `get_field(col, 'foo') + 1`
/// - Output: Inner projection: `get_field(col, 'foo') AS __extracted_0`, Outer: `__extracted_0 + 1`
///
/// This enables the beneficial parts to be pushed down while keeping non-beneficial
/// expressions (like literals and computations) above.
fn try_split_projection(
    plan: Arc<dyn ExecutionPlan>,
    alias_generator: &AliasGenerator,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };

    let input_schema = projection.input().schema();
    let mut extractor =
        LeafExpressionExtractor::new(input_schema.as_ref(), alias_generator);

    // Extract leaf-pushable sub-expressions from each projection expression
    let mut outer_exprs = Vec::new();
    let mut has_extractions = false;

    for proj_expr in projection.expr() {
        // If this is already an expression from an extraction don't try to re-extract it (would cause infinite recursion)
        if proj_expr.alias.starts_with("__extracted") {
            outer_exprs.push(proj_expr.clone());
            continue;
        }

        // Only extract from root-level expressions. If the entire expression is
        // already PlaceAtLeaves (like `get_field(col, 'foo')`), it can be pushed as-is.
        // We only need to split when there's a root expression with leaf-pushable
        // sub-expressions (like `get_field(col, 'foo') + 1`).
        if matches!(
            proj_expr.expr.placement(),
            ExpressionPlacement::PlaceAtLeaves
        ) {
            outer_exprs.push(proj_expr.clone());
            continue;
        }

        let rewritten = extractor.extract(Arc::clone(&proj_expr.expr))?;
        if !Arc::ptr_eq(&rewritten, &proj_expr.expr) {
            has_extractions = true;
        }
        outer_exprs.push(ProjectionExpr::new(rewritten, proj_expr.alias.clone()));
    }

    if !has_extractions {
        return Ok(Transformed::no(plan));
    }

    // Collect columns needed by outer expressions that aren't extracted
    extractor.collect_columns_needed(&outer_exprs)?;

    // Build inner projection from extracted expressions + needed columns
    let inner_exprs = extractor.build_inner_projection()?;

    if inner_exprs.is_empty() {
        return Ok(Transformed::no(plan));
    }

    // Create the inner projection (to be pushed down)
    let inner = ProjectionExec::try_new(inner_exprs, Arc::clone(projection.input()))?;

    // Rewrite outer expressions to reference the inner projection's output schema
    let inner_schema = inner.schema();
    let final_outer_exprs = extractor.finalize_outer_exprs(outer_exprs, &inner_schema)?;

    // Create the outer projection (stays above)
    let outer = ProjectionExec::try_new(final_outer_exprs, Arc::new(inner))?;

    Ok(Transformed::yes(Arc::new(outer)))
}

/// Extracts beneficial leaf-pushable sub-expressions from larger expressions.
///
/// Similar to `JoinFilterRewriter`, this struct walks expression trees top-down
/// and extracts sub-expressions where `placement() == ExpressionPlacement::PlaceAtLeaves`
/// (beneficial leaf-pushable expressions like field accessors).
///
/// The extracted expressions are replaced with column references pointing to
/// an inner projection that computes these sub-expressions.
struct LeafExpressionExtractor<'a> {
    /// Extracted leaf-pushable expressions: maps expression -> alias
    extracted: IndexMap<Arc<dyn PhysicalExpr>, String>,
    /// Columns needed by outer expressions: maps input column index -> alias
    columns_needed: IndexMap<usize, String>,
    /// Input schema for the projection
    input_schema: &'a Schema,
    /// Alias generator for unique names
    alias_generator: &'a AliasGenerator,
}

impl<'a> LeafExpressionExtractor<'a> {
    fn new(input_schema: &'a Schema, alias_generator: &'a AliasGenerator) -> Self {
        Self {
            extracted: IndexMap::new(),
            columns_needed: IndexMap::new(),
            input_schema,
            alias_generator,
        }
    }

    /// Extracts beneficial leaf-pushable sub-expressions from the given expression.
    ///
    /// Walks the expression tree top-down and replaces beneficial leaf-pushable
    /// sub-expressions with column references to the inner projection.
    fn extract(&mut self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        // Top-down: check self first, then recurse to children
        if matches!(expr.placement(), ExpressionPlacement::PlaceAtLeaves) {
            // Extract this entire sub-tree
            return Ok(self.add_extracted_expr(expr));
        }

        // Not extractable at this level - recurse into children
        let children = expr.children();
        if children.is_empty() {
            return Ok(expr);
        }

        let mut new_children = Vec::with_capacity(children.len());
        let mut any_changed = false;

        for child in children {
            let new_child = self.extract(Arc::clone(child))?;
            if !Arc::ptr_eq(&new_child, child) {
                any_changed = true;
            }
            new_children.push(new_child);
        }

        if any_changed {
            expr.with_new_children(new_children)
        } else {
            Ok(expr)
        }
    }

    /// Adds an expression to the extracted set and returns a column reference.
    ///
    /// If the same expression was already extracted, reuses the existing alias.
    fn add_extracted_expr(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        if let Some(alias) = self.extracted.get(&expr) {
            // Already extracted - return a column reference
            // The index will be determined later in finalize
            Arc::new(Column::new(alias, 0)) as Arc<dyn PhysicalExpr>
        } else {
            // New extraction
            let alias = self.alias_generator.next("__extracted");
            self.extracted.insert(expr, alias.clone());
            Arc::new(Column::new(&alias, 0)) as Arc<dyn PhysicalExpr>
        }
    }

    /// Collects columns from outer expressions that need to be passed through inner projection.
    fn collect_columns_needed(&mut self, outer_exprs: &[ProjectionExpr]) -> Result<()> {
        for proj in outer_exprs {
            proj.expr.apply(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    // Check if this column references an extracted expression (by alias)
                    let is_extracted =
                        self.extracted.values().any(|alias| alias == col.name());

                    if !is_extracted && !self.columns_needed.contains_key(&col.index()) {
                        // This is an original input column - need to pass it through
                        let field = self.input_schema.field(col.index());
                        self.columns_needed
                            .insert(col.index(), field.name().clone());
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(())
    }

    /// Builds the inner projection expressions from extracted expressions and needed columns.
    fn build_inner_projection(&self) -> Result<Vec<ProjectionExpr>> {
        let mut result: Vec<ProjectionExpr> = self
            .extracted
            .iter()
            .map(|(expr, alias)| ProjectionExpr::new(Arc::clone(expr), alias.clone()))
            .collect();

        // Add columns needed by outer expressions
        for (&col_idx, alias) in &self.columns_needed {
            let field = self.input_schema.field(col_idx);
            result.push(ProjectionExpr::new(
                Arc::new(Column::new(field.name(), col_idx)),
                alias.clone(),
            ));
        }

        Ok(result)
    }

    /// Finalizes the outer expressions by fixing column indices to match the inner projection.
    fn finalize_outer_exprs(
        &self,
        outer_exprs: Vec<ProjectionExpr>,
        inner_schema: &Schema,
    ) -> Result<Vec<ProjectionExpr>> {
        // Build a map from alias name to index in inner projection
        let mut alias_to_idx: IndexMap<&str, usize> = self
            .extracted
            .values()
            .enumerate()
            .map(|(idx, alias)| (alias.as_str(), idx))
            .collect();

        // Add columns needed by outer expressions
        let base_idx = self.extracted.len();
        for (i, (_, alias)) in self.columns_needed.iter().enumerate() {
            alias_to_idx.insert(alias, base_idx + i);
        }

        // Build a map from original column index to inner projection index
        let mut col_idx_to_inner: IndexMap<usize, usize> = IndexMap::new();
        for (i, (&col_idx, _)) in self.columns_needed.iter().enumerate() {
            col_idx_to_inner.insert(col_idx, base_idx + i);
        }

        // Rewrite column references in outer expressions
        outer_exprs
            .into_iter()
            .map(|proj| {
                let new_expr = Arc::clone(&proj.expr)
                    .transform(|e| {
                        if let Some(col) = e.as_any().downcast_ref::<Column>() {
                            // First check if it's a reference to an extracted expression
                            if let Some(&idx) = alias_to_idx.get(col.name()) {
                                let field = inner_schema.field(idx);
                                return Ok(Transformed::yes(Arc::new(Column::new(
                                    field.name(),
                                    idx,
                                ))
                                    as Arc<dyn PhysicalExpr>));
                            }
                            // Then check if it's an original column that needs remapping
                            if let Some(&inner_idx) = col_idx_to_inner.get(&col.index()) {
                                let field = inner_schema.field(inner_idx);
                                return Ok(Transformed::yes(Arc::new(Column::new(
                                    field.name(),
                                    inner_idx,
                                ))
                                    as Arc<dyn PhysicalExpr>));
                            }
                        }
                        Ok(Transformed::no(e))
                    })?
                    .data;
                Ok(ProjectionExpr::new(new_expr, proj.alias))
            })
            .collect()
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
    );

    let new_lhs_length = lhs_rewrite.data.0.schema().fields.len();
    let projections = match projections {
        None => match join.join_type() {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                // Build projections that ignore the newly projected columns.
                let mut projections = Vec::new();
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

        let join_filter = JoinFilter::new(
            new_expr.data,
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
) -> JoinFilter {
    let mut used_columns = HashSet::new();
    expr.apply(|expr| {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            used_columns.insert(col.index());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Closure cannot fail");

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

    let final_expr = expr
        .transform_up(|expr| match expr.as_any().downcast_ref::<Column>() {
            None => Ok(Transformed::no(expr)),
            Some(column) => {
                let new_idx = used_columns
                    .iter()
                    .filter(|idx| **idx < column.index())
                    .count();
                let new_column = Column::new(column.name(), new_idx);
                Ok(Transformed::yes(
                    Arc::new(new_column) as Arc<dyn PhysicalExpr>
                ))
            }
        })
        .expect("Closure cannot fail");

    JoinFilter::new(
        final_expr.data,
        new_column_indices,
        Arc::new(Schema::new(fields)),
    )
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
        let depends_on_this_side = self.depends_on_join_side(&expr, self.join_side)?;
        // We don't push down things that do not depend on this side (other side or no side).
        if !depends_on_this_side {
            return Ok(Transformed::no(expr));
        }

        // Recurse if there is a dependency to both sides or if the entire expression is volatile.
        let depends_on_other_side =
            self.depends_on_join_side(&expr, self.join_side.negate())?;
        let is_volatile = is_volatile_expression_tree(expr.as_ref());
        if depends_on_other_side || is_volatile {
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
        let rewritten_expr = expr.transform_up(|expr| {
            Ok(match expr.as_any().downcast_ref::<Column>() {
                None => Transformed::no(expr),
                Some(column) => {
                    let intermediate_column =
                        &self.intermediate_column_indices[column.index()];
                    assert_eq!(intermediate_column.side, self.join_side);

                    let join_side_index = intermediate_column.index;
                    let field = self.join_side_schema.field(join_side_index);
                    let new_column = Column::new(field.name(), join_side_index);
                    Transformed::yes(Arc::new(new_column) as Arc<dyn PhysicalExpr>)
                }
            })
        })?;
        self.join_side_projections.push((rewritten_expr.data, name));

        // Then, update the column indices
        let new_intermediate_idx = self.intermediate_column_indices.len();
        let idx = ColumnIndex {
            index: new_idx,
            side: self.join_side,
        };
        self.intermediate_column_indices.push(idx);

        Ok(new_intermediate_idx)
    }

    /// Checks whether the entire expression depends on the given `join_side`.
    fn depends_on_join_side(
        &mut self,
        expr: &Arc<dyn PhysicalExpr>,
        join_side: JoinSide,
    ) -> Result<bool> {
        let mut result = false;
        expr.apply(|expr| match expr.as_any().downcast_ref::<Column>() {
            None => Ok(TreeNodeRecursion::Continue),
            Some(c) => {
                let column_index = &self.intermediate_column_indices[c.index()];
                if column_index.side == join_side {
                    result = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        Ok(result)
    }
}

fn is_volatile_expression_tree(expr: &dyn PhysicalExpr) -> bool {
    if expr.is_volatile_node() {
        return true;
    }

    expr.children()
        .iter()
        .map(|expr| is_volatile_expression_tree(expr.as_ref()))
        .reduce(|lhs, rhs| lhs || rhs)
        .unwrap_or(false)
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
