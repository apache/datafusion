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

use std::sync::Arc;

use crate::joins::utils::{ColumnIndex, JoinFilter};
use crate::projection::ProjectionExec;

use super::ExecutionPlan;
use arrow_schema::SchemaRef;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{internal_err, JoinSide, Result};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

use itertools::Itertools;

/// Given the expression set of a projection, checks if the projection causes
/// any renaming or constructs a non-`Column` physical expression.
pub fn all_alias_free_columns(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> bool {
    exprs.iter().all(|(expr, alias)| {
        expr.as_any()
            .downcast_ref::<Column>()
            .map(|column| column.name() == alias)
            .unwrap_or(false)
    })
}

/// Updates a source provider's projected columns according to the given
/// projection operator's expressions. To use this function safely, one must
/// ensure that all expressions are `Column` expressions without aliases.
pub fn new_projections_for_columns(
    projection: &ProjectionExec,
    source: &[usize],
) -> Vec<usize> {
    projection
        .expr()
        .iter()
        .filter_map(|(expr, _)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|expr| source[expr.index()])
        })
        .collect()
}

/// Creates a new [`ProjectionExec`] instance with the given child plan and
/// projected expressions.
pub fn make_with_child(
    projection: &ProjectionExec,
    child: &Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    ProjectionExec::try_new(projection.expr().to_vec(), Arc::clone(child))
        .map(|e| Arc::new(e) as _)
}

/// Returns `true` if all the expressions in the argument are `Column`s.
pub fn all_columns(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> bool {
    exprs.iter().all(|(expr, _)| expr.as_any().is::<Column>())
}

/// The function operates in two modes:
///
/// 1) When `sync_with_child` is `true`:
///
///    The function updates the indices of `expr` if the expression resides
///    in the input plan. For instance, given the expressions `a@1 + b@2`
///    and `c@0` with the input schema `c@2, a@0, b@1`, the expressions are
///    updated to `a@0 + b@1` and `c@2`.
///
/// 2) When `sync_with_child` is `false`:
///
///    The function determines how the expression would be updated if a projection
///    was placed before the plan associated with the expression. If the expression
///    cannot be rewritten after the projection, it returns `None`. For example,
///    given the expressions `c@0`, `a@1` and `b@2`, and the [`ProjectionExec`] with
///    an output schema of `a, c_new`, then `c@0` becomes `c_new@1`, `a@1` becomes
///    `a@0`, but `b@2` results in `None` since the projection does not include `b`.
pub fn update_expr(
    expr: &Arc<dyn PhysicalExpr>,
    projected_exprs: &[(Arc<dyn PhysicalExpr>, String)],
    sync_with_child: bool,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    #[derive(Debug, PartialEq)]
    enum RewriteState {
        /// The expression is unchanged.
        Unchanged,
        /// Some part of the expression has been rewritten
        RewrittenValid,
        /// Some part of the expression has been rewritten, but some column
        /// references could not be.
        RewrittenInvalid,
    }

    let mut state = RewriteState::Unchanged;

    let new_expr = Arc::clone(expr)
        .transform_up(|expr: Arc<dyn PhysicalExpr>| {
            if state == RewriteState::RewrittenInvalid {
                return Ok(Transformed::no(expr));
            }

            let Some(column) = expr.as_any().downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            if sync_with_child {
                state = RewriteState::RewrittenValid;
                // Update the index of `column`:
                Ok(Transformed::yes(Arc::clone(
                    &projected_exprs[column.index()].0,
                )))
            } else {
                // default to invalid, in case we can't find the relevant column
                state = RewriteState::RewrittenInvalid;
                // Determine how to update `column` to accommodate `projected_exprs`
                projected_exprs
                    .iter()
                    .enumerate()
                    .find_map(|(index, (projected_expr, alias))| {
                        projected_expr.as_any().downcast_ref::<Column>().and_then(
                            |projected_column| {
                                (column.name().eq(projected_column.name())
                                    && column.index() == projected_column.index())
                                .then(|| {
                                    state = RewriteState::RewrittenValid;
                                    Arc::new(Column::new(alias, index)) as _
                                })
                            },
                        )
                    })
                    .map_or_else(
                        || Ok(Transformed::no(expr)),
                        |c| Ok(Transformed::yes(c)),
                    )
            }
        })
        .data();

    new_expr.map(|e| (state == RewriteState::RewrittenValid).then_some(e))
}

/// The on clause of the join, as vector of (left, right) columns.
pub type JoinOn = Vec<(PhysicalExprRef, PhysicalExprRef)>;
/// Reference for JoinOn.
pub type JoinOnRef<'a> = &'a [(PhysicalExprRef, PhysicalExprRef)];

pub struct JoinData {
    pub projected_left_child: ProjectionExec,
    pub projected_right_child: ProjectionExec,
    pub join_filter: Option<JoinFilter>,
    pub join_on: JoinOn,
}

/// Downcasts all the expressions in `exprs` to `Column`s. If any of the given
/// expressions is not a `Column`, returns `None`.
pub fn physical_to_column_exprs(
    exprs: &[(Arc<dyn PhysicalExpr>, String)],
) -> Option<Vec<(Column, String)>> {
    exprs
        .iter()
        .map(|(expr, alias)| {
            expr.as_any()
                .downcast_ref::<Column>()
                .map(|col| (col.clone(), alias.clone()))
        })
        .collect()
}

/// Some projection can't be pushed down left input or right input of hash join because filter or on need may need some columns that won't be used in later.
/// By embed those projection to hash join, we can reduce the cost of build_batch_from_indices in hash join (build_batch_from_indices need to can compute::take() for each column) and avoid unnecessary output creation.
pub fn try_embed_projection<Exec: EmbeddedProjection + 'static>(
    projection: &ProjectionExec,
    execution_plan: &Exec,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // Collect all column indices from the given projection expressions.
    let projection_index = collect_column_indices(projection.expr());

    if projection_index.is_empty() {
        return Ok(None);
    };

    // If the projection indices is the same as the input columns, we don't need to embed the projection to hash join.
    // Check the projection_index is 0..n-1 and the length of projection_index is the same as the length of execution_plan schema fields.
    if projection_index.len() == projection_index.last().unwrap() + 1
        && projection_index.len() == execution_plan.schema().fields().len()
    {
        return Ok(None);
    }

    let new_execution_plan =
        Arc::new(execution_plan.with_projection(Some(projection_index.to_vec()))?);

    // Build projection expressions for update_expr. Zip the projection_index with the new_execution_plan output schema fields.
    let embed_project_exprs = projection_index
        .iter()
        .zip(new_execution_plan.schema().fields())
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), *index)) as Arc<dyn PhysicalExpr>,
                field.name().to_owned(),
            )
        })
        .collect::<Vec<_>>();

    let mut new_projection_exprs = Vec::with_capacity(projection.expr().len());

    for (expr, alias) in projection.expr() {
        // update column index for projection expression since the input schema has been changed.
        let Some(expr) = update_expr(expr, embed_project_exprs.as_slice(), false)? else {
            return Ok(None);
        };
        new_projection_exprs.push((expr, alias.clone()));
    }
    // Old projection may contain some alias or expression such as `a + 1` and `CAST('true' AS BOOLEAN)`, but our projection_exprs in hash join just contain column, so we need to create the new projection to keep the original projection.
    let new_projection = Arc::new(ProjectionExec::try_new(
        new_projection_exprs,
        Arc::clone(&new_execution_plan) as _,
    )?);
    if is_projection_removable(&new_projection) {
        Ok(Some(new_execution_plan))
    } else {
        Ok(Some(new_projection))
    }
}

pub fn try_pushdown_through_join(
    projection: &ProjectionExec,
    join_left: &Arc<dyn ExecutionPlan>,
    join_right: &Arc<dyn ExecutionPlan>,
    join_on: JoinOnRef,
    schema: SchemaRef,
    filter: Option<&JoinFilter>,
) -> Result<Option<JoinData>> {
    // Convert projected expressions to columns. We can not proceed if this is not possible.
    let Some(projection_as_columns) = physical_to_column_exprs(projection.expr()) else {
        return Ok(None);
    };

    let (far_right_left_col_ind, far_left_right_col_ind) =
        join_table_borders(join_left.schema().fields().len(), &projection_as_columns);

    if !join_allows_pushdown(
        &projection_as_columns,
        &schema,
        far_right_left_col_ind,
        far_left_right_col_ind,
    ) {
        return Ok(None);
    }

    let new_filter = if let Some(filter) = filter {
        match update_join_filter(
            &projection_as_columns[0..=far_right_left_col_ind as _],
            &projection_as_columns[far_left_right_col_ind as _..],
            filter,
            join_left.schema().fields().len(),
        ) {
            Some(updated_filter) => Some(updated_filter),
            None => return Ok(None),
        }
    } else {
        None
    };

    let Some(new_on) = update_join_on(
        &projection_as_columns[0..=far_right_left_col_ind as _],
        &projection_as_columns[far_left_right_col_ind as _..],
        join_on,
        join_left.schema().fields().len(),
    ) else {
        return Ok(None);
    };

    let (new_left, new_right) = new_join_children(
        &projection_as_columns,
        far_right_left_col_ind,
        far_left_right_col_ind,
        join_left,
        join_right,
    )?;

    Ok(Some(JoinData {
        projected_left_child: new_left,
        projected_right_child: new_right,
        join_filter: new_filter,
        join_on: new_on,
    }))
}

/// If pushing down the projection over this join's children seems possible,
/// this function constructs the new [`ProjectionExec`]s that will come on top
/// of the original children of the join.
pub fn new_join_children(
    projection_as_columns: &[(Column, String)],
    far_right_left_col_ind: i32,
    far_left_right_col_ind: i32,
    left_child: &Arc<dyn ExecutionPlan>,
    right_child: &Arc<dyn ExecutionPlan>,
) -> Result<(ProjectionExec, ProjectionExec)> {
    let new_left = ProjectionExec::try_new(
        projection_as_columns[0..=far_right_left_col_ind as _]
            .iter()
            .map(|(col, alias)| {
                (
                    Arc::new(Column::new(col.name(), col.index())) as _,
                    alias.clone(),
                )
            })
            .collect_vec(),
        Arc::clone(left_child),
    )?;
    let left_size = left_child.schema().fields().len() as i32;
    let new_right = ProjectionExec::try_new(
        projection_as_columns[far_left_right_col_ind as _..]
            .iter()
            .map(|(col, alias)| {
                (
                    Arc::new(Column::new(
                        col.name(),
                        // Align projected expressions coming from the right
                        // table with the new right child projection:
                        (col.index() as i32 - left_size) as _,
                    )) as _,
                    alias.clone(),
                )
            })
            .collect_vec(),
        Arc::clone(right_child),
    )?;

    Ok((new_left, new_right))
}

/// Checks three conditions for pushing a projection down through a join:
/// - Projection must narrow the join output schema.
/// - Columns coming from left/right tables must be collected at the left/right
///   sides of the output table.
/// - Left or right table is not lost after the projection.
pub fn join_allows_pushdown(
    projection_as_columns: &[(Column, String)],
    join_schema: &SchemaRef,
    far_right_left_col_ind: i32,
    far_left_right_col_ind: i32,
) -> bool {
    // Projection must narrow the join output:
    projection_as_columns.len() < join_schema.fields().len()
    // Are the columns from different tables mixed?
    && (far_right_left_col_ind + 1 == far_left_right_col_ind)
    // Left or right table is not lost after the projection.
    && far_right_left_col_ind >= 0
    && far_left_right_col_ind < projection_as_columns.len() as i32
}

/// Returns the last index before encountering a column coming from the right table when traveling
/// through the projection from left to right, and the last index before encountering a column
/// coming from the left table when traveling through the projection from right to left.
/// If there is no column in the projection coming from the left side, it returns (-1, ...),
/// if there is no column in the projection coming from the right side, it returns (..., projection length).
pub fn join_table_borders(
    left_table_column_count: usize,
    projection_as_columns: &[(Column, String)],
) -> (i32, i32) {
    let far_right_left_col_ind = projection_as_columns
        .iter()
        .enumerate()
        .take_while(|(_, (projection_column, _))| {
            projection_column.index() < left_table_column_count
        })
        .last()
        .map(|(index, _)| index as i32)
        .unwrap_or(-1);

    let far_left_right_col_ind = projection_as_columns
        .iter()
        .enumerate()
        .rev()
        .take_while(|(_, (projection_column, _))| {
            projection_column.index() >= left_table_column_count
        })
        .last()
        .map(|(index, _)| index as i32)
        .unwrap_or(projection_as_columns.len() as i32);

    (far_right_left_col_ind, far_left_right_col_ind)
}

/// Tries to update the equi-join `Column`'s of a join as if the input of
/// the join was replaced by a projection.
pub fn update_join_on(
    proj_left_exprs: &[(Column, String)],
    proj_right_exprs: &[(Column, String)],
    hash_join_on: &[(PhysicalExprRef, PhysicalExprRef)],
    left_field_size: usize,
) -> Option<Vec<(PhysicalExprRef, PhysicalExprRef)>> {
    // TODO: Clippy wants the "map" call removed, but doing so generates
    //       a compilation error. Remove the clippy directive once this
    //       issue is fixed.
    #[allow(clippy::map_identity)]
    let (left_idx, right_idx): (Vec<_>, Vec<_>) = hash_join_on
        .iter()
        .map(|(left, right)| (left, right))
        .unzip();

    let new_left_columns = new_columns_for_join_on(&left_idx, proj_left_exprs, 0);
    let new_right_columns =
        new_columns_for_join_on(&right_idx, proj_right_exprs, left_field_size);

    match (new_left_columns, new_right_columns) {
        (Some(left), Some(right)) => Some(left.into_iter().zip(right).collect()),
        _ => None,
    }
}

/// Tries to update the column indices of a [`JoinFilter`] as if the input of
/// the join was replaced by a projection.
pub fn update_join_filter(
    projection_left_exprs: &[(Column, String)],
    projection_right_exprs: &[(Column, String)],
    join_filter: &JoinFilter,
    left_field_size: usize,
) -> Option<JoinFilter> {
    let mut new_left_indices = new_indices_for_join_filter(
        join_filter,
        JoinSide::Left,
        projection_left_exprs,
        0,
    )
    .into_iter();
    let mut new_right_indices = new_indices_for_join_filter(
        join_filter,
        JoinSide::Right,
        projection_right_exprs,
        left_field_size,
    )
    .into_iter();

    // Check if all columns match:
    (new_right_indices.len() + new_left_indices.len()
        == join_filter.column_indices().len())
    .then(|| {
        JoinFilter::new(
            Arc::clone(join_filter.expression()),
            join_filter
                .column_indices()
                .iter()
                .map(|col_idx| ColumnIndex {
                    index: if col_idx.side == JoinSide::Left {
                        new_left_indices.next().unwrap()
                    } else {
                        new_right_indices.next().unwrap()
                    },
                    side: col_idx.side,
                })
                .collect(),
            Arc::clone(join_filter.schema()),
        )
    })
}

pub trait EmbeddedProjection: ExecutionPlan + Sized {
    fn with_projection(&self, projection: Option<Vec<usize>>) -> Result<Self>;
}

/// Collect all column indices from the given projection expressions.
fn collect_column_indices(exprs: &[(Arc<dyn PhysicalExpr>, String)]) -> Vec<usize> {
    // Collect indices and remove duplicates.
    let mut indices = exprs
        .iter()
        .flat_map(|(expr, _)| collect_columns(expr))
        .map(|x| x.index())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    indices.sort();
    indices
}

/// This function determines and returns a vector of indices representing the
/// positions of columns in `projection_exprs` that are involved in `join_filter`,
/// and correspond to a particular side (`join_side`) of the join operation.
///
/// Notes: Column indices in the projection expressions are based on the join schema,
/// whereas the join filter is based on the join child schema. `column_index_offset`
/// represents the offset between them.
fn new_indices_for_join_filter(
    join_filter: &JoinFilter,
    join_side: JoinSide,
    projection_exprs: &[(Column, String)],
    column_index_offset: usize,
) -> Vec<usize> {
    join_filter
        .column_indices()
        .iter()
        .filter(|col_idx| col_idx.side == join_side)
        .filter_map(|col_idx| {
            projection_exprs
                .iter()
                .position(|(col, _)| col_idx.index + column_index_offset == col.index())
        })
        .collect()
}

/// Compare the inputs and outputs of the projection. All expressions must be
/// columns without alias, and projection does not change the order of fields.
/// For example, if the input schema is `a, b`, `SELECT a, b` is removable,
/// but `SELECT b, a` and `SELECT a+1, b` and `SELECT a AS c, b` are not.
fn is_projection_removable(projection: &ProjectionExec) -> bool {
    let exprs = projection.expr();
    exprs.iter().enumerate().all(|(idx, (expr, alias))| {
        let Some(col) = expr.as_any().downcast_ref::<Column>() else {
            return false;
        };
        col.name() == alias && col.index() == idx
    }) && exprs.len() == projection.input().schema().fields().len()
}

/// This function generates a new set of columns to be used in a hash join
/// operation based on a set of equi-join conditions (`hash_join_on`) and a
/// list of projection expressions (`projection_exprs`).
///
/// Notes: Column indices in the projection expressions are based on the join schema,
/// whereas the join on expressions are based on the join child schema. `column_index_offset`
/// represents the offset between them.
fn new_columns_for_join_on(
    hash_join_on: &[&PhysicalExprRef],
    projection_exprs: &[(Column, String)],
    column_index_offset: usize,
) -> Option<Vec<PhysicalExprRef>> {
    let new_columns = hash_join_on
        .iter()
        .filter_map(|on| {
            // Rewrite all columns in `on`
            Arc::clone(*on)
                .transform(|expr| {
                    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                        // Find the column in the projection expressions
                        let new_column = projection_exprs
                            .iter()
                            .enumerate()
                            .find(|(_, (proj_column, _))| {
                                column.name() == proj_column.name()
                                    && column.index() + column_index_offset
                                        == proj_column.index()
                            })
                            .map(|(index, (_, alias))| Column::new(alias, index));
                        if let Some(new_column) = new_column {
                            Ok(Transformed::yes(Arc::new(new_column)))
                        } else {
                            // If the column is not found in the projection expressions,
                            // it means that the column is not projected. In this case,
                            // we cannot push the projection down.
                            internal_err!(
                                "Column {:?} not found in projection expressions",
                                column
                            )
                        }
                    } else {
                        Ok(Transformed::no(expr))
                    }
                })
                .data()
                .ok()
        })
        .collect::<Vec<_>>();
    (new_columns.len() == hash_join_on.len()).then_some(new_columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use datafusion_common::ScalarValue;
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};

    #[test]
    fn test_collect_column_indices() -> Result<()> {
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("b", 7)),
            Operator::Minus,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
                Operator::Plus,
                Arc::new(Column::new("a", 1)),
            )),
        ));
        let column_indices = collect_column_indices(&[(expr, "b-(1+a)".to_string())]);
        assert_eq!(column_indices, vec![1, 7]);
        Ok(())
    }

    #[test]
    fn test_join_table_borders() -> Result<()> {
        let projections = vec![
            (Column::new("b", 1), "b".to_owned()),
            (Column::new("c", 2), "c".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("d", 3), "d".to_owned()),
            (Column::new("c", 2), "c".to_owned()),
            (Column::new("f", 5), "f".to_owned()),
            (Column::new("h", 7), "h".to_owned()),
            (Column::new("g", 6), "g".to_owned()),
        ];
        let left_table_column_count = 5;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (4, 5)
        );

        let left_table_column_count = 8;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (7, 8)
        );

        let left_table_column_count = 1;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (-1, 0)
        );

        let projections = vec![
            (Column::new("a", 0), "a".to_owned()),
            (Column::new("b", 1), "b".to_owned()),
            (Column::new("d", 3), "d".to_owned()),
            (Column::new("g", 6), "g".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("f", 5), "f".to_owned()),
            (Column::new("e", 4), "e".to_owned()),
            (Column::new("h", 7), "h".to_owned()),
        ];
        let left_table_column_count = 5;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (2, 7)
        );

        let left_table_column_count = 7;
        assert_eq!(
            join_table_borders(left_table_column_count, &projections),
            (6, 7)
        );

        Ok(())
    }
}
