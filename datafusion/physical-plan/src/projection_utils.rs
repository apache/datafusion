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

use crate::projection::ProjectionExec;

use super::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::Result;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::PhysicalExpr;

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
