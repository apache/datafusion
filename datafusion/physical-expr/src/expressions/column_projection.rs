// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Helpers to evaluate expressions against a batch that only retains the
//! input columns those expressions use.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};

use crate::expressions::{Column, LambdaVariable, Literal};
use crate::{PhysicalExpr, ScalarFunctionExpr};

/// The set of input columns used by a group of expressions.
///
/// This is used to avoid filtering input columns that are never read when an
/// expression evaluates its children on a subset of the input rows. Filtering
/// these columns is wasteful since for a record batch of `n` rows, filtering
/// requires at worst a copy of `n - 1` values per array. If these filtered
/// values will never be accessed, the time spent producing them is better
/// avoided.
///
/// For example, when evaluating `CASE WHEN B > 10 THEN D ELSE NULL END` over
/// the input columns `[A, B, C, D]`, only `B` and `D` are read. Projecting the
/// batch with the projection vector `[1, 3]` before filtering avoids copying
/// `A` and `C`. Because projecting changes the column indices to `[0, 1]`, the
/// expressions must be rewritten with [`Self::project_expr`] from
/// `CASE WHEN B@1 > 10 THEN D@3 ELSE NULL END` to
/// `CASE WHEN B@0 > 10 THEN D@1 ELSE NULL END` before they are evaluated
/// against the projected batch.
///
/// When `supports_projection` is false, evaluation must use the original
/// expressions and the full batch because an expression may depend on input
/// columns that are not visible in its children.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct ColumnProjection {
    /// The sorted indices of the input columns used by the expressions.
    projection: Vec<usize>,
    /// Whether the rewritten expressions are safe to evaluate against a
    /// projected batch.
    supports_projection: bool,
}

impl ColumnProjection {
    /// Determines the set of columns that are used in all of `exprs`.
    pub(super) fn new<'a>(
        exprs: impl IntoIterator<Item = &'a Arc<dyn PhysicalExpr>>,
    ) -> Self {
        // Use an ordered set so lambda variables continue to be positioned after columns
        let mut used_column_indices = BTreeSet::<usize>::new();
        let mut supports_projection = true;
        for expr in exprs {
            expr.apply(|expr| {
                if let Some(column) = expr.downcast_ref::<Column>() {
                    used_column_indices.insert(column.index());
                } else if let Some(lambda_variable) =
                    expr.downcast_ref::<LambdaVariable>()
                {
                    used_column_indices.insert(lambda_variable.index());
                } else if expr.downcast_ref::<Literal>().is_none()
                    && expr.downcast_ref::<ScalarFunctionExpr>().is_none()
                    && expr.children().is_empty()
                {
                    // Unknown leaves may read input columns without exposing a Column child.
                    supports_projection = false;
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("Closure cannot fail");
        }

        Self {
            projection: used_column_indices.into_iter().collect(),
            supports_projection,
        }
    }

    /// Rewrites the column references of `expr` so it can be evaluated against
    /// a batch projected with [`Self::projection_for`].
    ///
    /// `expr` must be one of the expressions this projection was created from.
    pub(super) fn project_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        let projected_index = |original: usize| {
            self.projection
                .binary_search(&original)
                .expect("column must be part of the projection")
        };

        Arc::clone(expr)
            .transform_down(|e| {
                if let Some(column) = e.downcast_ref::<Column>() {
                    let projected = projected_index(column.index());
                    if projected != column.index() {
                        return Ok(Transformed::yes(Arc::new(Column::new(
                            column.name(),
                            projected,
                        ))));
                    }
                } else if let Some(lambda_variable) = e.downcast_ref::<LambdaVariable>() {
                    let projected = projected_index(lambda_variable.index());
                    if projected != lambda_variable.index() {
                        return Ok(Transformed::yes(Arc::new(LambdaVariable::new(
                            projected,
                            Arc::clone(lambda_variable.field()),
                        ))));
                    }
                }
                Ok(Transformed::no(e))
            })
            .expect("Closure cannot fail")
            .data
    }

    /// Returns a projection when evaluating the projected expressions can avoid
    /// unused input columns of `batch`.
    pub(super) fn projection_for(&self, batch: &RecordBatch) -> Option<Vec<usize>> {
        if !self.supports_projection {
            return None;
        }
        let projection = self
            .projection
            .iter()
            .copied()
            .filter(|index| *index < batch.num_columns())
            .collect::<Vec<_>>();
        (projection.len() < batch.num_columns()).then_some(projection)
    }

    #[cfg(test)]
    pub(super) fn projection(&self) -> &[usize] {
        &self.projection
    }

    #[cfg(test)]
    pub(super) fn supports_projection(&self) -> bool {
        self.supports_projection
    }
}
