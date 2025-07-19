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

//! Self-join elimination optimizers
//!
//! This module contains optimizers that eliminate redundant self-joins:
//! - [`EliminateUniqueKeyedSelfJoin`]: Eliminates self-joins on unique constraints
//! - [`EliminateSelfJoinAggregation`]: Converts self-joins with aggregations to window functions

use std::sync::Arc;

use datafusion_common::{
    tree_node::{Transformed, TreeNode},
    Column, DFSchema, Dependency, Result, TableReference,
};
use datafusion_expr::{expr::Alias, Expr, LogicalPlan, TableScan};

mod aggregation;
mod unique_keyed;

pub use aggregation::EliminateSelfJoinAggregation;
use datafusion_common::tree_node::TransformedResult;
use indexmap::IndexSet;
pub use unique_keyed::EliminateUniqueKeyedSelfJoin;

/// Merges two table scans into a single scan that covers all columns and filters from both.
fn merge_table_scans(left_scan: &TableScan, right_scan: &TableScan) -> TableScan {
    let filters = left_scan
        .filters
        .iter()
        .chain(right_scan.filters.iter())
        .cloned()
        .collect();
    let projection = match (&left_scan.projection, &right_scan.projection) {
        (Some(left_projection), Some(right_projection)) => {
            // Compute the union of projections, maintaining sorted order
            let mut union: Vec<usize> = left_projection
                .iter()
                .chain(right_projection.iter())
                .cloned()
                .collect::<IndexSet<_>>() // Remove duplicates while preserving encounter order
                .into_iter()
                .collect();
            union.sort_unstable(); // Sort to ensure consistent column ordering
            Some(union)
        }
        (Some(left_projection), None) => Some(left_projection.clone()),
        (None, Some(right_projection)) => Some(right_projection.clone()),
        (None, None) => None,
    };
    let fetch = match (left_scan.fetch, right_scan.fetch) {
        (Some(left_fetch), Some(right_fetch)) => Some(left_fetch.max(right_fetch)),
        (Some(rows), None) | (None, Some(rows)) => Some(rows),
        (None, None) => None,
    };
    TableScan::try_new(
        left_scan.table_name.clone(),
        Arc::clone(&left_scan.source),
        projection,
        filters,
        fetch,
    )
    .unwrap()
}

/// Checks if two table scans reference the same underlying table.
///
/// This is the basic requirement for a self-join - both sides must scan the same table.
fn is_table_scan_same(left: &TableScan, right: &TableScan) -> bool {
    left.table_name == right.table_name
}

/// Extracts unique indexes (primary keys and unique constraints) from a schema.
///
/// Returns a vector of index sets, where each set represents the column indices
/// that form a unique constraint on the table.
///
/// # Example
/// For a table with columns [id, email, name] where id is PRIMARY KEY and email is UNIQUE:
/// Returns: [{0}, {1}]
fn unique_indexes(schema: &DFSchema) -> Vec<IndexSet<usize>> {
    schema
        .functional_dependencies()
        .iter()
        .filter(|dep| dep.mode == Dependency::Single)
        .map(|dep| dep.source_indices.iter().cloned().collect::<IndexSet<_>>())
        .collect::<Vec<_>>()
}

/// Tracks table alias renaming during self-join elimination.
///
/// When eliminating a self-join like `a JOIN b`, we need to rename references
/// from the eliminated side (e.g., `b.column`) to the preserved side (e.g., `a.column`).
#[derive(Debug, Clone)]
struct RenamedAlias {
    from: TableReference,
    to: TableReference,
}

impl RenamedAlias {
    /// Rewrites expressions to use the new table alias.
    ///
    /// # Behavior
    /// - Column references like `b.column` are rewritten to `a.column`
    /// - Top-level expressions are aliased to preserve the original name
    ///   (e.g., `b.amount` becomes `a.amount AS b.amount`)
    fn rewrite_expression(&self, expr: Expr) -> Result<Transformed<Expr>> {
        let mut is_top_level = true;
        expr.transform(|expr| {
            let result = match expr {
                Expr::Column(Column {
                    relation: Some(relation),
                    name,
                    spans,
                }) if relation == self.from => {
                    let col = Expr::Column(Column {
                        relation: Some(self.to.clone()),
                        name: name.clone(),
                        spans,
                    });
                    if is_top_level {
                        let alias = Expr::Alias(Alias {
                            expr: col.into(),
                            metadata: None,
                            name,
                            relation: Some(self.from.clone()),
                        });
                        Ok(Transformed::yes(alias))
                    } else {
                        Ok(Transformed::yes(col))
                    }
                }
                _ => Ok(Transformed::no(expr)),
            };
            if is_top_level {
                is_top_level = false;
            }
            result
        })
    }

    /// Recursively rewrites all expressions in a logical plan to use the new table alias.
    ///
    /// Walks through the entire plan tree and updates all column references from
    /// the eliminated table alias to the preserved one.
    fn rewrite_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        plan.transform_down(|plan| {
            let Transformed {
                data: plan,
                transformed,
                ..
            } = plan.map_expressions(|expr| self.rewrite_expression(expr))?;
            if transformed {
                Ok(Transformed::yes(plan.recompute_schema().unwrap()))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }
}

/// Result of a self-join elimination optimization.
///
/// Contains the optimized plan and any table alias renaming that needs to be
/// propagated to parent nodes.
#[derive(Debug, Clone)]
struct OptimizationResult {
    /// The optimized logical plan with the self-join eliminated
    plan: LogicalPlan,
    /// Optional alias renaming to apply to parent expressions
    renamed_alias: Option<RenamedAlias>,
}
