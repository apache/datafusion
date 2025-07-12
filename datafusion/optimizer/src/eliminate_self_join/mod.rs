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

use datafusion_common::{
    tree_node::{Transformed, TreeNode},
    Column, DFSchema, Dependency, Result, TableReference,
};
use datafusion_expr::{expr::Alias, Expr, LogicalPlan, TableScan};

mod aggregation;
mod unique_keyed;

pub use aggregation::EliminateAggregationSelfJoin;
use indexmap::IndexSet;
pub use unique_keyed::EliminateUniqueKeyedSelfJoin;

fn merge_table_scans(left_scan: &TableScan, right_scan: &TableScan) -> TableScan {
    let filters = left_scan
        .filters
        .iter()
        .chain(right_scan.filters.iter())
        .cloned()
        .collect();
    // FIXME: double iteration over the filters
    let projection = match (&left_scan.projection, &right_scan.projection) {
        (Some(left_projection), Some(right_projection)) => Some(
            left_projection
                .iter()
                .chain(right_projection.iter())
                .cloned()
                .collect::<IndexSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
        ),
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

// TODO: equality of `inner` `apachearrow::datatypes::SchemaRef` doesn't mean equality of the tables
fn is_table_scan_same(left: &TableScan, right: &TableScan) -> bool {
    // If the plans don't scan the same table then we cannot be sure for self-join
    left.table_name == right.table_name
}

fn unique_indexes(schema: &DFSchema) -> Vec<IndexSet<usize>> {
    schema
        .functional_dependencies()
        .iter()
        .filter(|dep| dep.mode == Dependency::Single)
        .map(|dep| dep.source_indices.iter().cloned().collect::<IndexSet<_>>())
        .collect::<Vec<_>>()
}

#[derive(Debug, Clone)]
struct RenamedAlias {
    from: TableReference,
    to: TableReference,
}

impl RenamedAlias {
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
                        Ok(Transformed::yes(dbg!(alias)))
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

    fn rewrite_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
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
    }
}

#[derive(Debug, Clone)]
struct OptimizationResult {
    plan: LogicalPlan,
    renamed_alias: Option<RenamedAlias>,
}
