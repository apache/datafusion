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

//! [`CteFilterPusher`] optimizer rule — pushes OR-combined filters from
//! CTE readers into the materialized CTE body to reduce materialization volume.

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::logical_plan::{
    Extension, LogicalPlan, MaterializedCteProducer, MaterializedCteReader,
};
use datafusion_expr::{BinaryExpr, Expr, Filter, Operator};

/// Optimizer rule that pushes OR-combined filters from materialized CTE
/// readers back into the CTE body.
///
/// When a materialized CTE has multiple readers, each with filters above them,
/// this rule OR-combines those filters and pushes the result into the CTE plan.
/// This reduces the amount of data materialized without breaking sharing semantics.
///
/// Example: CTE `inv` referenced as `inv1` with `d_moy=4` and `inv2` with `d_moy=5`.
/// This rule pushes `(d_moy=4 OR d_moy=5)` into the CTE body, so only months 4
/// and 5 are materialized instead of all 12 months.
///
/// Inspired by DuckDB's CTE Filter Pusher optimization.
#[derive(Debug, Default)]
pub struct CteFilterPusher {}

impl CteFilterPusher {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for CteFilterPusher {
    fn name(&self) -> &str {
        "cte_filter_pusher"
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

        plan.transform_down(|node| {
            let LogicalPlan::Extension(Extension { node: ext }) = &node else {
                return Ok(Transformed::no(node));
            };

            let Some(producer) = ext.as_any().downcast_ref::<MaterializedCteProducer>()
            else {
                return Ok(Transformed::no(node));
            };

            // Collect filters above each reader in the continuation
            let reader_filters =
                collect_reader_filters(&producer.continuation, &producer.name);

            // All readers must have at least one filter for pushdown to be useful
            if reader_filters.is_empty()
                || reader_filters.iter().any(|filters| filters.is_empty())
            {
                return Ok(Transformed::no(node));
            }

            // OR-combine: each reader's filters are AND-combined first,
            // then groups are OR-combined across readers
            let per_reader_predicates: Vec<Expr> = reader_filters
                .into_iter()
                .map(|filters| {
                    filters
                        .into_iter()
                        .reduce(|a, b| {
                            Expr::BinaryExpr(BinaryExpr::new(
                                Box::new(a),
                                Operator::And,
                                Box::new(b),
                            ))
                        })
                        .unwrap()
                })
                .collect();

            let combined = per_reader_predicates
                .into_iter()
                .reduce(|a, b| {
                    Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(a),
                        Operator::Or,
                        Box::new(b),
                    ))
                })
                .unwrap();

            // Remap column references: the filters use continuation-side qualifiers
            // (e.g., "inv1.d_moy"), but the CTE plan has its own schema.
            // We need to strip qualifiers to match the CTE's unqualified columns.
            let combined = strip_qualifiers(combined);

            // Verify all columns in the combined filter exist in the CTE schema
            let cte_schema = producer.cte_plan.schema();
            let filter_cols = combined.column_refs();
            let all_cols_valid = filter_cols
                .iter()
                .all(|col| cte_schema.has_column_with_unqualified_name(col.name()));

            if !all_cols_valid {
                return Ok(Transformed::no(node));
            }

            // Push the combined filter into the CTE plan
            let new_cte_plan = LogicalPlan::Filter(Filter::try_new(
                combined,
                Arc::clone(&producer.cte_plan),
            )?);

            let new_producer = MaterializedCteProducer {
                name: producer.name.clone(),
                cte_plan: Arc::new(new_cte_plan),
                continuation: Arc::clone(&producer.continuation),
                schema: Arc::clone(&producer.schema),
                force_materialized: producer.force_materialized,
            };

            Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(new_producer),
            })))
        })
    }
}

/// Collect filter predicates that sit above each MaterializedCteReader
/// for the given CTE name. Returns one Vec<Expr> per reader found.
fn collect_reader_filters(plan: &LogicalPlan, cte_name: &str) -> Vec<Vec<Expr>> {
    let mut results: Vec<Vec<Expr>> = Vec::new();
    collect_reader_filters_recursive(plan, cte_name, &[], &mut results);
    results
}

fn collect_reader_filters_recursive(
    plan: &LogicalPlan,
    cte_name: &str,
    pending_filters: &[Expr],
    results: &mut Vec<Vec<Expr>>,
) {
    // If we hit a reader for this CTE, record the accumulated filters
    if let LogicalPlan::Extension(Extension { node: ext }) = plan
        && let Some(reader) = ext.as_any().downcast_ref::<MaterializedCteReader>()
        && reader.name == cte_name
    {
        results.push(pending_filters.to_vec());
        return;
    }

    // If we hit a Filter node, accumulate its predicates
    if let LogicalPlan::Filter(filter) = plan {
        let mut new_filters = pending_filters.to_vec();
        new_filters.push(filter.predicate.clone());
        for input in plan.inputs() {
            collect_reader_filters_recursive(input, cte_name, &new_filters, results);
        }
        return;
    }

    // For other nodes, continue recursing (reset filters at multi-child boundaries
    // since filters above a join apply to the join output, not individual inputs)
    let inputs = plan.inputs();
    if inputs.len() == 1 {
        // Single-child: propagate pending filters through
        collect_reader_filters_recursive(inputs[0], cte_name, pending_filters, results);
    } else {
        // Multi-child (joins, unions): filters above don't apply to specific children
        for input in inputs {
            collect_reader_filters_recursive(input, cte_name, &[], results);
        }
    }
}

/// Strip table qualifiers from column references in an expression.
/// CTE readers have qualified columns (e.g., "inv1.d_moy") but the CTE plan
/// uses unqualified names (e.g., "d_moy").
fn strip_qualifiers(expr: Expr) -> Expr {
    expr.transform(|e| {
        if let Expr::Column(mut col) = e {
            col.relation = None;
            Ok(Transformed::yes(Expr::Column(col)))
        } else {
            Ok(Transformed::no(e))
        }
    })
    .unwrap()
    .data
}
