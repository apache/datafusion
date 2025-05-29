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

use std::collections::HashSet;

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    Column, Result,
};
use datafusion_expr::{
    expr::Alias, Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder, Projection,
    SubqueryAlias, TableScan,
};

use super::{
    is_table_scan_same, merge_table_scans, unique_indexes, OptimizationResult,
    RenamedAlias,
};

#[derive(Default, Debug)]
pub struct EliminateUniqueKeyedSelfJoin;

impl EliminateUniqueKeyedSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
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

#[derive(Debug)]
struct Resolution {
    /// `TableScan`
    table_scan: TableScan,
    /// Column indexes into `TableScan` that form a unique index
    column_indexes: HashSet<usize>,
}

fn resolve_columns_to_indexes(
    branch: &LogicalPlan,
    mut columns: Vec<Column>,
) -> Resolution {
    let mut column_indexes = HashSet::with_capacity(columns.len());
    let mut scan = None;
    branch
        .apply_with_subqueries(|plan| match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { alias, .. }) => {
                columns.iter_mut().for_each(|item| match &item.relation {
                    Some(table_ref) if alias == table_ref => {
                        item.relation = None;
                    }
                    _ => {}
                });
                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::Projection(Projection { expr, schema, .. }) => {
                let mut aliases = Vec::with_capacity(columns.len());
                for col in &mut columns {
                    let Ok(idx) = schema.index_of_column(col) else {
                        continue;
                    };
                    match &expr[idx] {
                        Expr::Column(column) => {
                            aliases.push(column.clone());
                        }
                        Expr::Alias(Alias {
                            expr,
                            relation,
                            name,
                            ..
                        }) => {
                            assert!(
                                relation.is_none(),
                                "what to do with `Alias` relation"
                            );
                            assert_eq!(
                                name.as_str(),
                                col.name.as_str(),
                                "`Alias` and `Column` mismatch"
                            );
                            if let Expr::Column(column) = expr.as_ref() {
                                aliases.push(column.clone());
                            }
                        }
                        _ => {}
                    }
                }
                columns = aliases;
                Ok(TreeNodeRecursion::Continue)
            }
            LogicalPlan::TableScan(
                table_scan @ TableScan {
                    projected_schema, ..
                },
            ) => {
                let schema = projected_schema.as_ref();
                for col in &mut columns {
                    let idx = schema.index_of_column_by_name(None, col.name()).unwrap();
                    column_indexes.insert(idx);
                }
                scan = Some(table_scan.clone());
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        })
        .unwrap();

    let table_scan = scan.expect("Join children without a `TableScan`");

    Resolution {
        table_scan,
        column_indexes,
    }
}

fn is_join_on_unique_index(join: &Join) -> bool {
    let left_unique = unique_indexes(join.left.schema().as_ref());
    let right_unique = unique_indexes(join.right.schema().as_ref());
    // If either of the sides doesn't have a unique constraint then elimination is impossible
    if left_unique.is_empty() || right_unique.is_empty() {
        return false;
    }

    // Resolve join-on to table scan indexes
    let (left_on, right_on) = join
        .on
        .iter()
        .cloned()
        .map(|on| match on {
            (Expr::Column(left_col), Expr::Column(right_col)) => (left_col, right_col),
            _ => {
                unreachable!("Join condition is not a column equality");
            }
        })
        .collect::<(Vec<_>, Vec<_>)>();
    let left_resolved = resolve_columns_to_indexes(&join.left, left_on.clone());
    let right_resolved = resolve_columns_to_indexes(&join.right, right_on.clone());

    if !is_table_scan_same(&left_resolved.table_scan, &right_resolved.table_scan)
        && left_resolved.column_indexes == right_resolved.column_indexes
    {
        return false;
    }

    let left_unique_index = left_unique
        .iter()
        .find(|unique| left_resolved.column_indexes.is_superset(unique));
    let right_unique_index = right_unique
        .iter()
        .find(|unique| right_resolved.column_indexes.is_superset(unique));

    match (left_unique_index, right_unique_index) {
        (Some(left_unique_index), Some(right_unique_index)) => {
            left_unique_index == right_unique_index
        }
        _ => false,
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
        } = plan
            .transform_up(|plan| {
                match &plan {
                    LogicalPlan::Join(
                        join @ Join {
                            left,
                            right,
                            join_type,
                            filter: None,
                            ..
                        },
                    ) if *join_type == JoinType::Inner
                        && is_join_on_unique_index(join) =>
                    {
                        // If we reach here, it means we can eliminate the self join
                        if let Some(OptimizationResult {
                            plan,
                            renamed_alias,
                        }) = optimize(left.as_ref(), right.as_ref())
                        {
                            renamed = renamed_alias;
                            Ok(Transformed::yes(plan))
                        } else {
                            Ok(Transformed::no(plan))
                        }
                    }
                    // This is called `EliminateSelfJoin` after all
                    _ => Ok(Transformed::no(plan)),
                }
            })
            .unwrap();

        if transformed {
            if let Some(renamed) = renamed {
                let Transformed { data: plan, .. } =
                    renamed.rewrite_logical_plan(plan)?;
                Ok(Transformed::yes(plan))
            } else {
                Ok(Transformed::yes(plan))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
