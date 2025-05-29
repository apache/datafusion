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

//! [`EliminateAggregationSelfJoin`] eliminates aggregation expressions
//! over self joins that can be translated to window expressions.

use std::{collections::HashSet, sync::Arc};

use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeContainer},
    Column, Result, ScalarValue, TableReference,
};
use datafusion_expr::{
    expr::{AggregateFunction, Alias, Sort, WindowFunction, WindowFunctionParams},
    Aggregate, BinaryExpr, Expr, Join, LogicalPlan, Operator, Projection, SubqueryAlias,
    TableScan, Window, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};

use super::{is_table_scan_same, merge_table_scans, unique_indexes, RenamedAlias};

#[derive(Default, Debug)]
pub struct EliminateAggregationSelfJoin;

impl EliminateAggregationSelfJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Given [`Expr`] try to narrow enum variants to comparison expression between
/// two columns.
fn try_narrow_filter_to_column_comparison(
    expr: &Expr,
) -> Option<(&Column, Operator, &Column)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if matches!(
                op,
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            ) =>
        {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(left), Expr::Column(right)) => Some((left, *op, right)),
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortOrder {
    Ascending,
    Descending,
}

impl SortOrder {
    fn is_asc(&self) -> bool {
        *self == SortOrder::Ascending
    }
}

struct OrderBound {
    sort_order: SortOrder,
    start_bound: WindowFrameBound,
    end_bound: WindowFrameBound,
}

fn operator_to_order_bound(op: Operator) -> OrderBound {
    match op {
        Operator::Lt => OrderBound {
            sort_order: SortOrder::Ascending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        },
        Operator::LtEq => OrderBound {
            sort_order: SortOrder::Ascending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::CurrentRow,
        },
        Operator::Gt => OrderBound {
            sort_order: SortOrder::Descending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
        },
        Operator::GtEq => OrderBound {
            sort_order: SortOrder::Descending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            end_bound: WindowFrameBound::CurrentRow,
        },
        _ => {
            unreachable!("`operator_to_order_bound` called with non-comparison operator")
        }
    }
}

#[derive(Debug)]
struct SelfJoinBranch<'a> {
    // TODO: alias may not be `Option` as alias is required to make query unambiguous
    alias: Option<&'a TableReference>,
    /// Backing table
    table_scan: &'a TableScan,
}

/// Given [`LogicalPlan`] try to narrow enum variants to a [`TableScan`] and optionally
/// [`SubqueryAlias`]. Otherwise query might include nodes that may make this optimization
/// impossible.
///
/// Display indent for logical plans are as follows.
///
/// ```text
/// SubqueryAlias: b
///     TableScan: purchases projection=[user_id, purchase_date, amount]
/// ```
fn try_narrow_join_to_table_scan_alias(branch: &LogicalPlan) -> Option<SelfJoinBranch> {
    match branch {
        LogicalPlan::TableScan(table_scan) => Some(SelfJoinBranch {
            alias: None,
            table_scan,
        }),
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            match input.as_ref() {
                LogicalPlan::TableScan(table_scan) => Some(SelfJoinBranch {
                    alias: Some(alias),
                    table_scan,
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Debug)]
struct OptimizationResult {
    window: Window,
    renamed_alias: Option<RenamedAlias>,
}

fn try_replace_with_window(
    Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    }: &Aggregate,
) -> Option<OptimizationResult> {
    let join = match input.as_ref() {
        LogicalPlan::Join(
            join @ Join {
                filter: Some(_), ..
            },
        ) => join,
        _ => return None,
    };
    let left = try_narrow_join_to_table_scan_alias(&join.left)?;
    let right = try_narrow_join_to_table_scan_alias(&join.right)?;

    // `TableScan`s have to be same for self-join
    if !is_table_scan_same(left.table_scan, right.table_scan) {
        return None;
    }
    let table_schema = left.table_scan.projected_schema.as_ref();

    // Filter expression should refer to the same column
    let (left_filter_col, op, right_filter_col) =
        try_narrow_filter_to_column_comparison(join.filter.as_ref().unwrap())?;
    let left_filter_idx = table_schema
        .index_of_column_by_name(None, left_filter_col.name())
        .unwrap();
    let right_filter_idx = table_schema
        .index_of_column_by_name(None, right_filter_col.name())
        .unwrap();
    if left_filter_idx != right_filter_idx {
        return None;
    }

    // Column indexes from `GROUP BY ...`
    let mut group_by_idx = HashSet::with_capacity(group_expr.len());
    for expr in group_expr {
        match expr {
            Expr::Column(Column { relation, name, .. }) => {
                // Each column of `GROUP BY ...` either doesn't have a `TableReference` or
                // it has to be LHS or RHS
                debug_assert!(
                    relation.is_none()
                        || relation.as_ref() == left.alias
                        || relation.as_ref() == right.alias
                );
                let idx = table_schema
                    .index_of_column_by_name(None, name.as_str())
                    .unwrap();
                group_by_idx.insert(idx);
            }
            // If `GROUP BY ...` expression isn't a column reference conservatively
            // assume it isn't self-join
            _ => return None,
        }
    }

    // Column indexes from `JOIN ON ...`
    let mut on_idx = HashSet::with_capacity(join.on.len());
    for on in &join.on {
        match on {
            (
                Expr::Column(Column {
                    name: left_name, ..
                }),
                Expr::Column(Column {
                    name: right_name, ..
                }),
            ) => {
                let left_idx = table_schema
                    .index_of_column_by_name(None, left_name.as_str())
                    .unwrap();
                let right_idx = table_schema
                    .index_of_column_by_name(None, right_name.as_str())
                    .unwrap();
                if left_idx != right_idx {
                    return None;
                }
                on_idx.insert(left_idx);
            }
            _ => return None,
        }
    }

    // If `JOIN ON ...` forms a unique constraint then it's invalid
    let forms_unique_constraint = unique_indexes(schema)
        .iter()
        .any(|unique_constraint| on_idx.is_superset(unique_constraint));
    if forms_unique_constraint {
        return None;
    }

    // `GROUP BY ...` columns should equal `JOIN ON ...` columns
    let on_and_filter = on_idx
        .iter()
        .chain(Some(left_filter_idx).iter())
        .cloned()
        .collect::<HashSet<_>>();
    if on_and_filter != group_by_idx {
        return None;
    }

    // Transform filter to sorting for window function
    let OrderBound {
        sort_order,
        start_bound,
        end_bound,
    } = operator_to_order_bound(op);
    let sort_col = Expr::Column(Column::new_unqualified(&left_filter_col.name));
    let order_by = vec![Sort::new(sort_col, sort_order.is_asc(), false)];

    // Transform maybe qualified columns from `GROUP BY ...` to `PARTITION BY ...`
    let partition_by = group_expr
        .iter()
        .map(|expr| match expr {
            Expr::Column(Column { name, .. }) => {
                Expr::Column(Column::new_unqualified(name))
            }
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();

    let mut window_expr = Vec::with_capacity(aggr_expr.len());
    for aggr_expr in aggr_expr {
        let AggregateFunction { func, params } = match aggr_expr {
            Expr::AggregateFunction(aggr_expr) => aggr_expr,
            _ => unreachable!("`Aggregate::aggr_expr` isn't a `Expr::AggregateFunction`"),
        };
        // TODO: try to handle different fields of `AggregateFunctionParams`
        if params.distinct
            || params.filter.is_some()
            || params.order_by.is_some()
            || params.null_treatment.is_some()
        {
            return None;
        }

        let Transformed { data: args, .. } = params
            .args
            .clone()
            .map_elements(|expr| match expr {
                Expr::Column(Column { name, .. }) => Ok(Transformed::yes(Expr::Column(
                    Column::new_unqualified(name),
                ))),
                _ => Ok(Transformed::no(expr)),
            })
            .unwrap();

        window_expr.push(Expr::WindowFunction(WindowFunction {
            fun: WindowFunctionDefinition::AggregateUDF(Arc::clone(func)),
            params: WindowFunctionParams {
                args,
                order_by: order_by.clone(),
                partition_by: partition_by.clone(),
                window_frame: WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    start_bound.clone(),
                    end_bound.clone(),
                ),
                null_treatment: None,
            },
        }));
    }

    let table_scan = merge_table_scans(left.table_scan, right.table_scan);
    let mut plan = LogicalPlan::TableScan(table_scan);
    // Readd eliminated aliases if any
    let renamed_alias = if let Some(left) = left.alias {
        let alias = SubqueryAlias::try_new(plan.into(), left.table()).unwrap();
        plan = LogicalPlan::SubqueryAlias(alias);
        right.alias.map(|right| RenamedAlias {
            from: right.clone(),
            to: left.clone(),
        })
    } else if let Some(table_reference) = right.alias {
        let alias = SubqueryAlias::try_new(plan.into(), table_reference.table()).unwrap();
        plan = LogicalPlan::SubqueryAlias(alias);
        None
    } else {
        None
    };
    let window = Window::try_new(window_expr, Arc::new(plan)).unwrap();

    Some(OptimizationResult {
        window,
        renamed_alias,
    })
}

impl OptimizerRule for EliminateAggregationSelfJoin {
    fn name(&self) -> &str {
        "eliminate_aggregation_self_join"
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
        } = plan.transform_up(|plan| {
            let projection = match plan {
                LogicalPlan::Projection(ref projection) => projection,
                _ => return Ok(Transformed::no(plan)),
            };
            let aggregate = match projection.input.as_ref() {
                LogicalPlan::Aggregate(aggregate) if aggregate.aggr_expr.len() == 1 => {
                    aggregate
                }
                _ => return Ok(Transformed::no(plan)),
            };
            let OptimizationResult {
                window,
                renamed_alias,
            } = match try_replace_with_window(aggregate) {
                Some(optimized) => optimized,
                None => return Ok(Transformed::no(plan)),
            };
            renamed = renamed_alias;

            let aggr_expr_name = aggregate.aggr_expr[0].name_for_alias()?;
            let window_expr_name = window.window_expr[0].name_for_alias()?;

            let projection_expr = projection
                .expr
                .iter()
                .cloned()
                .map(|expr| match &expr {
                    Expr::Column(Column {
                        relation: None,
                        name,
                        spans,
                    }) if name.as_str() == aggr_expr_name.as_str() => {
                        let alias = Alias {
                            expr: Box::new(Expr::Column(Column {
                                relation: None,
                                name: window_expr_name.clone(),
                                spans: spans.to_owned(),
                            })),
                            relation: None,
                            name: name.to_owned(),
                            metadata: None,
                        };
                        Expr::Alias(alias)
                    }
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        Expr::Column(Column {
                            relation: None,
                            name,
                            spans,
                        }) if name.as_str() == aggr_expr_name.as_str() => {
                            let alias = Alias {
                                expr: Box::new(Expr::Column(Column {
                                    relation: None,
                                    name: window_expr_name.clone(),
                                    spans: spans.to_owned(),
                                })),
                                relation: None,
                                name: alias.name.to_owned(),
                                metadata: None,
                            };
                            Expr::Alias(alias)
                        }
                        _ => expr,
                    },
                    _ => expr,
                })
                .collect::<Vec<_>>();

            let window = LogicalPlan::Window(window).into();
            let projection = Projection::try_new(projection_expr, window)?;
            Ok(Transformed::yes(LogicalPlan::Projection(projection)))
        })?;

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
