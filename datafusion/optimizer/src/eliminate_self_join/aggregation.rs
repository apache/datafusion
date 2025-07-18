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

//! [`EliminateSelfJoinAggregation`] eliminates aggregation expressions
//! over self joins that can be translated to window expressions.
//!
//! # Overview
//!
//! This optimizer transforms self-join queries with aggregations into more efficient
//! window function queries.
//!
//! # Example Transformation
//!
//! ```sql
//! -- Original query (self-join with aggregation)
//! SELECT a.user_id, a.purchase_date, SUM(b.amount) AS running_total
//! FROM purchases a
//! JOIN purchases b ON a.user_id = b.user_id
//!                  AND b.purchase_date <= a.purchase_date
//! GROUP BY a.user_id, a.purchase_date;
//!
//! -- Optimized query (window function)
//! SELECT user_id, purchase_date,
//!        SUM(amount) OVER (
//!          PARTITION BY user_id, purchase_date
//!          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
//!        ) AS running_total
//! FROM purchases;
//! ```

use std::sync::Arc;

use super::{is_table_scan_same, merge_table_scans, unique_indexes, RenamedAlias};
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeContainer};
use datafusion_common::{Column, DFSchema, Result, ScalarValue, TableReference};
use datafusion_expr::expr::{
    AggregateFunction, Alias, Sort, WindowFunction, WindowFunctionParams,
};
use datafusion_expr::{
    Aggregate, BinaryExpr, Expr, Join, LogicalPlan, Operator, Projection, SubqueryAlias,
    TableScan, Window, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};

use indexmap::IndexSet;

#[derive(Default, Debug)]
pub struct EliminateSelfJoinAggregation;

impl OptimizerRule for EliminateSelfJoinAggregation {
    fn name(&self) -> &str {
        "eliminate_self_join_to_window"
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
        } = plan.transform_up(|plan| Self::eliminate_inner(plan, &mut renamed))?;

        if transformed {
            if let Some(renamed) = renamed {
                let plan = renamed.rewrite_logical_plan(plan)?;
                Ok(Transformed::yes(plan))
            } else {
                Ok(Transformed::yes(plan))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

impl EliminateSelfJoinAggregation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }

    /// Core transformation logic that converts join+aggregate to window function
    ///
    /// Looks for this pattern in the logical plan:
    /// ```text
    /// Projection
    ///   └── Aggregate (with single aggregate function)
    ///       └── Join (inner join with filter)
    ///           ├── TableScan or SubqueryAlias(TableScan)
    ///           └── TableScan or SubqueryAlias(TableScan)
    /// ```
    fn eliminate_inner(
        plan: LogicalPlan,
        renamed: &mut Option<RenamedAlias>,
    ) -> Result<Transformed<LogicalPlan>> {
        // Only process Projection nodes
        let LogicalPlan::Projection(projection) = &plan else {
            // TODO: We can eliminate this condition as Projection might not always
            //       emerge in some cases where this optimization is still possible
            return Ok(Transformed::no(plan));
        };

        // Check if the input is an Aggregate with single aggregate expression
        let aggregate = match projection.input.as_ref() {
            LogicalPlan::Aggregate(aggregate)
            // TODO: We can generalize this restriction
            if aggregate.aggr_expr.len() == 1 => {
                aggregate
            }
            _ => return Ok(Transformed::no(plan)),
        };

        // Try to optimize the aggregate into a window function
        let Some(OptimizationResult {
            window,
            renamed_alias,
        }) = try_replace_with_window(aggregate)
        else {
            return Ok(Transformed::no(plan));
        };

        // Update column references in the projection to use window function output
        let aggr_expr_name = aggregate.aggr_expr[0].name_for_alias()?;
        let window_expr_name = window.window_expr[0].name_for_alias()?;

        // Map aggregate column references to window column references
        let projection_expr = projection
            .expr
            .iter()
            .cloned()
            .map(|expr| match &expr {
                // Direct column reference: sum(b.amount) -> window_expr_output
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
                // Aliased column: sum(b.amount) AS total -> window_expr_output AS total
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
            .map(|expr| {
                // Apply any table alias renaming (e.g., b -> a)
                if let Some(renamed_alias) = &renamed_alias {
                    renamed_alias.rewrite_expression(expr).unwrap().data
                } else {
                    expr
                }
            })
            .collect::<Vec<_>>();
        *renamed = renamed_alias;

        let window = LogicalPlan::Window(window).into();
        let projection = Projection::try_new(projection_expr, window)?;
        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    }
}

/// Given [`Expr`] try to narrow enum variants to comparison expression between
/// two columns.
///
/// # Example
///
/// - `b.purchase_date <= a.purchase_date` -> Some((b.purchase_date, <=, a.purchase_date))
/// - `b.amount + 1 <= a.amount` -> None (not column-to-column comparison)
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

/// Represents the window frame bounds based on the comparison operator
struct OrderBound {
    sort_order: SortOrder,
    start_bound: WindowFrameBound,
    end_bound: WindowFrameBound,
}

/// Converts comparison operators to window frame specifications
///
/// # Mappings
///
/// - `<` : ORDER BY ASC, ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
/// - `<=`: ORDER BY ASC, ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
/// - `>` : ORDER BY DESC, ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
/// - `>=`: ORDER BY DESC, ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
///
/// # Example
///
/// For `b.date <= a.date`:
/// - Sort order: Ascending (earliest to latest)
/// - Frame: All rows from start up to and including current row
/// - This gives us cumulative/running total behavior
fn operator_to_order_bound(op: Operator) -> OrderBound {
    match op {
        Operator::Lt => OrderBound {
            sort_order: SortOrder::Ascending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)), // UNBOUNDED
            end_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))), // Exclude current
        },
        Operator::LtEq => OrderBound {
            sort_order: SortOrder::Ascending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)), // UNBOUNDED
            end_bound: WindowFrameBound::CurrentRow, // Include current
        },
        Operator::Gt => OrderBound {
            sort_order: SortOrder::Descending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)), // UNBOUNDED
            end_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))), // Exclude current
        },
        Operator::GtEq => OrderBound {
            sort_order: SortOrder::Descending,
            start_bound: WindowFrameBound::Preceding(ScalarValue::UInt64(None)), // UNBOUNDED
            end_bound: WindowFrameBound::CurrentRow, // Include current
        },
        _ => {
            unreachable!("`operator_to_order_bound` called with non-comparison operator")
        }
    }
}

#[derive(Debug)]
struct SelfJoinBranch<'a> {
    // TODO: Alias may not be `Option` as alias is required to make query unambiguous
    alias: Option<&'a TableReference>,
    /// Backing table
    table_scan: &'a TableScan,
}

/// Given [`LogicalPlan`], try to narrow it to a [`TableScan`] and optionally [`SubqueryAlias`].
/// Otherwise query might include nodes that may make this optimization impossible.
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

/// Attempts to convert join+aggregate pattern to window function
///
/// # Algorithm
///
/// 1. **Validate Join Pattern**: Ensure the join has a filter with comparison operators (<=, <, >=, >)
/// 2. **Extract Table Information**: Verify both sides of join reference the same table (self-join)
/// 3. **Analyze Filter**: Extract comparison columns and verify they:
///    - Come from different sides of the join (e.g., a.date <= b.date)
///    - Refer to the same underlying column in the table
/// 4. **Validate GROUP BY**: Ensure GROUP BY matches JOIN ON columns + filter column
/// 5. **Check Uniqueness**: Ensure JOIN ON doesn't form a unique constraint (would be different optimization)
/// 6. **Transform to Window**: Convert aggregate to window function with appropriate framing
///
/// # Example Transformation
///
/// Input:
/// ```text
/// Aggregate: groupBy=[[a.user_id, a.purchase_date]], aggr=[[sum(b.amount)]]
///   Join: a.user_id = b.user_id, Filter: b.purchase_date <= a.purchase_date
///     SubqueryAlias: a
///       TableScan: purchases
///     SubqueryAlias: b
///       TableScan: purchases
/// ```
///
/// Output:
/// ```text
/// Window: sum(amount) PARTITION BY [user_id, purchase_date]
///         ORDER BY [purchase_date ASC]
///         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
///   SubqueryAlias: a
///     TableScan: purchases
/// ```
fn try_replace_with_window(
    Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    }: &Aggregate,
) -> Option<OptimizationResult> {
    // Step 1: Check if input is a join with a filter
    let LogicalPlan::Join(
        join @ Join {
            filter: Some(ref join_filter),
            ..
        },
    ) = input.as_ref()
    else {
        return None;
    };

    // Step 2: Extract table information from both sides of the join
    let left = try_narrow_join_to_table_scan_alias(join.left.as_ref())?;
    let right = try_narrow_join_to_table_scan_alias(&join.right.as_ref())?;

    // Step 3: Verify it's a self-join (same table on both sides)
    if !is_table_scan_same(left.table_scan, right.table_scan) {
        return None;
    }

    // Step 4: Analyze the filter to extract comparison
    // Example: b.purchase_date <= a.purchase_date
    // Note: left_filter_col/right_filter_col refer to left/right sides of the comparison operator,
    // NOT the left/right sides of the join
    let (left_filter_col, op, right_filter_col) =
        try_narrow_filter_to_column_comparison(join_filter)?;

    // Step 4a: Determine which side of the join each filter column belongs to
    // We need to handle 4 cases for each column:
    // 1. Column has alias matching left side (e.g., a.purchase_date where left alias is "a")
    // 2. Column has alias matching right side (e.g., b.purchase_date where right alias is "b")
    // 3. Column has table name qualifier (e.g., purchases.purchase_date)
    // 4. Column has no qualifier (e.g., purchase_date) - ambiguous in self-join
    //
    // Example scenarios:
    // - "a JOIN b ON ... WHERE a.date <= b.date" -> valid (different sides)
    // - "purchases JOIN purchases b ON ... WHERE purchases.date <= b.date" -> valid
    // - "a JOIN b ON ... WHERE a.date <= a.date" -> invalid (same side)
    // - "a JOIN b ON ... WHERE date <= other_date" -> invalid (ambiguous)
    
    // Since it's a self-join, both sides have the same schema
    let table_schema = left.table_scan.projected_schema.as_ref();
    
    // Helper closure to resolve which side a column belongs to and get its unqualified name
    // Returns (unqualified_column_name, is_from_left_side)
    let resolve_column_side = |col: &Column| -> Option<(&str, bool)> {
        if let Some(relation) = col.relation.as_ref() {
            // Column has a qualifier - determine which side it belongs to
            if left.alias.is_some() && Some(relation) == left.alias {
                // Matches left alias (e.g., a.column)
                Some((col.name(), true)) // true = left side
            } else if right.alias.is_some() && Some(relation) == right.alias {
                // Matches right alias (e.g., b.column)
                Some((col.name(), false)) // false = right side
            } else if relation == &left.table_scan.table_name {
                // Matches table name - could be either side in a self-join
                // If left has no alias, it belongs to left; if right has no alias, it belongs to right
                if left.alias.is_none() {
                    Some((col.name(), true))
                } else if right.alias.is_none() {
                    Some((col.name(), false))
                } else {
                    // Both sides have aliases but column uses table name - ambiguous
                    None
                }
            } else {
                // Unknown qualifier
                None
            }
        } else {
            // No qualifier - ambiguous in self-join context
            None
        }
    };
    
    // Resolve both filter columns
    let (left_col_name, left_is_left_side) = resolve_column_side(left_filter_col)?;
    let (right_col_name, right_is_right_side) = resolve_column_side(right_filter_col)?;
    
    // Step 4b: Verify the filter compares columns from different sides
    // For a valid self-join filter like "b.date <= a.date", one column should be from
    // the left side and one from the right side
    if left_is_left_side == right_is_right_side {
        // Both columns are from the same side - invalid filter for this optimization
        return None;
    }
    
    // Step 4c: Get column indexes and verify they refer to the same underlying column
    let left_filter_idx = table_schema
        .index_of_column_by_name(None, left_col_name)?;
    let right_filter_idx = table_schema
        .index_of_column_by_name(None, right_col_name)?;

    // Since it's a self-join, both sides should have the same schema,
    // so the same column should have the same index
    if left_filter_idx != right_filter_idx {
        return None;
    }
    
    // At this point, we've verified:
    // - The filter compares the same column from different sides of the join
    // - We have the column index (left_filter_idx == right_filter_idx) for use in later steps

        // Step 5: Extract column indexes from GROUP BY
    // Example: GROUP BY a.user_id, a.purchase_date -> [0, 1]
    let mut group_by_idx = IndexSet::with_capacity(group_expr.len());
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

    // Step 6: Extract column indexes from JOIN ON
    // Example: JOIN ON a.user_id = b.user_id -> [0]
    let mut on_idx = IndexSet::with_capacity(join.on.len());
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

    // Step 7: Check if JOIN ON forms a unique constraint
    // If it does, this should be handled by EliminateUniqueKeyedSelfJoin instead
    let forms_unique_constraint = unique_indexes(schema)
        .iter()
        .any(|unique_constraint| on_idx.is_superset(unique_constraint));
    if forms_unique_constraint {
        return None;
    }

    // Step 8: Validate GROUP BY columns
    // GROUP BY must include all JOIN ON columns plus the filter column
    let on_and_filter = on_idx
        .iter()
        .chain(Some(left_filter_idx).iter())
        .cloned()
        .collect::<IndexSet<_>>();
    if on_and_filter != group_by_idx {
        return None;
    }

    // Step 9: Transform filter operator to window frame specification
    let OrderBound {
        sort_order,
        start_bound,
        end_bound,
    } = operator_to_order_bound(op);

    // Create ORDER BY clause for window function
    let sort_col = Expr::Column(Column::new_unqualified(&left_filter_col.name));
    let order_by = vec![Sort::new(sort_col, sort_order.is_asc(), false)];

    // Transform GROUP BY columns to PARTITION BY columns (remove qualifiers)
    let partition_by = group_expr
        .iter()
        .map(|expr| match expr {
            Expr::Column(Column { name, .. }) => {
                Expr::Column(Column::new_unqualified(name))
            }
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();

    // Step 10: Convert aggregate functions to window functions
    let mut window_expr = Vec::with_capacity(aggr_expr.len());
    for aggr_expr in aggr_expr {
        let AggregateFunction { func, params } = match aggr_expr {
            Expr::AggregateFunction(aggr_expr) => aggr_expr,
            _ => unreachable!("`Aggregate::aggr_expr` isn't a `Expr::AggregateFunction`"),
        };

        // Current limitations: no DISTINCT, filter, order by, or null treatment
        if params.distinct
            || params.filter.is_some()
            || !params.order_by.is_empty()
            || params.null_treatment.is_some()
        {
            return None;
        }

        // Remove qualifiers from aggregate arguments
        // Example: sum(b.amount) -> sum(amount)
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

        // Create window function with appropriate partitioning and ordering
        window_expr.push(Expr::WindowFunction(Box::new(WindowFunction {
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
        })));
    }

    // Step 11: Create the optimized plan with window function
    let table_scan = merge_table_scans(left.table_scan, right.table_scan);
    let mut plan = LogicalPlan::TableScan(table_scan);

    // Re-add eliminated aliases if any
    let renamed_alias = if let Some(left) = left.alias {
        let alias = SubqueryAlias::try_new(plan.into(), left.table()).unwrap();
        plan = LogicalPlan::SubqueryAlias(alias);
        // If right side had different alias, track the renaming
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::OptimizerContext;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, TableReference};
    use datafusion_expr::{
        col, logical_plan::builder::LogicalTableSource, JoinType, LogicalPlan,
        LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::count::count;
    use datafusion_functions_aggregate::sum::sum;

    fn create_table_scan(alias: Option<&str>) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("purchase_date", DataType::Date32, false),
            Field::new("amount", DataType::Float64, false),
        ]);
        let table_source = Arc::new(LogicalTableSource::new(Arc::new(schema)));

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            TableReference::bare("purchases"),
            table_source,
            None,
            vec![],
        )
        .unwrap();

        if let Some(alias) = alias {
            builder = builder.alias(alias).unwrap();
        }

        builder.build().unwrap()
    }

    fn optimize_plan(plan: LogicalPlan) -> Result<LogicalPlan> {
        let optimizer = EliminateSelfJoinAggregation::new();
        let config = OptimizerContext::default();
        let result = optimizer.rewrite(plan, &config)?;
        Ok(result.data)
    }

    #[test]
    fn test_eliminate_self_join_aggregation_less_than_equal() -> Result<()> {
        // Create self join with <= condition
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        // Build join with filter: b.purchase_date <= a.purchase_date
        let join_filter = col("b.purchase_date").lt_eq(col("a.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Window(_) => {
                        // Success - join was replaced with window function
                    }
                    _ => panic!(
                        "Expected Window after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_self_join_aggregation_less_than() -> Result<()> {
        // Create self join with < condition
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        // Build join with filter: b.purchase_date < a.purchase_date
        let join_filter = col("b.purchase_date").lt(col("a.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Window(_) => {
                        // Success - join was replaced with window function
                    }
                    _ => panic!(
                        "Expected Window after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_self_join_aggregation_greater_than_equal() -> Result<()> {
        // Create self join with >= condition
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        // Build join with filter: b.purchase_date >= a.purchase_date
        let join_filter = col("b.purchase_date").gt_eq(col("a.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Window(_) => {
                        // Success - join was replaced with window function
                    }
                    _ => panic!(
                        "Expected Window after optimization, got: {:?}",
                        proj.input
                    ),
                }
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_without_filter() -> Result<()> {
        // Create self join without filter
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                None, // No filter
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                let mut has_join = false;
                proj.input.apply(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        has_join = true;
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                })?;
                assert!(has_join, "Expected join to remain without filter");
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_wrong_group_by() -> Result<()> {
        // Create self join with filter but wrong GROUP BY columns
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_filter = col("b.purchase_date").lt_eq(col("a.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id")], // Missing purchase_date in GROUP BY
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                let mut has_join = false;
                proj.input.apply(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        has_join = true;
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                })?;
                assert!(has_join, "Expected join to remain with incorrect GROUP BY");
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_multiple_aggregates() -> Result<()> {
        // Create self join with multiple aggregate expressions
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_filter = col("b.purchase_date").lt_eq(col("a.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![
                    sum(col("b.amount")),
                    count(col("b.amount")), // Multiple aggregates
                ],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Current implementation only supports single aggregate
        match &optimized {
            LogicalPlan::Projection(proj) => {
                let mut has_join = false;
                proj.input.apply(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        has_join = true;
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                })?;
                assert!(has_join, "Expected join to remain with multiple aggregates");
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_different_filter_columns() -> Result<()> {
        // Create self join with filter on different columns
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        // Filter uses different columns than join
        let join_filter = col("b.amount").lt(col("a.amount"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                let mut has_join = false;
                proj.input.apply(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        has_join = true;
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                })?;
                assert!(
                    has_join,
                    "Expected join to remain with filter on different columns"
                );
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_wrong_join_type() -> Result<()> {
        // Create left join (not inner join)
        let left = create_table_scan(Some("a"));
        let right = create_table_scan(Some("b"));

        let join_filter = col("b.purchase_date").lt_eq(col("a.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Left, // Not inner join
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("a.purchase_date")],
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was NOT eliminated
        match &optimized {
            LogicalPlan::Projection(proj) => {
                let mut has_join = false;
                proj.input.apply(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        has_join = true;
                    }
                    Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
                })?;
                assert!(has_join, "Expected join to remain for non-inner join");
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }
}
