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
//! window function queries when the GROUP BY columns form a unique key.
//!
//! # Example Transformations
//!
//! ## Basic Example
//! ```sql
//! -- Original query (self-join with aggregation)
//! SELECT a.order_id, SUM(b.amount) AS running_total
//! FROM orders a
//! JOIN orders b ON b.order_id <= a.order_id
//! GROUP BY a.order_id;
//!
//! -- Optimized query (window function)
//! SELECT order_id,
//!        SUM(amount) OVER (
//!          ORDER BY order_id
//!          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
//!        ) AS running_total
//! FROM orders;
//! ```
//!
//! # Requirements
//!
//! The optimization requires that GROUP BY columns form a unique constraint
//! (e.g., PRIMARY KEY or UNIQUE) to ensure correctness.

use std::sync::Arc;

use super::{is_table_scan_same, merge_table_scans, unique_indexes, RenamedAlias};
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeContainer};
use datafusion_common::{
    Column, FunctionalDependencies, JoinType, Result, ScalarValue, TableReference,
    ToDFSchema,
};
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
        } =
            plan.transform_up(|plan| Self::eliminate_inner(plan.clone(), &mut renamed))?;

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
    /// [Optional] Projection
    ///   └── Aggregate (with one or more aggregate functions)
    ///       └── Join (inner join with filter)
    ///           ├── TableScan or SubqueryAlias(TableScan)
    ///           └── TableScan or SubqueryAlias(TableScan)
    /// ```
    fn eliminate_inner(
        plan: LogicalPlan,
        renamed: &mut Option<RenamedAlias>,
    ) -> Result<Transformed<LogicalPlan>> {
        // Handle two cases: Projection -> Aggregate or just Aggregate
        let (aggregate, projection) = match &plan {
            LogicalPlan::Projection(projection) => match projection.input.as_ref() {
                LogicalPlan::Aggregate(aggregate) => (aggregate, Some(projection)),
                _ => return Ok(Transformed::no(plan)),
            },
            LogicalPlan::Aggregate(aggregate) => (aggregate, None),
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

        // Update column references based on whether we have a projection
        if let Some(projection) = projection {
            // Map aggregate column references to window column references
            let mut projection_expr = Vec::with_capacity(projection.expr.len());

            for expr in projection.expr.iter() {
                // Check each aggregate expression
                let mut matched = false;
                for (aggr_idx, aggr_expr) in aggregate.aggr_expr.iter().enumerate() {
                    let aggr_expr_name = aggr_expr.name_for_alias()?;
                    let window_expr_name =
                        window.window_expr[aggr_idx].name_for_alias()?;

                    let mapped_expr = match &expr {
                        // Direct column reference: sum(b.amount) -> window_expr_output
                        Expr::Column(Column {
                            relation: None,
                            name,
                            spans,
                        }) if name.as_str() == aggr_expr_name.as_str() => {
                            matched = true;
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
                                matched = true;
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
                            _ => expr.clone(),
                        },
                        _ => expr.clone(),
                    };

                    if matched {
                        projection_expr.push(mapped_expr);
                        break;
                    }
                }

                if !matched {
                    projection_expr.push(expr.clone());
                }
            }

            // Apply any table alias renaming (e.g., b -> a)
            if let Some(renamed_alias) = &renamed_alias {
                projection_expr = projection_expr
                    .into_iter()
                    .map(|expr| renamed_alias.rewrite_expression(expr).unwrap().data)
                    .collect();
            }
            *renamed = renamed_alias;

            let window = LogicalPlan::Window(window).into();
            let projection = Projection::try_new(projection_expr, window)?;
            Ok(Transformed::yes(LogicalPlan::Projection(projection)))
        } else {
            // No projection - return the window directly
            *renamed = renamed_alias;
            Ok(Transformed::yes(LogicalPlan::Window(window)))
        }
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
/// 5. **Check Uniqueness**: Verify GROUP BY columns form a unique constraint
/// 6. **Transform to Window**: Convert aggregate to window function with appropriate framing
///
/// # Example Transformation
///
/// Input:
/// ```text
/// Aggregate: groupBy=[[a.order_id]], aggr=[[sum(b.amount)]]
///   Join: Filter: b.order_id <= a.order_id
///     SubqueryAlias: a
///       TableScan: orders
///     SubqueryAlias: b
///       TableScan: orders
/// ```
///
/// Output:
/// ```text
/// Window: sum(amount) ORDER BY [order_id ASC]
///         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
///   SubqueryAlias: a
///     TableScan: orders
/// ```
///
/// # Correctness
///
/// This optimization is only safe when GROUP BY columns are unique. Without
/// uniqueness, the self-join can produce duplicate rows that would be incorrectly
/// aggregated by the window function.
fn try_replace_with_window(
    Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }: &Aggregate,
) -> Option<OptimizationResult> {
    // Step 1.1: Check if input is a join with a filter
    let LogicalPlan::Join(
        join @ Join {
            filter: Some(ref join_filter),
            ..
        },
    ) = input.as_ref()
    else {
        return None;
    };

    // Step 1.2: Verify it's an inner join
    // This optimization only applies to INNER joins
    if join.join_type != JoinType::Inner {
        return None;
    }

    // Step 2: Extract table information from both sides of the join
    let left = try_narrow_join_to_table_scan_alias(join.left.as_ref())?;
    let right = try_narrow_join_to_table_scan_alias(join.right.as_ref())?;

    // Step 3: Verify it's a self-join (same table on both sides)
    if !is_table_scan_same(left.table_scan, right.table_scan) {
        return None;
    }

    // Step 4.1: Analyze the filter to extract comparison
    // Example: b.purchase_date <= a.purchase_date
    // Note: left_filter_col/right_filter_col refer to left/right sides of the comparison operator,
    // NOT the left/right sides of the join
    let (left_filter_col, op, right_filter_col) =
        try_narrow_filter_to_column_comparison(join_filter)?;

    // Step 4.2: Determine which side of the join each filter column belongs to
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

    // Get schemas from both sides - they may have different projections
    // Example: SELECT a.id, a.date FROM t a JOIN (SELECT id, date, amount FROM t) b ON ...
    // Left schema: [id, date], Right schema: [id, date, amount]
    let left_schema = left.table_scan.projected_schema.as_ref();
    let right_schema = right.table_scan.projected_schema.as_ref();

    // Helper closure to resolve which side a column belongs to and get its unqualified name
    // Returns (unqualified_column_name, is_from_left_side)
    let resolve_column_side = |col: &Column| -> Option<(String, bool)> {
        if let Some(relation) = col.relation.as_ref() {
            // Column has a qualifier - determine which side it belongs to
            if left.alias.is_some() && Some(relation) == left.alias {
                // Matches left alias (e.g., a.column)
                Some((col.name().to_string(), true)) // true = left side
            } else if right.alias.is_some() && Some(relation) == right.alias {
                // Matches right alias (e.g., b.column)
                Some((col.name().to_string(), false)) // false = right side
            } else if relation == &left.table_scan.table_name {
                // Matches table name - could be either side in a self-join
                // If left has no alias, it belongs to left; if right has no alias, it belongs to right
                if left.alias.is_none() {
                    Some((col.name().to_string(), true))
                } else if right.alias.is_none() {
                    Some((col.name().to_string(), false))
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
    let (right_col_name, right_is_left_side) = resolve_column_side(right_filter_col)?;

    // Step 4.3: Verify the filter compares columns from different sides
    // For a valid self-join filter like "b.date <= a.date", one column should be from
    // the left side and one from the right side
    if left_is_left_side == right_is_left_side {
        // Both columns are from the same side - invalid filter for this optimization
        return None;
    }

    // Step 4.4: Verify both columns exist in their respective schemas and refer to the same base column
    // Verify the columns exist in their respective schemas
    if left_is_left_side {
        // left_filter_col is from the left side of the join
        left_schema.index_of_column_by_name(None, left_col_name.as_str())?;
    } else {
        // left_filter_col is from the right side of the join
        right_schema.index_of_column_by_name(None, left_col_name.as_str())?;
    }

    if right_is_left_side {
        // right_filter_col is from the left side of the join
        left_schema.index_of_column_by_name(None, right_col_name.as_str())?;
    } else {
        // right_filter_col is from the right side of the join
        right_schema.index_of_column_by_name(None, right_col_name.as_str())?;
    }

    // Verify both columns have the same name (they should refer to the same base column)
    if left_col_name != right_col_name {
        return None;
    }

    // At this point, we've verified:
    // - The filter compares the same column from different sides of the join
    // - Both columns exist in their respective projected schemas
    // - We have the column index in the left schema for use in later steps

    // Step 5: Extract column names from GROUP BY
    // Example: GROUP BY a.user_id, a.purchase_date -> ["user_id", "purchase_date"]
    let mut group_by_names = IndexSet::with_capacity(group_expr.len());
    let mut group_by_side = None; // Track which side of the join GROUP BY columns come from
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

                // Determine which side this column belongs to
                let column_side = if relation.is_none() {
                    // Unqualified column - ambiguous in self-join
                    return None;
                } else if relation.as_ref() == left.alias {
                    // Column from left side
                    true
                } else if relation.as_ref() == right.alias {
                    // Column from right side
                    false
                } else {
                    // Unknown qualifier
                    return None;
                };

                // All GROUP BY columns must come from the same side
                match group_by_side {
                    None => group_by_side = Some(column_side),
                    Some(side) if side != column_side => {
                        // GROUP BY columns come from different sides - cannot optimize
                        return None;
                    }
                    _ => {} // Same side, continue
                }

                // Verify the column exists in the appropriate schema
                let column_exists = if column_side {
                    left_schema.field_with_unqualified_name(name).is_ok()
                } else {
                    right_schema.field_with_unqualified_name(name).is_ok()
                };

                if !column_exists {
                    return None;
                }

                group_by_names.insert(name.as_str());
            }
            // If `GROUP BY ...` expression isn't a column reference conservatively
            // assume it isn't self-join
            _ => {
                return None;
            }
        }
    }

    // GROUP BY must be from the left side of the join for this optimization
    if group_by_side != Some(true) {
        return None;
    }

    // Step 6: Extract column names from JOIN ON
    // Example: JOIN ON a.user_id = b.user_id -> ["user_id"]
    let mut on_names = IndexSet::with_capacity(join.on.len());
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
                // In a self-join, the join columns should have the same name
                if left_name != right_name {
                    return None;
                }
                // Verify the column exists in both schemas
                if left_schema.field_with_unqualified_name(left_name).is_err()
                    || right_schema
                        .field_with_unqualified_name(right_name)
                        .is_err()
                {
                    return None;
                }
                on_names.insert(left_name.as_str());
            }
            _ => {
                return None;
            }
        }
    }

    // Step 7: Validate GROUP BY columns
    // GROUP BY must include all JOIN ON columns plus the filter column
    let on_and_filter = on_names
        .iter()
        .chain(Some(left_col_name.as_str()).iter())
        .cloned()
        .collect::<IndexSet<_>>();
    if on_and_filter != group_by_names {
        return None;
    }

    // Step 8: Check if GROUP BY columns form a unique constraint
    // This is critical for correctness - we can only apply this optimization
    // if the GROUP BY columns are guaranteed to be unique

    // Since GROUP BY uses left-side columns, get the left schema
    let left_base_schema = left.table_scan.source.schema();

    // Convert GROUP BY column names to indices in the left schema
    let mut group_by_indices = Vec::with_capacity(group_by_names.len());
    for col_name in &group_by_names {
        if let Some((_, field)) = left_base_schema.column_with_name(col_name) {
            if let Ok(idx) = left_base_schema.index_of(field.name()) {
                group_by_indices.push(idx);
            } else {
                return None;
            }
        } else {
            return None;
        }
    }
    let group_by_indices_set: IndexSet<usize> = group_by_indices.into_iter().collect();

    // Get unique constraints from the schema
    let fd_opt = left.table_scan.source.constraints().map(|c| {
        FunctionalDependencies::new_from_constraints(
            Some(c),
            left.table_scan.source.schema().fields.len(),
        )
    });

    let mut dfs = left_base_schema.to_dfschema().unwrap();
    if let Some(fd) = fd_opt {
        dfs = dfs.with_functional_dependencies(fd).ok()?;
    }
    let unique_constraints = unique_indexes(&dfs);

    // Check if GROUP BY columns form a subset of any unique constraint
    let has_unique_constraint = unique_constraints.iter().any(|unique_idx| {
        // GROUP BY columns must be a subset of a unique constraint
        group_by_indices_set.is_subset(unique_idx)
    });

    if !has_unique_constraint {
        // Cannot apply optimization without unique constraint on GROUP BY columns
        return None;
    }

    // Step 9: Transform filter operator to window frame specification
    let OrderBound {
        sort_order,
        start_bound,
        end_bound,
    } = operator_to_order_bound(op);

    // Create ORDER BY clause for window function
    // Use the unqualified column name since we're building a window over the merged table
    let sort_col = Expr::Column(Column::new_unqualified(left_col_name));
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
            .ok()?;

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
    use datafusion_common::{Constraint, Constraints, Result, TableReference};
    use datafusion_expr::{
        col, logical_plan::builder::LogicalTableSource, JoinType, LogicalPlan,
        LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::average::avg;
    use datafusion_functions_aggregate::count::count;
    use datafusion_functions_aggregate::min_max::{max, min};
    use datafusion_functions_aggregate::sum::sum;

    fn create_table_scan(alias: Option<&str>, unique: bool) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("user_id", DataType::Int32, false),
            Field::new("purchase_date", DataType::Date32, false),
            Field::new("amount", DataType::Float64, false),
        ]);

        // Create constraints - (user_id, purchase_date) is unique
        let constraints = Constraints::new_unverified(vec![
            Constraint::Unique(vec![0, 1]), // (user_id, purchase_date) is unique
        ]);

        let table_source = if unique {
            Arc::new(
                LogicalTableSource::new(Arc::new(schema)).with_constraints(constraints),
            )
        } else {
            Arc::new(LogicalTableSource::new(Arc::new(schema)))
        };

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

    fn create_table_scan_with_unique_constraint(alias: Option<&str>) -> LogicalPlan {
        let schema = Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
            Field::new("order_date", DataType::Date32, false),
            Field::new("amount", DataType::Float64, false),
        ]);

        // Create constraints - order_id is PRIMARY KEY (index 0)
        let constraints = Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]), // order_id is unique
        ]);

        let table_source = Arc::new(
            LogicalTableSource::new(Arc::new(schema)).with_constraints(constraints),
        );

        let mut builder = LogicalPlanBuilder::scan_with_filters(
            TableReference::bare("orders"),
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
    fn test_eliminate_self_join_aggregation_with_unique_constraint() -> Result<()> {
        // Create self join with unique constraint on GROUP BY column
        let left = create_table_scan_with_unique_constraint(Some("a"));
        let right = create_table_scan_with_unique_constraint(Some("b"));

        // Build join with filter: b.order_id <= a.order_id
        let join_filter = col("b.order_id").lt_eq(col("a.order_id"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (Vec::<Column>::new(), Vec::<Column>::new()), // No equi-join conditions
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.order_id")], // GROUP BY on unique column
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.order_id"),
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
    fn test_eliminate_self_join_aggregation_less_than_equal() -> Result<()> {
        // Create self join with <= condition
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
    fn test_elimination_with_multiple_aggregates() -> Result<()> {
        // Test that optimization works with multiple aggregate expressions
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
                col(count(col("b.amount")).name_for_alias()?),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Window(window) => {
                        // Verify we have 2 window expressions (sum and count)
                        assert_eq!(
                            window.window_expr.len(),
                            2,
                            "Expected 2 window expressions, got {}",
                            window.window_expr.len()
                        );
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
    fn test_no_elimination_with_different_filter_columns() -> Result<()> {
        // Create self join with filter on different columns
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
                let plan = proj.input.as_ref().clone();
                plan.transform(|node| {
                    if matches!(node, LogicalPlan::Join(_)) {
                        has_join = true;
                    }
                    Ok(Transformed::no(node))
                })?;
                assert!(has_join, "Expected join to remain for non-inner join");
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_without_unique_constraint() -> Result<()> {
        // Create table without unique constraints on GROUP BY columns
        // Note: The default create_table_scan doesn't add unique constraints
        let left = create_table_scan(Some("a"), false);
        let right = create_table_scan(Some("b"), false);

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

        // Verify the join was NOT eliminated because GROUP BY columns
        // (user_id, purchase_date) don't form a unique constraint
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
                    "Expected join to remain without unique constraint on GROUP BY columns"
                );
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_self_join_aggregation_without_projection() -> Result<()> {
        // Test optimization when Aggregate is the top-level node (no Projection)
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Window(_) => {
                // Success - join was replaced with window function
            }
            _ => panic!("Expected Window at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_self_join_with_multiple_aggregates() -> Result<()> {
        // Test optimization with multiple aggregate functions
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
                    count(col("b.amount")),
                    max(col("b.amount")),
                    min(col("b.amount")),
                ],
            )?
            .project(vec![
                col("a.user_id"),
                col("a.purchase_date"),
                col(sum(col("b.amount")).name_for_alias()?).alias("total"),
                col(count(col("b.amount")).name_for_alias()?).alias("count"),
                col(max(col("b.amount")).name_for_alias()?).alias("max_amount"),
                col(min(col("b.amount")).name_for_alias()?).alias("min_amount"),
            ])?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Projection(proj) => {
                match proj.input.as_ref() {
                    LogicalPlan::Window(window) => {
                        // Verify we have 4 window expressions
                        assert_eq!(
                            window.window_expr.len(),
                            4,
                            "Expected 4 window expressions, got {}",
                            window.window_expr.len()
                        );
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
    fn test_eliminate_self_join_with_multiple_aggregates_no_projection() -> Result<()> {
        // Test optimization with multiple aggregate functions and no projection
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

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
                vec![sum(col("b.amount")), avg(col("b.amount"))],
            )?
            .build()?;

        let optimized = optimize_plan(join_plan)?;

        // Verify the join was eliminated and replaced with window function
        match &optimized {
            LogicalPlan::Window(window) => {
                // Verify we have 2 window expressions
                assert_eq!(
                    window.window_expr.len(),
                    2,
                    "Expected 2 window expressions, got {}",
                    window.window_expr.len()
                );
            }
            _ => panic!("Expected Window at top level, got: {optimized:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_no_elimination_with_group_by_from_different_sides() -> Result<()> {
        // Test that optimization is NOT applied when GROUP BY columns come from different sides
        let left = create_table_scan(Some("a"), true);
        let right = create_table_scan(Some("b"), true);

        let join_filter = col("a.purchase_date").lt_eq(col("b.purchase_date"));

        let join_plan = LogicalPlanBuilder::from(left)
            .join(
                right,
                JoinType::Inner,
                (vec!["a.user_id"], vec!["b.user_id"]),
                Some(join_filter),
            )?
            .aggregate(
                vec![col("a.user_id"), col("b.purchase_date")], // GROUP BY from different sides
                vec![sum(col("b.amount"))],
            )?
            .project(vec![
                col("a.user_id"),
                col("b.purchase_date"),
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
                    "Expected join to remain when GROUP BY columns come from different sides"
                );
            }
            _ => panic!("Expected Projection at top level, got: {optimized:?}"),
        }

        Ok(())
    }
}
