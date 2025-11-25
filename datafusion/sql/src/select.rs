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

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::query::to_order_by_exprs_with_select;
use crate::utils::{
    check_columns_satisfy_exprs, extract_aliases, rebase_expr, resolve_aliases_to_exprs,
    resolve_columns, resolve_positions_to_exprs, rewrite_recursive_unnests_bottom_up,
    CheckColumnsMustReferenceAggregatePurpose, CheckColumnsSatisfyExprsPurpose,
};

use datafusion_common::error::DataFusionErrorBuilder;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_common::{RecursionUnnestOption, UnnestOptions};
use datafusion_expr::expr::{Alias, PlannedReplaceSelectItem, WildcardOptions};
use datafusion_expr::expr_rewriter::{
    normalize_col, normalize_col_with_schemas_and_ambiguity_check, normalize_sorts,
};
use datafusion_expr::select_expr::SelectExpr;
use datafusion_expr::utils::{
    expr_as_column_expr, expr_to_columns, find_aggregate_exprs, find_window_exprs,
};
use datafusion_expr::{
    Aggregate, Expr, Filter, GroupingSet, LogicalPlan, LogicalPlanBuilder,
    LogicalPlanBuilderOptions, Partitioning, SortExpr,
};

use indexmap::IndexMap;
use sqlparser::ast::{
    visit_expressions_mut, Distinct, Expr as SQLExpr, GroupByExpr, NamedWindowExpr,
    OrderBy, SelectItemQualifiedWildcardKind, WildcardAdditionalOptions, WindowType,
};
use sqlparser::ast::{NamedWindowDefinition, Select, SelectItem, TableWithJoins};

/// Result of the `aggregate` function, containing the aggregate plan and
/// rewritten expressions that reference the aggregate output columns.
struct AggregatePlanResult {
    /// The aggregate logical plan
    plan: LogicalPlan,
    /// SELECT expressions rewritten to reference aggregate output columns
    select_exprs: Vec<Expr>,
    /// HAVING expression rewritten to reference aggregate output columns
    having_expr: Option<Expr>,
    /// QUALIFY expression rewritten to reference aggregate output columns
    qualify_expr: Option<Expr>,
    /// ORDER BY expressions rewritten to reference aggregate output columns
    order_by_exprs: Vec<SortExpr>,
}

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logic plan from an SQL select
    pub(super) fn select_to_plan(
        &self,
        mut select: Select,
        query_order_by: Option<OrderBy>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Check for unsupported syntax first
        if !select.cluster_by.is_empty() {
            return not_impl_err!("CLUSTER BY");
        }
        if !select.lateral_views.is_empty() {
            return not_impl_err!("LATERAL VIEWS");
        }

        if select.top.is_some() {
            return not_impl_err!("TOP");
        }
        if !select.sort_by.is_empty() {
            return not_impl_err!("SORT BY");
        }

        // Process `from` clause
        let plan = self.plan_from_tables(select.from, planner_context)?;
        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));

        // Process `where` clause
        let base_plan = self.plan_selection(select.selection, plan, planner_context)?;

        // Handle named windows before processing the projection expression
        check_conflicting_windows(&select.named_window)?;
        self.match_window_definitions(&mut select.projection, &select.named_window)?;

        // Process the SELECT expressions
        let select_exprs = self.prepare_select_exprs(
            &base_plan,
            select.projection,
            empty_from,
            planner_context,
        )?;

        // Having and group by clause may reference aliases defined in select projection
        let projected_plan = self.project(base_plan.clone(), select_exprs)?;
        let select_exprs = projected_plan.expressions();

        let order_by =
            to_order_by_exprs_with_select(query_order_by, Some(&select_exprs))?;

        // Place the fields of the base plan at the front so that when there are references
        // with the same name, the fields of the base plan will be searched first.
        // See https://github.com/apache/datafusion/issues/9162
        let mut combined_schema = base_plan.schema().as_ref().clone();
        combined_schema.merge(projected_plan.schema());

        // Order-by expressions prioritize referencing columns from the select list,
        // then from the FROM clause.
        let order_by_rex = self.order_by_to_sort_expr(
            order_by,
            projected_plan.schema().as_ref(),
            planner_context,
            true,
            Some(base_plan.schema().as_ref()),
        )?;
        let order_by_rex = normalize_sorts(order_by_rex, &projected_plan)?;

        // This alias map is resolved and looked up in both having exprs and group by exprs
        let alias_map = extract_aliases(&select_exprs);

        // Optionally the HAVING expression.
        let having_expr_opt = select
            .having
            .map::<Result<Expr>, _>(|having_expr| {
                let having_expr = self.sql_expr_to_logical_expr(
                    having_expr,
                    &combined_schema,
                    planner_context,
                )?;
                // This step "dereferences" any aliases in the HAVING clause.
                //
                // This is how we support queries with HAVING expressions that
                // refer to aliased columns.
                //
                // For example:
                //
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING m > 10;
                //
                // are rewritten as, respectively:
                //
                //   SELECT c1, MAX(c2) AS m FROM t GROUP BY c1 HAVING MAX(c2) > 10;
                //
                let having_expr = resolve_aliases_to_exprs(having_expr, &alias_map)?;
                normalize_col(having_expr, &projected_plan)
            })
            .transpose()?;

        // All of the group by expressions
        let group_by_exprs = if let GroupByExpr::Expressions(exprs, _) = select.group_by {
            exprs
                .into_iter()
                .map(|e| {
                    let group_by_expr = self.sql_expr_to_logical_expr(
                        e,
                        &combined_schema,
                        planner_context,
                    )?;

                    // Aliases from the projection can conflict with same-named expressions in the input
                    let mut alias_map = alias_map.clone();
                    for f in base_plan.schema().fields() {
                        alias_map.remove(f.name());
                    }
                    let group_by_expr =
                        resolve_aliases_to_exprs(group_by_expr, &alias_map)?;
                    let group_by_expr =
                        resolve_positions_to_exprs(group_by_expr, &select_exprs)?;
                    let group_by_expr = normalize_col(group_by_expr, &projected_plan)?;
                    self.validate_schema_satisfies_exprs(
                        base_plan.schema(),
                        std::slice::from_ref(&group_by_expr),
                    )?;
                    Ok(group_by_expr)
                })
                .collect::<Result<Vec<Expr>>>()?
        } else {
            // 'group by all' groups wrt. all select expressions except 'AggregateFunction's.
            // Filter and collect non-aggregate select expressions
            select_exprs
                .iter()
                .filter(|select_expr| match select_expr {
                    Expr::AggregateFunction(_) => false,
                    Expr::Alias(Alias { expr, name: _, .. }) => {
                        !matches!(**expr, Expr::AggregateFunction(_))
                    }
                    _ => true,
                })
                .cloned()
                .collect()
        };

        // Optionally the QUALIFY expression.
        let qualify_expr_opt = select
            .qualify
            .map::<Result<Expr>, _>(|qualify_expr| {
                let qualify_expr = self.sql_expr_to_logical_expr(
                    qualify_expr,
                    &combined_schema,
                    planner_context,
                )?;
                // This step "dereferences" any aliases in the QUALIFY clause.
                //
                // This is how we support queries with QUALIFY expressions that
                // refer to aliased columns.
                //
                // For example:
                //
                //   select row_number() over (PARTITION BY id) as rk from users qualify rk > 1;
                //
                // are rewritten as, respectively:
                //
                //   select row_number() over (PARTITION BY id) as rk from users qualify row_number() over (PARTITION BY id) > 1;
                //
                let qualify_expr = resolve_aliases_to_exprs(qualify_expr, &alias_map)?;
                normalize_col(qualify_expr, &projected_plan)
            })
            .transpose()?;

        // The outer expressions we will search through for aggregates.
        // First, find aggregates in SELECT, HAVING, and QUALIFY
        let select_having_qualify_aggrs = find_aggregate_exprs(
            select_exprs
                .iter()
                .chain(having_expr_opt.iter())
                .chain(qualify_expr_opt.iter()),
        );

        // Find aggregates in ORDER BY
        let order_by_aggrs = find_aggregate_exprs(order_by_rex.iter().map(|s| &s.expr));

        // Combine: all aggregates from SELECT/HAVING/QUALIFY, plus ORDER BY aggregates
        // that aren't already in SELECT/HAVING/QUALIFY
        let mut aggr_exprs = select_having_qualify_aggrs;
        for order_by_aggr in order_by_aggrs {
            if !aggr_exprs.iter().any(|e| e == &order_by_aggr) {
                aggr_exprs.push(order_by_aggr);
            }
        }

        // Process group by, aggregation or having
        let AggregatePlanResult {
            plan,
            select_exprs: mut select_exprs_post_aggr,
            having_expr: having_expr_post_aggr,
            qualify_expr: qualify_expr_post_aggr,
            order_by_exprs: order_by_rex,
        } = if !group_by_exprs.is_empty() || !aggr_exprs.is_empty() {
            self.aggregate(
                &base_plan,
                &select_exprs,
                having_expr_opt.as_ref(),
                qualify_expr_opt.as_ref(),
                &order_by_rex,
                &group_by_exprs,
                &aggr_exprs,
            )?
        } else {
            match having_expr_opt {
                Some(having_expr) => return plan_err!("HAVING clause references: {having_expr} must appear in the GROUP BY clause or be used in an aggregate function"),
                None => AggregatePlanResult {
                    plan: base_plan.clone(),
                    select_exprs: select_exprs.clone(),
                    having_expr: having_expr_opt,
                    qualify_expr: qualify_expr_opt,
                    order_by_exprs: order_by_rex,
                }
            }
        };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr {
            LogicalPlanBuilder::from(plan)
                .having(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        // The outer expressions we will search through for window functions.
        // Window functions may be sourced from the SELECT list or from the QUALIFY expression.
        let windows_expr_haystack = select_exprs_post_aggr
            .iter()
            .chain(qualify_expr_post_aggr.iter());
        // All of the window expressions (deduplicated and rewritten to reference aggregates as
        // columns from input).
        let window_func_exprs = find_window_exprs(windows_expr_haystack);

        // Process window functions after aggregation as they can reference
        // aggregate functions in their body
        let plan = if window_func_exprs.is_empty() {
            plan
        } else {
            let plan = LogicalPlanBuilder::window_plan(plan, window_func_exprs.clone())?;

            // Re-write the projection
            select_exprs_post_aggr = select_exprs_post_aggr
                .iter()
                .map(|expr| rebase_expr(expr, &window_func_exprs, &plan))
                .collect::<Result<Vec<Expr>>>()?;

            plan
        };

        // Process QUALIFY clause after window functions
        // QUALIFY filters the results of window functions, similar to how HAVING filters aggregates
        let plan = if let Some(qualify_expr) = qualify_expr_post_aggr {
            // Validate that QUALIFY is used with window functions
            if window_func_exprs.is_empty() {
                return plan_err!(
                    "QUALIFY clause requires window functions in the SELECT list or QUALIFY clause"
                );
            }

            // now attempt to resolve columns and replace with fully-qualified columns
            let windows_projection_exprs = window_func_exprs
                .iter()
                .map(|expr| resolve_columns(expr, &plan))
                .collect::<Result<Vec<Expr>>>()?;

            // Rewrite the qualify expression to reference columns from the window plan
            let qualify_expr_post_window =
                rebase_expr(&qualify_expr, &windows_projection_exprs, &plan)?;

            // Validate that the qualify expression can be resolved from the window plan schema
            self.validate_schema_satisfies_exprs(
                plan.schema(),
                std::slice::from_ref(&qualify_expr_post_window),
            )?;

            LogicalPlanBuilder::from(plan)
                .filter(qualify_expr_post_window)?
                .build()?
        } else {
            plan
        };

        // Try processing unnest expression or do the final projection
        let plan = self.try_process_unnest(plan, select_exprs_post_aggr)?;

        // Process distinct clause
        let plan = match select.distinct {
            None => Ok(plan),
            Some(Distinct::Distinct) => {
                LogicalPlanBuilder::from(plan).distinct()?.build()
            }
            Some(Distinct::On(on_expr)) => {
                if !aggr_exprs.is_empty()
                    || !group_by_exprs.is_empty()
                    || !window_func_exprs.is_empty()
                {
                    return not_impl_err!("DISTINCT ON expressions with GROUP BY, aggregation or window functions are not supported ");
                }

                let on_expr = on_expr
                    .into_iter()
                    .map(|e| {
                        self.sql_expr_to_logical_expr(e, plan.schema(), planner_context)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Build the final plan
                LogicalPlanBuilder::from(base_plan)
                    .distinct_on(on_expr, select_exprs, None)?
                    .build()
            }
        }?;

        // DISTRIBUTE BY
        let plan = if !select.distribute_by.is_empty() {
            let x = select
                .distribute_by
                .iter()
                .map(|e| {
                    self.sql_expr_to_logical_expr(
                        e.clone(),
                        &combined_schema,
                        planner_context,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            LogicalPlanBuilder::from(plan)
                .repartition(Partitioning::DistributeBy(x))?
                .build()?
        } else {
            plan
        };

        let plan = self.order_by(plan, order_by_rex)?;
        Ok(plan)
    }

    /// Try converting Expr(Unnest(Expr)) to Projection/Unnest/Projection
    pub(super) fn try_process_unnest(
        &self,
        input: LogicalPlan,
        select_exprs: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        // Try process group by unnest
        let input = self.try_process_aggregate_unnest(input)?;

        let mut intermediate_plan = input;
        let mut intermediate_select_exprs = select_exprs;
        // Fast path: If there is are no unnests in the select_exprs, wrap the plan in a projection
        if !intermediate_select_exprs
            .iter()
            .any(has_unnest_expr_recursively)
        {
            return LogicalPlanBuilder::from(intermediate_plan)
                .project(intermediate_select_exprs)?
                .build();
        }

        // Each expr in select_exprs can contains multiple unnest stage
        // The transformation happen bottom up, one at a time for each iteration
        // Only exhaust the loop if no more unnest transformation is found
        for i in 0.. {
            let mut unnest_columns = IndexMap::new();
            // from which column used for projection, before the unnest happen
            // including non unnest column and unnest column
            let mut inner_projection_exprs = vec![];

            // expr returned here maybe different from the originals in inner_projection_exprs
            // for example:
            // - unnest(struct_col) will be transformed into unnest(struct_col).field1, unnest(struct_col).field2
            // - unnest(array_col) will be transformed into unnest(array_col).element
            // - unnest(array_col) + 1 will be transformed into unnest(array_col).element +1
            let outer_projection_exprs = rewrite_recursive_unnests_bottom_up(
                &intermediate_plan,
                &mut unnest_columns,
                &mut inner_projection_exprs,
                &intermediate_select_exprs,
            )?;

            // No more unnest is possible
            if unnest_columns.is_empty() {
                // The original expr does not contain any unnest
                if i == 0 {
                    return LogicalPlanBuilder::from(intermediate_plan)
                        .project(intermediate_select_exprs)?
                        .build();
                }
                break;
            } else {
                // Set preserve_nulls to false to ensure compatibility with DuckDB and PostgreSQL
                let mut unnest_options = UnnestOptions::new().with_preserve_nulls(false);
                let mut unnest_col_vec = vec![];

                for (col, maybe_list_unnest) in unnest_columns.into_iter() {
                    if let Some(list_unnest) = maybe_list_unnest {
                        unnest_options = list_unnest.into_iter().fold(
                            unnest_options,
                            |options, unnest_list| {
                                options.with_recursions(RecursionUnnestOption {
                                    input_column: col.clone(),
                                    output_column: unnest_list.output_column,
                                    depth: unnest_list.depth,
                                })
                            },
                        );
                    }
                    unnest_col_vec.push(col);
                }
                let plan = LogicalPlanBuilder::from(intermediate_plan)
                    .project(inner_projection_exprs)?
                    .unnest_columns_with_options(unnest_col_vec, unnest_options)?
                    .build()?;
                intermediate_plan = plan;
                intermediate_select_exprs = outer_projection_exprs;
            }
        }

        LogicalPlanBuilder::from(intermediate_plan)
            .project(intermediate_select_exprs)?
            .build()
    }

    fn try_process_aggregate_unnest(&self, input: LogicalPlan) -> Result<LogicalPlan> {
        match input {
            // Fast path if there are no unnest in group by
            LogicalPlan::Aggregate(ref agg)
                if !&agg.group_expr.iter().any(has_unnest_expr_recursively) =>
            {
                Ok(input)
            }
            LogicalPlan::Aggregate(agg) => {
                let agg_expr = agg.aggr_expr.clone();
                let (new_input, new_group_by_exprs) =
                    self.try_process_group_by_unnest(agg)?;
                let options = LogicalPlanBuilderOptions::new()
                    .with_add_implicit_group_by_exprs(true);
                LogicalPlanBuilder::from(new_input)
                    .with_options(options)
                    .aggregate(new_group_by_exprs, agg_expr)?
                    .build()
            }
            LogicalPlan::Filter(mut filter) => {
                filter.input =
                    Arc::new(self.try_process_aggregate_unnest(Arc::unwrap_or_clone(
                        filter.input,
                    ))?);
                Ok(LogicalPlan::Filter(filter))
            }
            _ => Ok(input),
        }
    }

    /// Try converting Unnest(Expr) of group by to Unnest/Projection.
    /// Return the new input and group_by_exprs of Aggregate.
    /// Select exprs can be different from agg exprs, for example:
    fn try_process_group_by_unnest(
        &self,
        agg: Aggregate,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        let mut aggr_expr_using_columns: Option<HashSet<Expr>> = None;

        let Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        } = agg;

        // Process unnest of group_by_exprs, and input of agg will be rewritten
        // for example:
        //
        // ```
        // Aggregate: groupBy=[[UNNEST(Column(Column { relation: Some(Bare { table: "tab" }), name: "array_col" }))]], aggr=[[]]
        //   TableScan: tab
        // ```
        //
        // will be transformed into
        //
        // ```
        // Aggregate: groupBy=[[unnest(tab.array_col)]], aggr=[[]]
        //   Unnest: lists[unnest(tab.array_col)] structs[]
        //     Projection: tab.array_col AS unnest(tab.array_col)
        //       TableScan: tab
        // ```
        let mut intermediate_plan = Arc::unwrap_or_clone(input);
        let mut intermediate_select_exprs = group_expr;

        loop {
            let mut unnest_columns = IndexMap::new();
            let mut inner_projection_exprs = vec![];

            let outer_projection_exprs = rewrite_recursive_unnests_bottom_up(
                &intermediate_plan,
                &mut unnest_columns,
                &mut inner_projection_exprs,
                &intermediate_select_exprs,
            )?;

            if unnest_columns.is_empty() {
                break;
            } else {
                let mut unnest_options = UnnestOptions::new().with_preserve_nulls(false);

                let mut projection_exprs = match &aggr_expr_using_columns {
                    Some(exprs) => (*exprs).clone(),
                    None => {
                        let mut columns = HashSet::new();
                        for expr in &aggr_expr {
                            expr.apply(|expr| {
                                if let Expr::Column(c) = expr {
                                    columns.insert(Expr::Column(c.clone()));
                                }
                                Ok(TreeNodeRecursion::Continue)
                            })
                            // As the closure always returns Ok, this "can't" error
                            .expect("Unexpected error");
                        }
                        aggr_expr_using_columns = Some(columns.clone());
                        columns
                    }
                };
                projection_exprs.extend(inner_projection_exprs);

                let mut unnest_col_vec = vec![];

                for (col, maybe_list_unnest) in unnest_columns.into_iter() {
                    if let Some(list_unnest) = maybe_list_unnest {
                        unnest_options = list_unnest.into_iter().fold(
                            unnest_options,
                            |options, unnest_list| {
                                options.with_recursions(RecursionUnnestOption {
                                    input_column: col.clone(),
                                    output_column: unnest_list.output_column,
                                    depth: unnest_list.depth,
                                })
                            },
                        );
                    }
                    unnest_col_vec.push(col);
                }

                intermediate_plan = LogicalPlanBuilder::from(intermediate_plan)
                    .project(projection_exprs)?
                    .unnest_columns_with_options(unnest_col_vec, unnest_options)?
                    .build()?;

                intermediate_select_exprs = outer_projection_exprs;
            }
        }

        Ok((intermediate_plan, intermediate_select_exprs))
    }

    pub(crate) fn plan_selection(
        &self,
        selection: Option<SQLExpr>,
        plan: LogicalPlan,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(predicate_expr) => {
                let fallback_schemas = plan.fallback_normalize_schemas();
                let outer_query_schema = planner_context.outer_query_schema().cloned();
                let outer_query_schema_vec = outer_query_schema
                    .as_ref()
                    .map(|schema| vec![schema])
                    .unwrap_or_else(Vec::new);

                let filter_expr =
                    self.sql_to_expr(predicate_expr, plan.schema(), planner_context)?;

                // Check for aggregation functions
                let aggregate_exprs =
                    find_aggregate_exprs(std::slice::from_ref(&filter_expr));
                if !aggregate_exprs.is_empty() {
                    return plan_err!(
                        "Aggregate functions are not allowed in the WHERE clause. Consider using HAVING instead"
                    );
                }

                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas_and_ambiguity_check(
                    filter_expr,
                    &[&[plan.schema()], &fallback_schemas, &outer_query_schema_vec],
                    &[using_columns],
                )?;

                Ok(LogicalPlan::Filter(Filter::try_new(
                    filter_expr,
                    Arc::new(plan),
                )?))
            }
            None => Ok(plan),
        }
    }

    pub(crate) fn plan_from_tables(
        &self,
        mut from: Vec<TableWithJoins>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()?),
            1 => {
                let input = from.remove(0);
                self.plan_table_with_joins(input, planner_context)
            }
            _ => {
                let mut from = from.into_iter();

                let mut left = LogicalPlanBuilder::from({
                    let input = from.next().unwrap();
                    self.plan_table_with_joins(input, planner_context)?
                });
                let old_outer_from_schema = {
                    let left_schema = Some(Arc::clone(left.schema()));
                    planner_context.set_outer_from_schema(left_schema)
                };
                for input in from {
                    // Join `input` with the current result (`left`).
                    let right = self.plan_table_with_joins(input, planner_context)?;
                    left = left.cross_join(right)?;
                    // Update the outer FROM schema.
                    let left_schema = Some(Arc::clone(left.schema()));
                    planner_context.set_outer_from_schema(left_schema);
                }
                planner_context.set_outer_from_schema(old_outer_from_schema);
                left.build()
            }
        }
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    pub(crate) fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<SelectExpr>> {
        let mut prepared_select_exprs = vec![];
        let mut error_builder = DataFusionErrorBuilder::new();

        for expr in projection {
            match self.sql_select_to_rex(expr, plan, empty_from, planner_context) {
                Ok(expr) => prepared_select_exprs.push(expr),
                Err(err) => error_builder.add_error(err),
            }
        }
        error_builder.error_or(prepared_select_exprs)
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: SelectItem,
        plan: &LogicalPlan,
        empty_from: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<SelectExpr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_to_expr(expr, plan.schema(), planner_context)?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[plan.schema()]],
                    &plan.using_columns()?,
                )?;

                Ok(SelectExpr::Expression(col))
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let select_expr =
                    self.sql_to_expr(expr, plan.schema(), planner_context)?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    select_expr,
                    &[&[plan.schema()]],
                    &plan.using_columns()?,
                )?;
                let name = self.ident_normalizer.normalize(alias);
                // avoiding adding an alias if the column name is the same.
                let expr = match &col {
                    Expr::Column(column) if column.name.eq(&name) => col,
                    _ => col.alias(name),
                };

                Ok(SelectExpr::Expression(expr))
            }
            SelectItem::Wildcard(options) => {
                Self::check_wildcard_options(&options)?;
                if empty_from {
                    return plan_err!("SELECT * with no tables specified is not valid");
                }
                let planned_options = self.plan_wildcard_options(
                    plan,
                    empty_from,
                    planner_context,
                    options,
                )?;

                Ok(SelectExpr::Wildcard(planned_options))
            }
            SelectItem::QualifiedWildcard(object_name, options) => {
                Self::check_wildcard_options(&options)?;
                let object_name = match object_name {
                    SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                        object_name
                    }
                    SelectItemQualifiedWildcardKind::Expr(_) => {
                        return plan_err!(
                            "Qualified wildcard with expression not supported"
                        )
                    }
                };
                let qualifier = self.object_name_to_table_reference(object_name)?;
                let planned_options = self.plan_wildcard_options(
                    plan,
                    empty_from,
                    planner_context,
                    options,
                )?;

                Ok(SelectExpr::QualifiedWildcard(qualifier, planned_options))
            }
        }
    }

    fn check_wildcard_options(options: &WildcardAdditionalOptions) -> Result<()> {
        let WildcardAdditionalOptions {
            // opt_exclude is handled
            opt_exclude: _opt_exclude,
            opt_except: _opt_except,
            opt_rename,
            opt_replace: _opt_replace,
            opt_ilike: _opt_ilike,
            wildcard_token: _wildcard_token,
        } = options;

        if opt_rename.is_some() {
            not_impl_err!("wildcard * with RENAME not supported ")
        } else {
            Ok(())
        }
    }

    /// If there is a REPLACE statement in the projected expression in the form of
    /// "REPLACE (some_column_within_an_expr AS some_column)", we should plan the
    /// replace expressions first.
    fn plan_wildcard_options(
        &self,
        plan: &LogicalPlan,
        empty_from: bool,
        planner_context: &mut PlannerContext,
        options: WildcardAdditionalOptions,
    ) -> Result<WildcardOptions> {
        let planned_option = WildcardOptions {
            ilike: options.opt_ilike,
            exclude: options.opt_exclude,
            except: options.opt_except,
            replace: None,
            rename: options.opt_rename,
        };
        if let Some(replace) = options.opt_replace {
            let replace_expr = replace
                .items
                .iter()
                .map(|item| {
                    self.sql_select_to_rex(
                        SelectItem::UnnamedExpr(item.expr.clone()),
                        plan,
                        empty_from,
                        planner_context,
                    )
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter_map(|expr| match expr {
                    SelectExpr::Expression(expr) => Some(expr),
                    _ => None,
                })
                .collect::<Vec<_>>();

            let planned_replace = PlannedReplaceSelectItem {
                items: replace.items.into_iter().map(|i| *i).collect(),
                planned_expressions: replace_expr,
            };
            Ok(planned_option.with_replace(planned_replace))
        } else {
            Ok(planned_option)
        }
    }

    /// Wrap a plan in a projection
    pub(crate) fn project(
        &self,
        input: LogicalPlan,
        expr: Vec<SelectExpr>,
    ) -> Result<LogicalPlan> {
        // convert to Expr for validate_schema_satisfies_exprs
        let exprs = expr
            .iter()
            .filter_map(|e| match e {
                SelectExpr::Expression(expr) => Some(expr.to_owned()),
                _ => None,
            })
            .collect::<Vec<_>>();
        self.validate_schema_satisfies_exprs(input.schema(), &exprs)?;

        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Create an aggregate plan.
    ///
    /// An aggregate plan consists of grouping expressions, aggregate expressions, an
    /// optional HAVING expression (which is a filter on the output of the aggregate),
    /// and an optional QUALIFY clause which may reference aggregates.
    ///
    /// # Arguments
    ///
    /// * `input`           - The input plan that will be aggregated. The grouping, aggregate, and
    ///   "having" expressions must all be resolvable from this plan.
    /// * `select_exprs`    - The projection expressions from the SELECT clause.
    /// * `having_expr_opt` - Optional HAVING clause.
    /// * `qualify_expr_opt` - Optional QUALIFY clause.
    /// * `group_by_exprs`  - Grouping expressions from the GROUP BY clause. These can be column
    ///   references or more complex expressions.
    /// * `aggr_exprs`      - Aggregate expressions, such as `SUM(a)` or `COUNT(1)`.
    ///
    /// # Return
    ///
    /// The return value is a quadruplet of the following items:
    ///
    /// * `plan`                   - A [LogicalPlan::Aggregate] plan for the newly created aggregate.
    /// * `select_exprs_post_aggr` - The projection expressions rewritten to reference columns from
    ///   the aggregate
    /// * `having_expr_post_aggr`  - The "having" expression rewritten to reference a column from
    ///   the aggregate
    /// * `qualify_expr_post_aggr`  - The "qualify" expression rewritten to reference a column from
    ///   the aggregate
    /// * `order_by_post_aggr`     - The ORDER BY expressions rewritten to reference columns from
    ///   the aggregate
    #[allow(clippy::too_many_arguments)]
    fn aggregate(
        &self,
        input: &LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: Option<&Expr>,
        qualify_expr_opt: Option<&Expr>,
        order_by_exprs: &[SortExpr],
        group_by_exprs: &[Expr],
        aggr_exprs: &[Expr],
    ) -> Result<AggregatePlanResult> {
        // create the aggregate plan
        let options =
            LogicalPlanBuilderOptions::new().with_add_implicit_group_by_exprs(true);
        let plan = LogicalPlanBuilder::from(input.clone())
            .with_options(options)
            .aggregate(group_by_exprs.to_vec(), aggr_exprs.to_vec())?
            .build()?;
        let group_by_exprs = if let LogicalPlan::Aggregate(agg) = &plan {
            &agg.group_expr
        } else {
            unreachable!();
        };

        // in this next section of code we are re-writing the projection to refer to columns
        // output by the aggregate plan. For example, if the projection contains the expression
        // `SUM(a)` then we replace that with a reference to a column `SUM(a)` produced by
        // the aggregate plan.

        // combine the original grouping and aggregate expressions into one list (note that
        // we do not add the "having" expression since that is not part of the projection)
        let mut aggr_projection_exprs = vec![];
        for expr in group_by_exprs {
            match expr {
                Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                    aggr_projection_exprs.extend_from_slice(exprs)
                }
                Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                    aggr_projection_exprs.extend_from_slice(exprs)
                }
                Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                    for exprs in lists_of_exprs {
                        aggr_projection_exprs.extend_from_slice(exprs)
                    }
                }
                _ => aggr_projection_exprs.push(expr.clone()),
            }
        }
        aggr_projection_exprs.extend_from_slice(aggr_exprs);

        // now attempt to resolve columns and replace with fully-qualified columns
        let aggr_projection_exprs = aggr_projection_exprs
            .iter()
            .map(|expr| resolve_columns(expr, input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we replace any expressions that are not a column with a column referencing
        // an output column from the aggregate schema
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we re-write the projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &aggr_projection_exprs, input))
            .collect::<Result<Vec<Expr>>>()?;

        // finally, we have some validation that the re-written projection can be resolved
        // from the aggregate output columns
        check_columns_satisfy_exprs(
            &column_exprs_post_aggr,
            &select_exprs_post_aggr,
            CheckColumnsSatisfyExprsPurpose::Aggregate(
                CheckColumnsMustReferenceAggregatePurpose::Projection,
            ),
        )?;

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, input)?;

            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                std::slice::from_ref(&having_expr_post_aggr),
                CheckColumnsSatisfyExprsPurpose::Aggregate(
                    CheckColumnsMustReferenceAggregatePurpose::Having,
                ),
            )?;

            Some(having_expr_post_aggr)
        } else {
            None
        };

        // Rewrite the QUALIFY expression to use the columns produced by the
        // aggregation.
        let qualify_expr_post_aggr = if let Some(qualify_expr) = qualify_expr_opt {
            let qualify_expr_post_aggr =
                rebase_expr(qualify_expr, &aggr_projection_exprs, input)?;

            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                std::slice::from_ref(&qualify_expr_post_aggr),
                CheckColumnsSatisfyExprsPurpose::Aggregate(
                    CheckColumnsMustReferenceAggregatePurpose::Qualify,
                ),
            )?;

            Some(qualify_expr_post_aggr)
        } else {
            None
        };

        // Rewrite the ORDER BY expressions to use the columns produced by the
        // aggregation. If an ORDER BY expression matches a SELECT expression
        // (ignoring aliases), use the SELECT's output column name to avoid
        // duplication when the SELECT expression has an alias.
        let order_by_post_aggr = order_by_exprs
            .iter()
            .map(|sort_expr| {
                let rewritten_expr =
                    rebase_expr(&sort_expr.expr, &aggr_projection_exprs, input)?;

                // Check if this ORDER BY expression matches any aliased SELECT expression
                // If so, use the SELECT's alias instead of the raw expression
                let final_expr = select_exprs_post_aggr
                    .iter()
                    .find_map(|select_expr| {
                        // Only consider aliased expressions
                        if let Expr::Alias(alias) = select_expr {
                            if alias.expr.as_ref() == &rewritten_expr {
                                // Use the alias name
                                return Some(Expr::Column(alias.name.clone().into()));
                            }
                        }
                        None
                    })
                    .unwrap_or(rewritten_expr);

                Ok(sort_expr.with_expr(final_expr))
            })
            .collect::<Result<Vec<SortExpr>>>()?;

        let all_valid_exprs: Vec<Expr> = column_exprs_post_aggr
            .iter()
            .cloned()
            .chain(select_exprs_post_aggr.iter().filter_map(|e| {
                if let Expr::Alias(alias) = e {
                    Some(Expr::Column(alias.name.clone().into()))
                } else {
                    None
                }
            }))
            .collect();

        let order_by_exprs_only: Vec<Expr> =
            order_by_post_aggr.iter().map(|s| s.expr.clone()).collect();
        check_columns_satisfy_exprs(
            &all_valid_exprs,
            &order_by_exprs_only,
            CheckColumnsSatisfyExprsPurpose::Aggregate(
                CheckColumnsMustReferenceAggregatePurpose::OrderBy,
            ),
        )?;

        Ok(AggregatePlanResult {
            plan,
            select_exprs: select_exprs_post_aggr,
            having_expr: having_expr_post_aggr,
            qualify_expr: qualify_expr_post_aggr,
            order_by_exprs: order_by_post_aggr,
        })
    }

    // If the projection is done over a named window, that window
    // name must be defined. Otherwise, it gives an error.
    fn match_window_definitions(
        &self,
        projection: &mut [SelectItem],
        named_windows: &[NamedWindowDefinition],
    ) -> Result<()> {
        let named_windows: Vec<(&NamedWindowDefinition, String)> = named_windows
            .iter()
            .map(|w| (w, self.ident_normalizer.normalize(w.0.clone())))
            .collect();
        for proj in projection.iter_mut() {
            if let SelectItem::ExprWithAlias { expr, alias: _ }
            | SelectItem::UnnamedExpr(expr) = proj
            {
                let mut err = None;
                let _ = visit_expressions_mut(expr, |expr| {
                    if let SQLExpr::Function(f) = expr {
                        if let Some(WindowType::NamedWindow(ident)) = &f.over {
                            let normalized_ident =
                                self.ident_normalizer.normalize(ident.clone());
                            for (
                                NamedWindowDefinition(_, window_expr),
                                normalized_window_ident,
                            ) in named_windows.iter()
                            {
                                if normalized_ident.eq(normalized_window_ident) {
                                    f.over = Some(match window_expr {
                                        NamedWindowExpr::NamedWindow(ident) => {
                                            WindowType::NamedWindow(ident.clone())
                                        }
                                        NamedWindowExpr::WindowSpec(spec) => {
                                            WindowType::WindowSpec(spec.clone())
                                        }
                                    })
                                }
                            }
                            // All named windows must be defined with a WindowSpec.
                            if let Some(WindowType::NamedWindow(ident)) = &f.over {
                                err =
                                    Some(plan_err!("The window {ident} is not defined!"));
                                return ControlFlow::Break(());
                            }
                        }
                    }
                    ControlFlow::Continue(())
                });
                if let Some(err) = err {
                    return err;
                }
            }
        }
        Ok(())
    }
}

// If there are any multiple-defined windows, we raise an error.
fn check_conflicting_windows(window_defs: &[NamedWindowDefinition]) -> Result<()> {
    for (i, window_def_i) in window_defs.iter().enumerate() {
        for window_def_j in window_defs.iter().skip(i + 1) {
            if window_def_i.0 == window_def_j.0 {
                return plan_err!(
                    "The window {} is defined multiple times!",
                    window_def_i.0
                );
            }
        }
    }
    Ok(())
}

/// Returns true if the expression recursively contains an `Expr::Unnest` expression
fn has_unnest_expr_recursively(expr: &Expr) -> bool {
    let mut has_unnest = false;
    let _ = expr.apply(|e| {
        if let Expr::Unnest(_) = e {
            has_unnest = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    has_unnest
}
