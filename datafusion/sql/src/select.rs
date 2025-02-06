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
use std::sync::Arc;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::{
    check_columns_satisfy_exprs, extract_aliases, rebase_expr, resolve_aliases_to_exprs,
    resolve_columns, resolve_positions_to_exprs, rewrite_recursive_unnests_bottom_up,
    CheckColumnsSatisfyExprsPurpose,
};

use datafusion_common::error::DataFusionErrorBuilder;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{not_impl_err, plan_err, Result};
use datafusion_common::{RecursionUnnestOption, UnnestOptions};
use datafusion_expr::expr::{Alias, PlannedReplaceSelectItem, WildcardOptions};
use datafusion_expr::expr_rewriter::{
    normalize_col, normalize_col_with_schemas_and_ambiguity_check, normalize_sorts,
};
use datafusion_expr::utils::{
    expr_as_column_expr, expr_to_columns, find_aggregate_exprs, find_window_exprs,
};
use datafusion_expr::{
    qualified_wildcard_with_options, wildcard_with_options, Aggregate, Expr, Filter,
    GroupingSet, LogicalPlan, LogicalPlanBuilder, Partitioning,
};

use indexmap::IndexMap;
use sqlparser::ast::{
    Distinct, Expr as SQLExpr, GroupByExpr, NamedWindowExpr, OrderByExpr,
    WildcardAdditionalOptions, WindowType,
};
use sqlparser::ast::{NamedWindowDefinition, Select, SelectItem, TableWithJoins};

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Generate a logic plan from an SQL select
    pub(super) fn select_to_plan(
        &self,
        mut select: Select,
        order_by: Vec<OrderByExpr>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // Check for unsupported syntax first
        if !select.cluster_by.is_empty() {
            return not_impl_err!("CLUSTER BY");
        }
        if !select.lateral_views.is_empty() {
            return not_impl_err!("LATERAL VIEWS");
        }
        if select.qualify.is_some() {
            return not_impl_err!("QUALIFY");
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
        match_window_definitions(&mut select.projection, &select.named_window)?;
        // Process the SELECT expressions
        let select_exprs = self.prepare_select_exprs(
            &base_plan,
            select.projection,
            empty_from,
            planner_context,
        )?;

        // Having and group by clause may reference aliases defined in select projection
        let projected_plan = self.project(base_plan.clone(), select_exprs.clone())?;

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

        // The outer expressions we will search through for aggregates.
        // Aggregates may be sourced from the SELECT list or from the HAVING expression.
        let aggr_expr_haystack = select_exprs.iter().chain(having_expr_opt.iter());
        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(aggr_expr_haystack);

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

        // Process group by, aggregation or having
        let (plan, mut select_exprs_post_aggr, having_expr_post_aggr) = if !group_by_exprs
            .is_empty()
            || !aggr_exprs.is_empty()
        {
            self.aggregate(
                &base_plan,
                &select_exprs,
                having_expr_opt.as_ref(),
                &group_by_exprs,
                &aggr_exprs,
            )?
        } else {
            match having_expr_opt {
                Some(having_expr) => return plan_err!("HAVING clause references: {having_expr} must appear in the GROUP BY clause or be used in an aggregate function"),
                None => (base_plan.clone(), select_exprs.clone(), having_expr_opt)
            }
        };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr {
            LogicalPlanBuilder::from(plan)
                .having(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        // Process window function
        let window_func_exprs = find_window_exprs(&select_exprs_post_aggr);

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

        self.order_by(plan, order_by_rex)
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
            LogicalPlan::Aggregate(agg) => {
                let agg_expr = agg.aggr_expr.clone();
                let (new_input, new_group_by_exprs) =
                    self.try_process_group_by_unnest(agg)?;
                LogicalPlanBuilder::from(new_input)
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

    fn plan_selection(
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
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
        planner_context: &mut PlannerContext,
    ) -> Result<Vec<Expr>> {
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
    ) -> Result<Expr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_to_expr(expr, plan.schema(), planner_context)?;
                let col = normalize_col_with_schemas_and_ambiguity_check(
                    expr,
                    &[&[plan.schema()]],
                    &plan.using_columns()?,
                )?;
                Ok(col)
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
                Ok(expr)
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
                Ok(wildcard_with_options(planned_options))
            }
            SelectItem::QualifiedWildcard(object_name, options) => {
                Self::check_wildcard_options(&options)?;
                let qualifier = self.object_name_to_table_reference(object_name)?;
                let planned_options = self.plan_wildcard_options(
                    plan,
                    empty_from,
                    planner_context,
                    options,
                )?;
                Ok(qualified_wildcard_with_options(qualifier, planned_options))
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
                .collect::<Result<Vec<_>>>()?;
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
    fn project(&self, input: LogicalPlan, expr: Vec<Expr>) -> Result<LogicalPlan> {
        self.validate_schema_satisfies_exprs(input.schema(), &expr)?;
        LogicalPlanBuilder::from(input).project(expr)?.build()
    }

    /// Create an aggregate plan.
    ///
    /// An aggregate plan consists of grouping expressions, aggregate expressions, and an
    /// optional HAVING expression (which is a filter on the output of the aggregate).
    ///
    /// # Arguments
    ///
    /// * `input`           - The input plan that will be aggregated. The grouping, aggregate, and
    ///                       "having" expressions must all be resolvable from this plan.
    /// * `select_exprs`    - The projection expressions from the SELECT clause.
    /// * `having_expr_opt` - Optional HAVING clause.
    /// * `group_by_exprs`  - Grouping expressions from the GROUP BY clause. These can be column
    ///                       references or more complex expressions.
    /// * `aggr_exprs`      - Aggregate expressions, such as `SUM(a)` or `COUNT(1)`.
    ///
    /// # Return
    ///
    /// The return value is a triplet of the following items:
    ///
    /// * `plan`                   - A [LogicalPlan::Aggregate] plan for the newly created aggregate.
    /// * `select_exprs_post_aggr` - The projection expressions rewritten to reference columns from
    ///                              the aggregate
    /// * `having_expr_post_aggr`  - The "having" expression rewritten to reference a column from
    ///                              the aggregate
    fn aggregate(
        &self,
        input: &LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: Option<&Expr>,
        group_by_exprs: &[Expr],
        aggr_exprs: &[Expr],
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        // create the aggregate plan
        let plan = LogicalPlanBuilder::from(input.clone())
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
            CheckColumnsSatisfyExprsPurpose::ProjectionMustReferenceAggregate,
        )?;

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, input)?;

            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                std::slice::from_ref(&having_expr_post_aggr),
                CheckColumnsSatisfyExprsPurpose::HavingMustReferenceAggregate,
            )?;

            Some(having_expr_post_aggr)
        } else {
            None
        };

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr))
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

// If the projection is done over a named window, that window
// name must be defined. Otherwise, it gives an error.
fn match_window_definitions(
    projection: &mut [SelectItem],
    named_windows: &[NamedWindowDefinition],
) -> Result<()> {
    for proj in projection.iter_mut() {
        if let SelectItem::ExprWithAlias {
            expr: SQLExpr::Function(f),
            alias: _,
        }
        | SelectItem::UnnamedExpr(SQLExpr::Function(f)) = proj
        {
            for NamedWindowDefinition(window_ident, window_expr) in named_windows.iter() {
                if let Some(WindowType::NamedWindow(ident)) = &f.over {
                    if ident.eq(window_ident) {
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
            }
            // All named windows must be defined with a WindowSpec.
            if let Some(WindowType::NamedWindow(ident)) = &f.over {
                return plan_err!("The window {ident} is not defined!");
            }
        }
    }
    Ok(())
}
