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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::{
    check_columns_satisfy_exprs, extract_aliases, normalize_ident, rebase_expr,
    resolve_aliases_to_exprs, resolve_columns, resolve_positions_to_exprs,
};
use datafusion_common::{DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::expr_rewriter::{normalize_col, normalize_col_with_schemas};
use datafusion_expr::logical_plan::builder::project;
use datafusion_expr::logical_plan::Join as HashJoin;
use datafusion_expr::logical_plan::JoinConstraint as HashJoinConstraint;
use datafusion_expr::utils::{
    expand_qualified_wildcard, expand_wildcard, expr_as_column_expr, expr_to_columns,
    find_aggregate_exprs, find_column_exprs, find_window_exprs,
};
use datafusion_expr::Expr::Alias;
use datafusion_expr::{
    Expr, Filter, GroupingSet, LogicalPlan, LogicalPlanBuilder, Partitioning,
};
use sqlparser::ast::{Expr as SQLExpr, WildcardAdditionalOptions};
use sqlparser::ast::{Select, SelectItem, TableWithJoins};
use std::collections::HashSet;
use std::sync::Arc;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Generate a logic plan from an SQL select
    pub(super) fn select_to_plan(
        &self,
        select: Select,
        planner_context: &mut PlannerContext,
        outer_query_schema: Option<&DFSchema>,
    ) -> Result<LogicalPlan> {
        // check for unsupported syntax first
        if !select.cluster_by.is_empty() {
            return Err(DataFusionError::NotImplemented("CLUSTER BY".to_string()));
        }
        if !select.lateral_views.is_empty() {
            return Err(DataFusionError::NotImplemented("LATERAL VIEWS".to_string()));
        }
        if select.qualify.is_some() {
            return Err(DataFusionError::NotImplemented("QUALIFY".to_string()));
        }
        if select.top.is_some() {
            return Err(DataFusionError::NotImplemented("TOP".to_string()));
        }

        // process `from` clause
        let plan = self.plan_from_tables(select.from, planner_context)?;
        let empty_from = matches!(plan, LogicalPlan::EmptyRelation(_));
        // build from schema for unqualifier column ambiguous check
        // we should get only one field for unqualifier column from schema.
        let from_schema = self.build_schema_for_ambiguous_check(&plan)?;

        // process `where` clause
        let plan = self.plan_selection(
            select.selection,
            plan,
            outer_query_schema,
            planner_context,
        )?;

        // process the SELECT expressions, with wildcards expanded.
        let select_exprs = self.prepare_select_exprs(
            &plan,
            select.projection,
            empty_from,
            planner_context,
            &from_schema,
        )?;

        // having and group by clause may reference aliases defined in select projection
        let projected_plan = self.project(plan.clone(), select_exprs.clone())?;
        let mut combined_schema = (**projected_plan.schema()).clone();
        combined_schema.merge(plan.schema());

        // this alias map is resolved and looked up in both having exprs and group by exprs
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
                let having_expr = resolve_aliases_to_exprs(&having_expr, &alias_map)?;
                normalize_col(having_expr, &projected_plan)
            })
            .transpose()?;

        // The outer expressions we will search through for
        // aggregates. Aggregates may be sourced from the SELECT...
        let mut aggr_expr_haystack = select_exprs.clone();
        // ... or from the HAVING.
        if let Some(having_expr) = &having_expr_opt {
            aggr_expr_haystack.push(having_expr.clone());
        }

        // All of the aggregate expressions (deduplicated).
        let aggr_exprs = find_aggregate_exprs(&aggr_expr_haystack);

        // All of the group by expressions
        let group_by_exprs = select
            .group_by
            .into_iter()
            .map(|e| {
                let group_by_expr =
                    self.sql_expr_to_logical_expr(e, &combined_schema, planner_context)?;
                // aliases from the projection can conflict with same-named expressions in the input
                let mut alias_map = alias_map.clone();
                for f in plan.schema().fields() {
                    alias_map.remove(f.name());
                }
                let group_by_expr = resolve_aliases_to_exprs(&group_by_expr, &alias_map)?;
                let group_by_expr =
                    resolve_positions_to_exprs(&group_by_expr, &select_exprs)
                        .unwrap_or(group_by_expr);
                let group_by_expr = normalize_col(group_by_expr, &projected_plan)?;
                self.validate_schema_satisfies_exprs(
                    plan.schema(),
                    &[group_by_expr.clone()],
                )?;
                Ok(group_by_expr)
            })
            .collect::<Result<Vec<Expr>>>()?;

        // process group by, aggregation or having
        let (plan, mut select_exprs_post_aggr, having_expr_post_aggr) = if !group_by_exprs
            .is_empty()
            || !aggr_exprs.is_empty()
        {
            self.aggregate(
                plan,
                &select_exprs,
                having_expr_opt.as_ref(),
                group_by_exprs,
                aggr_exprs,
            )?
        } else {
            match having_expr_opt {
                Some(having_expr) => return Err(DataFusionError::Plan(
                    format!("HAVING clause references: {having_expr} must appear in the GROUP BY clause or be used in an aggregate function"))),
                None => (plan, select_exprs, having_expr_opt)
            }
        };

        let plan = if let Some(having_expr_post_aggr) = having_expr_post_aggr {
            LogicalPlanBuilder::from(plan)
                .filter(having_expr_post_aggr)?
                .build()?
        } else {
            plan
        };

        // process window function
        let window_func_exprs = find_window_exprs(&select_exprs_post_aggr);

        let plan = if window_func_exprs.is_empty() {
            plan
        } else {
            let plan = LogicalPlanBuilder::window_plan(plan, window_func_exprs.clone())?;

            // re-write the projection
            select_exprs_post_aggr = select_exprs_post_aggr
                .iter()
                .map(|expr| rebase_expr(expr, &window_func_exprs, &plan))
                .collect::<Result<Vec<Expr>>>()?;

            plan
        };

        // final projection
        let plan = project(plan, select_exprs_post_aggr)?;

        // process distinct clause
        let plan = if select.distinct {
            LogicalPlanBuilder::from(plan).distinct()?.build()
        } else {
            Ok(plan)
        }?;

        // DISTRIBUTE BY
        if !select.distribute_by.is_empty() {
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
                .build()
        } else {
            Ok(plan)
        }
    }

    /// Generate a logic plan from selection clause, the function contain optimization for cross join to inner join
    /// Related PR: <https://github.com/apache/arrow-datafusion/pull/1566>
    fn plan_selection(
        &self,
        selection: Option<SQLExpr>,
        plan: LogicalPlan,
        outer_query_schema: Option<&DFSchema>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match selection {
            Some(predicate_expr) => {
                let mut join_schema = (**plan.schema()).clone();
                let mut all_schemas: Vec<DFSchemaRef> = vec![];
                for schema in plan.all_schemas() {
                    all_schemas.push(schema.clone());
                }
                if let Some(outer) = outer_query_schema {
                    all_schemas.push(Arc::new(outer.clone()));
                    join_schema.merge(outer);
                }
                let x: Vec<&DFSchemaRef> = all_schemas.iter().collect();

                let filter_expr =
                    self.sql_to_expr(predicate_expr, &join_schema, planner_context)?;
                let mut using_columns = HashSet::new();
                expr_to_columns(&filter_expr, &mut using_columns)?;
                let filter_expr = normalize_col_with_schemas(
                    filter_expr,
                    x.as_slice(),
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

    fn plan_from_tables(
        &self,
        mut from: Vec<TableWithJoins>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match from.len() {
            0 => Ok(LogicalPlanBuilder::empty(true).build()?),
            1 => {
                let from = from.remove(0);
                self.plan_table_with_joins(from, planner_context)
            }
            _ => {
                let mut plans = from
                    .into_iter()
                    .map(|t| self.plan_table_with_joins(t, planner_context));

                let mut left = LogicalPlanBuilder::from(plans.next().unwrap()?);

                for right in plans {
                    left = left.cross_join(right?)?;
                }
                Ok(left.build()?)
            }
        }
    }

    /// Returns the `Expr`'s corresponding to a SQL query's SELECT expressions.
    ///
    /// Wildcards are expanded into the concrete list of columns.
    fn prepare_select_exprs(
        &self,
        plan: &LogicalPlan,
        projection: Vec<SelectItem>,
        empty_from: bool,
        planner_context: &mut PlannerContext,
        from_schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        projection
            .into_iter()
            .map(|expr| {
                self.sql_select_to_rex(
                    expr,
                    plan,
                    empty_from,
                    planner_context,
                    from_schema,
                )
            })
            .flat_map(|result| match result {
                Ok(vec) => vec.into_iter().map(Ok).collect(),
                Err(err) => vec![Err(err)],
            })
            .collect::<Result<Vec<Expr>>>()
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(
        &self,
        sql: SelectItem,
        plan: &LogicalPlan,
        empty_from: bool,
        planner_context: &mut PlannerContext,
        from_schema: &DFSchema,
    ) -> Result<Vec<Expr>> {
        match sql {
            SelectItem::UnnamedExpr(expr) => {
                let expr = self.sql_to_expr(expr, plan.schema(), planner_context)?;
                self.column_reference_ambiguous_check(from_schema, &[expr.clone()])?;
                Ok(vec![normalize_col(expr, plan)?])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let select_expr =
                    self.sql_to_expr(expr, plan.schema(), planner_context)?;
                self.column_reference_ambiguous_check(
                    from_schema,
                    &[select_expr.clone()],
                )?;
                let expr = Alias(Box::new(select_expr), normalize_ident(alias));
                Ok(vec![normalize_col(expr, plan)?])
            }
            SelectItem::Wildcard(options) => {
                Self::check_wildcard_options(options)?;

                if empty_from {
                    return Err(DataFusionError::Plan(
                        "SELECT * with no tables specified is not valid".to_string(),
                    ));
                }
                // do not expand from outer schema
                expand_wildcard(plan.schema().as_ref(), plan)
            }
            SelectItem::QualifiedWildcard(ref object_name, options) => {
                Self::check_wildcard_options(options)?;

                let qualifier = format!("{object_name}");
                // do not expand from outer schema
                expand_qualified_wildcard(&qualifier, plan.schema().as_ref())
            }
        }
    }

    /// ambiguous check for unqualifier column
    fn column_reference_ambiguous_check(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    None => {
                        // should get only one field in from_schema.
                        if schema.fields_with_unqualified_name(&col.name).len() != 1 {
                            Err(DataFusionError::Internal(format!(
                                "column reference {} is ambiguous",
                                col.name
                            )))
                        } else {
                            Ok(())
                        }
                    }
                    _ => Ok(()),
                },
                _ => Ok(()),
            })
    }

    fn check_wildcard_options(options: WildcardAdditionalOptions) -> Result<()> {
        let WildcardAdditionalOptions {
            opt_exclude,
            opt_except,
            opt_rename,
        } = options;

        if opt_exclude.is_some() || opt_except.is_some() || opt_rename.is_some() {
            Err(DataFusionError::NotImplemented(
                "wildcard * with EXCLUDE, EXCEPT or RENAME not supported ".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// build schema for unqualifier column ambiguous check
    fn build_schema_for_ambiguous_check(&self, plan: &LogicalPlan) -> Result<DFSchema> {
        let mut fields = plan.schema().fields().clone();

        let metadata = plan.schema().metadata().clone();
        if let LogicalPlan::Join(HashJoin {
            join_constraint: HashJoinConstraint::Using,
            ref on,
            ref left,
            ..
        }) = plan
        {
            // For query: select id from t1 join t2 using(id), this is legal.
            // We should dedup the fields for cols in using clause.
            for join_keys in on.iter() {
                let join_col = &join_keys.0.try_into_col()?;
                let left_field = left.schema().field_from_column(join_col)?;
                fields.retain(|field| {
                    field.unqualified_column().name
                        != left_field.unqualified_column().name
                });
                fields.push(left_field.clone());
            }
        }

        DFSchema::new_with_metadata(fields, metadata)
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
        input: LogicalPlan,
        select_exprs: &[Expr],
        having_expr_opt: Option<&Expr>,
        group_by_exprs: Vec<Expr>,
        aggr_exprs: Vec<Expr>,
    ) -> Result<(LogicalPlan, Vec<Expr>, Option<Expr>)> {
        // create the aggregate plan
        let plan = LogicalPlanBuilder::from(input.clone())
            .aggregate(group_by_exprs.clone(), aggr_exprs.clone())?
            .build()?;

        // in this next section of code we are re-writing the projection to refer to columns
        // output by the aggregate plan. For example, if the projection contains the expression
        // `SUM(a)` then we replace that with a reference to a column `SUM(a)` produced by
        // the aggregate plan.

        // combine the original grouping and aggregate expressions into one list (note that
        // we do not add the "having" expression since that is not part of the projection)
        let mut aggr_projection_exprs = vec![];
        for expr in &group_by_exprs {
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
        aggr_projection_exprs.extend_from_slice(&aggr_exprs);

        // now attempt to resolve columns and replace with fully-qualified columns
        let aggr_projection_exprs = aggr_projection_exprs
            .iter()
            .map(|expr| resolve_columns(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we replace any expressions that are not a column with a column referencing
        // an output column from the aggregate schema
        let column_exprs_post_aggr = aggr_projection_exprs
            .iter()
            .map(|expr| expr_as_column_expr(expr, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // next we re-write the projection
        let select_exprs_post_aggr = select_exprs
            .iter()
            .map(|expr| rebase_expr(expr, &aggr_projection_exprs, &input))
            .collect::<Result<Vec<Expr>>>()?;

        // finally, we have some validation that the re-written projection can be resolved
        // from the aggregate output columns
        check_columns_satisfy_exprs(
            &column_exprs_post_aggr,
            &select_exprs_post_aggr,
            "Projection references non-aggregate values",
        )?;

        // Rewrite the HAVING expression to use the columns produced by the
        // aggregation.
        let having_expr_post_aggr = if let Some(having_expr) = having_expr_opt {
            let having_expr_post_aggr =
                rebase_expr(having_expr, &aggr_projection_exprs, &input)?;

            check_columns_satisfy_exprs(
                &column_exprs_post_aggr,
                &[having_expr_post_aggr.clone()],
                "HAVING clause references non-aggregate values",
            )?;

            Some(having_expr_post_aggr)
        } else {
            None
        };

        Ok((plan, select_exprs_post_aggr, having_expr_post_aggr))
    }
}
