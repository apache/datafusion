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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{
    not_impl_err, plan_err, DFSchema, Diagnostic, Result, Span, TableReference,
};
use datafusion_expr::builder::subquery_alias;
use datafusion_expr::{expr::Unnest, Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::{Subquery, SubqueryAlias};
use sqlparser::ast::{FunctionArg, FunctionArgExpr, Spanned, TableFactor};

mod join;

impl<S: ContextProvider> SqlToRel<'_, S> {
    /// Create a `LogicalPlan` that scans the named relation
    fn create_relation(
        &self,
        relation: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let relation_span = relation.span();
        let (plan, alias) = match relation {
            TableFactor::Table {
                name, alias, args, ..
            } => {
                if let Some(func_args) = args {
                    let tbl_func_name = name.0.first().unwrap().value.to_string();
                    let args = func_args
                        .args
                        .into_iter()
                        .flat_map(|arg| {
                            if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg
                            {
                                self.sql_expr_to_logical_expr(
                                    expr,
                                    &DFSchema::empty(),
                                    planner_context,
                                )
                            } else {
                                plan_err!("Unsupported function argument type: {:?}", arg)
                            }
                        })
                        .collect::<Vec<_>>();
                    let provider = self
                        .context_provider
                        .get_table_function_source(&tbl_func_name, args)?;
                    let plan = LogicalPlanBuilder::scan(
                        TableReference::Bare {
                            table: "tmp_table".into(),
                        },
                        provider,
                        None,
                    )?
                    .build()?;
                    (plan, alias)
                } else {
                    // Normalize name and alias
                    let table_ref = self.object_name_to_table_reference(name)?;
                    let table_name = table_ref.to_string();
                    let cte = planner_context.get_cte(&table_name);
                    (
                        match (
                            cte,
                            self.context_provider.get_table_source(table_ref.clone()),
                        ) {
                            (Some(cte_plan), _) => Ok(cte_plan.clone()),
                            (_, Ok(provider)) => LogicalPlanBuilder::scan(
                                table_ref.clone(),
                                provider,
                                None,
                            )?
                            .build(),
                            (None, Err(e)) => {
                                let e = e.with_diagnostic(Diagnostic::new_error(
                                    format!("table '{}' not found", table_ref),
                                    Span::try_from_sqlparser_span(relation_span),
                                ));
                                Err(e)
                            }
                        }?,
                        alias,
                    )
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let logical_plan = self.query_to_plan(*subquery, planner_context)?;
                (logical_plan, alias)
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
            } => (
                self.plan_table_with_joins(*table_with_joins, planner_context)?,
                alias,
            ),
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset: false,
                with_offset_alias: None,
                with_ordinality,
            } => {
                if with_ordinality {
                    return not_impl_err!("UNNEST with ordinality is not supported yet");
                }

                // Unnest table factor has empty input
                let schema = DFSchema::empty();
                let input = LogicalPlanBuilder::empty(true).build()?;
                // Unnest table factor can have multiple arguments.
                // We treat each argument as a separate unnest expression.
                let unnest_exprs = array_exprs
                    .into_iter()
                    .map(|sql_expr| {
                        let expr = self.sql_expr_to_logical_expr(
                            sql_expr,
                            &schema,
                            planner_context,
                        )?;
                        Self::check_unnest_arg(&expr, &schema)?;
                        Ok(Expr::Unnest(Unnest::new(expr)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if unnest_exprs.is_empty() {
                    return plan_err!("UNNEST must have at least one argument");
                }
                let logical_plan = self.try_process_unnest(input, unnest_exprs)?;
                (logical_plan, alias)
            }
            TableFactor::UNNEST { .. } => {
                return not_impl_err!(
                    "UNNEST table factor with offset is not supported yet"
                );
            }
            // @todo Support TableFactory::TableFunction?
            _ => {
                return not_impl_err!(
                    "Unsupported ast node {relation:?} in create_relation"
                );
            }
        };

        let optimized_plan = optimize_subquery_sort(plan)?.data;
        if let Some(alias) = alias {
            self.apply_table_alias(optimized_plan, alias)
        } else {
            Ok(optimized_plan)
        }
    }

    pub(crate) fn create_relation_subquery(
        &self,
        subquery: TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // At this point for a syntactically valid query the outer_from_schema is
        // guaranteed to be set, so the `.unwrap()` call will never panic. This
        // is the case because we only call this method for lateral table
        // factors, and those can never be the first factor in a FROM list. This
        // means we arrived here through the `for` loop in `plan_from_tables` or
        // the `for` loop in `plan_table_with_joins`.
        let old_from_schema = planner_context
            .set_outer_from_schema(None)
            .unwrap_or_else(|| Arc::new(DFSchema::empty()));
        let new_query_schema = match planner_context.outer_query_schema() {
            Some(old_query_schema) => {
                let mut new_query_schema = old_from_schema.as_ref().clone();
                new_query_schema.merge(old_query_schema);
                Some(Arc::new(new_query_schema))
            }
            None => Some(Arc::clone(&old_from_schema)),
        };
        let old_query_schema = planner_context.set_outer_query_schema(new_query_schema);

        let plan = self.create_relation(subquery, planner_context)?;
        let outer_ref_columns = plan.all_out_ref_exprs();

        planner_context.set_outer_query_schema(old_query_schema);
        planner_context.set_outer_from_schema(Some(old_from_schema));

        // We can omit the subquery wrapper if there are no columns
        // referencing the outer scope.
        if outer_ref_columns.is_empty() {
            return Ok(plan);
        }

        match plan {
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                subquery_alias(
                    LogicalPlan::Subquery(Subquery {
                        subquery: input,
                        outer_ref_columns,
                    }),
                    alias,
                )
            }
            plan => Ok(LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(plan),
                outer_ref_columns,
            })),
        }
    }
}

fn optimize_subquery_sort(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // When initializing subqueries, we examine sort options since they might be unnecessary.
    // They are only important if the subquery result is affected by the ORDER BY statement,
    // which can happen when we have:
    // 1. DISTINCT ON / ARRAY_AGG ... => Handled by an `Aggregate` and its requirements.
    // 2. RANK / ROW_NUMBER ... => Handled by a `WindowAggr` and its requirements.
    // 3. LIMIT => Handled by a `Sort`, so we need to search for it.
    let mut has_limit = false;
    let new_plan = plan.transform_down(|c| {
        if let LogicalPlan::Limit(_) = c {
            has_limit = true;
            return Ok(Transformed::no(c));
        }
        match c {
            LogicalPlan::Sort(s) => {
                if !has_limit {
                    has_limit = false;
                    return Ok(Transformed::yes(s.input.as_ref().clone()));
                }
                Ok(Transformed::no(LogicalPlan::Sort(s)))
            }
            _ => Ok(Transformed::no(c)),
        }
    });
    new_plan
}
