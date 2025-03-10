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
use datafusion_common::{plan_err, DFSchema, Diagnostic, Result, Span, Spans};
use datafusion_expr::expr::Exists;
use datafusion_expr::expr::InSubquery;
use datafusion_expr::{Expr, Subquery};
use sqlparser::ast::Expr as SQLExpr;
use sqlparser::ast::{Query, SelectItem, SetExpr};
use std::sync::Arc;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn parse_exists_subquery(
        &self,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let old_outer_query_schema =
            planner_context.set_outer_query_schema(Some(input_schema.clone().into()));
        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        planner_context.set_outer_query_schema(old_outer_query_schema);
        Ok(Expr::Exists(Exists {
            subquery: Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
                spans: None,
            },
            negated,
        }))
    }

    pub(super) fn parse_in_subquery(
        &self,
        expr: SQLExpr,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let old_outer_query_schema =
            planner_context.set_outer_query_schema(Some(input_schema.clone().into()));
        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        planner_context.set_outer_query_schema(old_outer_query_schema);
        let expr = Box::new(self.sql_to_expr(expr, input_schema, planner_context)?);
        Ok(Expr::InSubquery(InSubquery::new(
            expr,
            Subquery {
                subquery: Arc::new(sub_plan),
                outer_ref_columns,
                spans: None,
            },
            negated,
        )))
    }

    pub(super) fn parse_scalar_subquery(
        &self,
        subquery: Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let old_outer_query_schema =
            planner_context.set_outer_query_schema(Some(input_schema.clone().into()));
        let mut spans = Spans::new();
        if let SetExpr::Select(select) = subquery.body.as_ref() {
            for item in &select.projection {
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    if let Some(span) = Span::try_from_sqlparser_span(alias.span) {
                        spans.add_span(span);
                    }
                }
            }
        }
        let sub_plan = self.query_to_plan(subquery, planner_context)?;
        let outer_ref_columns = sub_plan.all_out_ref_exprs();
        planner_context.set_outer_query_schema(old_outer_query_schema);
        if sub_plan.schema().fields().len() > 1 {
            let fields = sub_plan.schema().fields();
            let error_message = format!(
                "Scalar subquery should only return one column, but found {}: {}",
                fields.len(),
                sub_plan.schema().field_names().join(", ")
            );

            return plan_err!("{}", &error_message).map_err(|err| {
                let spans_vec = spans.clone();
                let primary_span = spans_vec.first().clone();

                let mut diagnostic = Diagnostic::new_error(
                    "Scalar subquery returns multiple columns",
                    primary_span,
                );

                let columns_info = format!(
                    "Found {} columns: {}",
                    fields.len(),
                    sub_plan.schema().field_names().join(", ")
                );

                if spans_vec.0.len() > 1 {
                    diagnostic.add_note(columns_info.clone(), primary_span);
                    for (i, span) in spans_vec.iter().skip(1).enumerate() {
                        diagnostic.add_note(
                            format!("Extra column {}", i + 1),
                            Some(span.clone()),
                        );
                    }
                } else {
                    diagnostic.add_note(columns_info, primary_span);
                }

                diagnostic.add_help(
                    "Select only one column in the subquery or use a row constructor",
                    None,
                );

                err.with_diagnostic(diagnostic)
            });
        }

        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(sub_plan),
            outer_ref_columns,
            spans: Some(spans),
        }))
    }
}
