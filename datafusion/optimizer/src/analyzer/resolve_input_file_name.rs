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

//! Rewrite `input_file_name()` to an internal scan-projected column.

use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::metadata_columns::{
    INPUT_FILE_NAME_COL, append_input_file_name_field,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result, plan_err};
use datafusion_expr::logical_plan::{
    Distinct, Filter, Limit, Sort, SubqueryAlias, TableScan,
};
use datafusion_expr::{Expr, LogicalPlan, Projection};

use super::AnalyzerRule;

#[derive(Debug, Default)]
pub struct ResolveInputFileName {}

impl ResolveInputFileName {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for ResolveInputFileName {
    fn name(&self) -> &str {
        "resolve_input_file_name"
    }

    fn analyze(
        &self,
        plan: LogicalPlan,
        _options: &ConfigOptions,
    ) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| self.rewrite_plan(plan))
            .map(|res| res.data)
    }
}

impl ResolveInputFileName {
    fn rewrite_plan(&self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                let mut contains_input_file_name = false;
                let rewritten_exprs = expr
                    .into_iter()
                    .map(|expr| {
                        let (expr, rewritten) = self.rewrite_projection_expr(expr)?;
                        contains_input_file_name |= rewritten;
                        Ok(expr)
                    })
                    .collect::<Result<Vec<_>>>()?;

                if !contains_input_file_name {
                    let projection =
                        Projection::try_new_with_schema(rewritten_exprs, input, schema)?;
                    return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                }

                let new_input = Arc::new(self.annotate_table_scan(input.as_ref())?);
                let projection = Projection::try_new(rewritten_exprs, new_input)?;

                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }
            _ => {
                self.ensure_not_in_non_projection(&plan)?;
                Ok(Transformed::no(plan))
            }
        }
    }

    fn ensure_not_in_non_projection(&self, plan: &LogicalPlan) -> Result<()> {
        for expr in plan.expressions() {
            if contains_input_file_name(&expr)? {
                return plan_err!(
                    "input_file_name() is only supported in the SELECT list"
                );
            }
        }
        Ok(())
    }

    fn rewrite_projection_expr(&self, expr: Expr) -> Result<(Expr, bool)> {
        if is_input_file_name(&expr) {
            let rewritten = Expr::Column(Column::from_name(INPUT_FILE_NAME_COL))
                .alias("input_file_name");
            return Ok((rewritten, true));
        }

        let mut found = false;
        let transformed = expr.transform_up(|expr| {
            if is_input_file_name(&expr) {
                found = true;
                Ok(Transformed::yes(Expr::Column(Column::from_name(
                    INPUT_FILE_NAME_COL,
                ))))
            } else {
                Ok(Transformed::no(expr))
            }
        })?;

        Ok((transformed.data, found))
    }

    fn annotate_table_scan(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::TableScan(scan) => self.rewrite_table_scan(scan),
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let new_input = Arc::new(self.annotate_table_scan(input.as_ref())?);
                let alias = SubqueryAlias::try_new(new_input, alias.clone())?;
                Ok(LogicalPlan::SubqueryAlias(alias))
            }
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let new_input = Arc::new(self.annotate_table_scan(input.as_ref())?);
                let filter = Filter::try_new(predicate.clone(), new_input)?;
                Ok(LogicalPlan::Filter(filter))
            }
            LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                let new_input = Arc::new(self.annotate_table_scan(input.as_ref())?);
                Ok(LogicalPlan::Sort(Sort {
                    expr: expr.clone(),
                    input: new_input,
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Limit(Limit { skip, fetch, input }) => {
                let new_input = Arc::new(self.annotate_table_scan(input.as_ref())?);
                Ok(LogicalPlan::Limit(Limit {
                    skip: skip.clone(),
                    fetch: fetch.clone(),
                    input: new_input,
                }))
            }
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let new_input = Arc::new(self.annotate_table_scan(input.as_ref())?);
                Ok(LogicalPlan::Distinct(Distinct::All(new_input)))
            }
            LogicalPlan::Distinct(Distinct::On(_)) => {
                plan_err!("input_file_name() is not supported with DISTINCT ON")
            }
            LogicalPlan::Join(_) => plan_err!(
                "input_file_name() cannot be used with joins - the file source would be ambiguous. Use input_file_name() in a subquery on a single table instead."
            ),
            _ => plan_err!(
                "input_file_name() is only supported for file-backed table scans"
            ),
        }
    }

    fn rewrite_table_scan(&self, scan: &TableScan) -> Result<LogicalPlan> {
        if scan
            .source
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == INPUT_FILE_NAME_COL)
        {
            return plan_err!(
                "input_file_name() cannot be used because the table schema already contains '{INPUT_FILE_NAME_COL}'"
            );
        }

        if scan
            .projected_schema
            .fields()
            .iter()
            .any(|field| field.name() == INPUT_FILE_NAME_COL)
        {
            return Ok(LogicalPlan::TableScan(scan.clone()));
        }

        let new_schema = append_input_file_name_field(
            scan.projected_schema.as_ref(),
            Some(scan.table_name.clone()),
        )?;

        let mut new_scan = scan.clone();
        new_scan.projected_schema = Arc::new(new_schema);
        Ok(LogicalPlan::TableScan(new_scan))
    }
}

fn is_input_file_name(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(func) if func.name() == "input_file_name" && func.args.is_empty()
    )
}

fn contains_input_file_name(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| Ok(is_input_file_name(expr)))
}
