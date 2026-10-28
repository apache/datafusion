// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::expr::reject_window_functions;
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{DFSchema, HashMap, Result, not_impl_err};
use datafusion_expr::{LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::Values as SQLValues;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_values_to_plan(
        &self,
        values: SQLValues,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let SQLValues {
            explicit_row: _,
            rows,
            value_keyword,
        } = values;
        if value_keyword {
            return not_impl_err!(
                "`VALUE` keyword not supported. Did you mean `VALUES`?"
            )?;
        }

        let empty_schema = Arc::new(DFSchema::empty());
        let values = rows
            .into_iter()
            .map(|row| {
                row.content
                    .into_iter()
                    .map(|v| {
                        let window_span = self.window_function_span(&v, &HashMap::new());
                        let expr = self.sql_to_expr(v, &empty_schema, planner_context)?;
                        // A VALUES row has no input rows to compute a window over
                        reject_window_functions(
                            &expr,
                            "VALUES",
                            "Compute the window function in a SELECT over a table or subquery instead",
                            window_span,
                        )?;
                        Ok(expr)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = planner_context.table_schema().unwrap_or(empty_schema);
        if schema.fields().is_empty() {
            LogicalPlanBuilder::values(values)?.build()
        } else {
            LogicalPlanBuilder::values_with_schema(values, &schema)?.build()
        }
    }
}
