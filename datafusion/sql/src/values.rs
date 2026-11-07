// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use crate::expr::reject_window_functions;
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use arrow::datatypes::DataType;
use datafusion_common::config::Dialect;
use datafusion_common::{DFSchema, HashMap, Result, not_impl_err};
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder};
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
        let mut values = rows
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
            if self.options.dialect == Dialect::Spark {
                coerce_spark_decimal_float_values(&mut values, &schema)?;
            }
            LogicalPlanBuilder::values(values)?.build()
        } else {
            LogicalPlanBuilder::values_with_schema(values, &schema)?.build()
        }
    }
}

/// Spark widens a DECIMAL mixed with FLOAT or DOUBLE to DOUBLE. Normalize these
/// columns before the generic VALUES builder applies its decimal-preferring rule.
fn coerce_spark_decimal_float_values(
    values: &mut [Vec<Expr>],
    schema: &DFSchema,
) -> Result<()> {
    let Some(first) = values.first() else {
        return Ok(());
    };
    let width = first.len();
    if values.iter().any(|row| row.len() != width) {
        // Leave shape validation and its diagnostics to LogicalPlanBuilder.
        return Ok(());
    }
    let mut column_types = vec![(false, false); width];
    for row in values.iter() {
        for (expr, (has_decimal, has_float)) in row.iter().zip(&mut column_types) {
            let data_type = expr.get_type(schema)?;
            *has_decimal |= data_type.is_decimal();
            *has_float |= data_type.is_floating();
        }
    }
    for row in values {
        for (expr, (has_decimal, has_float)) in row.iter_mut().zip(&column_types) {
            if *has_decimal && *has_float && expr.get_type(schema)?.is_decimal() {
                *expr = std::mem::take(expr).cast_to(&DataType::Float64, schema)?;
            }
        }
    }
    Ok(())
}
