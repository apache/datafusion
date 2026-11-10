// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::plan_err;
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Expr, GroupingSet};
use sqlparser::ast::Expr as SQLExpr;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(super) fn sql_grouping_sets_to_expr(
        &self,
        exprs: Vec<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args: Result<Vec<Vec<_>>> = exprs
            .into_iter()
            .map(|v| {
                v.into_iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
                    .collect()
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::GroupingSets(args?)))
    }

    pub(super) fn sql_rollup_to_expr(
        &self,
        exprs: Vec<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args: Result<Vec<_>> = exprs
            .into_iter()
            .map(|v| {
                if v.len() != 1 {
                    plan_err!(
                        "Tuple expressions are not supported for Rollup expressions"
                    )
                } else {
                    self.sql_expr_to_logical_expr(v[0].clone(), schema, planner_context)
                }
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::Rollup(args?)))
    }

    pub(super) fn sql_cube_to_expr(
        &self,
        exprs: Vec<Vec<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let args: Result<Vec<_>> = exprs
            .into_iter()
            .map(|v| {
                if v.len() != 1 {
                    plan_err!("Tuple expressions not are supported for Cube expressions")
                } else {
                    self.sql_expr_to_logical_expr(v[0].clone(), schema, planner_context)
                }
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::Cube(args?)))
    }
}
