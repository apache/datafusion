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
use datafusion_common::{DFSchema, DataFusionError, Result};
use datafusion_expr::{Expr, GroupingSet};
use sqlparser::ast::Expr as SQLExpr;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
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
                    Err(DataFusionError::Internal(
                        "Tuple expressions are not supported for Rollup expressions"
                            .to_string(),
                    ))
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
                    Err(DataFusionError::Internal(
                        "Tuple expressions not are supported for Cube expressions"
                            .to_string(),
                    ))
                } else {
                    self.sql_expr_to_logical_expr(v[0].clone(), schema, planner_context)
                }
            })
            .collect();
        Ok(Expr::GroupingSet(GroupingSet::Cube(args?)))
    }
}
