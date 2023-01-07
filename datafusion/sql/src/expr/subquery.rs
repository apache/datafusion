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
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Expr, Subquery};
use sqlparser::ast::Expr as SQLExpr;
use sqlparser::ast::Query;
use std::sync::Arc;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn parse_exists_subquery(
        &self,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        Ok(Expr::Exists {
            subquery: Subquery {
                subquery: Arc::new(self.subquery_to_plan(
                    subquery,
                    planner_context,
                    input_schema,
                )?),
            },
            negated,
        })
    }

    pub(super) fn parse_in_subquery(
        &self,
        expr: SQLExpr,
        subquery: Query,
        negated: bool,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        Ok(Expr::InSubquery {
            expr: Box::new(self.sql_to_expr(expr, input_schema, planner_context)?),
            subquery: Subquery {
                subquery: Arc::new(self.subquery_to_plan(
                    subquery,
                    planner_context,
                    input_schema,
                )?),
            },
            negated,
        })
    }

    pub(super) fn parse_scalar_subquery(
        &self,
        subquery: Query,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        Ok(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(self.subquery_to_plan(
                subquery,
                planner_context,
                input_schema,
            )?),
        }))
    }
}
