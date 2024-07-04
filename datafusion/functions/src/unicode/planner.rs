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

//! SQL planning extensions like [`PositionPlanner`]

use datafusion_common::Result;
use datafusion_expr::{
    expr::ScalarFunction,
    planner::{PlannerResult, UserDefinedSQLPlanner},
    sqlparser, Expr,
};

#[derive(Default)]
pub struct PositionPlanner {}

impl UserDefinedSQLPlanner for PositionPlanner {
    fn plan_udf(
        &self,
        sql: &sqlparser::ast::Expr,
        args: Vec<Expr>,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        let sqlparser::ast::Expr::Position { .. } = sql else {
            return Ok(PlannerResult::Original(args));
        };

        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(crate::unicode::strpos(), args),
        )))
    }
}
