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
use datafusion_common::{DFSchema, Result, not_impl_err};
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
                row.into_iter()
                    .map(|v| self.sql_to_expr(v, &empty_schema, planner_context))
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
