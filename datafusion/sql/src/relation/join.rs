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
use crate::utils::normalize_ident;
use datafusion_common::{Column, DFSchemaRef, DataFusionError, Result};
use datafusion_expr::expr_rewriter::normalize_col_with_schemas;
use datafusion_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{Join, JoinConstraint, JoinOperator, TableWithJoins};
use std::collections::HashMap;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        // From clause may exist CTEs, we should separate them from global CTEs.
        // CTEs in from clause are allowed to be duplicated.
        // Such as `select * from (WITH source AS (select 1 as e) SELECT * FROM source) t1, (WITH source AS (select 1 as e) SELECT * FROM source) t2;` which is valid.
        // So always use original global CTEs to plan CTEs in from clause.
        // Btw, don't need to add CTEs in from to global CTEs.
        let origin_planner_context = planner_context.clone();
        let left = self.create_relation(t.relation, planner_context)?;
        match t.joins.len() {
            0 => {
                *planner_context = origin_planner_context;
                Ok(left)
            }
            _ => {
                let mut joins = t.joins.into_iter();
                *planner_context = origin_planner_context.clone();
                let mut left = self.parse_relation_join(
                    left,
                    joins.next().unwrap(), // length of joins > 0
                    planner_context,
                )?;
                for join in joins {
                    *planner_context = origin_planner_context.clone();
                    left = self.parse_relation_join(left, join, planner_context)?;
                }
                *planner_context = origin_planner_context;
                Ok(left)
            }
        }
    }

    fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let right = self.create_relation(join.relation, planner_context)?;
        match join.join_operator {
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin => self.parse_cross_join(left, right),
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported JOIN operator {other:?}"
            ))),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let join_schema = left.schema().join(right.schema())?;

                // parse ON expression
                let expr = self.sql_to_expr(sql_expr, &join_schema, planner_context)?;

                // ambiguous check
                ensure_any_column_reference_is_unambiguous(
                    &expr,
                    &[left.schema().clone(), right.schema().clone()],
                )?;

                // normalize all columns in expression
                let using_columns = expr.to_columns()?;
                let filter = normalize_col_with_schemas(
                    expr,
                    &[left.schema(), right.schema()],
                    &[using_columns],
                )?;

                LogicalPlanBuilder::from(left)
                    .join(
                        right,
                        join_type,
                        (Vec::<Column>::new(), Vec::<Column>::new()),
                        Some(filter),
                    )?
                    .build()
            }
            JoinConstraint::Using(idents) => {
                let keys: Vec<Column> = idents
                    .into_iter()
                    .map(|x| Column::from_name(normalize_ident(x)))
                    .collect();
                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                // https://issues.apache.org/jira/browse/ARROW-10727
                Err(DataFusionError::NotImplemented(
                    "NATURAL JOIN is not supported (https://issues.apache.org/jira/browse/ARROW-10727)".to_string(),
                ))
            }
            JoinConstraint::None => Err(DataFusionError::NotImplemented(
                "NONE constraint is not supported".to_string(),
            )),
        }
    }
}

/// Ensure any column reference of the expression is unambiguous.
/// Assume we have two schema:
/// schema1: a, b ,c
/// schema2: a, d, e
///
/// `schema1.a + schema2.a` is unambiguous.
/// `a + d` is ambiguous, because `a` may come from schema1 or schema2.
fn ensure_any_column_reference_is_unambiguous(
    expr: &Expr,
    schemas: &[DFSchemaRef],
) -> Result<()> {
    if schemas.len() == 1 {
        return Ok(());
    }
    // all referenced columns in the expression that don't have relation
    let referenced_cols = expr.to_columns()?;
    let mut no_relation_cols = referenced_cols
        .iter()
        .filter_map(|col| {
            if col.relation.is_none() {
                Some((col.name.as_str(), 0))
            } else {
                None
            }
        })
        .collect::<HashMap<&str, u8>>();
    // find the name of the column existing in multi schemas.
    let ambiguous_col_name = schemas
        .iter()
        .flat_map(|schema| schema.fields())
        .map(|field| field.name())
        .find(|col_name| {
            no_relation_cols.entry(col_name).and_modify(|v| *v += 1);
            matches!(
                no_relation_cols.get_key_value(col_name.as_str()),
                Some((_, 2..))
            )
        });

    if let Some(col_name) = ambiguous_col_name {
        let maybe_field = schemas
            .iter()
            .flat_map(|schema| {
                schema
                    .field_with_unqualified_name(col_name)
                    .map(|f| f.qualified_name())
                    .ok()
            })
            .collect::<Vec<_>>();
        Err(DataFusionError::Plan(format!(
            "reference \'{}\' is ambiguous, could be {};",
            col_name,
            maybe_field.join(","),
        )))
    } else {
        Ok(())
    }
}
