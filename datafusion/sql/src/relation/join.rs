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
use datafusion_common::{
    not_impl_err, plan_datafusion_err, plan_err, Column, DFSchema, Result, ScalarValue,
};
use datafusion_expr::logical_plan::LateralTableFunction;
use datafusion_expr::{Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    FunctionArg, FunctionArgExpr, Join, JoinConstraint, JoinOperator, ObjectName,
    TableFactor, TableWithJoins,
};
use std::collections::HashSet;
use std::sync::Arc;

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let mut left = if is_lateral(&t.relation) {
            self.create_relation_subquery(t.relation, planner_context)?
        } else {
            self.create_relation(t.relation, planner_context)?
        };
        let old_outer_from_schema = planner_context.outer_from_schema();
        for join in t.joins {
            planner_context.extend_outer_from_schema(left.schema())?;
            left = self.parse_relation_join(left, join, planner_context)?;
        }
        planner_context.set_outer_from_schema(old_outer_from_schema);
        Ok(left)
    }

    pub(crate) fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let is_lateral = is_lateral_join(&join)?;

        // For LATERAL joins, check if it's a table function before planning
        if is_lateral {
            if let Some(lateral_tf) = self.try_create_lateral_table_function(
                &left,
                &join.relation,
                planner_context,
            )? {
                // LateralTableFunction already represents the complete join result
                return Ok(lateral_tf);
            }
        }

        // Normal join planning
        let right = if is_lateral {
            self.create_relation_subquery(join.relation, planner_context)?
        } else {
            self.create_relation(join.relation, planner_context)?
        };
        match join.join_operator {
            JoinOperator::LeftOuter(constraint) | JoinOperator::Left(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) | JoinOperator::Right(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::LeftSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftSemi,
                planner_context,
            ),
            JoinOperator::RightSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightSemi,
                planner_context,
            ),
            JoinOperator::LeftAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftAnti,
                planner_context,
            ),
            JoinOperator::RightAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightAnti,
                planner_context,
            ),
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin(JoinConstraint::None) => {
                self.parse_cross_join(left, right)
            }
            other => not_impl_err!("Unsupported JOIN operator {other:?}"),
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
                LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(expr))?
                    .build()
            }
            JoinConstraint::Using(object_names) => {
                let keys = object_names
                    .into_iter()
                    .map(|object_name| {
                        let ObjectName(mut object_names) = object_name;
                        if object_names.len() != 1 {
                            not_impl_err!(
                                "Invalid identifier in USING clause. Expected single identifier, got {}", ObjectName(object_names)
                            )
                        } else {
                            let id = object_names.swap_remove(0);
                            id.as_ident()
                                .ok_or_else(|| {
                                    plan_datafusion_err!(
                                        "Expected identifier in USING clause"
                                    )
                                })
                                .map(|ident| Column::from_name(self.ident_normalizer.normalize(ident.clone())))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                LogicalPlanBuilder::from(left)
                    .join_using(right, join_type, keys)?
                    .build()
            }
            JoinConstraint::Natural => {
                let left_cols: HashSet<&String> =
                    left.schema().fields().iter().map(|f| f.name()).collect();
                let keys: Vec<Column> = right
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .filter(|f| left_cols.contains(f))
                    .map(Column::from_name)
                    .collect();
                if keys.is_empty() {
                    self.parse_cross_join(left, right)
                } else {
                    LogicalPlanBuilder::from(left)
                        .join_using(right, join_type, keys)?
                        .build()
                }
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .join_on(right, join_type, [])?
                .build(),
        }
    }

    /// Try to create a LateralTableFunction node if the relation is a table function
    /// with outer references. Returns None if this is not a lateral table function case.
    fn try_create_lateral_table_function(
        &self,
        left: &LogicalPlan,
        relation: &TableFactor,
        planner_context: &mut PlannerContext,
    ) -> Result<Option<LogicalPlan>> {
        // Check if this is a table function call (either Table or Function variant)
        let (tbl_func_name, func_args_vec, alias) = match relation {
            TableFactor::Table {
                name,
                args: Some(func_args),
                alias,
                ..
            } => {
                let name_str = name
                    .0
                    .first()
                    .and_then(|ident| ident.as_ident())
                    .ok_or_else(|| plan_datafusion_err!("Invalid table function name"))?
                    .to_string();
                (name_str, func_args.args.clone(), alias.as_ref())
            }
            TableFactor::Function {
                name, args, alias, ..
            } => {
                let name_str = name
                    .0
                    .first()
                    .and_then(|ident| ident.as_ident())
                    .ok_or_else(|| plan_datafusion_err!("Invalid function name"))?
                    .to_string();
                (name_str, args.clone(), alias.as_ref())
            }
            _ => return Ok(None),
        };

        // Parse arguments to expressions
        // Use the outer from schema so that column references are properly recognized
        let schema_for_args = planner_context
            .outer_from_schema()
            .unwrap_or_else(|| Arc::new(DFSchema::empty()));

        let args = func_args_vec
            .iter()
            .map(|arg| {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
                    self.sql_expr_to_logical_expr(
                        expr.clone(),
                        &schema_for_args,
                        planner_context,
                    )
                } else {
                    plan_err!("Unsupported function argument type")
                }
            })
            .collect::<Result<Vec<_>>>()?;

        // LATERAL functions evaluate row-by-row when referencing outer columns
        let has_column_refs = args.iter().any(|expr| {
            matches!(expr, Expr::Column(_)) || expr.contains_outer()
        });

        if has_column_refs {
            // For table functions with outer references, we need to get the schema
            // but can't actually call the function yet (outer refs not resolved).
            // We'll replace outer references with placeholder literals to get the schema.
            let placeholder_args: Vec<Expr> = args
                .iter()
                .enumerate()
                .map(|(idx, arg)| {
                    if matches!(arg, Expr::Column(_)) || arg.contains_outer() {
                        let data_type = arg.get_type(&schema_for_args)?;

                        // Use incrementing values (1, 2, 3...) to ensure valid ranges for functions
                        // like generate_series(start, end) where start < end
                        let val = (idx + 1) as i64;

                        let placeholder =
                            ScalarValue::try_new_placeholder(&data_type, val)?;

                        Ok(Expr::Literal(placeholder, None))
                    } else {
                        Ok(arg.clone())
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let provider = self
                .context_provider
                .get_table_function_source(&tbl_func_name, placeholder_args)?;
            let tf_schema = provider.schema();

            let qualifier = alias
                .map(|a| self.ident_normalizer.normalize(a.name.clone()))
                .unwrap_or_else(|| format!("{tbl_func_name}()"));

            let tf_df_schema =
                DFSchema::try_from_qualified_schema(qualifier.as_str(), &tf_schema)?;

            let combined_schema = left.schema().join(&tf_df_schema)?;

            let lateral_tf = LateralTableFunction {
                input: Arc::new(left.clone()),
                function_name: tbl_func_name.clone(),
                args,
                schema: Arc::new(combined_schema),
                table_function_schema: Arc::new(tf_df_schema),
            };
            return Ok(Some(LogicalPlan::LateralTableFunction(lateral_tf)));
        }

        Ok(None)
    }
}

/// Return `true` iff the given [`TableFactor`] is lateral.
pub(crate) fn is_lateral(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Derived { lateral, .. } => *lateral,
        TableFactor::Function { lateral, .. } => *lateral,
        TableFactor::UNNEST { .. } => true,
        _ => false,
    }
}

/// Return `true` iff the given [`Join`] is lateral.
pub(crate) fn is_lateral_join(join: &Join) -> Result<bool> {
    let is_lateral_syntax = is_lateral(&join.relation);
    let is_apply_syntax = match join.join_operator {
        JoinOperator::FullOuter(..)
        | JoinOperator::Right(..)
        | JoinOperator::RightOuter(..)
        | JoinOperator::RightAnti(..)
        | JoinOperator::RightSemi(..)
            if is_lateral_syntax =>
        {
            return not_impl_err!(
                "LATERAL syntax is not supported for \
                 FULL OUTER and RIGHT [OUTER | ANTI | SEMI] joins"
            );
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => true,
        _ => false,
    };
    Ok(is_lateral_syntax || is_apply_syntax)
}
