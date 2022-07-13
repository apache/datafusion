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

//! Optimizer rule to reduce left/right/full join to inner join if possible.
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::{
    logical_plan::{Filter, Join, JoinType, LogicalPlan, Projection},
    utils::from_plan,
};
use datafusion_expr::{Expr, Operator};

use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub struct ReduceOuterJoin;

impl ReduceOuterJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ReduceOuterJoin {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        let mut nonnullable_cols: Vec<Column> = vec![];

        reduce_outer_join(self, plan, &mut nonnullable_cols, optimizer_config)
    }

    fn name(&self) -> &str {
        "reduce_outer_join"
    }
}

/// Attempt to reduce outer joins to inner joins.
/// for query: select ... from a left join b on ... where b.xx = 100;
/// if b.xx is null, and b.xx = 100 returns false, filterd those null rows.
/// Therefore, there is no need to produce null rows for output, we can use
/// inner join instead of left join.
///
/// Generally, an outer join can be reduced to inner join if quals from where
/// return false while any inputs are null and columns of those quals are come from
/// nullable side of outer join.
fn reduce_outer_join(
    _optimizer: &ReduceOuterJoin,
    plan: &LogicalPlan,
    nonnullable_cols: &mut Vec<Column>,
    _optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(Filter { input, predicate }) => match &**input {
            LogicalPlan::Join(join) => {
                extract_nonnullable_columns(
                    predicate,
                    nonnullable_cols,
                    join.left.schema(),
                    join.right.schema(),
                    true,
                )?;
                Ok(LogicalPlan::Filter(Filter {
                    predicate: predicate.clone(),
                    input: Arc::new(reduce_outer_join(
                        _optimizer,
                        input,
                        nonnullable_cols,
                        _optimizer_config,
                    )?),
                }))
            }
            _ => Ok(LogicalPlan::Filter(Filter {
                predicate: predicate.clone(),
                input: Arc::new(reduce_outer_join(
                    _optimizer,
                    input,
                    nonnullable_cols,
                    _optimizer_config,
                )?),
            })),
        },
        LogicalPlan::Join(join) => {
            let mut new_join_type = join.join_type;

            if join.join_type == JoinType::Left
                || join.join_type == JoinType::Right
                || join.join_type == JoinType::Full
            {
                let mut left_nonnullable = false;
                let mut right_nonnullable = false;
                for col in nonnullable_cols.iter() {
                    if join.left.schema().field_from_column(col).is_ok() {
                        left_nonnullable = true;
                    }
                    if join.right.schema().field_from_column(col).is_ok() {
                        right_nonnullable = true;
                    }
                }

                match join.join_type {
                    JoinType::Left => {
                        if right_nonnullable {
                            new_join_type = JoinType::Inner;
                        }
                    }
                    JoinType::Right => {
                        if left_nonnullable {
                            new_join_type = JoinType::Inner;
                        }
                    }
                    JoinType::Full => {
                        if left_nonnullable && right_nonnullable {
                            new_join_type = JoinType::Inner;
                        } else if left_nonnullable {
                            new_join_type = JoinType::Left;
                        } else if right_nonnullable {
                            new_join_type = JoinType::Right;
                        }
                    }
                    _ => {}
                };
            }

            let left_plan = reduce_outer_join(
                _optimizer,
                &join.left,
                &mut nonnullable_cols.clone(),
                _optimizer_config,
            )?;
            let right_plan = reduce_outer_join(
                _optimizer,
                &join.right,
                &mut nonnullable_cols.clone(),
                _optimizer_config,
            )?;

            Ok(LogicalPlan::Join(Join {
                left: Arc::new(left_plan),
                right: Arc::new(right_plan),
                join_type: new_join_type,
                join_constraint: join.join_constraint,
                on: join.on.clone(),
                filter: join.filter.clone(),
                schema: join.schema.clone(),
                null_equals_null: join.null_equals_null,
            }))
        }
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            alias: _,
        }) => {
            let projection = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    // strip alias, as they should not be part of filters
                    let expr = match &expr[i] {
                        Expr::Alias(expr, _) => expr.as_ref().clone(),
                        expr => expr.clone(),
                    };

                    (field.qualified_name(), expr)
                })
                .collect::<HashMap<_, _>>();

            // re-write all Columns based on this projection
            for col in nonnullable_cols.iter_mut() {
                if let Some(Expr::Column(column)) = projection.get(&col.flat_name()) {
                    *col = column.clone();
                }
            }

            // optimize inner
            let new_input = reduce_outer_join(
                _optimizer,
                input,
                nonnullable_cols,
                _optimizer_config,
            )?;

            from_plan(plan, expr, &[new_input])
        }
        _ => {
            let expr = plan.expressions();

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|plan| {
                    reduce_outer_join(
                        _optimizer,
                        plan,
                        nonnullable_cols,
                        _optimizer_config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            from_plan(plan, &expr, &new_inputs)
        }
    }
}

/// Recursively traversese expr, if expr returns false when
/// any inputs are null, treats columns of both sides as nonnullable columns.
///
/// For and/or expr, extracts from all sub exprs and merges the columns.
/// For or expr, if one of sub exprs returns true, discards all columns from or expr.
/// For IS NOT NULL/NOT expr, always returns false for NULL input.
///     extracts columns from these exprs.
/// For all other exprs, fall through
fn extract_nonnullable_columns(
    expr: &Expr,
    nonnullable_cols: &mut Vec<Column>,
    left_schema: &Arc<DFSchema>,
    right_schema: &Arc<DFSchema>,
    top_level: bool,
) -> Result<()> {
    match expr {
        Expr::Column(col) => {
            nonnullable_cols.push(col.clone());
            Ok(())
        }
        Expr::BinaryExpr { left, op, right } => match op {
            // If one of the inputs are null for these operators, the results should be false.
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                extract_nonnullable_columns(
                    left,
                    nonnullable_cols,
                    left_schema,
                    right_schema,
                    false,
                )?;
                extract_nonnullable_columns(
                    right,
                    nonnullable_cols,
                    left_schema,
                    right_schema,
                    false,
                )
            }
            Operator::And | Operator::Or => {
                // treat And as Or if does not from top level, such as
                // not (c1 < 10 and c2 > 100)
                if top_level && *op == Operator::And {
                    extract_nonnullable_columns(
                        left,
                        nonnullable_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    )?;
                    extract_nonnullable_columns(
                        right,
                        nonnullable_cols,
                        left_schema,
                        right_schema,
                        top_level,
                    )?;
                    return Ok(());
                }

                let mut left_nonnullable_cols: Vec<Column> = vec![];
                let mut right_nonnullable_cols: Vec<Column> = vec![];

                extract_nonnullable_columns(
                    left,
                    &mut left_nonnullable_cols,
                    left_schema,
                    right_schema,
                    top_level,
                )?;
                extract_nonnullable_columns(
                    right,
                    &mut right_nonnullable_cols,
                    left_schema,
                    right_schema,
                    top_level,
                )?;

                // for query: select *** from a left join b where b.c1 ... or b.c2 ...
                // this can be reduced to inner join.
                // for query: select *** from a left join b where a.c1 ... or b.c2 ...
                // this can not be reduced.
                // If columns of relation exist in both sub exprs, any columns of this relation
                // can be added to non nullable columns.
                if !left_nonnullable_cols.is_empty() && !right_nonnullable_cols.is_empty()
                {
                    for left_col in &left_nonnullable_cols {
                        for right_col in &right_nonnullable_cols {
                            if (left_schema.field_from_column(left_col).is_ok()
                                && left_schema.field_from_column(right_col).is_ok())
                                || (right_schema.field_from_column(left_col).is_ok()
                                    && right_schema.field_from_column(right_col).is_ok())
                            {
                                nonnullable_cols.push(left_col.clone());
                                break;
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        },
        Expr::Not(arg) => extract_nonnullable_columns(
            arg,
            nonnullable_cols,
            left_schema,
            right_schema,
            false,
        ),
        Expr::IsNotNull(arg) => {
            if !top_level {
                return Ok(());
            }
            extract_nonnullable_columns(
                arg,
                nonnullable_cols,
                left_schema,
                right_schema,
                false,
            )
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use datafusion_expr::{
        binary_expr, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        Operator::{And, Or},
    };

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) {
        let rule = ReduceOuterJoin::new();
        let optimized_plan = rule
            .optimize(plan, &mut OptimizerConfig::new())
            .expect("failed to optimize plan");
        let formatted_plan = format!("{:?}", optimized_plan);
        assert_eq!(formatted_plan, expected);
        assert_eq!(plan.schema(), optimized_plan.schema());
    }

    #[test]
    fn reduce_left_with_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                &t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_null())?
            .build()?;
        let expected = "\
        Filter: #t2.b IS NULL\
        \n  Left Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_left_with_not_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                &t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_not_null())?
            .build()?;
        let expected = "\
        Filter: #t2.b IS NOT NULL\
        \n  Inner Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_right_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                &t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                Or,
                col("t1.c").lt(lit(20u32)),
            ))?
            .build()?;
        let expected = "\
        Filter: #t1.b > UInt32(10) OR #t1.c < UInt32(20)\
        \n  Inner Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }

    #[test]
    fn reduce_full_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // reduce to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                &t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                col("t1.b").gt(lit(10u32)),
                And,
                col("t2.c").lt(lit(20u32)),
            ))?
            .build()?;
        let expected = "\
        Filter: #t1.b > UInt32(10) AND #t2.c < UInt32(20)\
        \n  Inner Join: #t1.a = #t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_eq(&plan, expected);

        Ok(())
    }
}
