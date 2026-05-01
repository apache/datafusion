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

//! [`EliminateJoin`] rewrites unnecessary `INNER JOIN`s
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Dependency, NullEquality, Result, ScalarValue};
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::{
    Expr, Join, JoinConstraint, JoinType, LogicalPlan, Projection,
    logical_plan::EmptyRelation,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Eliminates joins when join condition is false.
///
/// It also rewrites projected inner joins to semi joins when the right side
/// is only used to test existence and is unique on the join keys.
#[derive(Default, Debug)]
pub struct EliminateJoin;

impl EliminateJoin {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateJoin {
    fn name(&self) -> &str {
        "eliminate_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(join)
                if join.join_type == JoinType::Inner && join.on.is_empty() =>
            {
                match join.filter {
                    Some(Expr::Literal(ScalarValue::Boolean(Some(false)), _)) => Ok(
                        Transformed::yes(LogicalPlan::EmptyRelation(EmptyRelation {
                            produce_one_row: false,
                            schema: join.schema,
                        })),
                    ),
                    _ => Ok(Transformed::no(LogicalPlan::Join(join))),
                }
            }
            LogicalPlan::Projection(projection) => {
                eliminate_projected_inner_join(projection)
            }
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn supports_rewrite(&self) -> bool {
        true
    }
}

fn eliminate_projected_inner_join(
    projection: Projection,
) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Join(join) = projection.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    };

    if !can_convert_projected_inner_to_semi(&projection, join)? {
        return Ok(Transformed::no(LogicalPlan::Projection(projection)));
    }

    let semi_join = Join::try_new(
        Arc::clone(&join.left),
        Arc::clone(&join.right),
        join.on.clone(),
        join.filter.clone(),
        JoinType::LeftSemi,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )?;

    let projection =
        Projection::try_new(projection.expr, Arc::new(LogicalPlan::Join(semi_join)))?;

    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

fn can_convert_projected_inner_to_semi(
    projection: &Projection,
    join: &Join,
) -> Result<bool> {
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.on.is_empty()
        || join.null_aware
    {
        return Ok(false);
    }

    if !projection_exprs_only_reference_left(&projection.expr, join)? {
        return Ok(false);
    }

    let right_keys = join
        .on
        .iter()
        .map(|(_, right)| right.clone())
        .collect::<Vec<_>>();

    Ok(plan_unique_on(&join.right, &right_keys, join.null_equality))
}

fn projection_exprs_only_reference_left(
    projection_exprs: &[Expr],
    join: &Join,
) -> Result<bool> {
    let mut columns = HashSet::new();
    for expr in projection_exprs {
        expr_to_columns(expr, &mut columns)?;
    }

    Ok(columns
        .iter()
        .all(|column| join.left.schema().index_of_column(column).is_ok()))
}

fn plan_unique_on(
    plan: &LogicalPlan,
    key_exprs: &[Expr],
    null_equality: NullEquality,
) -> bool {
    if key_exprs.is_empty() {
        return false;
    }

    let schema = plan.schema();
    let Some(key_indices) = key_exprs
        .iter()
        .map(|expr| match expr {
            Expr::Column(column) => schema.index_of_column(column).ok(),
            _ => None,
        })
        .collect::<Option<HashSet<_>>>()
    else {
        return false;
    };

    let field_count = schema.fields().len();
    schema.functional_dependencies().iter().any(|dependency| {
        dependency.mode == Dependency::Single
            && dependency.target_indices.len() == field_count
            && dependency
                .source_indices
                .iter()
                .all(|idx| key_indices.contains(idx))
            // Nullable unique constraints can contain multiple NULL keys, but
            // regular equality joins do not match NULLs.
            && (null_equality == NullEquality::NullEqualsNothing
                || !dependency.nullable
                || dependency
                    .source_indices
                    .iter()
                    .all(|idx| !schema.field(*idx).is_nullable()))
    })
}

#[cfg(test)]
mod tests {
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::eliminate_join::EliminateJoin;
    use crate::test::{test_table_scan_fields, test_table_scan_with_name};
    use arrow::datatypes::Schema;
    use datafusion_common::{Constraint, Constraints, Result};
    use datafusion_expr::JoinType::Inner;
    use datafusion_expr::test::function_stub::max;
    use datafusion_expr::{
        col, lit,
        logical_plan::builder::{LogicalPlanBuilder, table_source_with_constraints},
    };
    use std::sync::Arc;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateJoin::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn join_on_false() -> Result<()> {
        let plan = LogicalPlanBuilder::empty(false)
            .join_on(
                LogicalPlanBuilder::empty(false).build()?,
                Inner,
                Some(lit(false)),
            )?
            .build()?;

        assert_optimized_plan_equal!(plan, @"EmptyRelation: rows=0")
    }

    #[test]
    fn projected_inner_join_to_left_semi() -> Result<()> {
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("right")?)
            .aggregate(vec![col("right.a")], vec![max(col("right.c"))])?
            .project(vec![col("max(right.c)").alias("max_c"), col("right.a")])?
            .alias("sq")?
            .build()?;

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("left")?)
            .join(right, Inner, (vec!["a"], vec!["a"]), None)?
            .project(vec![col("left.a"), col("left.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: left.a, left.b
          LeftSemi Join: left.a = sq.a
            TableScan: left
            SubqueryAlias: sq
              Projection: max(right.c) AS max_c, right.a
                Aggregate: groupBy=[[right.a]], aggr=[[max(right.c)]]
                  TableScan: right
        "
        )
    }

    #[test]
    fn projected_inner_join_to_left_semi_with_unique_constraint() -> Result<()> {
        let right_schema = Schema::new(test_table_scan_fields());
        let right_source = table_source_with_constraints(
            &right_schema,
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
        );
        let right = LogicalPlanBuilder::scan("right", right_source, None)?.build()?;

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("left")?)
            .join(right, Inner, (vec!["a"], vec!["a"]), None)?
            .project(vec![col("left.a"), col("left.b")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: left.a, left.b
          LeftSemi Join: left.a = right.a
            TableScan: left
            TableScan: right
        "
        )
    }

    #[test]
    fn projected_inner_join_with_filter_to_left_semi() -> Result<()> {
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("right")?)
            .aggregate(vec![col("right.a")], vec![max(col("right.c"))])?
            .project(vec![col("right.a"), col("max(right.c)").alias("max_c")])?
            .alias("sq")?
            .build()?;

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("left")?)
            .join(
                right,
                Inner,
                (vec!["a"], vec!["a"]),
                Some(col("left.b").lt(col("sq.max_c"))),
            )?
            .project(vec![col("left.a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: left.a
          LeftSemi Join: left.a = sq.a Filter: left.b < sq.max_c
            TableScan: left
            SubqueryAlias: sq
              Projection: right.a, max(right.c) AS max_c
                Aggregate: groupBy=[[right.a]], aggr=[[max(right.c)]]
                  TableScan: right
        "
        )
    }

    #[test]
    fn projected_inner_join_preserves_duplicates_without_unique_right() -> Result<()> {
        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("left")?)
            .join(
                test_table_scan_with_name("right")?,
                Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .project(vec![col("left.a")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: left.a
          Inner Join: left.a = right.a
            TableScan: left
            TableScan: right
        "
        )
    }

    #[test]
    fn projected_inner_join_keeps_right_projection_columns() -> Result<()> {
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("right")?)
            .aggregate(vec![col("right.a")], vec![max(col("right.c"))])?
            .build()?;

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("left")?)
            .join(right, Inner, (vec!["a"], vec!["a"]), None)?
            .project(vec![col("left.a"), col("max(right.c)")])?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @r"
        Projection: left.a, max(right.c)
          Inner Join: left.a = right.a
            TableScan: left
            Aggregate: groupBy=[[right.a]], aggr=[[max(right.c)]]
              TableScan: right
        "
        )
    }
}
