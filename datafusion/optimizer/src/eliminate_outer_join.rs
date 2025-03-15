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

//! [`EliminateOuterJoin`] converts `LEFT/RIGHT/FULL` joins to `INNER` joins
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::logical_plan::{JoinType, LogicalPlan};
use std::collections::HashSet;

use crate::optimizer::ApplyOrder;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::utils::{evaluates_to_not_true, NullColumnsSet};
use std::sync::Arc;

///
/// Attempt to replace outer joins with inner joins.
///
/// Outer joins are typically more expensive to compute at runtime
/// than inner joins and prevent various forms of predicate pushdown
/// and other optimizations, so removing them if possible is beneficial.
///
/// Inner joins filter out rows that do match. Outer joins pass rows
/// that do not match padded with nulls. If there is a filter in the
/// query that would filter any such null rows after the join the rows
/// introduced by the outer join are filtered.
///
/// For example, in the `select ... from a left join b on ... where b.xx = 100;`
///
/// For rows when `b.xx` is null (as it would be after an outer join),
/// the `b.xx = 100` predicate filters them out and there is no
/// need to produce null rows for output.
///
/// Generally, an outer join can be rewritten to inner join if the
/// filters from the WHERE clause return false while any inputs are
/// null and columns of those quals are come from nullable side of
/// outer join.
#[derive(Default, Debug)]
pub struct EliminateOuterJoin;

impl EliminateOuterJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Attempt to eliminate outer joins.
impl OptimizerRule for EliminateOuterJoin {
    fn name(&self) -> &str {
        "eliminate_outer_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        fn schema_columns(schema: &DFSchemaRef) -> NullColumnsSet<'_> {
            schema
                .iter()
                .map(|(qualifier, field)| (qualifier, field.name()))
                .collect::<HashSet<_>>()
        }

        fn generate_null_on_left(join_type: JoinType) -> bool {
            matches!(join_type, JoinType::Right | JoinType::Full)
        }

        fn generate_null_on_right(join_type: JoinType) -> bool {
            matches!(join_type, JoinType::Left | JoinType::Full)
        }

        let LogicalPlan::Filter(mut filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        let mut join = match Arc::as_ref(&filter.input) {
            LogicalPlan::Join(join) if join.join_type.is_outer() => join.clone(),
            _ => {
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
        };

        let join_type = join.join_type;
        let left_columns = schema_columns(join.left.schema());
        let right_columns = schema_columns(join.right.schema());

        // the outer join can be eliminated if the predicate returns false when the null side column is null
        let eliminate_left = generate_null_on_left(join_type)
            && evaluates_to_not_true(&filter.predicate, &left_columns);
        let eliminate_right = generate_null_on_right(join_type)
            && evaluates_to_not_true(&filter.predicate, &right_columns);

        join.join_type = eliminate_outer(join_type, eliminate_left, eliminate_right);
        filter.input = Arc::new(LogicalPlan::Join(join));

        Ok(Transformed::yes(LogicalPlan::Filter(filter)))
    }
}

pub fn eliminate_outer(
    join_type: JoinType,
    left_non_nullable: bool,
    right_non_nullable: bool,
) -> JoinType {
    let mut new_join_type = join_type;
    match join_type {
        JoinType::Left => {
            if right_non_nullable {
                new_join_type = JoinType::Inner;
            }
        }
        JoinType::Right => {
            if left_non_nullable {
                new_join_type = JoinType::Inner;
            }
        }
        JoinType::Full => {
            if left_non_nullable && right_non_nullable {
                new_join_type = JoinType::Inner;
            } else if left_non_nullable {
                new_join_type = JoinType::Left;
            } else if right_non_nullable {
                new_join_type = JoinType::Right;
            }
        }
        _ => {}
    }
    new_join_type
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::{
        binary_expr, cast, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        try_cast,
        Operator::{And, Or},
    };

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(EliminateOuterJoin::new()), plan, expected)
    }

    #[test]
    fn eliminate_left_with_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // could not eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_null())?
            .build()?;
        let expected = "\
        Filter: t2.b IS NULL\
        \n  Left Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_left_with_not_null() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Left,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(col("t2.b").is_not_null())?
            .build()?;
        let expected = "\
        Filter: t2.b IS NOT NULL\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_right_with_or() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
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
        Filter: t1.b > UInt32(10) OR t1.c < UInt32(20)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_full_with_and() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
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
        Filter: t1.b > UInt32(10) AND t2.c < UInt32(20)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_full_with_type_cast() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                cast(col("t1.b"), DataType::Int64).gt(lit(10u32)),
                And,
                try_cast(col("t2.c"), DataType::Int64).lt(lit(20u32)),
            ))?
            .build()?;
        let expected = "\
        Filter: CAST(t1.b AS Int64) > UInt32(10) AND TRY_CAST(t2.c AS Int64) < UInt32(20)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn eliminate_full_to_left() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;

        // eliminate to inner join
        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Full,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(binary_expr(
                (col("t1.b") + lit(10u32)).gt(lit(10u32)),
                And,
                col("t2.c").lt(lit(20u32)).is_true(),
            ))?
            .build()?;
        let expected = "\
        Filter: t1.b + UInt32(10) > UInt32(10) AND t2.c < UInt32(20) IS TRUE\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }
}
