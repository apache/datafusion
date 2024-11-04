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
use datafusion_common::{Column, DFSchema, DFSchemaRef, Result};
use datafusion_expr::logical_plan::{JoinType, LogicalPlan};
use datafusion_expr::Expr;
use std::collections::HashSet;

use crate::optimizer::ApplyOrder;
use crate::utils::is_restrict_null_predicate;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::utils::split_conjunction_owned;
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

    fn eliminate_outer_join(
        &self,
        plan: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Filter(mut filter) = plan else {
            return Ok(Transformed::no(plan));
        };

        let mut join = match Arc::unwrap_or_clone(filter.input) {
            LogicalPlan::Join(join) if join.join_type.is_outer() => join,
            other_input => {
                filter.input = Arc::new(other_input);
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
        };

        let join_type = join.join_type;
        let predicates = split_conjunction_owned(filter.predicate.clone());
        let left_columns = schema_columns(join.left.schema());
        let right_columns = schema_columns(join.right.schema());
        let join_schema = &join.schema;

        let new_join_type = match join_type {
            JoinType::Left => {
                eliminate_left_outer_join(predicates, &right_columns, join_schema)
            }
            JoinType::Right => {
                eliminate_right_outer_join(predicates, &left_columns, join_schema)
            }
            JoinType::Full => eliminate_full_outer_join(
                predicates,
                &left_columns,
                &right_columns,
                join_schema,
            ),
            others => unreachable!("{}", others),
        };

        join.join_type = new_join_type;
        filter.input = Arc::new(LogicalPlan::Join(join));

        Ok(Transformed::yes(LogicalPlan::Filter(filter)))
    }
}

fn eliminate_left_outer_join(
    predicates: Vec<Expr>,
    right_columns: &HashSet<Column>,
    join_schema: &DFSchema,
) -> JoinType {
    for predicate in predicates {
        let columns = predicate.column_refs();
        if has_bound_filter(&predicate, &columns, right_columns, join_schema) {
            return JoinType::Inner;
        }
    }
    JoinType::Left
}

fn eliminate_right_outer_join(
    predicates: Vec<Expr>,
    left_columns: &HashSet<Column>,
    join_schema: &DFSchema,
) -> JoinType {
    for predicate in predicates {
        let columns = predicate.column_refs();
        if has_bound_filter(&predicate, &columns, left_columns, join_schema) {
            return JoinType::Inner;
        }
    }
    JoinType::Right
}

fn eliminate_full_outer_join(
    predicates: Vec<Expr>,
    left_columns: &HashSet<Column>,
    right_columns: &HashSet<Column>,
    join_schema: &DFSchema,
) -> JoinType {
    let mut left_exist = false;
    let mut right_exist = false;
    for predicate in predicates {
        let columns = predicate.column_refs();

        if !left_exist
            && has_bound_filter(&predicate, &columns, left_columns, join_schema)
        {
            left_exist = true;
        }

        if !right_exist
            && has_bound_filter(&predicate, &columns, right_columns, join_schema)
        {
            right_exist = true;
        }

        if left_exist && right_exist {
            break;
        }
    }

    if left_exist && right_exist {
        JoinType::Inner
    } else if left_exist {
        JoinType::Left
    } else if right_exist {
        JoinType::Right
    } else {
        JoinType::Full
    }
}

fn has_bound_filter(
    predicate: &Expr,
    predicate_columns: &HashSet<&Column>,
    child_schema_columns: &HashSet<Column>,
    join_schema: &DFSchema,
) -> bool {
    let predicate_cloned = predicate.clone();
    let cols = predicate_columns
        .iter()
        .filter(|col| child_schema_columns.contains(*col))
        .cloned();
    is_restrict_null_predicate(join_schema, predicate_cloned, cols).unwrap_or(false)
}

fn schema_columns(schema: &DFSchemaRef) -> HashSet<Column> {
    schema
        .iter()
        .map(|(qualifier, field)| Column::new(qualifier.cloned(), field.name()))
        .collect::<HashSet<_>>()
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
        self.eliminate_outer_join(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{
        binary_expr, cast, col, lit,
        logical_plan::builder::LogicalPlanBuilder,
        try_cast, ColumnarValue,
        Operator::{And, Gt, Or},
        ScalarUDF, ScalarUDFImpl, Signature, Volatility,
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
    fn eliminate_full_with_hybrid_filter() -> Result<()> {
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
            .filter(binary_expr(col("t1.b"), Gt, col("t2.b")))?
            .build()?;
        let expected = "\
        Filter: t1.b > t2.b\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[derive(Debug)]
    struct DoNothingUdf {
        signature: Signature,
    }

    impl DoNothingUdf {
        pub fn new() -> Self {
            Self {
                signature: Signature::any(1, Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for DoNothingUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "do_nothing"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            Ok(arg_types[0].clone())
        }

        fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
            Ok(args[0].clone())
        }
    }

    #[test]
    fn eliminate_right_with_udf() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let fun = Arc::new(ScalarUDF::new_from_impl(DoNothingUdf::new()));

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(
                Expr::ScalarFunction(ScalarFunction::new_udf(fun, vec![col("t1.b")]))
                    .gt(lit(10u32)),
            )?
            .build()?;

        let expected = "\
        Filter: do_nothing(t1.b) > UInt32(10)\
        \n  Inner Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[derive(Debug)]
    struct AlwaysNullUdf {
        signature: Signature,
    }

    impl AlwaysNullUdf {
        pub fn new() -> Self {
            Self {
                signature: Signature::any(1, Volatility::Immutable),
            }
        }
    }

    impl ScalarUDFImpl for AlwaysNullUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "always_null"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Null)
        }

        fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
            Ok(match &args[0] {
                ColumnarValue::Array(array) => {
                    ColumnarValue::create_null_array(array.len())
                }
                ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::Null),
            })
        }
    }

    #[test]
    fn eliminate_right_with_null_udf() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let fun = Arc::new(ScalarUDF::new_from_impl(AlwaysNullUdf::new()));

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(
                Expr::ScalarFunction(ScalarFunction::new_udf(fun, vec![col("t1.b")]))
                    .is_null(),
            )?
            .build()?;

        let expected = "\
        Filter: always_null(t1.b) IS NULL\
        \n  Right Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }

    #[derive(Debug)]
    struct VolatileUdf {
        signature: Signature,
    }

    impl VolatileUdf {
        pub fn new() -> Self {
            Self {
                signature: Signature::any(1, Volatility::Volatile),
            }
        }
    }

    impl ScalarUDFImpl for VolatileUdf {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn name(&self) -> &str {
            "volatile_func"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Boolean)
        }
    }

    #[test]
    fn eliminate_right_with_volatile_udf() -> Result<()> {
        let t1 = test_table_scan_with_name("t1")?;
        let t2 = test_table_scan_with_name("t2")?;
        let fun = Arc::new(ScalarUDF::new_from_impl(VolatileUdf::new()));

        let plan = LogicalPlanBuilder::from(t1)
            .join(
                t2,
                JoinType::Right,
                (vec![Column::from_name("a")], vec![Column::from_name("a")]),
                None,
            )?
            .filter(Expr::ScalarFunction(ScalarFunction::new_udf(
                fun,
                vec![col("t1.b")],
            )))?
            .build()?;

        let expected = "\
        Filter: volatile_func(t1.b)\
        \n  Right Join: t1.a = t2.a\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_optimized_plan_equal(plan, expected)
    }
}
