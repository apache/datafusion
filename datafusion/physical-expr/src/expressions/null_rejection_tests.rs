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

//! Unit tests for `PhysicalExpr::is_null` and `PhysicalExpr::is_not_true`
//! trait methods across all expression types.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::IsFalsy;
    use crate::PhysicalExpr;
    use crate::expressions::{
        CastExpr, Column, LikeExpr, Literal, NegativeExpr, binary, col, in_list,
        is_not_null, is_null, lit, not, try_cast,
    };

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::Operator;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
        ])
    }

    fn null_cols(indices: &[usize]) -> HashSet<usize> {
        indices.iter().copied().collect()
    }

    // ---- Column ----

    #[test]
    fn column_is_null_when_in_null_set() {
        let col = Column::new("a", 0);
        assert_eq!(col.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(col.is_null(&null_cols(&[1])), IsFalsy::Never);
        assert_eq!(col.is_null(&null_cols(&[])), IsFalsy::Never);
    }

    #[test]
    fn column_is_not_true_when_in_null_set() {
        let col = Column::new("a", 0);
        assert_eq!(col.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(col.is_not_true(&null_cols(&[1])), IsFalsy::Never);
    }

    // ---- Literal ----

    #[test]
    fn literal_null_is_null() {
        let null_lit = Literal::new(ScalarValue::Int32(None));
        assert_eq!(null_lit.is_null(&null_cols(&[])), IsFalsy::Always);
        assert_eq!(null_lit.is_not_true(&null_cols(&[])), IsFalsy::Always);
    }

    #[test]
    fn literal_non_null_is_not_null() {
        let val = Literal::new(ScalarValue::Int32(Some(42)));
        assert_eq!(val.is_null(&null_cols(&[0, 1])), IsFalsy::Never);
        assert_eq!(val.is_not_true(&null_cols(&[0, 1])), IsFalsy::Never);
    }

    // ---- BinaryExpr: Comparisons ----

    #[test]
    fn comparison_is_null_when_child_is_null() -> Result<()> {
        let s = schema();
        // a > 5: when a (index 0) is null → NULL
        let expr = binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn comparison_both_columns_null() -> Result<()> {
        let s = schema();
        // a = b: when either is null → NULL
        let expr = binary(col("a", &s)?, Operator::Eq, col("b", &s)?, &s)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[0, 1])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Never);
        Ok(())
    }

    // ---- BinaryExpr: AND ----

    #[test]
    fn and_is_not_true_when_either_child_not_true() -> Result<()> {
        let s = schema();
        // (a > 5) AND (b > 10)
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::And,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?;
        // a is null → left side not-true → AND not-true
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        // b is null → right side not-true → AND not-true
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Always);
        // neither null → both sides could be true
        assert_eq!(expr.is_not_true(&null_cols(&[2])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn and_is_null_three_valued() -> Result<()> {
        let s = schema();
        // (a > 5) AND (b > 10)
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::And,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?;
        // a null, b not null: NULL AND (b > 10)
        //   b > 10 could be TRUE → NULL AND TRUE = NULL
        //   b > 10 could be FALSE → NULL AND FALSE = FALSE
        //   Can't determine → None
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Sometimes);

        // Both null: NULL AND NULL = NULL
        assert_eq!(expr.is_null(&null_cols(&[0, 1])), IsFalsy::Always);

        // Neither null: not null
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn and_is_null_with_false_child() -> Result<()> {
        let s = schema();
        // FALSE AND (a > 5): left is FALSE → AND = FALSE regardless → not null
        let expr = binary(
            lit(false),
            Operator::And,
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            &s,
        )?;
        // FALSE AND NULL = FALSE → not null
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Never);
        Ok(())
    }

    // ---- BinaryExpr: OR ----

    #[test]
    fn or_is_not_true_only_when_both_not_true() -> Result<()> {
        let s = schema();
        // (a > 5) OR (b > 10)
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::Or,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?;
        // only a is null → right side could be true → OR not guaranteed not-true
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        // both null → both sides not-true → OR not-true
        assert_eq!(expr.is_not_true(&null_cols(&[0, 1])), IsFalsy::Always);
        Ok(())
    }

    // ---- BinaryExpr: IS DISTINCT FROM / IS NOT DISTINCT FROM ----

    #[test]
    fn is_distinct_from_never_null() -> Result<()> {
        let s = schema();
        let expr = binary(col("a", &s)?, Operator::IsDistinctFrom, col("b", &s)?, &s)?;
        assert_eq!(expr.is_null(&null_cols(&[0, 1])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn is_distinct_from_both_null() -> Result<()> {
        let s = schema();
        // a IS DISTINCT FROM b: when both NULL → FALSE → not-true
        let expr = binary(col("a", &s)?, Operator::IsDistinctFrom, col("b", &s)?, &s)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0, 1])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn is_distinct_from_one_null() -> Result<()> {
        let s = schema();
        // a IS DISTINCT FROM b: when only a is NULL → TRUE → passes
        let expr = binary(col("a", &s)?, Operator::IsDistinctFrom, col("b", &s)?, &s)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn is_not_distinct_from_both_null() -> Result<()> {
        let s = schema();
        // a IS NOT DISTINCT FROM b: when both NULL → TRUE → passes
        let expr = binary(
            col("a", &s)?,
            Operator::IsNotDistinctFrom,
            col("b", &s)?,
            &s,
        )?;
        assert_eq!(expr.is_not_true(&null_cols(&[0, 1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn is_not_distinct_from_one_null() -> Result<()> {
        let s = schema();
        // a IS NOT DISTINCT FROM b: when only a is NULL → FALSE → not-true
        let expr = binary(
            col("a", &s)?,
            Operator::IsNotDistinctFrom,
            col("b", &s)?,
            &s,
        )?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn is_not_distinct_from_as_is_true() -> Result<()> {
        let s = schema();
        // Physical representation of IS TRUE(a): a IS NOT DISTINCT FROM true
        // When a is NULL → NULL IS NOT DISTINCT FROM true = FALSE → not-true (rejected)
        let expr = binary(col("a", &s)?, Operator::IsNotDistinctFrom, lit(true), &s)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        // When a is not null → could be TRUE or FALSE
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Sometimes);
        Ok(())
    }

    #[test]
    fn is_distinct_from_as_is_not_true() -> Result<()> {
        let s = schema();
        // Physical representation of IS NOT TRUE(a): a IS DISTINCT FROM true
        // When a is NULL → NULL IS DISTINCT FROM true = TRUE → passes
        let expr = binary(col("a", &s)?, Operator::IsDistinctFrom, lit(true), &s)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        // When a is not null → could be TRUE or FALSE
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Sometimes);
        Ok(())
    }

    // ---- NotExpr ----

    #[test]
    fn not_is_not_true_uses_is_null() -> Result<()> {
        let s = schema();
        // NOT(a > 5): when a is null → a > 5 is NULL → NOT(NULL) = NULL → not-true
        let expr = not(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn not_is_not_null_correctly_handled() -> Result<()> {
        let s = schema();
        // NOT(IS NOT NULL(a)): when a is NULL → IS NOT NULL(NULL) = FALSE →
        // NOT(FALSE) = TRUE → passes filter → should NOT be null-rejecting
        let expr = not(is_not_null(col("a", &s)?)?)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    // ---- IsNullExpr ----

    #[test]
    fn is_null_expr_never_rejects_nulls() -> Result<()> {
        let s = schema();
        let expr = is_null(col("a", &s)?)?;
        // IS NULL(NULL) = TRUE → passes filter
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    // ---- IsNotNullExpr ----

    #[test]
    fn is_not_null_expr_rejects_nulls() -> Result<()> {
        let s = schema();
        let expr = is_not_null(col("a", &s)?)?;
        // IS NOT NULL(NULL) = FALSE → rejected
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        // IS NOT NULL(non-null) = TRUE → passes
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    // ---- CastExpr ----

    #[test]
    fn cast_propagates_null() -> Result<()> {
        let s = schema();
        let expr = Arc::new(CastExpr::new(col("a", &s)?, DataType::Int64, None));
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        Ok(())
    }

    // ---- TryCastExpr ----

    #[test]
    fn try_cast_propagates_null() -> Result<()> {
        let s = schema();
        let expr = try_cast(col("a", &s)?, &s, DataType::Int64)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    // ---- NegativeExpr ----

    #[test]
    fn negative_propagates_null() -> Result<()> {
        let s = schema();
        let expr = Arc::new(NegativeExpr::new(col("a", &s)?));
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        Ok(())
    }

    // ---- LikeExpr ----

    #[test]
    fn like_is_null_when_expr_is_null() -> Result<()> {
        let s = schema();
        // c LIKE '%foo': when c (index 2) is null → NULL
        let expr = Arc::new(LikeExpr::new(false, false, col("c", &s)?, lit("%foo")));
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[2])), IsFalsy::Always);
        Ok(())
    }

    // ---- InListExpr ----

    #[test]
    fn in_list_is_null_when_probe_is_null() -> Result<()> {
        let s = schema();
        // a IN (1, 2, 3): when a (index 0) is null → NULL
        let expr = in_list(
            col("a", &s)?,
            vec![lit(1i32), lit(2i32), lit(3i32)],
            &false,
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        Ok(())
    }

    // ---- Arithmetic ----

    #[test]
    fn arithmetic_is_null_when_child_is_null() -> Result<()> {
        let s = schema();
        // a + b: null if either is null
        let expr = binary(col("a", &s)?, Operator::Plus, col("b", &s)?, &s)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Never);
        Ok(())
    }

    // ---- CaseExpr ----

    #[test]
    fn case_all_branches_not_null() -> Result<()> {
        let s = schema();
        // CASE WHEN a > 5 THEN true ELSE false END
        // Both branches are non-null → CASE is not null
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
                lit(true),
            )],
            Some(lit(false)),
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(case_expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn case_all_branches_null() -> Result<()> {
        let s = schema();
        // CASE WHEN a > 5 THEN NULL END (implicit ELSE NULL)
        // All branches are NULL → CASE is NULL
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
                lit(ScalarValue::Int32(None)),
            )],
            None,
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(case_expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn case_mixed_branches() -> Result<()> {
        let s = schema();
        // CASE WHEN a > 5 THEN NULL ELSE 42 END
        // THEN is NULL, ELSE is not → not all NULL → IsFalsy::Never
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
                lit(ScalarValue::Int32(None)),
            )],
            Some(lit(42i32)),
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn case_then_depends_on_null_column() -> Result<()> {
        let s = schema();
        // CASE WHEN true THEN a + 1 ELSE 0 END
        // THEN branch (a + 1) is NULL when a is NULL, but ELSE is not
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                lit(true),
                binary(col("a", &s)?, Operator::Plus, lit(1i32), &s)?,
            )],
            Some(lit(0i32)),
        )?;
        // THEN is null when a null, but ELSE is not → not all branches null
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn case_implicit_else_null() -> Result<()> {
        let s = schema();
        // CASE WHEN a > 5 THEN 42 END (no ELSE → implicit ELSE NULL)
        // THEN is not null, but implicit ELSE is NULL → not all null
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
                lit(42i32),
            )],
            None,
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(case_expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn case_implicit_else_null_all_then_null() -> Result<()> {
        let s = schema();
        // CASE WHEN a > 5 THEN NULL END (no ELSE → implicit ELSE NULL)
        // THEN is NULL, implicit ELSE is NULL → all null
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
                lit(ScalarValue::Int32(None)),
            )],
            None,
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(case_expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn case_when_condition_is_null() -> Result<()> {
        // CASE WHEN NULL THEN 42 ELSE 0 END
        // WHEN condition is NULL → this branch is never taken, but
        // we don't evaluate WHEN conditions — we only check THEN/ELSE values.
        // THEN (42) is not null → not all branches null
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(lit(ScalarValue::Boolean(None)), lit(42i32))],
            Some(lit(0i32)),
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(case_expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn case_when_null_then_null_else_null() -> Result<()> {
        // CASE WHEN NULL THEN NULL ELSE NULL END
        // All result values are NULL regardless of WHEN condition
        let case_expr = crate::expressions::CaseExpr::try_new(
            None,
            vec![(
                lit(ScalarValue::Boolean(None)),
                lit(ScalarValue::Int32(None)),
            )],
            Some(lit(ScalarValue::Int32(None))),
        )?;
        assert_eq!(case_expr.is_null(&null_cols(&[])), IsFalsy::Always);
        assert_eq!(case_expr.is_not_true(&null_cols(&[])), IsFalsy::Always);
        Ok(())
    }

    // ---- Negative semantic test cases ----

    #[test]
    fn or_with_is_null_does_not_reject() -> Result<()> {
        let s = schema();
        // (a > 5) OR (a IS NULL): when a is NULL →
        //   a > 5 = NULL, a IS NULL = TRUE → NULL OR TRUE = TRUE → passes!
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::Or,
            is_null(col("a", &s)?)?,
            &s,
        )?;
        // Should NOT reject: IS NULL(NULL)=TRUE makes OR true
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn not_is_null_does_reject() -> Result<()> {
        let s = schema();
        // NOT(IS NULL(a)): when a is NULL →
        //   IS NULL(NULL) = TRUE → NOT(TRUE) = FALSE → rejected
        // But IS NULL never returns NULL → is_null returns false →
        // NOT.is_not_true = is_null(inner) = false
        let expr = not(is_null(col("a", &s)?)?)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn and_with_unrelated_null_col() -> Result<()> {
        let s = schema();
        // (a > 5) AND (b > 10): when only c is null →
        //   a > 5 is not affected, b > 10 is not affected → AND could be true
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::And,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?;
        assert_eq!(expr.is_not_true(&null_cols(&[2])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn or_with_literal_true_does_not_reject() -> Result<()> {
        let s = schema();
        // (a > 5) OR TRUE: always TRUE regardless of a → never rejected
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::Or,
            lit(true),
            &s,
        )?;
        // lit(true) is not null and not not-true → is_not_true = false
        // OR requires BOTH to be not-true → false
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn comparison_with_null_literal() -> Result<()> {
        let s = schema();
        // a = NULL: NULL literal makes this always NULL regardless of a
        let expr = binary(
            col("a", &s)?,
            Operator::Eq,
            lit(ScalarValue::Int32(None)),
            &s,
        )?;
        // Even when a is NOT null, the NULL literal makes result NULL → not-true
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn empty_null_columns_no_rejection() -> Result<()> {
        let s = schema();
        // a > 5 with no null columns → could be true → not rejected
        let expr = binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?;
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Never);
        assert_eq!(expr.is_not_true(&null_cols(&[])), IsFalsy::Never);
        Ok(())
    }

    // ---- Edge cases ----

    #[test]
    fn double_not() -> Result<()> {
        let s = schema();
        // NOT(NOT(a > 5)): when a is NULL →
        //   a > 5 = NULL → NOT(NULL) = NULL → NOT(NULL) = NULL → not-true
        // Inner NOT: is_not_true calls is_null(a > 5) = true → true
        // Outer NOT: is_not_true calls is_null(NOT(a > 5))
        //   NOT.is_null calls inner.is_null = is_null(a > 5) = true → true
        let expr = not(not(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?)?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn deeply_nested_cast_not() -> Result<()> {
        let s = schema();
        // CAST(NOT(a > 5) AS Int64): when a is NULL →
        //   a > 5 = NULL → NOT(NULL) = NULL → CAST(NULL) = NULL → not-true
        let inner = not(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?;
        let expr = Arc::new(CastExpr::new(inner, DataType::Int64, None));
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn is_not_null_inside_or() -> Result<()> {
        let s = schema();
        // (a IS NOT NULL) OR (b > 5): when a AND b are null →
        //   IS NOT NULL(NULL) = FALSE, NULL > 5 = NULL
        //   FALSE OR NULL = NULL → not-true
        let expr = binary(
            is_not_null(col("a", &s)?)?,
            Operator::Or,
            binary(col("b", &s)?, Operator::Gt, lit(5i32), &s)?,
            &s,
        )?;
        // Both null → IS NOT NULL(NULL)=FALSE → is_not_true=true (via is_null crossover)
        //             b > 5 with b null → is_not_true=true
        //   OR: both not-true → true
        assert_eq!(expr.is_not_true(&null_cols(&[0, 1])), IsFalsy::Always);
        // Only a null → IS NOT NULL(NULL)=FALSE → not-true,
        //   but b > 5 with b not null → not not-true → OR = false
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn not_of_and() -> Result<()> {
        let s = schema();
        // NOT(a > 5 AND b > 10): when a is null →
        //   AND.is_null with one child null = None (can't determine)
        //   NOT.is_not_true = is_null(AND(...)) = None
        let expr = not(binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::And,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?)?;
        // One null child → AND.is_null = None → NOT.is_not_true = None
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Sometimes);
        // Both null → AND.is_null = IsFalsy::Always → NOT.is_not_true = IsFalsy::Always
        assert_eq!(expr.is_not_true(&null_cols(&[0, 1])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn like_with_null_pattern_column() -> Result<()> {
        let s = schema();
        // c LIKE c: both expr and pattern reference same column
        // when c (index 2) is null → NULL LIKE NULL = NULL
        let expr = Arc::new(LikeExpr::new(false, false, col("c", &s)?, col("c", &s)?));
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[2])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn all_columns_null() -> Result<()> {
        let s = schema();
        // (a + b) > 5: when both a and b are null → NULL + NULL = NULL → NULL > 5 = NULL
        let expr = binary(
            binary(col("a", &s)?, Operator::Plus, col("b", &s)?, &s)?,
            Operator::Gt,
            lit(5i32),
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[0, 1, 2])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[0, 1, 2])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn in_list_negated() -> Result<()> {
        let s = schema();
        // a NOT IN (1, 2, 3): when a is null → NULL NOT IN (...) = NULL
        let expr = in_list(
            col("a", &s)?,
            vec![lit(1i32), lit(2i32), lit(3i32)],
            &true,
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn nested_or_and_combination() -> Result<()> {
        let s = schema();
        // (a > 5 OR b > 10) AND (a > 3): when a is null →
        //   OR: a > 5 not-true, b > 10 not affected → OR not not-true (false)
        //   AND: OR=false, a > 3 not-true → either not-true → AND not-true (true)
        //   Wait: AND needs either child to be not-true.
        //   Left (OR): is_not_true = false (b side could be true)
        //   Right (a > 3): is_not_true = true (a is null)
        //   AND: either true → true
        let expr = binary(
            binary(
                binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
                Operator::Or,
                binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
                &s,
            )?,
            Operator::And,
            binary(col("a", &s)?, Operator::Gt, lit(3i32), &s)?,
            &s,
        )?;
        assert_eq!(expr.is_not_true(&null_cols(&[0])), IsFalsy::Always);
        Ok(())
    }

    // ---- Dedicated is_null tests ----

    #[test]
    fn not_is_null_propagates() -> Result<()> {
        let s = schema();
        // NOT(a > 5): is_null delegates to inner
        // a is null → a > 5 is NULL → NOT(NULL) = NULL
        let expr = not(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn not_of_is_null_expr_is_never_null() -> Result<()> {
        let s = schema();
        // NOT(IS NULL(a)): IS NULL never returns NULL → NOT never gets NULL input
        // NOT(TRUE) = FALSE, NOT(FALSE) = TRUE — neither is NULL
        let expr = not(is_null(col("a", &s)?)?)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn or_is_null_three_valued() -> Result<()> {
        let s = schema();
        // (a > 5) OR (b > 10)
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::Or,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?;
        // a null, b not null: NULL OR (b > 10)
        //   b > 10 could be TRUE → NULL OR TRUE = TRUE
        //   b > 10 could be FALSE → NULL OR FALSE = NULL
        //   Can't determine → None
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Sometimes);

        // Both null: NULL OR NULL = NULL
        assert_eq!(expr.is_null(&null_cols(&[0, 1])), IsFalsy::Always);

        // Neither null: not null
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn or_is_null_with_true_child() -> Result<()> {
        let s = schema();
        // (a > 5) OR TRUE: TRUE makes OR always TRUE
        // But we can't reliably detect "definitely TRUE" without is_true
        // → conservative None
        let expr = binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::Or,
            lit(true),
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Sometimes);
        Ok(())
    }

    #[test]
    fn or_is_null_with_false_and_null() -> Result<()> {
        let s = schema();
        // FALSE OR (b > 5): when b is null → FALSE OR NULL = NULL
        let expr = binary(
            lit(false),
            Operator::Or,
            binary(col("b", &s)?, Operator::Gt, lit(5i32), &s)?,
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Always);
        Ok(())
    }

    #[test]
    fn is_not_distinct_from_is_null() -> Result<()> {
        let s = schema();
        // IS NOT DISTINCT FROM never returns NULL
        let expr = binary(
            col("a", &s)?,
            Operator::IsNotDistinctFrom,
            col("b", &s)?,
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[0, 1])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn double_not_is_null() -> Result<()> {
        let s = schema();
        // NOT(NOT(a > 5)): is_null propagates through both NOTs
        // a null → a > 5 = NULL → NOT(NULL) = NULL → NOT(NULL) = NULL
        let expr = not(not(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn not_of_and_is_null() -> Result<()> {
        let s = schema();
        // NOT(a > 5 AND b > 10):
        // When both a and b null → AND is NULL → NOT(NULL) = NULL
        let expr = not(binary(
            binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?,
            Operator::And,
            binary(col("b", &s)?, Operator::Gt, lit(10i32), &s)?,
            &s,
        )?)?;
        assert_eq!(expr.is_null(&null_cols(&[0, 1])), IsFalsy::Always);
        // When only a null → AND.is_null = None → NOT.is_null = None
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Sometimes);
        Ok(())
    }

    #[test]
    fn is_not_null_of_comparison_is_null() -> Result<()> {
        let s = schema();
        // IS NOT NULL(a > 5): IS NOT NULL never returns NULL regardless of input
        let expr = is_not_null(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn is_null_of_comparison_is_null() -> Result<()> {
        let s = schema();
        // IS NULL(a > 5): IS NULL never returns NULL regardless of input
        let expr = is_null(binary(col("a", &s)?, Operator::Gt, lit(5i32), &s)?)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn in_list_negated_is_null() -> Result<()> {
        let s = schema();
        // a NOT IN (1, 2): when a is null → NULL
        let expr = in_list(col("a", &s)?, vec![lit(1i32), lit(2i32)], &true, &s)?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn like_both_sides_null() -> Result<()> {
        let s = schema();
        // a LIKE c: when a (index 0) is null → NULL
        //           when c (index 2) is null → NULL
        //           when neither → not null
        let expr = Arc::new(LikeExpr::new(false, false, col("a", &s)?, col("c", &s)?));
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[0, 2])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Never);
        Ok(())
    }

    #[test]
    fn nested_arithmetic_is_null() -> Result<()> {
        let s = schema();
        // (a + b) * a: null if a or b is null
        let expr = binary(
            binary(col("a", &s)?, Operator::Plus, col("b", &s)?, &s)?,
            Operator::Multiply,
            col("a", &s)?,
            &s,
        )?;
        assert_eq!(expr.is_null(&null_cols(&[0])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[1])), IsFalsy::Always);
        assert_eq!(expr.is_null(&null_cols(&[2])), IsFalsy::Never);
        assert_eq!(expr.is_null(&null_cols(&[])), IsFalsy::Never);
        Ok(())
    }
}
