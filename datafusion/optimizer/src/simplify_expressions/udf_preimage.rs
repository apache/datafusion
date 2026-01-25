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

use datafusion_common::{Result, internal_err, tree_node::Transformed};
use datafusion_expr::{Expr, Operator, and, lit, or};
use datafusion_expr_common::interval_arithmetic::Interval;

/// Rewrites a binary expression using its "preimage"
///
/// Specifically it rewrites expressions of the form `<expr> OP x` (e.g. `<expr> =
/// x`) where `<expr>` is known to have a pre-image (aka the entire single
/// range for which it is valid) and `x` is not `NULL`
///
/// For details see [`datafusion_expr::ScalarUDFImpl::preimage`]
///
pub(super) fn rewrite_with_preimage(
    preimage_interval: Interval,
    op: Operator,
    expr: Expr,
) -> Result<Transformed<Expr>> {
    let (lower, upper) = preimage_interval.into_bounds();
    let (lower, upper) = (lit(lower), lit(upper));

    let rewritten_expr = match op {
        // <expr> < x   ==>  <expr> < lower
        Operator::Lt => expr.lt(lower),
        // <expr> >= x  ==>  <expr> >= lower
        Operator::GtEq => expr.gt_eq(lower),
        // <expr> > x ==> <expr> >= upper
        Operator::Gt => expr.gt_eq(upper),
        // <expr> <= x ==> <expr> < upper
        Operator::LtEq => expr.lt(upper),
        // <expr> = x ==> (<expr> >= lower) and (<expr> < upper)
        Operator::Eq => and(expr.clone().gt_eq(lower), expr.lt(upper)),
        // <expr> != x ==> (<expr> < lower) or (<expr> >= upper)
        Operator::NotEq => or(expr.clone().lt(lower), expr.gt_eq(upper)),
        // <expr> is not distinct from x ==> (<expr> is NULL and x is NULL) or ((<expr> >= lower) and (<expr> < upper))
        // but since x is always not NULL => (<expr> is not NULL) and (<expr> >= lower) and (<expr> < upper)
        Operator::IsNotDistinctFrom => expr
            .clone()
            .is_not_null()
            .and(expr.clone().gt_eq(lower))
            .and(expr.lt(upper)),
        // <expr> is distinct from x ==> (<expr> < lower) or (<expr> >= upper) or (<expr> is NULL and x is not NULL) or (<expr> is not NULL and x is NULL)
        // but given that x is always not NULL => (<expr> < lower) or (<expr> >= upper) or (<expr> is NULL)
        Operator::IsDistinctFrom => expr
            .clone()
            .lt(lower)
            .or(expr.clone().gt_eq(upper))
            .or(expr.is_null()),
        _ => return internal_err!("Expect comparison operators"),
    };
    Ok(Transformed::yes(rewritten_expr))
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue};
    use datafusion_expr::{
        ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
        Signature, Volatility, and, binary_expr, col, lit, preimage::PreimageResult,
        simplify::SimplifyContext,
    };

    use super::Interval;
    use crate::simplify_expressions::ExprSimplifier;

    fn is_distinct_from(left: Expr, right: Expr) -> Expr {
        binary_expr(left, Operator::IsDistinctFrom, right)
    }

    fn is_not_distinct_from(left: Expr, right: Expr) -> Expr {
        binary_expr(left, Operator::IsNotDistinctFrom, right)
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct PreimageUdf {
        /// Defaults to an exact signature with one Int32 argument and Immutable volatility
        signature: Signature,
        /// If true, returns a preimage; otherwise, returns None
        enabled: bool,
    }

    impl PreimageUdf {
        fn new() -> Self {
            Self {
                signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
                enabled: true,
            }
        }

        /// Set the enabled flag
        fn with_enabled(mut self, enabled: bool) -> Self {
            self.enabled = enabled;
            self
        }

        /// Set the volatility
        fn with_volatility(mut self, volatility: Volatility) -> Self {
            self.signature.volatility = volatility;
            self
        }
    }

    impl ScalarUDFImpl for PreimageUdf {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "preimage_func"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(500))))
        }

        fn preimage(
            &self,
            args: &[Expr],
            lit_expr: &Expr,
            _info: &SimplifyContext,
        ) -> Result<PreimageResult> {
            if !self.enabled {
                return Ok(PreimageResult::None);
            }
            if args.len() != 1 {
                return Ok(PreimageResult::None);
            }

            let expr = args.first().cloned().expect("Should be column expression");
            match lit_expr {
                Expr::Literal(ScalarValue::Int32(Some(500)), _) => {
                    Ok(PreimageResult::Range {
                        expr,
                        interval: Box::new(Interval::try_new(
                            ScalarValue::Int32(Some(100)),
                            ScalarValue::Int32(Some(200)),
                        )?),
                    })
                }
                _ => Ok(PreimageResult::None),
            }
        }
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let simplify_context = SimplifyContext::default().with_schema(Arc::clone(schema));
        ExprSimplifier::new(simplify_context)
            .simplify(expr)
            .unwrap()
    }

    fn preimage_udf_expr() -> Expr {
        ScalarUDF::new_from_impl(PreimageUdf::new()).call(vec![col("x")])
    }

    fn non_immutable_udf_expr() -> Expr {
        ScalarUDF::new_from_impl(PreimageUdf::new().with_volatility(Volatility::Volatile))
            .call(vec![col("x")])
    }

    fn no_preimage_udf_expr() -> Expr {
        ScalarUDF::new_from_impl(PreimageUdf::new().with_enabled(false))
            .call(vec![col("x")])
    }

    fn test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![Field::new("x", DataType::Int32, true)].into(),
                Default::default(),
            )
            .unwrap(),
        )
    }

    fn test_schema_xy() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![
                    Field::new("x", DataType::Int32, false),
                    Field::new("y", DataType::Int32, false),
                ]
                .into(),
                Default::default(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_preimage_eq_rewrite() {
        // Equality rewrite when preimage and column expression are available.
        let schema = test_schema();
        let expr = preimage_udf_expr().eq(lit(500));
        let expected = and(col("x").gt_eq(lit(100)), col("x").lt(lit(200)));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_noteq_rewrite() {
        // Inequality rewrite expands to disjoint ranges.
        let schema = test_schema();
        let expr = preimage_udf_expr().not_eq(lit(500));
        let expected = col("x").lt(lit(100)).or(col("x").gt_eq(lit(200)));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_eq_rewrite_swapped() {
        // Equality rewrite works when the literal appears on the left.
        let schema = test_schema();
        let expr = lit(500).eq(preimage_udf_expr());
        let expected = and(col("x").gt_eq(lit(100)), col("x").lt(lit(200)));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_lt_rewrite() {
        // Less-than comparison rewrites to the lower bound.
        let schema = test_schema();
        let expr = preimage_udf_expr().lt(lit(500));
        let expected = col("x").lt(lit(100));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_lteq_rewrite() {
        // Less-than-or-equal comparison rewrites to the upper bound.
        let schema = test_schema();
        let expr = preimage_udf_expr().lt_eq(lit(500));
        let expected = col("x").lt(lit(200));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_gt_rewrite() {
        // Greater-than comparison rewrites to the upper bound (inclusive).
        let schema = test_schema();
        let expr = preimage_udf_expr().gt(lit(500));
        let expected = col("x").gt_eq(lit(200));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_gteq_rewrite() {
        // Greater-than-or-equal comparison rewrites to the lower bound.
        let schema = test_schema();
        let expr = preimage_udf_expr().gt_eq(lit(500));
        let expected = col("x").gt_eq(lit(100));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_is_not_distinct_from_rewrite() {
        // IS NOT DISTINCT FROM rewrites to equality plus expression not-null check
        // for non-null literal RHS.
        let schema = test_schema();
        let expr = is_not_distinct_from(preimage_udf_expr(), lit(500));
        let expected = col("x")
            .is_not_null()
            .and(col("x").gt_eq(lit(100)))
            .and(col("x").lt(lit(200)));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_is_distinct_from_rewrite() {
        // IS DISTINCT FROM adds an explicit NULL branch for the column.
        let schema = test_schema();
        let expr = is_distinct_from(preimage_udf_expr(), lit(500));
        let expected = col("x")
            .lt(lit(100))
            .or(col("x").gt_eq(lit(200)))
            .or(col("x").is_null());

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_non_literal_rhs_no_rewrite() {
        // Non-literal RHS should not be rewritten.
        let schema = test_schema_xy();
        let expr = preimage_udf_expr().eq(col("y"));
        let expected = expr.clone();

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_null_literal_no_rewrite_distinct_ops() {
        // NULL literal RHS should not be rewritten for DISTINCTness operators:
        // - `expr IS DISTINCT FROM NULL`  <=> `NOT (expr IS NULL)`
        // - `expr IS NOT DISTINCT FROM NULL` <=> `expr IS NULL`
        //
        // For normal comparisons (=, !=, <, <=, >, >=), `expr OP NULL` evaluates to NULL
        // under SQL tri-state logic, and DataFusion's simplifier constant-folds it.
        // https://docs.rs/datafusion/latest/datafusion/physical_optimizer/pruning/struct.PruningPredicate.html#boolean-tri-state-logic

        let schema = test_schema();

        let expr = is_distinct_from(preimage_udf_expr(), lit(ScalarValue::Int32(None)));
        assert_eq!(optimize_test(expr.clone(), &schema), expr);

        let expr =
            is_not_distinct_from(preimage_udf_expr(), lit(ScalarValue::Int32(None)));
        assert_eq!(optimize_test(expr.clone(), &schema), expr);
    }

    #[test]
    fn test_preimage_non_immutable_no_rewrite() {
        // Non-immutable UDFs should not participate in preimage rewrites.
        let schema = test_schema();
        let expr = non_immutable_udf_expr().eq(lit(500));
        let expected = expr.clone();

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_no_preimage_no_rewrite() {
        // If the UDF provides no preimage, the expression should remain unchanged.
        let schema = test_schema();
        let expr = no_preimage_udf_expr().eq(lit(500));
        let expected = expr.clone();

        assert_eq!(optimize_test(expr, &schema), expected);
    }
}
