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
use datafusion_expr::{
    Expr, Operator, and, binary_expr, lit, or, simplify::SimplifyContext,
};
use datafusion_expr_common::interval_arithmetic::Interval;

/// Rewrites a binary expression using its "preimage"
///
/// Specifically it rewrites expressions of the form `<expr> OP x` (e.g. `<expr> =
/// x`) where `<expr>` is known to have a pre-image (aka the entire single
/// range for which it is valid)
///
/// This rewrite is described in the [ClickHouse Paper] and is particularly
/// useful for simplifying expressions `date_part` or equivalent functions. The
/// idea is that if you have an expression like `date_part(YEAR, k) = 2024` and you
/// can find a [preimage] for `date_part(YEAR, k)`, which is the range of dates
/// covering the entire year of 2024. Thus, you can rewrite the expression to
/// `k >= '2024-01-01' AND k < '2025-01-01'`, which uses an inclusive lower bound
/// and exclusive upper bound and is often more optimizable.
///
/// [ClickHouse Paper]:  https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf
/// [preimage]: https://en.wikipedia.org/wiki/Image_(mathematics)#Inverse_image
///
pub(super) fn rewrite_with_preimage(
    _info: &SimplifyContext,
    preimage_interval: Interval,
    op: Operator,
    expr: Box<Expr>,
) -> Result<Transformed<Expr>> {
    let (lower, upper) = preimage_interval.into_bounds();
    let (lower, upper) = (lit(lower), lit(upper));

    let rewritten_expr = match op {
        // <expr> < x   ==>  <expr> < lower
        // <expr> >= x  ==>  <expr> >= lower
        Operator::Lt | Operator::GtEq => binary_expr(*expr, op, lower),
        // <expr> > x ==> <expr> >= upper
        Operator::Gt => binary_expr(*expr, Operator::GtEq, upper),
        // <expr> <= x ==> <expr> < upper
        Operator::LtEq => binary_expr(*expr, Operator::Lt, upper),
        // <expr> = x ==> (<expr> >= lower) and (<expr> < upper)
        //
        // <expr> is not distinct from x ==> (<expr> is NULL and x is NULL) or ((<expr> >= lower) and (<expr> < upper))
        // but since x is always not NULL => (<expr> >= lower) and (<expr> < upper)
        Operator::Eq | Operator::IsNotDistinctFrom => and(
            binary_expr(*expr.clone(), Operator::GtEq, lower),
            binary_expr(*expr, Operator::Lt, upper),
        ),
        // <expr> != x ==> (<expr> < lower) or (<expr> >= upper)
        Operator::NotEq => or(
            binary_expr(*expr.clone(), Operator::Lt, lower),
            binary_expr(*expr, Operator::GtEq, upper),
        ),
        // <expr> is distinct from x ==> (<expr> < lower) or (<expr> >= upper) or (<expr> is NULL and x is not NULL) or (<expr> is not NULL and x is NULL)
        // but given that x is always not NULL => (<expr> < lower) or (<expr> >= upper) or (<expr> is NULL)
        Operator::IsDistinctFrom => binary_expr(*expr.clone(), Operator::Lt, lower)
            .or(binary_expr(*expr.clone(), Operator::GtEq, upper))
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
        Signature, Volatility, and, binary_expr, col, expr::ScalarFunction, lit,
        simplify::SimplifyContext,
    };

    use super::Interval;
    use crate::simplify_expressions::ExprSimplifier;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct PreimageUdf {
        signature: Signature,
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
        ) -> Result<Option<Interval>> {
            if args.len() != 1 {
                return Ok(None);
            }
            match lit_expr {
                Expr::Literal(ScalarValue::Int32(Some(500)), _) => {
                    Ok(Some(Interval::try_new(
                        ScalarValue::Int32(Some(100)),
                        ScalarValue::Int32(Some(200)),
                    )?))
                }
                _ => Ok(None),
            }
        }

        fn column_expr(&self, args: &[Expr]) -> Option<Expr> {
            args.get(0).cloned()
        }
    }

    fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
        let simplifier = ExprSimplifier::new(
            SimplifyContext::default().with_schema(Arc::clone(schema)),
        );

        simplifier.simplify(expr).unwrap()
    }

    fn preimage_udf_expr() -> Expr {
        let udf = ScalarUDF::new_from_impl(PreimageUdf {
            signature: Signature::exact(vec![DataType::Int32], Volatility::Immutable),
        });

        Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(udf), vec![col("x")]))
    }

    fn test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::from_unqualified_fields(
                vec![Field::new("x", DataType::Int32, false)].into(),
                Default::default(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_preimage_eq_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::Eq, lit(500));
        let expected = and(
            binary_expr(col("x"), Operator::GtEq, lit(100)),
            binary_expr(col("x"), Operator::Lt, lit(200)),
        );

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_noteq_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::NotEq, lit(500));
        let expected = binary_expr(col("x"), Operator::Lt, lit(100)).or(binary_expr(
            col("x"),
            Operator::GtEq,
            lit(200),
        ));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_eq_rewrite_swapped() {
        let schema = test_schema();
        let expr = binary_expr(lit(500), Operator::Eq, preimage_udf_expr());
        let expected = and(
            binary_expr(col("x"), Operator::GtEq, lit(100)),
            binary_expr(col("x"), Operator::Lt, lit(200)),
        );

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_lt_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::Lt, lit(500));
        let expected = binary_expr(col("x"), Operator::Lt, lit(100));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_lteq_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::LtEq, lit(500));
        let expected = binary_expr(col("x"), Operator::Lt, lit(200));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_gt_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::Gt, lit(500));
        let expected = binary_expr(col("x"), Operator::GtEq, lit(200));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_gteq_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::GtEq, lit(500));
        let expected = binary_expr(col("x"), Operator::GtEq, lit(100));

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_is_not_distinct_from_rewrite() {
        let schema = test_schema();
        let expr =
            binary_expr(preimage_udf_expr(), Operator::IsNotDistinctFrom, lit(500));
        let expected = and(
            binary_expr(col("x"), Operator::GtEq, lit(100)),
            binary_expr(col("x"), Operator::Lt, lit(200)),
        );

        assert_eq!(optimize_test(expr, &schema), expected);
    }

    #[test]
    fn test_preimage_is_distinct_from_rewrite() {
        let schema = test_schema();
        let expr = binary_expr(preimage_udf_expr(), Operator::IsDistinctFrom, lit(500));
        let expected = binary_expr(col("x"), Operator::Lt, lit(100))
            .or(binary_expr(col("x"), Operator::GtEq, lit(200)))
            .or(col("x").is_null());

        assert_eq!(optimize_test(expr, &schema), expected);
    }
}
