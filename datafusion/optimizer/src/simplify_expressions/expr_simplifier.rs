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

//! Expression simplification API

use crate::{
    simplify_expressions::{ConstEvaluator, Simplifier},
    type_coercion::TypeCoercionRewriter,
};

use datafusion_common::{DFSchemaRef, Result};
use datafusion_expr::{expr_rewriter::ExprRewritable, Expr};

use super::SimplifyInfo;


/// This structure handles API for expression simplification
pub struct ExprSimplifier<S> {
    info: S,
}

impl<S: SimplifyInfo> ExprSimplifier<S> {
    /// Create a new `ExprSimplifier` with the given `info` such as an
    /// instance of [`SimplifyContext`]. See
    /// [`simplify`](Self::simplify) for an example.
    pub fn new(info: S) -> Self {
        Self { info }
    }

    /// Simplifies this [`Expr`]`s as much as possible, evaluating
    /// constants and applying algebraic simplifications.
    ///
    /// The types of the expression must match what operators expect,
    /// or else an error may occur trying to evaluate. See
    /// [`coerce`](Self::coerce) for a function to help.
    ///
    /// # Example:
    ///
    /// `b > 2 AND b > 2`
    ///
    /// can be written to
    ///
    /// `b > 2`
    ///
    /// ```
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_common::Result;
    /// use datafusion_physical_expr::execution_props::ExecutionProps;
    /// use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyInfo};
    ///
    /// /// Simple implementation that provides `Simplifier` the information it needs
    /// /// See SimplifyContext for a structure that does this.
    /// #[derive(Default)]
    /// struct Info {
    ///   execution_props: ExecutionProps,
    /// };
    ///
    /// impl SimplifyInfo for Info {
    ///   fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(false)
    ///   }
    ///   fn nullable(&self, expr: &Expr) -> Result<bool> {
    ///     Ok(true)
    ///   }
    ///   fn execution_props(&self) -> &ExecutionProps {
    ///     &self.execution_props
    ///   }
    /// }
    ///
    /// // Create the simplifier
    /// let simplifier = ExprSimplifier::new(Info::default());
    ///
    /// // b < 2
    /// let b_lt_2 = col("b").gt(lit(2));
    ///
    /// // (b < 2) OR (b < 2)
    /// let expr = b_lt_2.clone().or(b_lt_2.clone());
    ///
    /// // (b < 2) OR (b < 2) --> (b < 2)
    /// let expr = simplifier.simplify(expr).unwrap();
    /// assert_eq!(expr, b_lt_2);
    /// ```
    pub fn simplify(&self, expr: Expr) -> Result<Expr> {
        let mut simplifier = Simplifier::new(&self.info);
        let mut const_evaluator = ConstEvaluator::try_new(self.info.execution_props())?;

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        expr.rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)?
            // run both passes twice to try an minimize simplifications that we missed
            .rewrite(&mut const_evaluator)?
            .rewrite(&mut simplifier)
    }

    /// Apply type coercion to an [`Expr`] so that it can be
    /// evaluated as a [`PhysicalExpr`](datafusion_physical_expr::PhysicalExpr).
    ///
    /// See the [type coercion module](datafusion_expr::type_coercion)
    /// documentation for more details on type coercion
    ///
    // Would be nice if this API could use the SimplifyInfo
    // rather than creating an DFSchemaRef coerces rather than doing
    // it manually.
    // https://github.com/apache/arrow-datafusion/issues/3793
    pub fn coerce(&self, expr: Expr, schema: DFSchemaRef) -> Result<Expr> {
        let mut expr_rewrite = TypeCoercionRewriter { schema };

        expr.rewrite(&mut expr_rewrite)
    }
}

#[cfg(test)]
mod tests {
    use crate::simplify_expressions::SimplifyContext;

    use super::*;
    use arrow::datatypes::{Field, Schema, DataType};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{col, lit, when};
    use datafusion_physical_expr::execution_props::ExecutionProps;

    #[test]
    fn api_basic() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        let expr = lit(1) + lit(2);
        let expected = lit(3);
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    #[test]
    fn basic_coercion() {
        let schema = test_schema();
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(schema.clone()));

        // Note expr type is int32 (not int64)
        // (1i64 + 2i32) < i
        let expr = (lit(1i64) + lit(2i32)).lt(col("i"));
        // should fully simplify to 3 < i (though i has been coerced to i64)
        let expected = lit(3i64).lt(col("i"));

        // Would be nice if this API could use the SimplifyInfo
        // rather than creating an DFSchemaRef coerces rather than doing
        // it manually.
        // https://github.com/apache/arrow-datafusion/issues/3793
        let expr = simplifier.coerce(expr, schema).unwrap();

        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    fn test_schema() -> DFSchemaRef {
        Schema::new(vec![
            Field::new("i", DataType::Int64, false),
            Field::new("b", DataType::Boolean, true),
        ])
        .to_dfschema_ref()
        .unwrap()
    }

    #[test]
    fn simplify_and_constant_prop() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        // should be able to simplify to false
        // (i * (1 - 2)) > 0
        let expr = (col("i") * (lit(1) - lit(1))).gt(lit(0));
        let expected = lit(false);
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }

    #[test]
    fn simplify_and_constant_prop_with_case() {
        let props = ExecutionProps::new();
        let simplifier =
            ExprSimplifier::new(SimplifyContext::new(&props).with_schema(test_schema()));

        //   CASE
        //     WHEN i>5 AND false THEN i > 5
        //     WHEN i<5 AND true THEN i < 5
        //     ELSE false
        //   END
        //
        // Can be simplified to `i < 5`
        let expr = when(col("i").gt(lit(5)).and(lit(false)), col("i").gt(lit(5)))
            .when(col("i").lt(lit(5)).and(lit(true)), col("i").lt(lit(5)))
            .otherwise(lit(false))
            .unwrap();
        let expected = col("i").lt(lit(5));
        assert_eq!(expected, simplifier.simplify(expr).unwrap());
    }
}
