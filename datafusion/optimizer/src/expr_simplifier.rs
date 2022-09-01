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

//! Expression simplifier

use crate::simplify_expressions::{ConstEvaluator, Simplifier};
use datafusion_common::Result;
use datafusion_expr::{expr_rewriter::ExprRewritable, Expr};
use datafusion_physical_expr::execution_props::ExecutionProps;

#[allow(rustdoc::private_intra_doc_links)]
/// The information necessary to apply algebraic simplification to an
/// [Expr]. See [SimplifyContext](crate::optimizer::simplify_expressions::SimplifyContext)
/// for one implementation
pub trait SimplifyInfo {
    /// returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool>;

    /// returns true of this expr is nullable (could possibly be NULL)
    fn nullable(&self, expr: &Expr) -> Result<bool>;

    /// Returns details needed for partial expression evaluation
    fn execution_props(&self) -> &ExecutionProps;
}

/// trait for types that can be simplified
pub trait ExprSimplifiable: Sized {
    /// simplify this trait object using the given SimplifyInfo
    fn simplify<S: SimplifyInfo>(self, info: &S) -> Result<Self>;
}

impl ExprSimplifiable for Expr {
    /// Simplifies this [`Expr`]`s as much as possible, evaluating
    /// constants and applying algebraic simplifications
    ///
    /// # Example:
    /// `b > 2 AND b > 2`
    /// can be written to
    /// `b > 2`
    ///
    /// ```
    /// use datafusion_expr::{col, lit, Expr};
    /// use datafusion_common::Result;
    /// use datafusion_physical_expr::execution_props::ExecutionProps;
    /// use datafusion_optimizer::expr_simplifier::{SimplifyInfo, ExprSimplifiable};
    ///
    /// /// Simple implementation that provides `Simplifier` the information it needs
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
    /// // b < 2
    /// let b_lt_2 = col("b").gt(lit(2));
    ///
    /// // (b < 2) OR (b < 2)
    /// let expr = b_lt_2.clone().or(b_lt_2.clone());
    ///
    /// // (b < 2) OR (b < 2) --> (b < 2)
    /// let expr = expr.simplify(&Info::default()).unwrap();
    /// assert_eq!(expr, b_lt_2);
    /// ```
    fn simplify<S: SimplifyInfo>(self, info: &S) -> Result<Self> {
        let mut rewriter = Simplifier::new(info);
        let mut const_evaluator = ConstEvaluator::try_new(info.execution_props())?;

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        self.rewrite(&mut const_evaluator)?.rewrite(&mut rewriter)
    }
}
