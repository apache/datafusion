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
use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{expr_rewriter::ExprRewritable, Expr, ExprSchemable};
use datafusion_physical_expr::execution_props::ExecutionProps;

#[allow(rustdoc::private_intra_doc_links)]
/// The information necessary to apply algebraic simplification to an
/// [Expr]. See [SimplifyContext] for one concrete implementation.
///
/// This trait exists so that other systems can plug schema
/// information in without having to create `DFSchema` objects. If you
/// have a [`DFSchemaRef`] you can use [`SimplifyContext`]
pub trait SimplifyInfo {
    /// returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool>;

    /// returns true of this expr is nullable (could possibly be NULL)
    fn nullable(&self, expr: &Expr) -> Result<bool>;

    /// Returns details needed for partial expression evaluation
    fn execution_props(&self) -> &ExecutionProps;
}

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
    /// use datafusion_optimizer::expr_simplifier::{ExprSimplifier, SimplifyInfo};
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
        let mut rewriter = Simplifier::new(&self.info);
        let mut const_evaluator = ConstEvaluator::try_new(self.info.execution_props())?;

        // TODO iterate until no changes are made during rewrite
        // (evaluating constants can enable new simplifications and
        // simplifications can enable new constant evaluation)
        // https://github.com/apache/arrow-datafusion/issues/1160
        expr.rewrite(&mut const_evaluator)?.rewrite(&mut rewriter)
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

/// Provides simplification information based on DFSchema and
/// [`ExecutionProps`]. This is the default implementation used by DataFusion
///
/// For example:
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use datafusion_expr::{col, lit};
/// use datafusion_common::{DataFusionError, ToDFSchema};
/// use datafusion_physical_expr::execution_props::ExecutionProps;
/// use datafusion_optimizer::expr_simplifier::{SimplifyContext, ExprSimplifier};
///
/// // Create the schema
/// let schema = Schema::new(vec![
///     Field::new("i", DataType::Int64, false),
///   ])
///   .to_dfschema_ref().unwrap();
///
/// // Create the simplifier
/// let props = ExecutionProps::new();
/// let context = SimplifyContext::new(&props)
///    .with_schema(schema);
/// let simplifier = ExprSimplifier::new(context);
///
/// // Use the simplifier
///
/// // b < 2 or (1 > 3)
/// let expr = col("b").lt(lit(2)).or(lit(1).gt(lit(3)));
///
/// // b < 2
/// let simplified = simplifier.simplify(expr).unwrap();
/// assert_eq!(simplified, col("b").lt(lit(2)));
/// ```
pub struct SimplifyContext<'a> {
    schemas: Vec<DFSchemaRef>,
    props: &'a ExecutionProps,
}

impl<'a> SimplifyContext<'a> {
    /// Create a new SimplifyContext
    pub fn new(props: &'a ExecutionProps) -> Self {
        Self {
            schemas: vec![],
            props,
        }
    }

    /// Register a [`DFSchemaRef`] with this context
    pub fn with_schema(mut self, schema: DFSchemaRef) -> Self {
        self.schemas.push(schema);
        self
    }
}

impl<'a> SimplifyInfo for SimplifyContext<'a> {
    /// returns true if this Expr has boolean type
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        for schema in &self.schemas {
            if let Ok(DataType::Boolean) = expr.get_type(schema) {
                return Ok(true);
            }
        }

        Ok(false)
    }
    /// Returns true if expr is nullable
    fn nullable(&self, expr: &Expr) -> Result<bool> {
        self.schemas
            .iter()
            .find_map(|schema| {
                // expr may be from another input, so ignore errors
                // by converting to None to keep trying
                expr.nullable(schema.as_ref()).ok()
            })
            .ok_or_else(|| {
                // This means we weren't able to compute `Expr::nullable` with
                // *any* input schemas, signalling a problem
                DataFusionError::Internal(format!(
                    "Could not find columns in '{}' during simplify",
                    expr
                ))
            })
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.props
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_common::ToDFSchema;
    use datafusion_expr::{col, lit};

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
}
