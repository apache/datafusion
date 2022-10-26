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

//! Structs and traits to provide the information needed for expression simplification.

use arrow::datatypes::DataType;
use datafusion_common::{DFSchemaRef, DataFusionError, Result};
use datafusion_expr::{Expr, ExprSchemable};
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

/// Provides simplification information based on DFSchema and
/// [`ExecutionProps`]. This is the default implementation used by DataFusion
///
/// For example:
/// ```
/// use arrow::datatypes::{Schema, Field, DataType};
/// use datafusion_expr::{col, lit};
/// use datafusion_common::{DataFusionError, ToDFSchema};
/// use datafusion_physical_expr::execution_props::ExecutionProps;
/// use datafusion_optimizer::simplify_expressions::{SimplifyContext, ExprSimplifier};
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
