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

//! [`Unparser`] for converting `Expr` to SQL text

pub mod ast;
mod expr;
mod plan;
mod rewrite;
mod utils;

use self::dialect::{DefaultDialect, Dialect};
use crate::unparser::udlp_unparser::UserDefinedLogicalNodeUnparser;
pub use expr::expr_to_sql;
pub use plan::plan_to_sql;
use std::sync::Arc;
pub mod dialect;
pub mod udlp_unparser;

/// Convert a DataFusion [`Expr`] to [`sqlparser::ast::Expr`]
///
/// See [`expr_to_sql`] for background. `Unparser` allows greater control of
/// the conversion, but with a more complicated API.
///
/// To get more human-readable output, see [`Self::with_pretty`]
///
/// # Example
/// ```
/// use datafusion_expr::{col, lit};
/// use datafusion_sql::unparser::Unparser;
/// let expr = col("a").gt(lit(4)); // form an expression `a > 4`
/// let unparser = Unparser::default();
/// let sql = unparser.expr_to_sql(&expr).unwrap();// convert to AST
/// // use the Display impl to convert to SQL text
/// assert_eq!(sql.to_string(), "(a > 4)");
/// // now convert to pretty sql
/// let unparser = unparser.with_pretty(true);
/// let sql = unparser.expr_to_sql(&expr).unwrap();
/// assert_eq!(sql.to_string(), "a > 4"); // note lack of parenthesis
/// ```
///
/// [`Expr`]: datafusion_expr::Expr
pub struct Unparser<'a> {
    dialect: &'a dyn Dialect,
    pretty: bool,
    udlp_unparsers: Vec<Arc<dyn UserDefinedLogicalNodeUnparser>>,
}

impl<'a> Unparser<'a> {
    pub fn new(dialect: &'a dyn Dialect) -> Self {
        Self {
            dialect,
            pretty: false,
            udlp_unparsers: vec![],
        }
    }

    /// Create pretty SQL output, better suited for human consumption
    ///
    /// See example on the struct level documentation
    ///
    /// # Pretty Output
    ///
    /// By default, `Unparser` generates SQL text that will parse back to the
    /// same parsed [`Expr`], which is useful for creating machine readable
    /// expressions to send to other systems. However, the resulting expressions are
    /// not always nice to read for humans.
    ///
    /// For example
    ///
    /// ```sql
    /// ((a + 4) > 5)
    /// ```
    ///
    /// This method removes parenthesis using to the precedence rules of
    /// DataFusion. If the output is reparsed, the resulting [`Expr`] produces
    /// same value as the original in DataFusion, but with a potentially
    /// different order of operations.
    ///
    /// Note that this setting may create invalid SQL for other SQL query
    /// engines with different precedence rules
    ///
    /// # Example
    /// ```
    /// use datafusion_expr::{col, lit};
    /// use datafusion_sql::unparser::Unparser;
    /// let expr = col("a").gt(lit(4)).and(col("b").lt(lit(5))); // form an expression `a > 4 AND b < 5`
    /// let unparser = Unparser::default().with_pretty(true);
    /// let sql = unparser.expr_to_sql(&expr).unwrap();
    /// assert_eq!(sql.to_string(), "a > 4 AND b < 5"); // note lack of parenthesis
    /// ```
    ///
    /// [`Expr`]: datafusion_expr::Expr
    pub fn with_pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }

    /// Add a custom unparser for user defined logical nodes
    ///
    /// DataFusion allows user to define custom logical nodes. This method allows to add custom child unparsers for these nodes.
    /// Implementation of [`UserDefinedLogicalNodeUnparser`] can be added to the root unparser to handle custom logical nodes.
    ///
    /// The child unparsers are called iteratively.
    /// There are two methods in [`Unparser`] will be called:
    /// - `extension_to_statement`: This method is called when the custom logical node is a custom statement.
    ///     If multiple child unparsers return a non-None value, the last unparsing result will be returned.
    /// - `extension_to_sql`: This method is called when the custom logical node is part of a statement.
    ///    If multiple child unparsers are registered for the same custom logical node, all of them will be called in order.
    pub fn with_udlp_unparsers(
        mut self,
        udlp_unparsers: Vec<Arc<dyn UserDefinedLogicalNodeUnparser>>,
    ) -> Self {
        self.udlp_unparsers = udlp_unparsers;
        self
    }
}

impl Default for Unparser<'_> {
    fn default() -> Self {
        Self {
            dialect: &DefaultDialect {},
            pretty: false,
            udlp_unparsers: vec![],
        }
    }
}
