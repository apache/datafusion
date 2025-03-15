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

use std::fmt;

use arrow::datatypes::FieldRef;
use datafusion_common::{Column, TableReference};

use crate::{expr::WildcardOptions, Expr};

/// Represents a SELECT expression in a SQL query.
///
/// `SelectExpr` supports three types of expressions commonly found in the SELECT clause:
///
/// * Wildcard (`*`) - Selects all columns
/// * Qualified wildcard (`table.*`) - Selects all columns from a specific table
/// * Regular expression - Any other expression like columns, functions, literals etc.
///
/// This enum is typically used when you need to handle wildcards. After expanding `*` in the query,
/// you can use `Expr` for all other expressions.
///
/// # Examples
///
/// ```
/// use datafusion_expr::col;
/// use datafusion_expr::expr::WildcardOptions;
/// use datafusion_expr::select_expr::SelectExpr;
///
/// // SELECT *
/// let wildcard = SelectExpr::Wildcard(WildcardOptions::default());
///
/// // SELECT mytable.*
/// let qualified = SelectExpr::QualifiedWildcard(
///     "mytable".into(),
///     WildcardOptions::default()
/// );
///
/// // SELECT col1
/// let expr = SelectExpr::Expression(col("col1").into());
/// ```
#[derive(Clone, Debug)]
pub enum SelectExpr {
    /// Represents a wildcard (`*`) that selects all columns from all tables.
    /// The `WildcardOptions` control additional behavior like exclusions.
    Wildcard(WildcardOptions),

    /// Represents a qualified wildcard (`table.*`) that selects all columns from a specific table.
    /// The `TableReference` specifies the table and `WildcardOptions` control additional behavior.
    QualifiedWildcard(TableReference, WildcardOptions),

    /// Represents any other valid SELECT expression like column references,
    /// function calls, literals, etc.
    Expression(Expr),
}

impl fmt::Display for SelectExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SelectExpr::Wildcard(opt) => write!(f, "*{opt}"),
            SelectExpr::QualifiedWildcard(table, opt) => write!(f, "{table}.*{opt}"),
            SelectExpr::Expression(expr) => write!(f, "{expr}"),
        }
    }
}

impl From<Expr> for SelectExpr {
    fn from(expr: Expr) -> Self {
        SelectExpr::Expression(expr)
    }
}

/// Create an [`SelectExpr::Expression`] from a [`Column`]
impl From<Column> for SelectExpr {
    fn from(value: Column) -> Self {
        Expr::Column(value).into()
    }
}

/// Create an [`SelectExpr::Expression`] from an optional qualifier and a [`FieldRef`]. This is
/// useful for creating [`SelectExpr::Expression`] from a `DFSchema`.
///
/// See example on [`Expr`]
impl<'a> From<(Option<&'a TableReference>, &'a FieldRef)> for SelectExpr {
    fn from(value: (Option<&'a TableReference>, &'a FieldRef)) -> Self {
        Expr::from(Column::from(value)).into()
    }
}
