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

#[derive(Clone, Debug)]
pub enum SelectExpr {
    Wildcard(WildcardOptions),
    QualifiedWildcard(TableReference, WildcardOptions),
    Expression(Expr),
}

impl fmt::Display for SelectExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SelectExpr::Wildcard(_) => write!(f, "*"),
            SelectExpr::QualifiedWildcard(table, _) => write!(f, "{}.*", table),
            SelectExpr::Expression(expr) => write!(f, "{}", expr),
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
/// useful for creating [`SelectExpr::Expression`] from a [`DFSchema`].
///
/// See example on [`Expr`]
impl<'a> From<(Option<&'a TableReference>, &'a FieldRef)> for SelectExpr {
    fn from(value: (Option<&'a TableReference>, &'a FieldRef)) -> Self {
        Expr::from(Column::from(value)).into()
    }
}
