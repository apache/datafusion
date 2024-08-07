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

//! Extension methods for Expr.

use datafusion_expr::{Expr, Literal};

use super::expr_fn::get_field;

/// Return access to the named field. Example `expr["name"]`
///
/// ## Access field "my_field" from column "c1"
///
/// For example if column "c1" holds documents like this
///
/// ```json
/// {
///   "my_field": 123.34,
///   "other_field": "Boston",
/// }
/// ```
///
/// You can access column "my_field" with
///
/// ```
/// # use datafusion_expr::{col};
/// # use datafusion_functions::core::expr_ext::FieldAccessor;
/// let expr = col("c1")
///    .field("my_field");
/// assert_eq!(expr.schema_name().to_string(), "c1[my_field]");
/// ```
pub trait FieldAccessor {
    fn field(self, name: impl Literal) -> Expr;
}

impl FieldAccessor for Expr {
    fn field(self, name: impl Literal) -> Expr {
        get_field(self, name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_expr::col;

    #[test]
    fn test_field() {
        let expr1 = col("a").field("b");
        let expr2 = get_field(col("a"), "b");
        assert_eq!(expr1, expr2);
    }
}
