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

use datafusion_expr::Expr;

use super::expr_fn::get_field;

pub trait FieldAccessor {
    fn field(self, field: Expr) -> Expr;
}

impl FieldAccessor for Expr {
    fn field(self, field: Expr) -> Expr {
        get_field(self, field)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_expr::col;

    #[test]
    fn test_field() {
        let expr1 = col("a").field(col("b"));
        let expr2 = get_field(col("a"), col("b"));
        assert_eq!(expr1, expr2);
    }
}
