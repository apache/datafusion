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

use datafusion_expr::Expr;

use crate::extract::{array_element, array_slice};

pub trait IndexAccessor {
    fn index(self, key: Expr) -> Expr;
}

impl IndexAccessor for Expr {
    fn index(self, key: Expr) -> Expr {
        array_element(self, key)
    }
}

pub trait SliceAccessor {
    fn range(self, start: Expr, stop: Expr) -> Expr;
}

impl SliceAccessor for Expr {
    fn range(self, start: Expr, stop: Expr) -> Expr {
        array_slice(self, start, stop, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_expr::{col, lit};

    #[test]
    fn test_index() {
        let expr1 = col("a").index(lit(1));
        let expr2 = array_element(col("a"), lit(1));
        assert_eq!(expr1, expr2);
    }

    #[test]
    fn test_range() {
        let expr1 = col("a").range(lit(1), lit(2));
        let expr2 = array_slice(col("a"), lit(1), lit(2), None);
        assert_eq!(expr1, expr2);
    }
}
