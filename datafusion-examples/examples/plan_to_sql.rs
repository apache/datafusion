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

use datafusion::error::Result;

use datafusion::prelude::*;

/// This example demonstrates the programmatic construction of
/// SQL using the DataFusion Expr [`Expr`] and LogicalPlan [`LogicalPlan`] API.
///
///
/// The code in this example shows how to:
/// 1. Create SQL from a variety of Expr and LogicalPlan: [`main`]`
/// 2. Create a simple expression [`Exprs`] with fluent API
/// and convert to sql against data: [`simple_expr_to_sql_demo`]

#[tokio::main]
async fn main() -> Result<()> {
    // See how to evaluate expressions
    simple_expr_to_sql_demo();

    Ok(())
}

/// DataFusion can convert expressions to SQL
fn simple_expr_to_sql_demo() {
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));
    let sql = expr.to_string();
    assert_eq!(sql, "a < 5 OR a = 8");
}
