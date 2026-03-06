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

use datafusion_expr_common::interval_arithmetic::Interval;

use crate::Expr;

/// Return from [`crate::ScalarUDFImpl::preimage`]
pub enum PreimageResult {
    /// No preimage exists for the specified value
    None,
    /// For some UDF, a `preimage` implementation determines that:
    ///     the result `udf_result` in `udf_result = UDF(expr)`
    ///     is equivalent to `udf_result = UDF(i)` for any `i` in `interval`.
    ///
    /// Then, `is_boundary` indicates a boundary condition where:
    ///     the original expression `UDF(expr)` is compared to a value `lit` where:
    ///         `UDF(lit) == lit`
    /// This condition is important for two scenarios:
    /// 1. `<` and `>=` operators:
    ///    if `Some(false)`, expression rewrite should use `interval.upper`
    /// 2. `=` and `!=` operators:
    ///    if `Some(false)`, expression rewrite can use constant (false and true, respectively)
    ///
    /// if is_boundary is `None`, then the boundary condition never applies.
    Range {
        expr: Expr,
        interval: Box<Interval>,
        is_boundary: Option<bool>,
    },
}
