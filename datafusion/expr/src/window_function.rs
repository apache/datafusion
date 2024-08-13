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

use datafusion_common::ScalarValue;

use crate::{expr::WindowFunction, BuiltInWindowFunction, Expr, Literal};

/// Create an expression to represent the `rank` window function
pub fn rank() -> Expr {
    Expr::WindowFunction(WindowFunction::new(BuiltInWindowFunction::Rank, vec![]))
}

/// Create an expression to represent the `dense_rank` window function
pub fn dense_rank() -> Expr {
    Expr::WindowFunction(WindowFunction::new(
        BuiltInWindowFunction::DenseRank,
        vec![],
    ))
}

/// Create an expression to represent the `percent_rank` window function
pub fn percent_rank() -> Expr {
    Expr::WindowFunction(WindowFunction::new(
        BuiltInWindowFunction::PercentRank,
        vec![],
    ))
}

/// Create an expression to represent the `cume_dist` window function
pub fn cume_dist() -> Expr {
    Expr::WindowFunction(WindowFunction::new(BuiltInWindowFunction::CumeDist, vec![]))
}

/// Create an expression to represent the `ntile` window function
pub fn ntile(arg: Expr) -> Expr {
    Expr::WindowFunction(WindowFunction::new(BuiltInWindowFunction::Ntile, vec![arg]))
}

/// Create an expression to represent the `lag` window function
pub fn lag(
    arg: Expr,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> Expr {
    let shift_offset_lit = shift_offset
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_value.unwrap_or(ScalarValue::Null).lit();
    Expr::WindowFunction(WindowFunction::new(
        BuiltInWindowFunction::Lag,
        vec![arg, shift_offset_lit, default_lit],
    ))
}

/// Create an expression to represent the `lead` window function
pub fn lead(
    arg: Expr,
    shift_offset: Option<i64>,
    default_value: Option<ScalarValue>,
) -> Expr {
    let shift_offset_lit = shift_offset
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_value.unwrap_or(ScalarValue::Null).lit();
    Expr::WindowFunction(WindowFunction::new(
        BuiltInWindowFunction::Lead,
        vec![arg, shift_offset_lit, default_lit],
    ))
}

/// Create an expression to represent the `nth_value` window function
pub fn nth_value(arg: Expr, n: i64) -> Expr {
    Expr::WindowFunction(WindowFunction::new(
        BuiltInWindowFunction::NthValue,
        vec![arg, n.lit()],
    ))
}
