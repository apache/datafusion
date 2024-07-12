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

mod abs;
pub mod cast;
mod error;
mod if_expr;

pub use abs::Abs;
pub use error::{SparkError, SparkResult};
pub use if_expr::IfExpr;

/// Spark supports three evaluation modes when evaluating expressions, which affect
/// the behavior when processing input values that are invalid or would result in an
/// error, such as divide by zero errors, and also affects behavior when converting
/// between types.
#[derive(Debug, Hash, PartialEq, Clone, Copy)]
pub enum EvalMode {
    /// Legacy is the default behavior in Spark prior to Spark 4.0. This mode silently ignores
    /// or replaces errors during SQL operations. Operations resulting in errors (like
    /// division by zero) will produce NULL values instead of failing. Legacy mode also
    /// enables implicit type conversions.
    Legacy,
    /// Adheres to the ANSI SQL standard for error handling by throwing exceptions for
    /// operations that result in errors. Does not perform implicit type conversions.
    Ansi,
    /// Same as Ansi mode, except that it converts errors to NULL values without
    /// failing the entire query.
    Try,
}
