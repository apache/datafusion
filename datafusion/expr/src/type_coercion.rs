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

//! Type coercion rules for DataFusion
//!
//! Coercion is performed automatically by DataFusion when the types
//! of arguments passed to a function or needed by operators do not
//! exactly match the types required by that function / operator. In
//! this case, DataFusion will attempt to *coerce* the arguments to
//! types accepted by the function by inserting CAST operations.
//!
//! CAST operations added by coercion are lossless and never discard
//! information.
//!
//! For example coercion from i32 -> i64 might be
//! performed because all valid i32 values can be represented using an
//! i64. However, i64 -> i32 is never performed as there are i64
//! values which can not be represented by i32 values.

pub mod aggregates {
    pub use datafusion_expr_common::type_coercion::aggregates::*;
}
pub mod functions;
pub mod other;

use datafusion_common::plan_datafusion_err;
use datafusion_common::plan_err;
use datafusion_common::DFSchema;
use datafusion_common::Result;
pub use datafusion_expr_common::type_coercion::binary;

use arrow::datatypes::DataType;
use datafusion_expr_common::type_coercion::binary::comparison_coercion;
use datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer;

use crate::BinaryExpr;
use crate::Expr;
use crate::ExprSchemable;
use crate::LogicalPlan;

use std::fmt::Debug;

/// Determine whether the given data type `dt` represents signed numeric values.
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
    )
}

/// Determine whether the given data type `dt` is `Null`.
pub fn is_null(dt: &DataType) -> bool {
    *dt == DataType::Null
}

/// Determine whether the given data type `dt` is a `Timestamp`.
pub fn is_timestamp(dt: &DataType) -> bool {
    matches!(dt, DataType::Timestamp(_, _))
}

/// Determine whether the given data type 'dt' is a `Interval`.
pub fn is_interval(dt: &DataType) -> bool {
    matches!(dt, DataType::Interval(_))
}

/// Determine whether the given data type `dt` is a `Date` or `Timestamp`.
pub fn is_datetime(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// Determine whether the given data type `dt` is a `Utf8` or `LargeUtf8`.
pub fn is_utf8_or_large_utf8(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
}

/// Determine whether the given data type `dt` is a `Decimal`.
pub fn is_decimal(dt: &DataType) -> bool {
    matches!(dt, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
}

#[derive(Debug)]
pub struct DefaultTypeCoercion;
impl TypeCoercion for DefaultTypeCoercion {}

// Send and Sync because of trait Session
pub trait TypeCoercion: Debug + Send + Sync {
    fn coerce_binary_expr(
        &self,
        expr: BinaryExpr,
        schema: &DFSchema,
    ) -> Result<TypeCoerceResult<BinaryExpr>> {
        coerce_binary_expr(expr, schema)
            .map(|e| TypeCoerceResult::CoercedExpr(Expr::BinaryExpr(e)))
    }
}

/// Result of planning a raw expr with [`ExprPlanner`]
pub enum TypeCoerceResult<T> {
    CoercedExpr(Expr),
    CoercedPlan(LogicalPlan),
    /// The raw expression could not be planned, and is returned unmodified
    Original(T),
}

/// Public functions for DataFrame API

/// Coerce the given binary expression to a valid expression
pub fn coerce_binary_expr(expr: BinaryExpr, schema: &DFSchema) -> Result<BinaryExpr> {
    let BinaryExpr { left, op, right } = expr;

    let left_type = left.get_type(schema)?;
    let right_type = right.get_type(schema)?;

    let (left_type, right_type) =
        BinaryTypeCoercer::new(&left.get_type(schema)?, &op, &right.get_type(schema)?)
            .get_input_types()?;

    Ok(BinaryExpr::new(
        Box::new(left.cast_to(&left_type, schema)?),
        op,
        Box::new(right.cast_to(&right_type, schema)?),
    ))
}
