// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

pub use datafusion_expr_common::type_coercion::binary;

use arrow::datatypes::DataType;
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
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
    )
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
