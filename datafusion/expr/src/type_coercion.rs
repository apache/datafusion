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
//! exacty match the types required by that function / operator. In
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

use arrow::datatypes::DataType;

/// Determine if a DataType is signed numeric or not
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
    )
}

// Determine if a DataType is Null or not
pub fn is_null(dt: &DataType) -> bool {
    *dt == DataType::Null
}

/// Determine if a DataType is numeric or not
pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || matches!(
            dt,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        )
}

/// Determine if a DataType is Timestamp or not
pub fn is_timestamp(dt: &DataType) -> bool {
    matches!(dt, DataType::Timestamp(_, _))
}

/// Determine if a DataType is Date or not
pub fn is_date(dt: &DataType) -> bool {
    matches!(dt, DataType::Date32 | DataType::Date64)
}

pub mod aggregates;
pub mod binary;
pub mod functions;
pub mod other;
