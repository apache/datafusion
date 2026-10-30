// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! DataFusion "prelude" to simplify importing common types.
//!
//! Like the standard library's prelude, this module simplifies importing of
//! common items. Unlike the standard prelude, the contents of this module must
//! be imported manually:
//!
//! ```
//! use datafusion::prelude::*;
//! ```

pub use crate::dataframe;
pub use crate::dataframe::DataFrame;
pub use crate::execution::context::{SQLOptions, SessionConfig, SessionContext};
pub use crate::execution::options::{
    AvroReadOptions, CsvReadOptions, JsonReadOptions, ParquetReadOptions,
};

pub use datafusion_common::Column;
pub use datafusion_expr::{
    Expr,
    expr_fn::*,
    lit, lit_timestamp_nano,
    logical_plan::{JoinType, Partitioning},
};
pub use datafusion_functions::expr_fn::*;
#[cfg(feature = "nested_expressions")]
pub use datafusion_functions_nested::expr_fn::*;

pub use std::ops::Not;
pub use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
pub use std::ops::{BitAnd, BitOr, BitXor};
pub use std::ops::{Shl, Shr};
