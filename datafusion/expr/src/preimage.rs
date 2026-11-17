// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr_common::interval_arithmetic::Interval;

use crate::Expr;

/// Return from [`crate::ScalarUDFImpl::preimage`]
pub enum PreimageResult {
    /// No preimage exists for the specified value
    None,
    /// The expression always evaluates to the specified constant
    /// given that `expr` is within the interval
    Range { expr: Expr, interval: Box<Interval> },
}
