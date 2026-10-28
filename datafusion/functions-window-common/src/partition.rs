// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Arguments passed to created user-defined window function state
/// during physical execution.
#[derive(Debug, Default)]
pub struct PartitionEvaluatorArgs<'a> {
    /// The expressions passed as arguments to the user-defined window
    /// function.
    input_exprs: &'a [Arc<dyn PhysicalExpr>],
    /// The corresponding fields of expressions passed as arguments
    /// to the user-defined window function.
    input_fields: &'a [FieldRef],
    /// Set to `true` if the user-defined window function is reversed.
    is_reversed: bool,
    /// Set to `true` if `IGNORE NULLS` is specified.
    ignore_nulls: bool,
}

impl<'a> PartitionEvaluatorArgs<'a> {
    /// Create an instance of [`PartitionEvaluatorArgs`].
    ///
    /// # Arguments
    ///
    /// * `input_exprs` - The expressions passed as arguments
    ///   to the user-defined window function.
    /// * `input_fields` - The fields corresponding to the
    ///   arguments to the user-defined window function.
    /// * `is_reversed` - Set to `true` if and only if the user-defined
    ///   window function is reversible and is reversed.
    /// * `ignore_nulls` - Set to `true` when `IGNORE NULLS` is
    ///   specified.
    pub fn new(
        input_exprs: &'a [Arc<dyn PhysicalExpr>],
        input_fields: &'a [FieldRef],
        is_reversed: bool,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            input_exprs,
            input_fields,
            is_reversed,
            ignore_nulls,
        }
    }

    /// Returns the expressions passed as arguments to the user-defined
    /// window function.
    pub fn input_exprs(&self) -> &'a [Arc<dyn PhysicalExpr>] {
        self.input_exprs
    }

    /// Returns the [`FieldRef`]s corresponding to the input expressions
    /// to the user-defined window function.
    pub fn input_fields(&self) -> &'a [FieldRef] {
        self.input_fields
    }

    /// Returns `true` when the user-defined window function is
    /// reversed, otherwise returns `false`.
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    /// Returns `true` when `IGNORE NULLS` is specified, otherwise
    /// returns `false`.
    pub fn ignore_nulls(&self) -> bool {
        self.ignore_nulls
    }
}
