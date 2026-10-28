// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_common::arrow::datatypes::FieldRef;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Arguments passed to user-defined window function
#[derive(Debug, Default)]
pub struct ExpressionArgs<'a> {
    /// The expressions passed as arguments to the user-defined window
    /// function.
    input_exprs: &'a [Arc<dyn PhysicalExpr>],
    /// The corresponding fields of expressions passed as arguments
    /// to the user-defined window function.
    input_fields: &'a [FieldRef],
}

impl<'a> ExpressionArgs<'a> {
    /// Create an instance of [`ExpressionArgs`].
    ///
    /// # Arguments
    ///
    /// * `input_exprs` - The expressions passed as arguments
    ///   to the user-defined window function.
    /// * `input_fields` - The fields corresponding to the
    ///   arguments to the user-defined window function.
    pub fn new(
        input_exprs: &'a [Arc<dyn PhysicalExpr>],
        input_fields: &'a [FieldRef],
    ) -> Self {
        Self {
            input_exprs,
            input_fields,
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
}
