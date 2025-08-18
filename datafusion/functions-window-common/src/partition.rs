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

use datafusion_common::arrow::datatypes::DataType;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

/// Arguments passed to created user-defined window function state
/// during physical execution.
#[derive(Debug, Default)]
pub struct PartitionEvaluatorArgs<'a> {
    /// The expressions passed as arguments to the user-defined window
    /// function.
    input_exprs: &'a [Arc<dyn PhysicalExpr>],
    /// The corresponding data types of expressions passed as arguments
    /// to the user-defined window function.
    input_types: &'a [DataType],
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
    ///     to the user-defined window function.
    /// * `input_types` - The data types corresponding to the
    ///     arguments to the user-defined window function.
    /// * `is_reversed` - Set to `true` if and only if the user-defined
    ///     window function is reversible and is reversed.
    /// * `ignore_nulls` - Set to `true` when `IGNORE NULLS` is
    ///     specified.
    ///
    pub fn new(
        input_exprs: &'a [Arc<dyn PhysicalExpr>],
        input_types: &'a [DataType],
        is_reversed: bool,
        ignore_nulls: bool,
    ) -> Self {
        Self {
            input_exprs,
            input_types,
            is_reversed,
            ignore_nulls,
        }
    }

    /// Returns the expressions passed as arguments to the user-defined
    /// window function.
    pub fn input_exprs(&self) -> &'a [Arc<dyn PhysicalExpr>] {
        self.input_exprs
    }

    /// Returns the [`DataType`]s corresponding to the input expressions
    /// to the user-defined window function.
    pub fn input_types(&self) -> &'a [DataType] {
        self.input_types
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
