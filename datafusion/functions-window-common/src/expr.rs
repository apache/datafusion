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

/// Arguments passed to user-defined window function
#[derive(Debug, Default)]
pub struct ExpressionArgs<'a> {
    /// The expressions passed as arguments to the user-defined window
    /// function.
    input_exprs: &'a [Arc<dyn PhysicalExpr>],
    /// The corresponding data types of expressions passed as arguments
    /// to the user-defined window function.
    input_types: &'a [DataType],
}

impl<'a> ExpressionArgs<'a> {
    /// Create an instance of [`ExpressionArgs`].
    ///
    /// # Arguments
    ///
    /// * `input_exprs` - The expressions passed as arguments
    ///     to the user-defined window function.
    /// * `input_types` - The data types corresponding to the
    ///     arguments to the user-defined window function.
    ///
    pub fn new(
        input_exprs: &'a [Arc<dyn PhysicalExpr>],
        input_types: &'a [DataType],
    ) -> Self {
        Self {
            input_exprs,
            input_types,
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
}
