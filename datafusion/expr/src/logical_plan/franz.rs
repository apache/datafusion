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

use std::sync::Arc;
use std::time::Duration;

use crate::builder::validate_unique_names;
use super::LogicalPlanBuilder;
use crate::Expr;

use crate::expr_rewriter::normalize_cols;
use datafusion_common::Result;

use crate::logical_plan::{LogicalPlan, Window};

impl LogicalPlanBuilder {
    /// Apply franz window functions to extend the schema
    pub fn franz_window(
        self,
        window_expr: impl IntoIterator<Item = impl Into<Expr>>,
        _window_length: Duration,
    ) -> Result<Self> {
        let window_expr = normalize_cols(window_expr, &self.plan)?;
        validate_unique_names("Windows", &window_expr)?;
        Ok(Self::from(LogicalPlan::Window(Window::try_new(
            window_expr,
            Arc::new(self.plan),
        )?)))
    }
}
