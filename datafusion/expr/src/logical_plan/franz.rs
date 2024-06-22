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

use super::plan::StreamingWindowType;
use super::LogicalPlanBuilder;
use crate::builder::add_group_by_exprs_from_dependencies;
use crate::{Aggregate, Expr};

use crate::expr_rewriter::normalize_cols;
use datafusion_common::Result;

use crate::logical_plan::LogicalPlan;

impl LogicalPlanBuilder {
    /// Apply franz window functions to extend the schema
    pub fn streaming_window(
        self,
        group_expr: impl IntoIterator<Item = impl Into<Expr>>,
        aggr_expr: impl IntoIterator<Item = impl Into<Expr>>,
        window_length: Duration,
        slide: Option<Duration>,
    ) -> Result<Self> {
        let group_expr = normalize_cols(group_expr, &self.plan)?;
        let aggr_expr = normalize_cols(aggr_expr, &self.plan)?;

        let group_expr =
            add_group_by_exprs_from_dependencies(group_expr, self.plan.schema())?;
        let window: StreamingWindowType = slide.map_or_else(
            || StreamingWindowType::Tumbling(window_length),
            |_slide| StreamingWindowType::Sliding(window_length, _slide),
        );
        let aggr = Aggregate::try_new(Arc::new(self.plan), group_expr, aggr_expr)
            .map(|new_aggr| LogicalPlan::StreamingWindow(new_aggr, window))
            .map(Self::from);
        aggr
    }
}
