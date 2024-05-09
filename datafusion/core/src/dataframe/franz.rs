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

use std::time::Duration;

use super::{
    DataFrame, LogicalPlanBuilder,
};
use crate::error::Result;
use crate::logical_expr::Expr;

impl DataFrame {

    /// Return a new DataFrame that adds the result of evaluating one or more
    /// window functions ([`Expr::WindowFunction`]) to the existing columns
    /// TODO: add documentation
    pub fn franz_window(
        self,
        window_exprs: Vec<Expr>,
        window_length: Duration,
    ) -> Result<DataFrame> {
        let plan = LogicalPlanBuilder::from(self.plan)
            .franz_window(window_exprs, window_length)?
            .build()?;
        Ok(DataFrame::new(self.session_state, plan))
    }
}

#[cfg(test)]
mod tests {
}
