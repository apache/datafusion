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

//! Simple user defined logical plan node for testing

use datafusion_common::DFSchemaRef;
use datafusion_expr::{
    logical_plan::{Extension, UserDefinedLogicalNodeCore},
    Expr, LogicalPlan,
};
use std::{
    fmt::{self, Debug},
    sync::Arc,
};

/// Create a new user defined plan node, for testing
pub fn new(input: LogicalPlan) -> LogicalPlan {
    let node = Arc::new(TestUserDefinedPlanNode { input });
    LogicalPlan::Extension(Extension { node })
}

#[derive(PartialEq, Eq, PartialOrd, Hash)]
struct TestUserDefinedPlanNode {
    input: LogicalPlan,
}

impl Debug for TestUserDefinedPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for TestUserDefinedPlanNode {
    fn name(&self) -> &str {
        "TestUserDefined"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TestUserDefined")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion_common::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self {
            input: inputs.swap_remove(0),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        false // Disallow limit push-down by default
    }
}
