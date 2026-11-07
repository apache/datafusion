// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Simple user defined logical plan node for testing

use datafusion_common::DFSchemaRef;
use datafusion_expr::{
    Expr, LogicalPlan,
    logical_plan::{Extension, UserDefinedLogicalNodeCore},
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
