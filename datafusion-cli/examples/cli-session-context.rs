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

//! Shows an example of a custom session context that unions the input plan with itself.
//! To run this example, use `cargo run --example cli-session-context` from within the `datafusion-cli` directory.

use std::sync::Arc;

use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{context::SessionState, TaskContext},
    logical_expr::{LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use datafusion_cli::{
    cli_context::CliSessionContext, exec::exec_from_repl, print_options::PrintOptions,
};
use object_store::ObjectStore;

/// This is a toy example of a custom session context that unions the input plan with itself.
struct MyUnionerContext {
    ctx: SessionContext,
}

impl Default for MyUnionerContext {
    fn default() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }
}

#[async_trait::async_trait]
impl CliSessionContext for MyUnionerContext {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.ctx.task_ctx()
    }

    fn session_state(&self) -> SessionState {
        self.ctx.state()
    }

    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore + 'static>> {
        self.ctx.register_object_store(url, object_store)
    }

    fn register_table_options_extension_from_scheme(&self, _scheme: &str) {
        unimplemented!()
    }

    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError> {
        let new_plan = LogicalPlanBuilder::from(plan.clone())
            .union(plan.clone())?
            .build()?;

        self.ctx.execute_logical_plan(new_plan).await
    }
}

#[tokio::main]
/// Runs the example.
pub async fn main() {
    let my_ctx = MyUnionerContext::default();

    let mut print_options = PrintOptions {
        format: datafusion_cli::print_format::PrintFormat::Automatic,
        quiet: false,
        maxrows: datafusion_cli::print_options::MaxRows::Unlimited,
        color: true,
    };

    exec_from_repl(&my_ctx, &mut print_options).await.unwrap();
}
