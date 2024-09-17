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
extern crate wasm_bindgen;

use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::lit;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

pub fn set_panic_hook() {
    // When the `console_error_panic_hook` feature is enabled, we can call the
    // `set_panic_hook` function at least once during initialization, and then
    // we will get better error messages if our code ever panics.
    //
    // For more details see
    // https://github.com/rustwasm/console_error_panic_hook#readme
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Make console.log available as the log Rust function
#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen]
pub fn basic_exprs() {
    set_panic_hook();
    // Create a scalar value (from datafusion-common)
    let scalar = ScalarValue::from("Hello, World!");
    log(&format!("ScalarValue: {scalar:?}"));

    // Create an Expr (from datafusion-expr)
    let expr = lit(28) + lit(72);
    log(&format!("Expr: {expr:?}"));

    // Simplify Expr (using datafusion-phys-expr and datafusion-optimizer)
    let schema = Arc::new(DFSchema::empty());
    let execution_props = ExecutionProps::new();
    let simplifier =
        ExprSimplifier::new(SimplifyContext::new(&execution_props).with_schema(schema));
    let simplified_expr = simplifier.simplify(expr).unwrap();
    log(&format!("Simplified Expr: {simplified_expr:?}"));
}

#[wasm_bindgen]
pub fn basic_parse() {
    // Parse SQL (using datafusion-sql)
    let sql = "SELECT 2 + 37";
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    log(&format!("Parsed SQL: {ast:?}"));
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::execution::context::SessionContext;
    use datafusion_execution::{
        config::SessionConfig, disk_manager::DiskManagerConfig,
        runtime_env::RuntimeEnvBuilder,
    };
    use datafusion_physical_plan::collect;
    use datafusion_sql::parser::DFParser;
    use wasm_bindgen_test::wasm_bindgen_test;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[wasm_bindgen_test]
    fn datafusion_test() {
        basic_exprs();
        basic_parse();
    }

    #[wasm_bindgen_test]
    async fn basic_execute() {
        let sql = "SELECT 2 + 2;";

        // Execute SQL (using datafusion)
        let rt = RuntimeEnvBuilder::new()
            .with_disk_manager(DiskManagerConfig::Disabled)
            .build_arc()
            .unwrap();
        let session_config = SessionConfig::new().with_target_partitions(1);
        let session_context =
            Arc::new(SessionContext::new_with_config_rt(session_config, rt));

        let statement = DFParser::parse_sql(sql).unwrap().pop_back().unwrap();

        let logical_plan = session_context
            .state()
            .statement_to_plan(statement)
            .await
            .unwrap();
        let data_frame = session_context
            .execute_logical_plan(logical_plan)
            .await
            .unwrap();
        let physical_plan = data_frame.create_physical_plan().await.unwrap();

        let task_ctx = session_context.task_ctx();
        let _ = collect(physical_plan, task_ctx).await.unwrap();
    }
}
