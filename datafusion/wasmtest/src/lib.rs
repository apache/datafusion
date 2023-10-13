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
use datafusion_expr::lit;
use datafusion_optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion_physical_expr::execution_props::ExecutionProps;
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
pub fn try_datafusion() {
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

    // Parse SQL (using datafusion-sql)
    let sql = "SELECT 2 + 37";
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    log(&format!("Parsed SQL: {ast:?}"));
}
