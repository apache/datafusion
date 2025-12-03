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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]

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
    use datafusion::{
        arrow::{
            array::{ArrayRef, Int32Array, RecordBatch, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        datasource::MemTable,
        execution::context::SessionContext,
    };
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::{
        config::SessionConfig,
        disk_manager::{DiskManagerBuilder, DiskManagerMode},
        runtime_env::RuntimeEnvBuilder,
    };
    use datafusion_physical_plan::collect;
    use datafusion_sql::parser::DFParser;
    use object_store::{memory::InMemory, path::Path, ObjectStore};
    use url::Url;
    use wasm_bindgen_test::wasm_bindgen_test;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), allow(dead_code))]
    fn datafusion_test() {
        basic_exprs();
        basic_parse();
    }

    fn get_ctx() -> Arc<SessionContext> {
        let rt = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
            .unwrap();
        let session_config = SessionConfig::new().with_target_partitions(1);
        Arc::new(SessionContext::new_with_config_rt(session_config, rt))
    }

    fn create_test_data() -> (Arc<Schema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let data: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ];

        let batch = RecordBatch::try_new(schema.clone(), data).unwrap();
        (schema, batch)
    }

    #[wasm_bindgen_test(unsupported = tokio::test)]
    async fn basic_execute() {
        let sql = "SELECT 2 + 2;";

        // Execute SQL (using datafusion)

        let session_context = get_ctx();
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

    #[wasm_bindgen_test(unsupported = tokio::test)]
    async fn basic_df_function_execute() {
        let sql = "SELECT abs(-1.0);";
        let statement = DFParser::parse_sql(sql).unwrap().pop_back().unwrap();
        let ctx = get_ctx();
        let logical_plan = ctx.state().statement_to_plan(statement).await.unwrap();
        let data_frame = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let physical_plan = data_frame.create_physical_plan().await.unwrap();

        let task_ctx = ctx.task_ctx();
        let _ = collect(physical_plan, task_ctx).await.unwrap();
    }

    #[wasm_bindgen_test(unsupported = tokio::test)]
    async fn test_basic_aggregate() {
        let sql =
            "SELECT FIRST_VALUE(value) OVER (ORDER BY id) as first_val FROM test_table;";

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let data: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ];

        let batch = RecordBatch::try_new(schema.clone(), data).unwrap();
        let table = MemTable::try_new(schema.clone(), vec![vec![batch]]).unwrap();

        let ctx = get_ctx();
        ctx.register_table("test_table", Arc::new(table)).unwrap();

        let statement = DFParser::parse_sql(sql).unwrap().pop_back().unwrap();

        let logical_plan = ctx.state().statement_to_plan(statement).await.unwrap();
        let data_frame = ctx.execute_logical_plan(logical_plan).await.unwrap();
        let physical_plan = data_frame.create_physical_plan().await.unwrap();

        let task_ctx = ctx.task_ctx();
        let _ = collect(physical_plan, task_ctx).await.unwrap();
    }

    #[wasm_bindgen_test(unsupported = tokio::test)]
    async fn test_parquet_write() {
        let (schema, batch) = create_test_data();
        let mut buffer = Vec::new();
        let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
            &mut buffer,
            schema.clone(),
            None,
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[wasm_bindgen_test(unsupported = tokio::test)]
    async fn test_parquet_read_and_write() {
        let (schema, batch) = create_test_data();
        let mut buffer = Vec::new();
        let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new(
            &mut buffer,
            schema.clone(),
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let session_ctx = SessionContext::new();
        let store = InMemory::new();

        let path = Path::from("a.parquet");
        store.put(&path, buffer.into()).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx.register_object_store(&url, Arc::new(store));
        session_ctx
            .register_parquet("a", "memory:///a.parquet", Default::default())
            .await
            .unwrap();

        let df = session_ctx.sql("SELECT * FROM a").await.unwrap();

        let result = df.collect().await.unwrap();

        assert_eq!(
            batches_to_string(&result),
            "+----+-------+\n\
             | id | value |\n\
             +----+-------+\n\
             | 1  | a     |\n\
             | 2  | b     |\n\
             | 3  | c     |\n\
             +----+-------+"
        );
    }
}
