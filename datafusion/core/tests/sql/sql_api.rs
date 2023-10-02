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

use datafusion::prelude::*;
use tempfile::TempDir;

#[tokio::test]
async fn unsupported_ddl_returns_error() {
    // Verify SessionContext::with_sql_options errors appropriately
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    // disallow ddl
    let options = SQLOptions::new().with_allow_ddl(false);

    let sql = "create view test_view as select * from test";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DDL not supported: CreateView"
    );

    // allow ddl
    let options = options.with_allow_ddl(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

#[tokio::test]
async fn unsupported_dml_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_dml(false);

    let sql = "insert into test values (1)";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DML not supported: Insert Into"
    );

    let options = options.with_allow_dml(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

#[tokio::test]
async fn unsupported_copy_returns_error() {
    let tmpdir = TempDir::new().unwrap();
    let tmpfile = tmpdir.path().join("foo.parquet");

    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_dml(false);

    let sql = format!("copy (values(1)) to '{}'", tmpfile.to_string_lossy());
    let df = ctx.sql_with_options(&sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DML not supported: COPY"
    );

    let options = options.with_allow_dml(true);
    ctx.sql_with_options(&sql, options).await.unwrap();
}

#[tokio::test]
async fn unsupported_statement_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_statements(false);

    let sql = "set datafusion.execution.batch_size = 5";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: Statement not supported: SetVariable"
    );

    let options = options.with_allow_statements(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

#[tokio::test]
async fn ddl_can_not_be_planned_by_session_state() {
    let ctx = SessionContext::new();

    // make a table via SQL
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // can not create a logical plan for catalog DDL
    let sql = "drop table test";
    let plan = state.create_logical_plan(sql).await.unwrap();
    let physical_plan = state.create_physical_plan(&plan).await;
    assert_eq!(
        physical_plan.unwrap_err().strip_backtrace(),
        "This feature is not implemented: Unsupported logical plan: DropTable"
    );
}
