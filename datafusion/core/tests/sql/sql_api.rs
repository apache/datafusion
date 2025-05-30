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
async fn test_window_function() {
    let ctx = SessionContext::new();
    let df = ctx
        .sql(
            r#"SELECT
        t1.v1,
        SUM(t1.v1) OVER w + 1
        FROM
        generate_series(1, 10000) AS t1(v1)
        WINDOW
        w AS (ORDER BY t1.v1 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);"#,
        )
        .await;
    assert!(df.is_ok());
}

#[tokio::test]
async fn unsupported_ddl_returns_error() {
    // Verify SessionContext::with_sql_options errors appropriately
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    // disallow ddl
    let options = SQLOptions::new().with_allow_ddl(false);

    let sql = "CREATE VIEW test_view AS SELECT * FROM test";
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

    let sql = "INSERT INTO test VALUES (1)";
    let df = ctx.sql_with_options(sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: DML not supported: Insert Into"
    );

    let options = options.with_allow_dml(true);
    ctx.sql_with_options(sql, options).await.unwrap();
}

#[tokio::test]
async fn dml_output_schema() {
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};

    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();
    let sql = "INSERT INTO test VALUES (1)";
    let df = ctx.sql(sql).await.unwrap();
    let count_schema = Schema::new(vec![Field::new("count", DataType::UInt64, false)]);
    assert_eq!(Schema::from(df.schema()), count_schema);
}

#[tokio::test]
async fn unsupported_copy_returns_error() {
    let tmpdir = TempDir::new().unwrap();
    let tmpfile = tmpdir.path().join("foo.parquet");

    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let options = SQLOptions::new().with_allow_dml(false);

    let sql = format!(
        "COPY (values(1)) TO '{}' STORED AS parquet",
        tmpfile.to_string_lossy()
    );
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

// Disallow PREPARE and EXECUTE statements if `allow_statements` is false
#[tokio::test]
async fn disable_prepare_and_execute_statement() {
    let ctx = SessionContext::new();

    let prepare_sql = "PREPARE plan(INT) AS SELECT $1";
    let execute_sql = "EXECUTE plan(1)";
    let options = SQLOptions::new().with_allow_statements(false);
    let df = ctx.sql_with_options(prepare_sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: Statement not supported: Prepare"
    );
    let df = ctx.sql_with_options(execute_sql, options).await;
    assert_eq!(
        df.unwrap_err().strip_backtrace(),
        "Error during planning: Statement not supported: Execute"
    );

    let options = options.with_allow_statements(true);
    ctx.sql_with_options(prepare_sql, options).await.unwrap();
    ctx.sql_with_options(execute_sql, options).await.unwrap();
}

#[tokio::test]
async fn empty_statement_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // Give it an empty string which contains no statements
    let plan_res = state.create_logical_plan("").await;
    assert_eq!(
        plan_res.unwrap_err().strip_backtrace(),
        "Error during planning: No SQL statements were provided in the query string"
    );
}

#[tokio::test]
async fn multiple_statements_returns_error() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // Give it a string that contains multiple statements
    let plan_res = state
        .create_logical_plan(
            "INSERT INTO test (x) VALUES (1); INSERT INTO test (x) VALUES (2)",
        )
        .await;
    assert_eq!(
        plan_res.unwrap_err().strip_backtrace(),
        "This feature is not implemented: The context currently only supports a single SQL statement"
    );
}

#[tokio::test]
async fn ddl_can_not_be_planned_by_session_state() {
    let ctx = SessionContext::new();

    // make a table via SQL
    ctx.sql("CREATE TABLE test (x int)").await.unwrap();

    let state = ctx.state();

    // can not create a logical plan for catalog DDL
    let sql = "DROP TABLE test";
    let plan = state.create_logical_plan(sql).await.unwrap();
    let physical_plan = state.create_physical_plan(&plan).await;
    assert_eq!(
        physical_plan.unwrap_err().strip_backtrace(),
        "This feature is not implemented: Unsupported logical plan: DropTable"
    );
}
