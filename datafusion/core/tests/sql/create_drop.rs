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

use async_trait::async_trait;
use std::any::Any;
use std::io::Write;

use datafusion::datasource::datasource::TableProviderFactory;
use datafusion::execution::context::SessionState;
use datafusion_expr::TableType;
use tempfile::TempDir;

use super::*;

#[tokio::test]
async fn create_table_as() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "CREATE TABLE my_table AS SELECT * FROM aggregate_simple";
    ctx.sql(sql).await.unwrap();

    let sql_all = "SELECT * FROM my_table order by c1 LIMIT 1";
    let results_all = execute_to_batches(&ctx, sql_all).await;

    let expected = vec![
        "+---------+----------------+------+",
        "| c1      | c2             | c3   |",
        "+---------+----------------+------+",
        "| 0.00001 | 0.000000000001 | true |",
        "+---------+----------------+------+",
    ];

    assert_batches_eq!(expected, &results_all);

    Ok(())
}

#[tokio::test]
async fn create_or_replace_table_as() -> Result<()> {
    // the information schema used to introduce cyclic Arcs
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    // Create table
    let result = ctx
        .sql("CREATE TABLE y AS VALUES (1,2),(3,4)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(result.is_empty());

    // Replace table
    ctx.sql("CREATE OR REPLACE TABLE y AS VALUES (5,6)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let sql_all = "SELECT * FROM y";
    let results_all = execute_to_batches(&ctx, sql_all).await;

    let expected = vec![
        "+---------+---------+",
        "| column1 | column2 |",
        "+---------+---------+",
        "| 5       | 6       |",
        "+---------+---------+",
    ];

    assert_batches_eq!(expected, &results_all);

    // 'IF NOT EXISTS' cannot coexist with 'REPLACE'
    let result = ctx
        .sql("CREATE OR REPLACE TABLE if not exists y AS VALUES (7,8)")
        .await;
    assert!(
        result.is_err(),
        "'IF NOT EXISTS' cannot coexist with 'REPLACE'"
    );

    Ok(())
}

#[tokio::test]
async fn drop_table() -> Result<()> {
    let ctx = SessionContext::new();
    register_aggregate_simple_csv(&ctx).await?;

    let sql = "CREATE TABLE my_table AS SELECT * FROM aggregate_simple";
    ctx.sql(sql).await.unwrap();

    let sql = "DROP TABLE my_table";
    ctx.sql(sql).await.unwrap();

    let result = ctx.table("my_table");
    assert!(result.is_err(), "drop table should deregister table.");

    let sql = "DROP TABLE IF EXISTS my_table";
    ctx.sql(sql).await.unwrap();

    Ok(())
}

#[tokio::test]
async fn drop_view() -> Result<()> {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    plan_and_collect(&ctx, "CREATE VIEW v AS SELECT 1").await?;
    let rb = plan_and_collect(
        &ctx,
        "select * from information_schema.tables where table_name = 'v' and table_type = 'VIEW'",
    )
    .await?;
    assert_eq!(rb[0].num_rows(), 1);

    plan_and_collect(&ctx, "DROP VIEW v").await?;
    let rb = plan_and_collect(
        &ctx,
        "select * from information_schema.tables where table_name = 'v' and table_type = 'VIEW'",
    )
    .await?;
    assert!(rb.is_empty());
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "doesn't exist")]
async fn drop_view_nonexistent() {
    let ctx = SessionContext::new();
    ctx.sql("DROP VIEW non_existent_view")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic(expected = "doesn't exist")]
async fn drop_view_cant_drop_table() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t AS SELECT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("DROP VIEW t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[tokio::test]
#[should_panic(expected = "doesn't exist")]
async fn drop_table_cant_drop_view() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE VIEW v AS SELECT 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("DROP TABLE v")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

#[tokio::test]
async fn drop_view_if_exists() -> Result<()> {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));
    plan_and_collect(&ctx, "CREATE VIEW v AS SELECT 1").await?;
    let rb = plan_and_collect(&ctx, "DROP VIEW IF EXISTS non_existent_view").await?;
    // make sure we get an empty response back
    assert!(rb.is_empty());

    let rb = plan_and_collect(
        &ctx,
        "select * from information_schema.views where table_name = 'v'",
    )
    .await?;
    // confirm view exists
    assert_eq!(rb[0].num_rows(), 1);

    plan_and_collect(&ctx, "DROP VIEW IF EXISTS v").await?;
    let rb = plan_and_collect(
        &ctx,
        "select * from information_schema.views where table_name = 'v'",
    )
    .await?;
    // confirm view is gone
    assert!(rb.is_empty());
    Ok(())
}

#[tokio::test]
async fn csv_query_create_external_table() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, 10, c11, c12, c13 FROM aggregate_test_100 LIMIT 1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----+----+----+-------+------------+----------------------+----+-------+------------+-----------+-------------+--------------------+--------------------------------+",
        "| c1 | c2 | c3 | c4    | c5         | c6                   | c7 | c8    | c9         | Int64(10) | c11         | c12                | c13                            |",
        "+----+----+----+-------+------------+----------------------+----+-------+------------+-----------+-------------+--------------------+--------------------------------+",
        "| c  | 2  | 1  | 18109 | 2033001162 | -6513304855495910254 | 25 | 43062 | 1491205016 | 10        | 0.110830784 | 0.9294097332465232 | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |",
        "+----+----+----+-------+------------+----------------------+----+-------+------------+-----------+-------------+--------------------+--------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn create_external_table_with_timestamps() {
    let ctx = SessionContext::new();

    let data = "Jorge,2018-12-13T12:12:10.011Z\n\
                Andrew,2018-11-13T17:11:10.011Z";

    let tmp_dir = TempDir::new().unwrap();
    let file_path = tmp_dir.path().join("timestamps.csv");

    // scope to ensure the file is closed and written
    {
        std::fs::File::create(&file_path)
            .expect("creating temp file")
            .write_all(data.as_bytes())
            .expect("writing data");
    }

    let sql = format!(
        "CREATE EXTERNAL TABLE csv_with_timestamps (
                  name VARCHAR,
                  ts TIMESTAMP
              )
              STORED AS CSV
              LOCATION '{}'
              ",
        file_path.to_str().expect("path is utf8")
    );

    plan_and_collect(&ctx, &sql)
        .await
        .expect("Executing CREATE EXTERNAL TABLE");

    let sql = "SELECT * from csv_with_timestamps";
    let result = plan_and_collect(&ctx, sql).await.unwrap();
    let expected = vec![
        "+--------+-------------------------+",
        "| name   | ts                      |",
        "+--------+-------------------------+",
        "| Andrew | 2018-11-13 17:11:10.011 |",
        "| Jorge  | 2018-12-13 12:12:10.011 |",
        "+--------+-------------------------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
#[should_panic(expected = "already exists")]
async fn sql_create_duplicate_table() {
    // the information schema used to introduce cyclic Arcs
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    // Create table
    ctx.sql("CREATE TABLE y AS VALUES (1,2,3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Create table again
    let result = ctx
        .sql("CREATE TABLE y AS VALUES (1,2,3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(result, Vec::new());
}

#[tokio::test]
async fn sql_create_table_if_not_exists() -> Result<()> {
    // the information schema used to introduce cyclic Arcs
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    // Create table
    ctx.sql("CREATE TABLE y AS VALUES (1,2,3)")
        .await?
        .collect()
        .await?;

    // Create table again
    let result = ctx
        .sql("CREATE TABLE IF NOT EXISTS y AS VALUES (1,2,3)")
        .await?
        .collect()
        .await?;

    assert_eq!(result, Vec::new());

    // Create external table
    ctx.sql("CREATE EXTERNAL TABLE aggregate_simple STORED AS CSV WITH HEADER ROW LOCATION 'tests/aggregate_simple.csv'")
        .await?
        .collect()
        .await?;

    // Create external table
    let result = ctx.sql("CREATE EXTERNAL TABLE IF NOT EXISTS aggregate_simple STORED AS CSV WITH HEADER ROW LOCATION 'tests/aggregate_simple.csv'")
        .await?
        .collect()
        .await?;

    assert_eq!(result, Vec::new());

    Ok(())
}

#[tokio::test]
async fn create_pipe_delimited_csv_table() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "CREATE EXTERNAL TABLE aggregate_simple STORED AS CSV WITH HEADER ROW DELIMITER '|' LOCATION 'tests/aggregate_simple_pipe.csv'";
    ctx.sql(sql).await.unwrap();

    let sql_all = "SELECT * FROM aggregate_simple order by c1 LIMIT 1";
    let results_all = execute_to_batches(&ctx, sql_all).await;

    let expected = vec![
        "+---------+----------------+------+",
        "| c1      | c2             | c3   |",
        "+---------+----------------+------+",
        "| 0.00001 | 0.000000000001 | true |",
        "+---------+----------------+------+",
    ];

    assert_batches_eq!(expected, &results_all);

    Ok(())
}

struct TestTableProvider {}

impl TestTableProvider {}

#[async_trait]
impl TableProvider for TestTableProvider {
    fn as_any(&self) -> &dyn Any {
        unimplemented!("TestTableProvider is a stub for testing.")
    }

    fn schema(&self) -> SchemaRef {
        unimplemented!("TestTableProvider is a stub for testing.")
    }

    fn table_type(&self) -> TableType {
        unimplemented!("TestTableProvider is a stub for testing.")
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        _projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!("TestTableProvider is a stub for testing.")
    }
}

struct TestTableFactory {}

impl TableProviderFactory for TestTableFactory {
    fn create(&self, _name: &str, _path: &str) -> Arc<dyn TableProvider> {
        Arc::new(TestTableProvider {})
    }
}

#[tokio::test]
async fn create_custom_table() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_table_factory("DELTATABLE", Arc::new(TestTableFactory {}));

    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();
    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    Ok(())
}

#[tokio::test]
async fn create_bad_custom_table() {
    let ctx = SessionContext::new();

    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    let res = ctx.sql(sql).await;
    match res {
        Ok(_) => panic!("Registration of tables without factories should fail"),
        Err(e) => {
            assert!(
                e.to_string().contains("Unable to find factory for"),
                "Registration of tables without factories should throw correct error"
            )
        }
    }
}

#[tokio::test]
async fn create_csv_table_empty_file() -> Result<()> {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let sql = "CREATE EXTERNAL TABLE empty STORED AS CSV WITH HEADER ROW LOCATION 'tests/empty.csv'";
    ctx.sql(sql).await.unwrap();
    let sql =
        "select column_name, data_type, ordinal_position from information_schema.columns";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+-------------+-----------+------------------+",
        "| column_name | data_type | ordinal_position |",
        "+-------------+-----------+------------------+",
        "| c1          | Utf8      | 0                |",
        "| c2          | Utf8      | 1                |",
        "| c3          | Utf8      | 2                |",
        "+-------------+-----------+------------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

/// Execute SQL and return results
async fn plan_and_collect(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}
