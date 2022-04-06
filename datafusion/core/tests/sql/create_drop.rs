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

use std::io::Write;

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
