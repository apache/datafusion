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

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::test_util::TestTableFactory;

use super::*;

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
    ctx.sql("CREATE EXTERNAL TABLE aggregate_simple STORED AS CSV WITH HEADER ROW LOCATION 'tests/data/aggregate_simple.csv'")
        .await?
        .collect()
        .await?;

    // Create external table
    let result = ctx.sql("CREATE EXTERNAL TABLE IF NOT EXISTS aggregate_simple STORED AS CSV WITH HEADER ROW LOCATION 'tests/data/aggregate_simple.csv'")
        .await?
        .collect()
        .await?;

    assert_eq!(result, Vec::new());

    Ok(())
}

#[tokio::test]
async fn create_custom_table() -> Result<()> {
    let mut cfg = RuntimeConfig::new();
    cfg.table_factories
        .insert("DELTATABLE".to_string(), Arc::new(TestTableFactory {}));
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let ctx = SessionContext::with_config_rt(ses, Arc::new(env));

    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();
    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    Ok(())
}

#[tokio::test]
async fn create_external_table_with_ddl() -> Result<()> {
    let mut cfg = RuntimeConfig::new();
    cfg.table_factories
        .insert("MOCKTABLE".to_string(), Arc::new(TestTableFactory {}));
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let ctx = SessionContext::with_config_rt(ses, Arc::new(env));

    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS MOCKTABLE LOCATION 'mockprotocol://path/to/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();

    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    let table_schema = schema.table("dt").await.unwrap().schema();

    assert_eq!(3, table_schema.fields().len());

    assert_eq!(&DataType::Int32, table_schema.field(0).data_type());
    assert_eq!(&DataType::Utf8, table_schema.field(1).data_type());
    assert_eq!(&DataType::Boolean, table_schema.field(2).data_type());

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

    let sql = "CREATE EXTERNAL TABLE empty STORED AS CSV WITH HEADER ROW LOCATION 'tests/data/empty.csv'";
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
