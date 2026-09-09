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
use datafusion::catalog::{
    CatalogProvider, CatalogProviderFactory, MemoryCatalogProvider,
};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::CreateExternalCatalog;
use datafusion::test_util::TestTableFactory;
use datafusion_catalog::Session;
use datafusion_common::exec_err;

use super::*;

#[derive(Debug)]
struct TestCatalogFactory {}

#[async_trait]
impl CatalogProviderFactory for TestCatalogFactory {
    async fn create(
        &self,
        _state: &dyn Session,
        cmd: &CreateExternalCatalog,
    ) -> Result<Arc<dyn CatalogProvider>> {
        if cmd.options.contains_key("fail") {
            return exec_err!("catalog factory configured to fail");
        }
        Ok(Arc::new(MemoryCatalogProvider::new()))
    }
}

#[tokio::test]
async fn create_custom_table() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

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
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .table_factories_mut()
        .insert("MOCKTABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS MOCKTABLE LOCATION 'mockprotocol://path/to/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();

    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    let table_schema = schema.table("dt").await.unwrap().unwrap().schema();

    assert_eq!(3, table_schema.fields().len());

    assert_eq!(&DataType::Int32, table_schema.field(0).data_type());
    assert_eq!(&DataType::Utf8View, table_schema.field(1).data_type());
    assert_eq!(&DataType::Boolean, table_schema.field(2).data_type());

    Ok(())
}

#[tokio::test]
async fn create_drop_table() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "CREATE TABLE dt (a_id integer, a_str string, a_bool boolean);";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();

    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    // Drop the table
    let sql = "DROP TABLE dt;";
    ctx.sql(sql).await.unwrap();

    let exists = schema.table_exist("dt");
    assert!(!exists, "Table should have been dropped!");

    Ok(())
}

#[tokio::test]
async fn create_external_catalog_with_factory() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .catalog_factories_mut()
        .insert("TESTCATALOG".to_string(), Arc::new(TestCatalogFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL CATALOG cat STORED AS TESTCATALOG LOCATION 's3://bucket/warehouse' OPTIONS ('warehouse' 'cat')";
    ctx.sql(sql).await?;

    assert!(
        ctx.catalog("cat").is_some(),
        "Catalog should have been created!"
    );

    Ok(())
}

#[tokio::test]
async fn create_external_catalog_unknown_factory() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "CREATE EXTERNAL CATALOG cat STORED AS TESTCATALOG LOCATION 's3://bucket/warehouse'";
    let err = ctx.sql(sql).await.unwrap_err();
    assert_contains!(
        err.to_string(),
        "Unable to find catalog factory for TESTCATALOG"
    );

    Ok(())
}

#[tokio::test]
async fn create_external_catalog_factory_error_not_registered() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .catalog_factories_mut()
        .insert("TESTCATALOG".to_string(), Arc::new(TestCatalogFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL CATALOG cat STORED AS TESTCATALOG LOCATION 's3://x' OPTIONS ('fail' 'true')";
    let err = ctx.sql(sql).await.unwrap_err();
    assert_contains!(err.to_string(), "catalog factory configured to fail");
    assert!(
        ctx.catalog("cat").is_none(),
        "Catalog should not have been registered when the factory errors"
    );

    Ok(())
}

#[tokio::test]
async fn create_external_catalog_if_not_exists() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .catalog_factories_mut()
        .insert("TESTCATALOG".to_string(), Arc::new(TestCatalogFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL CATALOG cat STORED AS TESTCATALOG LOCATION 's3://x'";
    ctx.sql(sql).await?;

    // creating it again without IF NOT EXISTS should fail
    let err = ctx.sql(sql).await.unwrap_err();
    assert_contains!(err.to_string(), "already exists");

    // ... but should succeed with IF NOT EXISTS
    let sql = "CREATE EXTERNAL CATALOG IF NOT EXISTS cat STORED AS TESTCATALOG LOCATION 's3://x'";
    ctx.sql(sql).await?;

    Ok(())
}

#[tokio::test]
async fn create_drop_external_catalog() -> Result<()> {
    let mut state = SessionStateBuilder::new().with_default_features().build();
    state
        .catalog_factories_mut()
        .insert("TESTCATALOG".to_string(), Arc::new(TestCatalogFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL CATALOG cat STORED AS TESTCATALOG LOCATION 's3://x'";
    ctx.sql(sql).await?;
    assert!(ctx.catalog("cat").is_some());

    ctx.sql("DROP EXTERNAL CATALOG cat").await?;
    assert!(
        ctx.catalog("cat").is_none(),
        "Catalog should have been dropped!"
    );

    // dropping again should fail without IF EXISTS
    let err = ctx.sql("DROP EXTERNAL CATALOG cat").await.unwrap_err();
    assert_contains!(err.to_string(), "doesn't exist");

    // ... but should succeed with IF EXISTS
    ctx.sql("DROP EXTERNAL CATALOG IF EXISTS cat").await?;

    Ok(())
}
