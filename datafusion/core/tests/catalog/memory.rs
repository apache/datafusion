// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::Schema;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use datafusion_catalog::memory::*;
use datafusion_catalog::{SchemaProvider, TableProvider};
use datafusion_common::test_util::batches_to_string;
use datafusion_common_runtime::SpawnedTask;
use insta::assert_snapshot;
use std::sync::Arc;

#[tokio::test]
async fn async_schema_registration() -> datafusion_common::Result<()> {
    #[derive(Debug)]
    struct YieldingSchema {
        inner: MemorySchemaProvider,
        ctx: std::sync::Weak<SessionContext>,
    }

    #[async_trait::async_trait]
    impl SchemaProvider for YieldingSchema {
        fn table_names(&self) -> Vec<String> {
            self.inner.table_names()
        }

        async fn table(
            &self,
            name: &str,
        ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
            self.inner.table(name).await
        }

        fn table_exist(&self, name: &str) -> bool {
            self.inner.table_exist(name)
        }

        async fn register_table(
            &self,
            name: String,
            table: Arc<dyn TableProvider>,
        ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
            tokio::task::yield_now().await;
            assert!(
                self.ctx
                    .upgrade()
                    .unwrap()
                    .state_ref()
                    .try_write()
                    .is_some()
            );
            self.inner.register_table(name, table).await
        }

        async fn deregister_table(
            &self,
            name: &str,
        ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
            tokio::task::yield_now().await;
            assert!(
                self.ctx
                    .upgrade()
                    .unwrap()
                    .state_ref()
                    .try_write()
                    .is_some()
            );
            self.inner.deregister_table(name).await
        }
    }

    let ctx = Arc::new(SessionContext::new());
    ctx.catalog("datafusion").unwrap().register_schema(
        "public",
        Arc::new(YieldingSchema {
            inner: MemorySchemaProvider::new(),
            ctx: Arc::downgrade(&ctx),
        }),
    )?;

    // Spawning also checks that registration futures remain Send.
    SpawnedTask::spawn(async move {
        let table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())));
        assert!(ctx.register_table("t", table.clone()).await?.is_none());
        assert!(ctx.register_table("t", table).await.is_err());
        assert!(ctx.deregister_table("t").await?.is_some());
        assert!(ctx.deregister_table("t").await?.is_none());

        for sql in [
            "CREATE TABLE t AS SELECT 1 AS value",
            "CREATE OR REPLACE TABLE t AS SELECT 2 AS value",
            "CREATE VIEW v AS SELECT * FROM t",
            "CREATE OR REPLACE VIEW v AS SELECT * FROM t",
        ] {
            ctx.sql(sql).await?.collect().await?;
        }
        assert_eq!(ctx.table("v").await?.count().await?, 1);
        ctx.sql("DROP VIEW v").await?.collect().await?;
        ctx.sql("DROP TABLE t").await?.collect().await?;
        assert!(!ctx.table_exist("v")?);
        assert!(!ctx.table_exist("t")?);
        Ok(())
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn memory_catalog_dereg_nonempty_schema() {
    let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

    let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
    let test_table =
        Arc::new(EmptyTable::new(Arc::new(Schema::empty()))) as Arc<dyn TableProvider>;
    schema.register_table("t".into(), test_table).await.unwrap();

    cat.register_schema("foo", schema.clone()).unwrap();

    assert!(
        cat.deregister_schema("foo", false).is_err(),
        "dropping empty schema without cascade should error"
    );
    assert!(cat.deregister_schema("foo", true).unwrap().is_some());
}

#[tokio::test]
async fn memory_catalog_dereg_nonempty_schema_with_table_removal() {
    let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

    let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
    let test_table =
        Arc::new(EmptyTable::new(Arc::new(Schema::empty()))) as Arc<dyn TableProvider>;
    schema.register_table("t".into(), test_table).await.unwrap();

    cat.register_schema("foo", schema.clone()).unwrap();
    schema.deregister_table("t").await.unwrap();
    assert!(cat.deregister_schema("foo", false).unwrap().is_some());
}

#[test]
fn memory_catalog_dereg_empty_schema() {
    let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

    let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
    cat.register_schema("foo", schema).unwrap();

    assert!(cat.deregister_schema("foo", false).unwrap().is_some());
}

#[test]
fn memory_catalog_dereg_missing() {
    let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;
    assert!(cat.deregister_schema("foo", false).unwrap().is_none());
}

#[test]
fn default_register_schema_not_supported() {
    // mimic a new CatalogProvider and ensure it does not support registering schemas
    #[derive(Debug)]
    struct TestProvider {}
    impl CatalogProvider for TestProvider {
        fn schema_names(&self) -> Vec<String> {
            unimplemented!()
        }

        fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
            unimplemented!()
        }
    }

    let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
    let catalog = Arc::new(TestProvider {});

    match catalog.register_schema("foo", schema) {
        Ok(_) => panic!("unexpected OK"),
        Err(e) => assert_eq!(
            e.strip_backtrace(),
            "This feature is not implemented: Registering new schemas is not supported"
        ),
    }
}

#[tokio::test]
async fn test_mem_provider() {
    let provider = MemorySchemaProvider::new();
    let table_name = "test_table_exist";
    assert!(!provider.table_exist(table_name));
    assert!(
        provider
            .deregister_table(table_name)
            .await
            .unwrap()
            .is_none()
    );
    let test_table = EmptyTable::new(Arc::new(Schema::empty()));
    // register table successfully
    assert!(
        provider
            .register_table(table_name.to_string(), Arc::new(test_table))
            .await
            .unwrap()
            .is_none()
    );
    assert!(provider.table_exist(table_name));
    let other_table = EmptyTable::new(Arc::new(Schema::empty()));
    let result = provider
        .register_table(table_name.to_string(), Arc::new(other_table))
        .await;
    assert!(result.is_err(), "The table test_table_exist already exists");
}

#[tokio::test]
async fn test_schema_register_listing_table() {
    let testdata = datafusion::test_util::parquet_test_data();
    let testdir = if testdata.starts_with('/') {
        format!("file://{testdata}")
    } else {
        format!("file:///{testdata}")
    };
    let filename = if testdir.ends_with('/') {
        format!("{}{}", testdir, "alltypes_plain.parquet")
    } else {
        format!("{}/{}", testdir, "alltypes_plain.parquet")
    };

    let table_path = ListingTableUrl::parse(filename).unwrap();

    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    let ctx = SessionContext::new();

    let config = ListingTableConfig::new(table_path)
        .infer(&ctx.state())
        .await
        .unwrap();
    let table = ListingTable::try_new(config).unwrap();

    schema
        .register_table("alltypes_plain".to_string(), Arc::new(table))
        .await
        .unwrap();

    catalog.register_schema("active", Arc::new(schema)).unwrap();
    ctx.register_catalog("cat", Arc::new(catalog));

    let df = ctx
        .sql("SELECT id, bool_col FROM cat.active.alltypes_plain")
        .await
        .unwrap();

    let actual = df.collect().await.unwrap();

    assert_snapshot!(batches_to_string(&actual), @r"
    +----+----------+
    | id | bool_col |
    +----+----------+
    | 4  | true     |
    | 5  | false    |
    | 6  | true     |
    | 7  | false    |
    | 2  | true     |
    | 3  | false    |
    | 0  | true     |
    | 1  | false    |
    +----+----------+
    ");
}
