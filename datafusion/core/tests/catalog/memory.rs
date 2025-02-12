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

use arrow::datatypes::Schema;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use datafusion_catalog::memory::*;
use datafusion_catalog::{SchemaProvider, TableProvider};
use datafusion_common::assert_batches_eq;
use std::any::Any;
use std::sync::Arc;

#[test]
fn memory_catalog_dereg_nonempty_schema() {
    let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

    let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
    let test_table =
        Arc::new(EmptyTable::new(Arc::new(Schema::empty()))) as Arc<dyn TableProvider>;
    schema.register_table("t".into(), test_table).unwrap();

    cat.register_schema("foo", schema.clone()).unwrap();

    assert!(
        cat.deregister_schema("foo", false).is_err(),
        "dropping empty schema without cascade should error"
    );
    assert!(cat.deregister_schema("foo", true).unwrap().is_some());
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
        fn as_any(&self) -> &dyn Any {
            self
        }

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
    };
}

#[tokio::test]
async fn test_mem_provider() {
    let provider = MemorySchemaProvider::new();
    let table_name = "test_table_exist";
    assert!(!provider.table_exist(table_name));
    assert!(provider.deregister_table(table_name).unwrap().is_none());
    let test_table = EmptyTable::new(Arc::new(Schema::empty()));
    // register table successfully
    assert!(provider
        .register_table(table_name.to_string(), Arc::new(test_table))
        .unwrap()
        .is_none());
    assert!(provider.table_exist(table_name));
    let other_table = EmptyTable::new(Arc::new(Schema::empty()));
    let result = provider.register_table(table_name.to_string(), Arc::new(other_table));
    assert!(result.is_err());
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
        .unwrap();

    catalog.register_schema("active", Arc::new(schema)).unwrap();
    ctx.register_catalog("cat", Arc::new(catalog));

    let df = ctx
        .sql("SELECT id, bool_col FROM cat.active.alltypes_plain")
        .await
        .unwrap();

    let actual = df.collect().await.unwrap();

    let expected = [
        "+----+----------+",
        "| id | bool_col |",
        "+----+----------+",
        "| 4  | true     |",
        "| 5  | false    |",
        "| 6  | true     |",
        "| 7  | false    |",
        "| 2  | true     |",
        "| 3  | false    |",
        "| 0  | true     |",
        "| 1  | false    |",
        "+----+----------+",
    ];
    assert_batches_eq!(expected, &actual);
}
