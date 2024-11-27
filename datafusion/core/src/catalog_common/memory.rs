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

//! [`MemoryCatalogProvider`], [`MemoryCatalogProviderList`]: In-memory
//! implementations of [`CatalogProviderList`] and [`CatalogProvider`].

use crate::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider,
};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_common::{exec_err, DataFusionError, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use std::any::Any;
use std::sync::Arc;

/// Simple in-memory list of catalogs
#[derive(Debug)]
pub struct MemoryCatalogProviderList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl MemoryCatalogProviderList {
    /// Instantiates a new `MemoryCatalogProviderList` with an empty collection of catalogs
    pub fn new() -> Self {
        Self {
            catalogs: DashMap::new(),
        }
    }
}

impl Default for MemoryCatalogProviderList {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CatalogProviderList for MemoryCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Result<Option<Arc<dyn CatalogProvider>>> {
        Ok(self.catalogs.insert(name, catalog))
    }

    async fn catalog_names(&self) -> BoxStream<'static, Result<String>> {
        let catalog_names = self
            .catalogs
            .iter()
            .map(|keyval| Ok(keyval.key().clone()))
            .collect::<Vec<_>>();
        futures::stream::iter(catalog_names).boxed()
    }

    async fn catalog(&self, name: &str) -> Result<Option<Arc<dyn CatalogProvider>>> {
        Ok(self.catalogs.get(name).map(|c| Arc::clone(c.value())))
    }
}

/// Simple in-memory implementation of a catalog.
#[derive(Debug)]
pub struct MemoryCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl MemoryCatalogProvider {
    /// Instantiates a new MemoryCatalogProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: DashMap::new(),
        }
    }
}

impl Default for MemoryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn schema_names(&self) -> BoxStream<'static, Result<String>> {
        let schema_names = self
            .schemas
            .iter()
            .map(|keyval| Ok(keyval.key().clone()))
            .collect::<Vec<_>>();
        futures::stream::iter(schema_names).boxed()
    }

    async fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.get(name).map(|s| Arc::clone(s.value())))
    }

    async fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.into(), schema))
    }

    async fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if let Some(schema) = self.schema(name).await? {
            let table_names = schema.table_names().await.try_collect::<Vec<_>>().await?;
            match (table_names.is_empty(), cascade) {
                (true, _) | (false, true) => {
                    let (_, removed) = self.schemas.remove(name).unwrap();
                    Ok(Some(removed))
                }
                (false, false) => exec_err!(
                    "Cannot drop schema {} because other tables depend on it: {}",
                    name,
                    itertools::join(table_names.iter(), ", ")
                ),
            }
        } else {
            Ok(None)
        }
    }
}

/// Simple in-memory implementation of a schema.
#[derive(Debug)]
pub struct MemorySchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl MemorySchemaProvider {
    /// Instantiates a new MemorySchemaProvider with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }
}

impl Default for MemorySchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn table_names(&self) -> BoxStream<'static, Result<String>> {
        let table_names = self
            .tables
            .iter()
            .map(|keyval| Ok(keyval.key().clone()))
            .collect::<Vec<_>>();
        futures::stream::iter(table_names).boxed()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).map(|table| Arc::clone(table.value())))
    }

    async fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()).await {
            return exec_err!("The table {name} already exists");
        }
        Ok(self.tables.insert(name, table))
    }

    async fn deregister_table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    async fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::catalog::CatalogProvider;
    use crate::catalog_common::memory::MemorySchemaProvider;
    use crate::datasource::empty::EmptyTable;
    use crate::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
    use crate::prelude::SessionContext;
    use arrow_schema::Schema;
    use datafusion_common::assert_batches_eq;
    use std::any::Any;
    use std::sync::Arc;

    #[tokio::test]
    async fn memory_catalog_dereg_nonempty_schema() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        let test_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
            as Arc<dyn TableProvider>;
        schema.register_table("t".into(), test_table).await.unwrap();

        cat.register_schema("foo", schema.clone()).await.unwrap();

        assert!(
            cat.deregister_schema("foo", false).await.is_err(),
            "dropping empty schema without cascade should error"
        );
        assert!(cat.deregister_schema("foo", true).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn memory_catalog_dereg_empty_schema() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        cat.register_schema("foo", schema).await.unwrap();

        assert!(cat.deregister_schema("foo", false).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn memory_catalog_dereg_missing() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;
        assert!(cat.deregister_schema("foo", false).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn default_register_schema_not_supported() {
        // mimic a new CatalogProvider and ensure it does not support registering schemas
        #[derive(Debug)]
        struct TestProvider {}
        #[async_trait]
        impl CatalogProvider for TestProvider {
            fn as_any(&self) -> &dyn Any {
                self
            }

            async fn schema_names(&self) -> BoxStream<'static, Result<String>> {
                unimplemented!()
            }

            async fn schema(
                &self,
                _name: &str,
            ) -> Result<Option<Arc<dyn SchemaProvider>>> {
                unimplemented!()
            }
        }

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        let catalog = Arc::new(TestProvider {});

        match catalog.register_schema("foo", schema).await {
            Ok(_) => panic!("unexpected OK"),
            Err(e) => assert_eq!(e.strip_backtrace(), "This feature is not implemented: Registering new schemas is not supported"),
        };
    }

    #[tokio::test]
    async fn test_mem_provider() {
        let provider = MemorySchemaProvider::new();
        let table_name = "test_table_exist";
        assert!(!provider.table_exist(table_name).await);
        assert!(provider
            .deregister_table(table_name)
            .await
            .unwrap()
            .is_none());
        let test_table = EmptyTable::new(Arc::new(Schema::empty()));
        // register table successfully
        assert!(provider
            .register_table(table_name.to_string(), Arc::new(test_table))
            .await
            .unwrap()
            .is_none());
        assert!(provider.table_exist(table_name).await);
        let other_table = EmptyTable::new(Arc::new(Schema::empty()));
        let result = provider
            .register_table(table_name.to_string(), Arc::new(other_table))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_register_listing_table() {
        let testdata = crate::test_util::parquet_test_data();
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

        catalog
            .register_schema("active", Arc::new(schema))
            .await
            .unwrap();
        ctx.register_catalog("cat", Arc::new(catalog))
            .await
            .unwrap();

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
}
