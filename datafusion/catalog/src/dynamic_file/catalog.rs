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

//! [`DynamicFileCatalog`] that creates tables from file paths

use crate::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use async_trait::async_trait;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Wrap another catalog provider list
#[derive(Debug)]
pub struct DynamicFileCatalog {
    /// The inner catalog provider list
    inner: Arc<dyn CatalogProviderList>,
    /// The factory that can create a table provider from the file path
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileCatalog {
    pub fn new(
        inner: Arc<dyn CatalogProviderList>,
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { inner, factory }
    }
}

impl CatalogProviderList for DynamicFileCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.register_catalog(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.inner.catalog(name).map(|catalog| {
            Arc::new(DynamicFileCatalogProvider::new(
                catalog,
                Arc::clone(&self.factory),
            )) as _
        })
    }
}

/// Wraps another catalog provider
#[derive(Debug)]
struct DynamicFileCatalogProvider {
    /// The inner catalog provider
    inner: Arc<dyn CatalogProvider>,
    /// The factory that can create a table provider from the file path
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileCatalogProvider {
    pub fn new(
        inner: Arc<dyn CatalogProvider>,
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { inner, factory }
    }
}

impl CatalogProvider for DynamicFileCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name).map(|schema| {
            Arc::new(DynamicFileSchemaProvider::new(
                schema,
                Arc::clone(&self.factory),
            )) as _
        })
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// Implements the [DynamicFileSchemaProvider] that can create tables provider from the file path.
///
/// The provider will try to create a table provider from the file path if the table provider
/// isn't exist in the inner schema provider.
#[derive(Debug)]
pub struct DynamicFileSchemaProvider {
    /// The inner schema provider
    inner: Arc<dyn SchemaProvider>,
    /// The factory that can create a table provider from the file path
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileSchemaProvider {
    /// Create a new [DynamicFileSchemaProvider] with the given inner schema provider.
    pub fn new(
        inner: Arc<dyn SchemaProvider>,
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { inner, factory }
    }
}

#[async_trait]
impl SchemaProvider for DynamicFileSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.inner.table(name).await? {
            return Ok(Some(table));
        };

        self.factory.try_new(name).await
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

/// [UrlTableFactory] is a factory that can create a table provider from the given url.
#[async_trait]
pub trait UrlTableFactory: Debug + Sync + Send {
    /// create a new table provider from the provided url
    async fn try_new(
        &self,
        url: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>>;
}
