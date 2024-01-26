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
use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::{CatalogProviderList, CatalogProvider};
use datafusion::datasource::listing::{
    ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use parking_lot::RwLock;
use std::any::Any;
use std::sync::{Arc, Weak};

/// Wraps another catalog, automatically creating table providers
/// for local files if needed
pub struct DynamicFileCatalog {
    inner: Arc<dyn CatalogProviderList>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicFileCatalog {
    pub fn new(inner: Arc<dyn CatalogProviderList>, state: Weak<RwLock<SessionState>>) -> Self {
        Self { inner, state }
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
        let state = self.state.clone();
        self.inner
            .catalog(name)
            .map(|catalog| Arc::new(DynamicFileCatalogProvider::new(catalog, state)) as _)
    }
}

/// Wraps another catalog provider
struct DynamicFileCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicFileCatalogProvider {
    pub fn new(
        inner: Arc<dyn CatalogProvider>,
        state: Weak<RwLock<SessionState>>,
    ) -> Self {
        Self { inner, state }
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
        let state = self.state.clone();
        self.inner
            .schema(name)
            .map(|schema| Arc::new(DynamicFileSchemaProvider::new(schema, state)) as _)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// Wraps another schema provider
struct DynamicFileSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicFileSchemaProvider {
    pub fn new(
        inner: Arc<dyn SchemaProvider>,
        state: Weak<RwLock<SessionState>>,
    ) -> Self {
        Self { inner, state }
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

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let inner_table = self.inner.table(name).await;
        if inner_table.is_some() {
            return inner_table;
        }

        // if the inner schema provider didn't have a table by
        // that name, try to treat it as a listing table
        let state = self.state.upgrade()?.read().clone();
        let config = ListingTableConfig::new(ListingTableUrl::parse(name).ok()?)
            .infer(&state)
            .await
            .ok()?;
        Some(Arc::new(ListingTable::try_new(config).ok()?))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}
