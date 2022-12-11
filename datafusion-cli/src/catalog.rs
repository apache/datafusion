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

use datafusion::catalog::catalog::{CatalogList, CatalogProvider};
use datafusion::catalog::schema::{SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use std::any::Any;
use std::sync::Arc;

/// Wraps another catalog, automatically creating table providers
/// for local files if needed
pub struct DynamicFileCatalog {
    inner: Arc<dyn CatalogList>,
}

impl DynamicFileCatalog {
    pub fn new(inner: Arc<dyn CatalogList>) -> Self {
        Self {inner}
    }
}

impl CatalogList for DynamicFileCatalog {
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
        println!("Providing catalog for name {}", name);
        self.inner.catalog(name)
            .map(|catalog| Arc::new(DynamicFileCatalogProvider::new(catalog)) as _)

        // TODO: fallback to sorting out filenames
    }
}

/// Wraps another catalog provider
struct DynamicFileCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
}

impl DynamicFileCatalogProvider {
    pub fn new(inner: Arc<dyn CatalogProvider>) -> Self {
        Self { inner }
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
        println!("Providing schema for {name}");
        self.inner.schema(name)
            .map(|schema| Arc::new(DynamicFileSchemaProvider::new(schema)) as _)
        // TODO fallback to sorting out other filenames (with periods)
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
}

impl DynamicFileSchemaProvider {
    pub fn new(inner: Arc<dyn SchemaProvider>) -> Self {
        Self { inner }
    }
}


impl SchemaProvider for DynamicFileSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        println!("Getting provider for {name}");


        self.inner.table(name)
            .or_else(|| {
                todo!();
            })

            //let factory ListingTableFactory

            // todo wrap here

    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}
