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

use std::any::Any;
use std::sync::{Arc, Weak};

use crate::object_storage::{get_object_store, AwsOptions, GcpOptions};

use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};

use datafusion::common::plan_datafusion_err;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;

use async_trait::async_trait;
use dirs::home_dir;
use parking_lot::RwLock;

use crate::object_storage::{get_object_store, AwsOptions, GcpOptions};

/// Wraps another catalog, automatically creating table providers
/// for local files if needed
pub struct DynamicFileCatalog {
    inner: Arc<dyn CatalogProviderList>,
    state: Weak<RwLock<SessionState>>,
}

impl DynamicFileCatalog {
    pub fn new(
        inner: Arc<dyn CatalogProviderList>,
        state: Weak<RwLock<SessionState>>,
    ) -> Self {
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

/// Wraps another schema provider. [DynamicFileSchemaProvider] is responsible for registering the required
/// object stores for the file locations.
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

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let inner_table = self.inner.table(name).await;
        if inner_table.is_ok() {
            if let Some(inner_table) = inner_table? {
                return Ok(Some(inner_table));
            }
        }

        // if the inner schema provider didn't have a table by
        // that name, try to treat it as a listing table
        let mut state = self
            .state
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("locking error"))?
            .read()
            .clone();
        let mut builder = SessionStateBuilder::from(state.clone());
        let optimized_name = substitute_tilde(name.to_owned());
        let table_url = ListingTableUrl::parse(optimized_name.as_str())?;
        let scheme = table_url.scheme();
        let url = table_url.as_ref();

        // If the store is already registered for this URL then `get_store`
        // will return `Ok` which means we don't need to register it again. However,
        // if `get_store` returns an `Err` then it means the corresponding store is
        // not registered yet and we need to register it
        match state.runtime_env().object_store_registry.get_store(url) {
            Ok(_) => { /*Nothing to do here, store for this URL is already registered*/ }
            Err(_) => {
                // Register the store for this URL. Here we don't have access
                // to any command options so the only choice is to use an empty collection
                match scheme {
                    "s3" | "oss" | "cos" => {
                        if let Some(table_options) = builder.table_options() {
                            table_options.extensions.insert(AwsOptions::default())
                        }
                    }
                    "gs" | "gcs" => {
                        if let Some(table_options) = builder.table_options() {
                            table_options.extensions.insert(GcpOptions::default())
                        }
                    }
                    _ => {}
                };
                state = builder.build();
                let store = get_object_store(
                    &state,
                    table_url.scheme(),
                    url,
                    &state.default_table_options(),
                )
                .await?;
                state.runtime_env().register_object_store(url, store);
            }
        }
        self.inner.table(name).await
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use datafusion::catalog::SchemaProvider;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn setup_context() -> (SessionContext, Arc<dyn SchemaProvider>) {
        let ctx = SessionContext::new();
        ctx.register_catalog_list(Arc::new(DynamicFileCatalog::new(
            ctx.state().catalog_list().clone(),
            ctx.state_weak_ref(),
        )));

        let provider = &DynamicFileCatalog::new(
            ctx.state().catalog_list().clone(),
            ctx.state_weak_ref(),
        ) as &dyn CatalogProviderList;
        let catalog = provider
            .catalog(provider.catalog_names().first().unwrap())
            .unwrap();
        let schema = catalog
            .schema(catalog.schema_names().first().unwrap())
            .unwrap();
        (ctx, schema)
    }

    #[tokio::test]
    async fn query_http_location_test() -> Result<()> {
        // This is a unit test so not expecting a connection or a file to be
        // available
        let domain = "example.com";
        let location = format!("http://{domain}/file.parquet");

        let (ctx, schema) = setup_context();

        // That's a non registered table so expecting None here
        let table = schema.table(&location).await?;
        assert!(table.is_none());

        // It should still create an object store for the location in the SessionState
        let store = ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        assert_eq!(format!("{store}"), "HttpStore");

        // The store must be configured for this domain
        let expected_domain = format!("Domain(\"{domain}\")");
        assert!(format!("{store:?}").contains(&expected_domain));

        Ok(())
    }

    #[tokio::test]
    async fn query_s3_location_test() -> Result<()> {
        let bucket = "examples3bucket";
        let location = format!("s3://{bucket}/file.parquet");

        let (ctx, schema) = setup_context();

        let table = schema.table(&location).await?;
        assert!(table.is_none());

        let store = ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;
        assert_eq!(format!("{store}"), format!("AmazonS3({bucket})"));

        // The store must be configured for this domain
        let expected_bucket = format!("bucket: \"{bucket}\"");
        assert!(format!("{store:?}").contains(&expected_bucket));

        Ok(())
    }

    #[tokio::test]
    async fn query_gs_location_test() -> Result<()> {
        let bucket = "examplegsbucket";
        let location = format!("gs://{bucket}/file.parquet");

        let (ctx, schema) = setup_context();

        let table = schema.table(&location).await?;
        assert!(table.is_none());

        let store = ctx
            .runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;
        assert_eq!(format!("{store}"), format!("GoogleCloudStorage({bucket})"));

        // The store must be configured for this domain
        let expected_bucket = format!("bucket_name_encoded: \"{bucket}\"");
        assert!(format!("{store:?}").contains(&expected_bucket));

        Ok(())
    }

    #[tokio::test]
    async fn query_invalid_location_test() {
        let location = "ts://file.parquet";
        let (_ctx, schema) = setup_context();

        assert!(schema.table(location).await.is_err());
    }
}
