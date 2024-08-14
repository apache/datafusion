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

//! dynamic_file_schema contains a SchemaProvider that creates tables from file paths

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "dirs")]
use dirs::home_dir;
use datafusion_catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion_common::plan_datafusion_err;

use crate::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use crate::datasource::TableProvider;
use crate::error::Result;
use crate::execution::session_state::StateStore;

pub struct DynamicFileCatalog {
    inner: Arc<dyn CatalogProviderList>,
    state_store: Arc<StateStore>,
}

impl DynamicFileCatalog {
    pub fn new(
        inner: Arc<dyn CatalogProviderList>,
        state_store: Arc<StateStore>,
    ) -> Self {
        Self { inner, state_store }
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
        Some(self.inner.catalog(name)
            .unwrap_or(Arc::new(DynamicFileCatalogProvider::new(Arc::clone(&self.state_store))) as _))
    }
}


/// Wraps another catalog provider
struct DynamicFileCatalogProvider {
    state_store: Arc<StateStore>,
}

impl DynamicFileCatalogProvider {
    pub fn new(state_store: Arc<StateStore>) -> Self {
        Self {
            state_store,
        }
    }
}

impl CatalogProvider for DynamicFileCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec![]
    }

    fn schema(&self, _: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(DynamicFileSchemaProvider::new(
            Arc::new(DynamicListTableFactory::new(Arc::clone(&self.state_store))),
        )))
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!("register_schema is not supported for DynamicFileCatalogProvider")
    }
}

/// Implements the [DynamicFileSchemaProvider] that can create tables provider from the file path.
///
/// The provider will try to create a table provider from the file path if the table provider
/// isn't exist in the inner schema provider. The required object store must be registered in the session context.
pub struct DynamicFileSchemaProvider {
    factory: Arc<dyn UrlTableFactory>,
}

impl DynamicFileSchemaProvider {
    /// Create a new [DynamicFileSchemaProvider] with the given inner schema provider.
    pub fn new(
        factory: Arc<dyn UrlTableFactory>,
    ) -> Self {
        Self { factory }
    }
}

#[async_trait]
impl SchemaProvider for DynamicFileSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        unimplemented!("table_names is not supported for DynamicFileSchemaProvider")
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("register_table is not supported for DynamicFileSchemaProvider")
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let optimized_url = substitute_tilde(name.to_owned());
        self.factory.try_new(optimized_url.as_str()).await
    }

    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("deregister_table is not supported for DynamicFileSchemaProvider")
    }

    fn table_exist(&self, _name: &str) -> bool {
        unimplemented!("table_exist is not supported for DynamicFileSchemaProvider")
    }
}

/// Substitute the tilde character in the file path with the user home directory.
#[cfg(feature = "dirs")]
pub fn substitute_tilde(cur: String) -> String {
    if let Some(usr_dir_path) = home_dir() {
        if let Some(usr_dir) = usr_dir_path.to_str() {
            if cur.starts_with('~') && !usr_dir.is_empty() {
                return cur.replacen('~', usr_dir, 1);
            }
        }
    }
    cur
}

/// Do nothing if the feature "dirs" is disabled.
#[cfg(not(feature = "dirs"))]
pub fn substitute_tilde(cur: String) -> String {
    cur
}

/// [UrlTableFactory] is a factory that can create a table provider from the given url.
#[async_trait]
pub trait UrlTableFactory: Sync + Send {
    /// create a new table provider from the provided url
    async fn try_new(&self, url: &str) -> Result<Option<Arc<dyn TableProvider>>>;
}

/// [DynamicListTableFactory] is a factory that can create a [ListingTable] from the given url.
#[derive(Default)]
pub struct DynamicListTableFactory {
    state_store: Arc<StateStore>,
}

impl DynamicListTableFactory {
    /// Create a new [DynamicListTableFactory] with the given state store.
    pub fn new(state_store: Arc<StateStore>) -> Self {
        Self { state_store }
    }
}

#[async_trait]
impl UrlTableFactory for DynamicListTableFactory {
    async fn try_new(&self, url: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Ok(table_url) = ListingTableUrl::parse(url) else {
            return Ok(None);
        };

        let state = &self
            .state_store
            .get_state()
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("locking error"))?
            .read()
            .clone();
        if let Ok(cfg) = ListingTableConfig::new(table_url.clone())
            .infer(state)
            .await
        {
            ListingTable::try_new(cfg)
                .map(|table| Some(Arc::new(table) as Arc<dyn TableProvider>))
        } else {
            Ok(None)
        }
    }
}

#[cfg(all(not(target_os = "windows"), not(feature = "dirs")))]
#[cfg(test)]
mod tests {
    use crate::catalog::dynamic_file_schema::substitute_tilde;
    use dirs::home_dir;

    #[test]
    fn test_substitute_tilde() {
        use std::env;
        use std::path::MAIN_SEPARATOR;
        let original_home = home_dir();
        let test_home_path = if cfg!(windows) {
            "C:\\Users\\user"
        } else {
            "/home/user"
        };
        env::set_var(
            if cfg!(windows) { "USERPROFILE" } else { "HOME" },
            test_home_path,
        );
        let input = "~/Code/datafusion/benchmarks/data/tpch_sf1/part/part-0.parquet";
        let expected = format!(
            "{}{}Code{}datafusion{}benchmarks{}data{}tpch_sf1{}part{}part-0.parquet",
            test_home_path,
            MAIN_SEPARATOR,
            MAIN_SEPARATOR,
            MAIN_SEPARATOR,
            MAIN_SEPARATOR,
            MAIN_SEPARATOR,
            MAIN_SEPARATOR,
            MAIN_SEPARATOR
        );
        let actual = substitute_tilde(input.to_string());
        assert_eq!(actual, expected);
        match original_home {
            Some(home_path) => env::set_var(
                if cfg!(windows) { "USERPROFILE" } else { "HOME" },
                home_path.to_str().unwrap(),
            ),
            None => env::remove_var(if cfg!(windows) { "USERPROFILE" } else { "HOME" }),
        }
    }
}
