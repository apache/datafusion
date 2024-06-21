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
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use dirs::home_dir;
use parking_lot::{Mutex, RwLock};

use datafusion_common::plan_datafusion_err;

use crate::catalog::schema::SchemaProvider;
use crate::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use crate::datasource::TableProvider;
use crate::error::Result;
use crate::execution::context::SessionState;

/// Implements the [DynamicFileSchemaProvider] that can create tables provider from the file path.
///
/// The provider will try to create a table provider from the file path if the table provider
/// isn't exist in the inner schema provider. The required object store must be registered in the session context.
pub struct DynamicFileSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    state_store: StateStore,
}

impl DynamicFileSchemaProvider {
    /// Create a new [DynamicFileSchemaProvider] with the given inner schema provider.
    pub fn new(inner: Arc<dyn SchemaProvider>) -> Self {
        Self {
            inner,
            state_store: StateStore::new(),
        }
    }

    /// register the state store to the schema provider.
    pub fn with_state(&self, state: Weak<RwLock<SessionState>>) {
        self.state_store.with_state(state);
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
        let inner_table = self.inner.table(name).await?;
        if inner_table.is_some() {
            return Ok(inner_table);
        }
        let optimized_url = substitute_tilde(name.to_owned());
        let table_url = ListingTableUrl::parse(optimized_url.as_str())?;
        let state = &self
            .state_store
            .get_state()
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("locking error"))?
            .read()
            .clone();
        let cfg = ListingTableConfig::new(table_url.clone())
            .infer(state)
            .await?;

        Ok(Some(Arc::new(ListingTable::try_new(cfg)?)))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
}
fn substitute_tilde(cur: String) -> String {
    if let Some(usr_dir_path) = home_dir() {
        if let Some(usr_dir) = usr_dir_path.to_str() {
            if cur.starts_with('~') && !usr_dir.is_empty() {
                return cur.replacen('~', usr_dir, 1);
            }
        }
    }
    cur
}

/// The state store that stores the reference of the runtime session state.
pub(crate) struct StateStore {
    state: Arc<Mutex<Option<Weak<RwLock<SessionState>>>>>,
}

impl StateStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_state(&self, state: Weak<RwLock<SessionState>>) {
        let mut lock = self.state.lock();
        *lock = Some(state);
    }

    pub fn get_state(&self) -> Weak<RwLock<SessionState>> {
        self.state.lock().clone().unwrap()
    }
}

impl Default for StateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::dynamic_file_schema::substitute_tilde;
    use dirs::home_dir;

    #[cfg(not(target_os = "windows"))]
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
