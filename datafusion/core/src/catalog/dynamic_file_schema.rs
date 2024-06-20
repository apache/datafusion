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

/// Wraps another schema provider
pub struct DynamicFileSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    state_store: StateStore,
}

impl DynamicFileSchemaProvider {
    pub fn new(inner: Arc<dyn SchemaProvider>) -> Self {
        Self {
            inner,
            state_store: StateStore::new(),
        }
    }

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

pub struct StateStore {
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