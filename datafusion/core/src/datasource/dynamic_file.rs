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

//! dynamic_file_schema contains an [`UrlTableFactory`] implementation that
//! can create a [`ListingTable`] from the given url.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::{SessionStore, UrlTableFactory};
use datafusion_common::plan_datafusion_err;

use crate::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use crate::datasource::TableProvider;
use crate::error::Result;
use crate::execution::context::SessionState;

/// [DynamicListTableFactory] is a factory that can create a [ListingTable] from the given url.
#[derive(Default, Debug)]
pub struct DynamicListTableFactory {
    /// The session store that contains the current session.
    session_store: SessionStore,
}

impl DynamicListTableFactory {
    /// Create a new [DynamicListTableFactory] with the given state store.
    pub fn new(session_store: SessionStore) -> Self {
        Self { session_store }
    }

    /// Get the session store.
    pub fn session_store(&self) -> &SessionStore {
        &self.session_store
    }
}

#[async_trait]
impl UrlTableFactory for DynamicListTableFactory {
    async fn try_new(&self, url: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let Ok(table_url) = ListingTableUrl::parse(url) else {
            return Ok(None);
        };

        let state = &self
            .session_store()
            .get_session()
            .upgrade()
            .and_then(|session| {
                session
                    .read()
                    .as_any()
                    .downcast_ref::<SessionState>()
                    .cloned()
            })
            .ok_or_else(|| plan_datafusion_err!("get current SessionStore error"))?;

        match ListingTableConfig::new(table_url.clone())
            .infer_options(state)
            .await
        {
            Ok(cfg) => {
                let cfg = cfg
                    .infer_partitions_from_path(state)
                    .await?
                    .infer_schema(state)
                    .await?;
                ListingTable::try_new(cfg)
                    .map(|table| Some(Arc::new(table) as Arc<dyn TableProvider>))
            }
            Err(_) => Ok(None),
        }
    }
}
