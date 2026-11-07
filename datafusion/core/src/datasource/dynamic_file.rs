// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! dynamic_file_schema contains an [`UrlTableFactory`] implementation that
//! can create a [`ListingTable`] from the given url.

use std::sync::Arc;

use crate::datasource::TableProvider;
use crate::datasource::listing::ListingTableConfigExt;
use crate::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use crate::error::Result;
use crate::execution::context::SessionState;

use datafusion_catalog::UrlTableFactory;
use datafusion_common::plan_datafusion_err;
use datafusion_session::SessionStore;

use async_trait::async_trait;
use futures::future::BoxFuture;

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
    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn try_new<'life0, 'life1, 'async_trait>(
        &'life0 self,
        url: &'life1 str,
    ) -> BoxFuture<'async_trait, Result<Option<Arc<dyn TableProvider>>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.try_new_boxed(url)
    }
}

impl DynamicListTableFactory {
    fn try_new_boxed<'a>(
        &'a self,
        url: &'a str,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn TableProvider>>>> {
        Box::pin(self.try_new_inner(url))
    }

    async fn try_new_inner(&self, url: &str) -> Result<Option<Arc<dyn TableProvider>>> {
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
