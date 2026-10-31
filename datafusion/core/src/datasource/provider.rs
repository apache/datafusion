// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Data source traits

use std::sync::Arc;

use datafusion_catalog::Session;
use datafusion_expr::CreateExternalTable;
pub use datafusion_expr::{TableProviderFilterPushDown, TableType};
use futures::future::BoxFuture;

use crate::catalog::{TableProvider, TableProviderFactory};
use crate::datasource::listing_table_factory::ListingTableFactory;
use crate::datasource::stream::StreamTableFactory;
use crate::error::Result;

/// The default [`TableProviderFactory`]
///
/// If [`CreateExternalTable`] is unbounded calls [`StreamTableFactory::create`],
/// otherwise calls [`ListingTableFactory::create`]
#[derive(Debug, Default)]
pub struct DefaultTableFactory {
    stream: StreamTableFactory,
    listing: ListingTableFactory,
}

impl DefaultTableFactory {
    /// Creates a new [`DefaultTableFactory`]
    pub fn new() -> Self {
        Self::default()
    }
}
impl TableProviderFactory for DefaultTableFactory {
    fn create<'a>(
        &'a self,
        state: &'a dyn Session,
        cmd: &'a CreateExternalTable,
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        self.create_boxed(state, cmd)
    }
}

impl DefaultTableFactory {
    fn create_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        cmd: &'a CreateExternalTable,
    ) -> BoxFuture<'a, Result<Arc<dyn TableProvider>>> {
        Box::pin(self.create_inner(state, cmd))
    }

    async fn create_inner(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let mut unbounded = cmd.unbounded;
        for (k, v) in &cmd.options {
            if k.eq_ignore_ascii_case("unbounded") && v.eq_ignore_ascii_case("true") {
                unbounded = true
            }
        }

        match unbounded {
            true => self.stream.create(state, cmd).await,
            false => self.listing.create(state, cmd).await,
        }
    }
}
