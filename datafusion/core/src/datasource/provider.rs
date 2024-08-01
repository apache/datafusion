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

//! Data source traits

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_catalog::Session;
use datafusion_expr::CreateExternalTable;
pub use datafusion_expr::{TableProviderFilterPushDown, TableType};

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

#[async_trait]
impl TableProviderFactory for DefaultTableFactory {
    async fn create(
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
