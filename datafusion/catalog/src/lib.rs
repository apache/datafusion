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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
#![deny(clippy::needless_pass_by_value)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![deny(clippy::allow_attributes)]

//! Interfaces and default implementations of catalogs and schemas.
//!
//! Implementations
//! * Information schema: [`information_schema`]
//! * Simple memory based catalog: [`MemoryCatalogProviderList`], [`MemoryCatalogProvider`], [`MemorySchemaProvider`]
//! * Listing schema: [`listing_schema`]

pub mod cte_worktable;
pub mod default_table_source;
pub mod information_schema;
pub mod listing_schema;
pub mod memory;
pub mod stream;
pub mod streaming;
pub mod view;

mod r#async;
mod catalog;
mod dynamic_file;
mod schema;
mod table;

pub use catalog::*;
pub use datafusion_session::Session;
pub use dynamic_file::catalog::*;
pub use memory::{
    MemTable, MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider,
};
pub use r#async::*;
pub use schema::*;
pub use table::*;

// For backwards compatibility,
mod session {
    pub use datafusion_session::Session;
}
