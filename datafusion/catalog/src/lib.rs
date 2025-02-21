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
#![cfg_attr(docsrs, feature(doc_auto_cfg))]

//! Interfaces and default implementations of catalogs and schemas.
//!
//! Implementations
//! * Information schema: [`information_schema`]
//! * Simple memory based catalog: [`MemoryCatalogProviderList`], [`MemoryCatalogProvider`], [`MemorySchemaProvider`]

pub mod memory;
#[deprecated(
    since = "46.0.0",
    note = "use datafusion_sql::resolve::resolve_table_references"
)]
pub use datafusion_sql::resolve::resolve_table_references;
#[deprecated(
    since = "46.0.0",
    note = "use datafusion_common::{ResolvedTableReference, TableReference}"
)]
pub use datafusion_sql::{ResolvedTableReference, TableReference};
pub use memory::{
    MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider,
};
mod r#async;
mod catalog;
mod dynamic_file;
pub mod information_schema;
mod schema;
mod session;
mod table;
pub use catalog::*;
pub use dynamic_file::catalog::*;
pub use r#async::*;
pub use schema::*;
pub use session::*;
pub use table::*;
pub mod streaming;
