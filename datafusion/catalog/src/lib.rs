// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! Default implementations of catalogs and schemas.
//!
//! The catalog interfaces are defined in [`datafusion_session`] and re-exported
//! by this crate.
//!
//! Implementations
//! * Information schema: [`information_schema`]
//! * Simple memory based catalog: [`MemoryCatalogProviderList`], [`MemoryCatalogProvider`], [`MemorySchemaProvider`]
//! * Listing schema: [`listing_schema`]

pub mod cte_worktable;
pub mod default_table_source;
pub mod empty;
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

pub use r#async::*;
pub use catalog::*;
pub use datafusion_session::Session;
pub use dynamic_file::catalog::*;
pub use memory::{
    MemTable, MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider,
};
pub use schema::*;
pub use table::*;
