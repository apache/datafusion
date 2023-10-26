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

//! DataFusion data sources: [`TableProvider`] and [`ListingTable`]
//!
//! [`ListingTable`]: crate::datasource::listing::ListingTable

pub mod avro_to_arrow;
pub mod default_table_source;
pub mod empty;
pub mod file_format;
pub mod listing;
pub mod listing_table_factory;
pub mod memory;
pub mod physical_plan;
pub mod provider;
mod statistics;
pub mod streaming;
pub mod view;

// backwards compatibility
pub use datafusion_execution::object_store;

pub use self::default_table_source::{
    provider_as_source, source_as_provider, DefaultTableSource,
};
pub use self::memory::MemTable;
pub use self::provider::TableProvider;
pub use self::view::ViewTable;
pub use crate::logical_expr::TableType;
pub use statistics::get_statistics_with_limit;
