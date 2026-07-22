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

// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! Session APIs for the DataFusion query execution environment
//!
//! This crate defines shared interfaces for session-related APIs and extension
//! points. Concrete query-engine implementations are provided by higher-level
//! DataFusion crates.
//!
//! Key components:
//! * [`Session`] - Describes a query execution context, including configurations,
//!   catalogs, and runtime state
//! * [`CatalogProviderList`], [`CatalogProvider`], and [`SchemaProvider`] -
//!   Describe catalog hierarchies
//! * [`TableProvider`] - Provides data for query planning and execution
//! * [`SessionStore`] - Handles session persistence and retrieval
//!
//! The session system enables:
//! * Configuration management for query execution
//! * Catalog and schema management
//! * Function registry access
//! * Runtime environment configuration
//! * Query state persistence

pub mod catalog;
pub mod physical_optimizer;
pub mod planner;
pub mod schema;
pub mod session;
pub mod table;

pub use crate::catalog::{
    CatalogProvider, CatalogProviderList, EmptyCatalogProviderList,
};
pub use crate::physical_optimizer::{PhysicalOptimizerContext, PhysicalOptimizerRule};
pub use crate::planner::{ExtensionPlanner, PhysicalPlanner, QueryPlanner};
pub use crate::schema::SchemaProvider;
pub use crate::session::{Session, SessionStore};
pub use crate::table::{
    ScanArgs, ScanResult, TableFunction, TableFunctionArgs, TableFunctionImpl,
    TableProvider, TableProviderFactory,
};
