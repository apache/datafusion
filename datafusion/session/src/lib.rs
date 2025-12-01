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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! Session management for DataFusion query execution environment
//!
//! This module provides the core session management functionality for DataFusion,
//! handling both Catalog (Table) and Datasource (File) configurations. It defines
//! the fundamental interfaces and implementations for maintaining query execution
//! state and configurations.
//!
//! Key components:
//! * [`Session`] - Manages query execution context, including configurations,
//!   catalogs, and runtime state
//! * [`SessionStore`] - Handles session persistence and retrieval
//!
//! The session system enables:
//! * Configuration management for query execution
//! * Catalog and schema management
//! * Function registry access
//! * Runtime environment configuration
//! * Query state persistence

pub mod session;

pub use crate::session::{Session, SessionStore};
