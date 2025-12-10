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

//! Describes the interface and built-in implementations of schemas,
//! representing collections of named tables.

use async_trait::async_trait;
use datafusion_common::{exec_err, DataFusionError};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::table::TableProvider;
use datafusion_common::Result;
use datafusion_expr::TableType;

/// Represents a schema, comprising a number of named tables.
///
/// Please see [`CatalogProvider`] for details of implementing a custom catalog.
///
/// [`CatalogProvider`]: super::CatalogProvider
#[async_trait]
pub trait SchemaProvider: Debug + Sync + Send {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError>;

    /// Retrieves the type of a specific table from the schema by name, if it exists, otherwise
    /// returns `None`.  Implementations for which this operation is cheap but [Self::table] is
    /// expensive can override this to improve operations that only need the type, e.g.
    /// `SELECT * FROM information_schema.tables`.
    async fn table_type(&self, name: &str) -> Result<Option<TableType>> {
        self.table(name).await.map(|o| o.map(|t| t.table_type()))
    }

    /// If supported by the implementation, adds a new table named `name` to
    /// this schema.
    ///
    /// If a table of the same name was already registered, returns "Table
    /// already exists" error.
    #[expect(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[expect(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool;
}
