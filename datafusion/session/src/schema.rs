// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Describes the interface and built-in implementations of schemas,
//! representing collections of named tables.

use async_trait::async_trait;
use datafusion_common::{DataFusionError, exec_err};
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
pub trait SchemaProvider: Any + Debug + Sync + Send {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_schema.schemata`.
    fn owner_name(&self) -> Option<&str> {
        None
    }

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

impl dyn SchemaProvider {
    /// Returns `true` if the schema provider is of type `T`.
    ///
    /// Prefer this over `downcast_ref::<T>().is_some()`. Works correctly when
    /// called on `Arc<dyn SchemaProvider>` via auto-deref.
    pub fn is<T: SchemaProvider>(&self) -> bool {
        (self as &dyn Any).is::<T>()
    }

    /// Attempts to downcast this schema provider to a concrete type `T`,
    /// returning `None` if the provider is not of that type.
    ///
    /// Works correctly when called on `Arc<dyn SchemaProvider>` via auto-deref,
    /// unlike `(&arc as &dyn Any).downcast_ref::<T>()` which would attempt to
    /// downcast the `Arc` itself.
    pub fn downcast_ref<T: SchemaProvider>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref()
    }
}
