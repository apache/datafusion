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

//! Interfaces and default implementations of catalogs and schemas.

pub mod information_schema;
pub mod listing_schema;
pub mod schema;

pub use datafusion_sql::{ResolvedTableReference, TableReference};

use crate::catalog::schema::SchemaProvider;
use dashmap::DashMap;
use datafusion_common::{exec_err, not_impl_err, DataFusionError, Result};
use std::any::Any;
use std::sync::Arc;

/// Represent a list of named catalogs
pub trait CatalogList: Sync + Send {
    /// Returns the catalog list as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Adds a new catalog to this catalog list
    /// If a catalog of the same name existed before, it is replaced in the list and returned.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}

/// Simple in-memory list of catalogs
pub struct MemoryCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl MemoryCatalogList {
    /// Instantiates a new `MemoryCatalogList` with an empty collection of catalogs
    pub fn new() -> Self {
        Self {
            catalogs: DashMap::new(),
        }
    }
}

impl Default for MemoryCatalogList {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogList for MemoryCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|c| c.value().clone())
    }
}

impl Default for MemoryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a catalog, comprising a number of named schemas.
pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`]
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // use variables to avoid unused variable warnings
        let _ = name;
        let _ = schema;
        not_impl_err!("Registering new schemas is not supported")
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, [`MemoryCatalogProvider`], a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    ///
    /// By default returns a "Not Implemented" error
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        not_impl_err!("Deregistering new schemas is not supported")
    }
}

/// Simple in-memory implementation of a catalog.
pub struct MemoryCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl MemoryCatalogProvider {
    /// Instantiates a new MemoryCatalogProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: DashMap::new(),
        }
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|s| s.value().clone())
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.into(), schema))
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if let Some(schema) = self.schema(name) {
            let table_names = schema.table_names();
            match (table_names.is_empty(), cascade) {
                (true, _) | (false, true) => {
                    let (_, removed) = self.schemas.remove(name).unwrap();
                    Ok(Some(removed))
                }
                (false, false) => exec_err!(
                    "Cannot drop schema {} because other tables depend on it: {}",
                    name,
                    itertools::join(table_names.iter(), ", ")
                ),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::MemorySchemaProvider;
    use crate::datasource::empty::EmptyTable;
    use crate::datasource::TableProvider;
    use arrow::datatypes::Schema;

    #[test]
    fn default_register_schema_not_supported() {
        // mimic a new CatalogProvider and ensure it does not support registering schemas
        struct TestProvider {}
        impl CatalogProvider for TestProvider {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn schema_names(&self) -> Vec<String> {
                unimplemented!()
            }

            fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
                unimplemented!()
            }
        }

        let schema = Arc::new(MemorySchemaProvider::new()) as _;
        let catalog = Arc::new(TestProvider {});

        match catalog.register_schema("foo", schema) {
            Ok(_) => panic!("unexpected OK"),
            Err(e) => assert_eq!(e.strip_backtrace(), "This feature is not implemented: Registering new schemas is not supported"),
        };
    }

    #[test]
    fn memory_catalog_dereg_nonempty_schema() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        let test_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
            as Arc<dyn TableProvider>;
        schema.register_table("t".into(), test_table).unwrap();

        cat.register_schema("foo", schema.clone()).unwrap();

        assert!(
            cat.deregister_schema("foo", false).is_err(),
            "dropping empty schema without cascade should error"
        );
        assert!(cat.deregister_schema("foo", true).unwrap().is_some());
    }

    #[test]
    fn memory_catalog_dereg_empty_schema() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        cat.register_schema("foo", schema.clone()).unwrap();

        assert!(cat.deregister_schema("foo", false).unwrap().is_some());
    }

    #[test]
    fn memory_catalog_dereg_missing() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;
        assert!(cat.deregister_schema("foo", false).unwrap().is_none());
    }
}
