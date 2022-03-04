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

use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::datasource::listing::{ListingTable, ListingTableConfig};
use crate::datasource::object_store::{ObjectStore, ObjectStoreRegistry};
use crate::datasource::TableProvider;
use crate::error::{DataFusionError, Result};

/// Represents a schema, comprising a number of named tables.
pub trait SchemaProvider: Sync + Send {
    /// Returns the schema provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Retrieves a specific table from the schema by name, provided it exists.
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support registering tables".to_owned(),
        ))
    }

    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support deregistering tables".to_owned(),
        ))
    }

    /// If supported by the implementation, checks the table exist in the schema provider or not.
    /// If no matched table in the schema provider, return false.
    /// Otherwise, return true.
    fn table_exist(&self, name: &str) -> bool;
}

/// Simple in-memory implementation of a schema.
pub struct MemorySchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl MemorySchemaProvider {
    /// Instantiates a new MemorySchemaProvider with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemorySchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.read();
        tables.get(name).cloned()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {} already exists",
                name
            )));
        }
        let mut tables = self.tables.write();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}

/// `ObjectStore` implementation of `SchemaProvider` to enable registering a `ListingTable`
pub struct ObjectStoreSchemaProvider {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
    object_store_registry: Arc<Mutex<ObjectStoreRegistry>>,
}

impl ObjectStoreSchemaProvider {
    /// Instantiates a new `ObjectStoreSchemaProvider` with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            object_store_registry: Arc::new(Mutex::new(ObjectStoreRegistry::new())),
        }
    }

    /// Assign an `ObjectStore` which enables calling `register_listing_table`.
    pub fn register_object_store(
        &self,
        scheme: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore>> {
        self.object_store_registry
            .lock()
            .register_store(scheme.into(), object_store)
    }

    /// Retrieves a `ObjectStore` instance by scheme
    pub fn object_store<'a>(
        &self,
        uri: &'a str,
    ) -> Result<(Arc<dyn ObjectStore>, &'a str)> {
        self.object_store_registry
            .lock()
            .get_by_uri(uri)
            .map_err(DataFusionError::from)
    }

    /// If supported by the implementation, adds a new table to this schema by creating a
    /// `ListingTable` from the provided `uri` and a previously registered `ObjectStore`.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    pub async fn register_listing_table(
        &self,
        name: &str,
        uri: &str,
        config: Option<ListingTableConfig>,
    ) -> Result<()> {
        let config = match config {
            Some(cfg) => cfg,
            None => {
                let (object_store, _path) = self.object_store(uri)?;
                ListingTableConfig::new(object_store, uri).infer().await?
            }
        };

        let table = Arc::new(ListingTable::try_new(config)?);
        self.register_table(name.into(), table)?;
        Ok(())
    }
}

impl Default for ObjectStoreSchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaProvider for ObjectStoreSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.read();
        tables.get(name).cloned()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {} already exists",
                name
            )));
        }
        let mut tables = self.tables.write();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::path::Path;
    use std::sync::Arc;

    use arrow::datatypes::Schema;

    use crate::assert_batches_eq;
    use crate::catalog::catalog::MemoryCatalogProvider;
    use crate::catalog::schema::{
        MemorySchemaProvider, ObjectStoreSchemaProvider, SchemaProvider,
    };
    use crate::datasource::empty::EmptyTable;
    use crate::datasource::object_store::local::LocalFileSystem;
    use crate::execution::context::ExecutionContext;

    use futures::StreamExt;

    #[tokio::test]
    async fn test_mem_provider() {
        let provider = MemorySchemaProvider::new();
        let table_name = "test_table_exist";
        assert!(!provider.table_exist(table_name));
        assert!(provider.deregister_table(table_name).unwrap().is_none());
        let test_table = EmptyTable::new(Arc::new(Schema::empty()));
        // register table successfully
        assert!(provider
            .register_table(table_name.to_string(), Arc::new(test_table))
            .unwrap()
            .is_none());
        assert!(provider.table_exist(table_name));
        let other_table = EmptyTable::new(Arc::new(Schema::empty()));
        let result =
            provider.register_table(table_name.to_string(), Arc::new(other_table));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_register_listing_table() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");

        let schema = ObjectStoreSchemaProvider::new();
        let _store = schema.register_object_store("test", Arc::new(LocalFileSystem {}));

        schema
            .register_listing_table("alltypes_plain", &filename, None)
            .await
            .unwrap();

        let catalog = MemoryCatalogProvider::new();
        catalog.register_schema("active", Arc::new(schema));

        let mut ctx = ExecutionContext::new();

        ctx.register_catalog("cat", Arc::new(catalog));

        let df = ctx
            .sql("SELECT id, bool_col FROM cat.active.alltypes_plain")
            .await
            .unwrap();

        let actual = df.collect().await.unwrap();

        let expected = vec![
            "+----+----------+",
            "| id | bool_col |",
            "+----+----------+",
            "| 4  | true     |",
            "| 5  | false    |",
            "| 6  | true     |",
            "| 7  | false    |",
            "| 2  | true     |",
            "| 3  | false    |",
            "| 0  | true     |",
            "| 1  | false    |",
            "+----+----------+",
        ];
        assert_batches_eq!(expected, &actual);
    }

    #[tokio::test]
    async fn test_schema_register_listing_tables() {
        let testdata = crate::test_util::parquet_test_data();

        let schema = ObjectStoreSchemaProvider::new();
        let store = schema
            .register_object_store("file", Arc::new(LocalFileSystem {}))
            .unwrap();

        let mut files = store.list_file(&testdata).await.unwrap();
        while let Some(file) = files.next().await {
            let sized_file = file.unwrap().sized_file;
            let path = Path::new(&sized_file.path);
            let file = path.file_name().unwrap();
            if file == OsStr::new("alltypes_dictionary.parquet")
                || file == OsStr::new("alltypes_plain.parquet")
            {
                let name = path.file_stem().unwrap().to_str().unwrap();
                schema
                    .register_listing_table(name, &sized_file.path, None)
                    .await
                    .unwrap();
            }
        }

        let tables = vec![
            String::from("alltypes_dictionary"),
            String::from("alltypes_plain"),
        ];

        let mut schema_tables = schema.table_names();
        schema_tables.sort();
        assert_eq!(schema_tables, tables);
    }

    #[tokio::test]
    #[should_panic(expected = "already exists")]
    async fn test_schema_register_same_listing_table() {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, "alltypes_plain.parquet");

        let schema = ObjectStoreSchemaProvider::new();
        let _store = schema.register_object_store("test", Arc::new(LocalFileSystem {}));

        schema
            .register_listing_table("alltypes_plain", &filename, None)
            .await
            .unwrap();

        schema
            .register_listing_table("alltypes_plain", &filename, None)
            .await
            .unwrap();
    }
}
