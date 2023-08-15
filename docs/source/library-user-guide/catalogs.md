<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Catalogs, Schemas, and Tables

This section describes how to create and manage catalogs, schemas, and tables in DataFusion. For those wanting to dive into the code quickly please see the [example](../../../datafusion-examples/examples/catalog.rs).

## General Concepts

Catalogs, schemas, and tables are organized in a hierarchy. A catalog contains schemas, and a schema contains tables.

Similarly to other concepts in Datafusion, you'll implement various traits to create your own catalogs, schemas, and tables. The following sections describe the traits you'll need to implement.

The `CatalogProvider` trait has methods to set a schema to a name, get a schema by name, and list all schemas. The `SchemaProvider`, which can be registered with a `CatalogProvider`, has methods to set a table to a name, get a table by name, list all tables, deregister a table, and check for a table's existence. The `TableProvider` trait has methods to scan underlying data and use it in Datafusion. The `TableProvider` trait is covered in more detail [here](./custom-table-providers.md).

We'll start with a the `SchemaProvider` trait as we need one to register with the `CatalogProvider`.

## Implementing `MemorySchemaProvider`

The `MemorySchemaProvider` is a simple implementation of the `SchemaProvider` trait. It stores tables in a `DashMap`.

```rust
pub struct MemorySchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}
```

`tables` is the key-value pair described above. The underlying state could also be another data structure or other storage mechanism.

Then we implement the `SchemaProvider` trait for `MemorySchemaProvider`.

```rust
#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|table| table.value().clone())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {name} already exists"
            )));
        }
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
```

Without getting into a `CatalogProvider` implementation, we can create a `MemorySchemaProvider` and register `TableProvider`s with it.

```rust
let schema_provider = Arc::new(MemorySchemaProvider::new());
let table_provider = _; // create a table provider

schema_provider.register_table("table_name".to_string(), table_provider);

let table = schema_provider.table("table_name").unwrap();
```

### Asynchronous `SchemaProvider`

It's often useful to fetch metadata about which tables are in a schema, from a remote source. For example, a schema provider could fetch metadata from a remote database. To support this, the `SchemaProvider` trait has an asynchronous `table` method.

The trait is roughly the same except for the `table` method, and the addition of the `#[async_trait]` attribute.

```rust
#[async_trait]
impl SchemaProvider for Schema {
    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // fetch metadata from remote source
    }
}
```

### Implementing `MemoryCatalogProvider`

As mentioned, the `CatalogProvider` can manage the schemas in a catalog, and the `MemoryCatalogProvider` is a simple implementation of the `CatalogProvider` trait. It stores schemas in a `DashMap`.

```rust
pub struct MemoryCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}
```

With that the `CatalogProvider` trait can be implemented.

```rust
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
        /// `cascade` is not used here, but can be used to control whether
        /// to delete all tables in the schema or not.
        if let Some(schema) = self.schema(name) {
            let (_, removed) = self.schemas.remove(name).unwrap();
            Ok(Some(removed))
        } else {
            Ok(None)
        }
    }
}
```

Again, this is fairly straightforward, as there's an underlying data structure to store the state, via key-value pairs.

## Recap

To recap, you need to:

1. Implement the `TableProvider` trait to create a table provider, or use an existing one.
2. Implement the `SchemaProvider` trait to create a schema provider, or use an existing one.
3. Implement the `CatalogProvider` trait to create a catalog provider, or use an existing one.
