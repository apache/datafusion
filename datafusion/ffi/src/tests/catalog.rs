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

//! This is an example of an async table provider that will call functions on
//! the tokio runtime of the library providing the function. Since we cannot
//! share a tokio runtime across the ffi boundary and the producer and consumer
//! may have different runtimes, we need to store a reference to the runtime
//! and enter it during streaming calls. The entering of the runtime will
//! occur by the datafusion_ffi crate during the streaming calls. This code
//! serves as an integration test of this feature. If we do not correctly
//! access the runtime, then you will get a panic when trying to do operations
//! such as spawning a tokio task.

use std::{any::Any, fmt::Debug, sync::Arc};

use crate::catalog_provider::FFI_CatalogProvider;
use arrow::datatypes::Schema;
use async_trait::async_trait;
use datafusion::{
    catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
        TableProvider,
    },
    common::exec_err,
    datasource::MemTable,
    error::{DataFusionError, Result},
};

/// This schema provider is intended only for unit tests. It prepopulates with one
/// table and only allows for tables named sales and purchases.
#[derive(Debug)]
pub struct FixedSchemaProvider {
    inner: MemorySchemaProvider,
}

pub fn fruit_table() -> Arc<dyn TableProvider + 'static> {
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::record_batch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("units", DataType::Int32, true),
        Field::new("price", DataType::Float64, true),
    ]));

    let partitions = vec![
        record_batch!(
            ("units", Int32, vec![10, 20, 30]),
            ("price", Float64, vec![1.0, 2.0, 5.0])
        )
        .unwrap(),
        record_batch!(
            ("units", Int32, vec![5, 7]),
            ("price", Float64, vec![1.5, 2.5])
        )
        .unwrap(),
    ];

    Arc::new(MemTable::try_new(schema, vec![partitions]).unwrap())
}

impl Default for FixedSchemaProvider {
    fn default() -> Self {
        let inner = MemorySchemaProvider::new();

        let table = fruit_table();

        let _ = inner
            .register_table("purchases".to_string(), table)
            .unwrap();

        Self { inner }
    }
}

#[async_trait]
impl SchemaProvider for FixedSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        self.inner.table(name).await
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if name.as_str() != "sales" && name.as_str() != "purchases" {
            return exec_err!(
                "FixedSchemaProvider only provides two tables: sales and purchases"
            );
        }

        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.inner.deregister_table(name)
    }
}

/// This catalog provider is intended only for unit tests. It prepopulates with one
/// schema and only allows for schemas named after four types of fruit.
#[derive(Debug)]
pub struct FixedCatalogProvider {
    inner: MemoryCatalogProvider,
}

impl Default for FixedCatalogProvider {
    fn default() -> Self {
        let inner = MemoryCatalogProvider::new();

        let _ = inner.register_schema("apple", Arc::new(FixedSchemaProvider::default()));

        Self { inner }
    }
}

impl CatalogProvider for FixedCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.inner.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if !["apple", "banana", "cherry", "date"].contains(&name) {
            return exec_err!("FixedCatalogProvider only provides four schemas: apple, banana, cherry, date");
        }

        self.inner.register_schema(name, schema)
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.deregister_schema(name, cascade)
    }
}

pub(crate) extern "C" fn create_catalog_provider() -> FFI_CatalogProvider {
    let catalog_provider = Arc::new(FixedCatalogProvider::default());
    FFI_CatalogProvider::new(catalog_provider, None)
}
