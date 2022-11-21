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

//! listing_schema contains a SchemaProvider that scans ObjectStores for tables automatically
use crate::catalog::schema::SchemaProvider;
use crate::datasource::datasource::TableProviderFactory;
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;
use datafusion_common::{DFSchema, DataFusionError};
use datafusion_expr::CreateExternalTable;
use futures::TryStreamExt;
use itertools::Itertools;
use object_store::ObjectStore;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A `SchemaProvider` that scans an `ObjectStore` to automatically discover tables
///
/// A subfolder relationship is assumed, i.e. given:
/// authority = s3://host.example.com:3000
/// path = /data/tpch
/// factory = `DeltaTableFactory`
///
/// A table called "customer" will be registered for the folder:
/// s3://host.example.com:3000/data/tpch/customer
///
/// assuming it contains valid deltalake data, i.e:
/// s3://host.example.com:3000/data/tpch/customer/part-00000-xxxx.snappy.parquet
/// s3://host.example.com:3000/data/tpch/customer/_delta_log/
pub struct ListingSchemaProvider {
    authority: String,
    path: object_store::path::Path,
    factory: Arc<dyn TableProviderFactory>,
    store: Arc<dyn ObjectStore>,
    tables: Arc<Mutex<HashMap<String, Arc<dyn TableProvider>>>>,
    format: String,
    has_header: bool,
}

impl ListingSchemaProvider {
    /// Create a new `ListingSchemaProvider`
    ///
    /// Arguments:
    /// `authority`: The scheme (i.e. s3://) + host (i.e. example.com:3000)
    /// `path`: The root path that contains subfolders which represent tables
    /// `factory`: The `TableProviderFactory` to use to instantiate tables for each subfolder
    /// `store`: The `ObjectStore` containing the table data
    /// `format`: The `FileFormat` of the tables
    /// `has_header`: Indicates whether the created external table has the has_header flag enabled
    pub fn new(
        authority: String,
        path: object_store::path::Path,
        factory: Arc<dyn TableProviderFactory>,
        store: Arc<dyn ObjectStore>,
        format: String,
        has_header: bool,
    ) -> Self {
        Self {
            authority,
            path,
            factory,
            store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            format,
            has_header,
        }
    }

    /// Reload table information from ObjectStore
    pub async fn refresh(&self, state: &SessionState) -> datafusion_common::Result<()> {
        let entries: Vec<_> = self
            .store
            .list(Some(&self.path))
            .await?
            .try_collect()
            .await?;
        let base = Path::new(self.path.as_ref());
        let mut tables = HashSet::new();
        for file in entries.iter() {
            let mut parent = Path::new(file.location.as_ref());
            while let Some(p) = parent.parent() {
                if p == base {
                    tables.insert(parent);
                }
                parent = p;
            }
        }
        for table in tables.iter() {
            let file_name = table
                .file_name()
                .ok_or_else(|| {
                    DataFusionError::Internal("Cannot parse file name!".to_string())
                })?
                .to_str()
                .ok_or_else(|| {
                    DataFusionError::Internal("Cannot parse file name!".to_string())
                })?;
            let table_name = file_name.split('.').collect_vec()[0];
            let table_path = table.to_str().ok_or_else(|| {
                DataFusionError::Internal("Cannot parse file name!".to_string())
            })?;
            if !self.table_exist(table_name) {
                let table_url = format!("{}/{}", self.authority, table_path);

                let provider = self
                    .factory
                    .create(
                        state,
                        &CreateExternalTable {
                            schema: Arc::new(DFSchema::empty()),
                            name: table_name.to_string(),
                            location: table_url,
                            file_type: self.format.clone(),
                            has_header: self.has_header,
                            delimiter: ',',
                            table_partition_cols: vec![],
                            if_not_exists: false,
                            definition: None,
                            file_compression_type: "".to_string(),
                            options: Default::default(),
                        },
                    )
                    .await?;
                let _ = self.register_table(table_name.to_string(), provider.clone())?;
            }
        }
        Ok(())
    }
}

impl SchemaProvider for ListingSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .keys()
            .map(|it| it.to_string())
            .collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .get(name)
            .cloned()
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .insert(name, table.clone());
        Ok(Some(table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.lock().expect("Can't lock tables").remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .contains_key(name)
    }
}
