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

//! Module containing helper methods for the various file formats

/// default max records to scan to infer the schema
pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1000;

pub mod avro;
pub mod csv;
pub mod json;
pub mod parquet;

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::FileScanConfig;
use crate::physical_plan::{ExecutionPlan, Statistics};

use async_trait::async_trait;
use object_store::{ObjectMeta, ObjectStore};

/// This trait abstracts all the file format specific implementations
/// from the `TableProvider`. This helps code re-utilization across
/// providers that support the the same file formats.
#[async_trait]
pub trait FileFormat: Send + Sync + fmt::Debug {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

#[cfg(test)]
pub(crate) mod test_util {
    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::test::object_store::local_unpartitioned_file;
    use object_store::local::LocalFileSystem;

    pub async fn scan_format(
        format: &dyn FileFormat,
        store_root: &str,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let store = Arc::new(LocalFileSystem::new()) as _;
        let meta = local_unpartitioned_file(format!("{}/{}", store_root, file_name));

        let file_schema = format.infer_schema(&store, &[meta.clone()]).await?;

        let statistics = format
            .infer_stats(&store, file_schema.clone(), &meta)
            .await?;

        let file_groups = vec![vec![PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            extensions: None,
        }]];

        let exec = format
            .create_physical_plan(
                FileScanConfig {
                    object_store_url: ObjectStoreUrl::local_filesystem(),
                    file_schema,
                    file_groups,
                    statistics,
                    projection,
                    limit,
                    table_partition_cols: vec![],
                },
                &[],
            )
            .await?;
        Ok(exec)
    }
}
