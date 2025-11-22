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
//! See write.rs for write related helper methods

pub mod arrow;
pub mod csv;
pub mod json;

#[cfg(feature = "avro")]
pub mod avro;

#[cfg(feature = "parquet")]
pub mod parquet;

pub mod options;

pub use datafusion_datasource::file_compression_type;
pub use datafusion_datasource::file_format::*;
pub use datafusion_datasource::write;

#[cfg(test)]
pub(crate) mod test_util {
    use arrow_schema::SchemaRef;
    use datafusion_catalog::Session;
    use datafusion_common::Result;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::TableSchema;
    use datafusion_datasource::{file_format::FileFormat, PartitionedFile};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use std::sync::Arc;

    use crate::test::object_store::local_unpartitioned_file;

    pub async fn scan_format(
        state: &dyn Session,
        format: &dyn FileFormat,
        schema: Option<SchemaRef>,
        store_root: &str,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn datafusion_physical_plan::ExecutionPlan>> {
        let store = Arc::new(object_store::local::LocalFileSystem::new()) as _;
        let meta = local_unpartitioned_file(format!("{store_root}/{file_name}"));

        let file_schema = if let Some(file_schema) = schema {
            file_schema
        } else {
            format
                .infer_schema(state, &store, std::slice::from_ref(&meta))
                .await?
        };

        let table_schema = TableSchema::new(file_schema.clone(), vec![]);

        let statistics = format
            .infer_stats(state, &store, file_schema.clone(), &meta)
            .await?;

        let file_groups = vec![vec![PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }]
        .into()];

        let exec = format
            .create_physical_plan(
                state,
                FileScanConfigBuilder::new(
                    ObjectStoreUrl::local_filesystem(),
                    format.file_source(table_schema),
                )
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .with_projection_indices(projection)?
                .with_limit(limit)
                .build(),
            )
            .await?;
        Ok(exec)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn write_parquet_results_error_handling() -> datafusion_common::Result<()> {
        use std::sync::Arc;

        use object_store::local::LocalFileSystem;
        use tempfile::TempDir;
        use url::Url;

        use crate::{
            dataframe::DataFrameWriteOptions,
            prelude::{CsvReadOptions, SessionContext},
        };

        let ctx = SessionContext::new();
        // register a local file system object store for /tmp directory
        let tmp_dir = TempDir::new()?;
        let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
        let local_url = Url::parse("file://local").unwrap();
        ctx.register_object_store(&local_url, local);

        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;
        let out_dir_url = "file://local/out";
        let e = df
            .write_parquet(out_dir_url, DataFrameWriteOptions::new(), None)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!(e.strip_backtrace(), "Arrow error: Parser error: Error while parsing value 'd' as type 'Int64' for column 0 at line 4. Row data: '[d,4]'");
        Ok(())
    }
}
