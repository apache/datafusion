// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
    use datafusion_datasource::TableSchema;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::{PartitionedFile, file_format::FileFormat};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use std::sync::Arc;

    use crate::test::object_store::local_unpartitioned_file;

    /// Runs `COPY ... TO <file_name> <copy_options>` and checks that the
    /// `DataSinkExec` reports `rows_written`, `elapsed_compute` and a
    /// `bytes_written` matching the size of the file on disk.
    pub async fn assert_copy_sink_metrics(
        file_name: &str,
        copy_options: &str,
    ) -> Result<()> {
        use crate::prelude::SessionContext;
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        use datafusion_execution::TaskContext;
        use futures::TryStreamExt;

        let ctx = SessionContext::new();
        let tmp_dir = tempfile::TempDir::new()?;
        let output_path = tmp_dir.path().join(file_name);
        let output_path_str = output_path.to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Utf8, false),
        ]));
        let ids: Vec<i32> = (0..1000).collect();
        let vals: Vec<String> = ids.iter().map(|i| format!("value-{}", i % 10)).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )?;
        ctx.register_batch("source", batch)?;

        let plan = ctx
            .sql(&format!(
                "COPY source TO '{output_path_str}' {copy_options}"
            ))
            .await?
            .create_physical_plan()
            .await?;
        let task_ctx = Arc::new(TaskContext::from(&ctx.state()));
        let _: Vec<_> = plan.execute(0, task_ctx)?.try_collect().await?;

        let metrics = plan
            .metrics()
            .expect("DataSinkExec should return file sink metrics")
            .aggregate_by_name();
        let metric = |name: &str| {
            metrics
                .iter()
                .find(|m| m.value().name() == name)
                .unwrap_or_else(|| panic!("should have {name} metric"))
                .value()
                .as_usize()
        };

        assert_eq!(metric("rows_written"), 1000);
        let file_size = std::fs::metadata(&output_path)?.len() as usize;
        assert!(file_size > 0);
        assert_eq!(metric("bytes_written"), file_size);
        assert!(metric("elapsed_compute") > 0);

        Ok(())
    }

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

        let table_schema = TableSchema::from(&file_schema);

        let statistics = format
            .infer_stats(state, &store, file_schema.clone(), &meta)
            .await?;

        let file_groups = vec![vec![PartitionedFile::new_from_meta(meta)].into()];

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
        assert_eq!(
            e.strip_backtrace(),
            "Arrow error: Parser error: Error while parsing value 'd' as type 'Int64' for column 0 at line 4. Row data: '[d,4]'"
        );
        Ok(())
    }
}
