// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Re-exports the [`datafusion_datasource_arrow::file_format`] module, and contains tests for it.
pub use datafusion_datasource_arrow::file_format::*;

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array, StringArray, StringViewArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;

    use crate::datasource::MemTable;
    use crate::execution::options::ArrowReadOptions;
    use crate::prelude::SessionContext;

    #[tokio::test]
    async fn test_write_empty_arrow_from_sql() -> Result<()> {
        let ctx = SessionContext::new();

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty_sql.arrow", tmp_dir.path().to_string_lossy());

        ctx.sql(&format!(
            "COPY (SELECT CAST(1 AS BIGINT) AS id LIMIT 0) TO '{path}' STORED AS ARROW",
        ))
        .await?
        .collect()
        .await?;

        assert!(std::path::Path::new(&path).exists());

        let read_df = ctx.read_arrow(&path, ArrowReadOptions::default()).await?;
        let stream = read_df.execute_stream().await?;

        assert_eq!(stream.schema().fields().len(), 1);
        assert_eq!(stream.schema().field(0).name(), "id");

        let results: Vec<_> = stream.collect().await;
        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_empty_arrow_from_record_batch() -> Result<()> {
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let empty_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )?;

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/empty_batch.arrow", tmp_dir.path().to_string_lossy());

        ctx.register_batch("empty_table", empty_batch)?;

        ctx.sql(&format!("COPY empty_table TO '{path}' STORED AS ARROW"))
            .await?
            .collect()
            .await?;

        assert!(std::path::Path::new(&path).exists());

        let read_df = ctx.read_arrow(&path, ArrowReadOptions::default()).await?;
        let stream = read_df.execute_stream().await?;

        assert_eq!(stream.schema().fields().len(), 2);
        assert_eq!(stream.schema().field(0).name(), "id");
        assert_eq!(stream.schema().field(1).name(), "name");

        let results: Vec<_> = stream.collect().await;
        let total_rows: usize = results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 0);

        Ok(())
    }

    /// Registers table `t`, whose batches are slices of one batch, so that the
    /// views of each batch reference the string data of all the batches.
    /// Returns the size of the string data.
    fn register_sliced_views(ctx: &SessionContext) -> Result<usize> {
        let num_rows = 40_000;
        let strings: StringViewArray = (0..num_rows)
            .map(|i| Some(format!("a string longer than twelve bytes {i:05}")))
            .collect();
        let data_size = strings.data_buffers().iter().map(|b| b.len()).sum();
        let partitions = Int32Array::from_iter_values((0..num_rows).map(|i| i % 4));
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8View, true),
            Field::new("p", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(strings), Arc::new(partitions)],
        )?;
        let batches = (0..8).map(|i| batch.slice(i * 5000, 5000)).collect();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![batches])?))?;
        Ok(data_size)
    }

    /// Returns the total size of the files under `path`
    fn total_file_size(path: &std::path::Path) -> std::io::Result<u64> {
        let mut size = 0;
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            size += if metadata.is_dir() {
                total_file_size(&entry.path())?
            } else {
                metadata.len()
            };
        }
        Ok(size)
    }

    #[tokio::test]
    async fn test_write_arrow_compacts_view_buffers() -> Result<()> {
        let ctx = SessionContext::new();
        let data_size = register_sliced_views(&ctx)?;

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/views.arrow", tmp_dir.path().to_string_lossy());
        // Uncompressed, so that the file size is the size of the written buffers
        ctx.sql(&format!(
            "COPY t TO '{path}' STORED AS ARROW OPTIONS ('format.compression' 'uncompressed')"
        ))
        .await?
        .collect()
        .await?;

        // Without compaction, each of the 8 batches holds all the string data
        let file_size = std::fs::metadata(&path)?.len() as usize;
        assert!(
            file_size < 2 * data_size,
            "wrote {file_size} bytes for {data_size} bytes of strings"
        );

        let rows = ctx
            .read_arrow(&path, ArrowReadOptions::default())
            .await?
            .count()
            .await?;
        assert_eq!(rows, 40_000);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_partitioned_arrow_compacts_view_buffers() -> Result<()> {
        let ctx = SessionContext::new();
        let data_size = register_sliced_views(&ctx)?;

        let tmp_dir = tempfile::TempDir::new()?;
        let path = format!("{}/partitioned/", tmp_dir.path().to_string_lossy());
        // Uncompressed, so that the file sizes are the sizes of the written buffers
        ctx.sql(&format!(
            "COPY t TO '{path}' STORED AS ARROW PARTITIONED BY (p) \
             OPTIONS ('format.compression' 'uncompressed')"
        ))
        .await?
        .collect()
        .await?;

        for p in 0..4 {
            assert!(std::path::Path::new(&format!("{path}p={p}")).is_dir());
        }
        // Without compaction, each batch written to a partition holds the
        // string data of all the input batches it was taken from
        let size = total_file_size(std::path::Path::new(&path))? as usize;
        assert!(
            size < 2 * data_size,
            "wrote {size} bytes for {data_size} bytes of strings"
        );

        let rows = ctx
            .read_arrow(&path, ArrowReadOptions::default())
            .await?
            .count()
            .await?;
        assert_eq!(rows, 40_000);

        Ok(())
    }
}
