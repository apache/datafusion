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

//! Re-exports the [`datafusion_datasource_arrow::file_format`] module, and contains tests for it.
pub use datafusion_datasource_arrow::file_format::*;

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;

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
}
