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

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_row::layout::RowType::{Compact, WordAligned};
use datafusion_row::reader::read_as_batch;
use datafusion_row::writer::write_batch_unchecked;
use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
use std::sync::Arc;

#[tokio::test]
async fn test_with_parquet() -> Result<()> {
    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    let exec = get_exec("alltypes_plain.parquet", &projection, None).await?;
    let schema = exec.schema().clone();

    let batches = collect(exec, task_ctx).await?;
    assert_eq!(1, batches.len());
    let batch = &batches[0];

    let mut vector = vec![0; 20480];
    let row_offsets =
        { write_batch_unchecked(&mut vector, 0, batch, 0, schema.clone(), Compact) };
    let output_batch = { read_as_batch(&vector, schema, &row_offsets, Compact)? };
    assert_eq!(*batch, output_batch);

    Ok(())
}

#[tokio::test]
async fn test_with_parquet_word_aligned() -> Result<()> {
    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7]);
    let exec = get_exec("alltypes_plain.parquet", &projection, None).await?;
    let schema = exec.schema().clone();

    let batches = collect(exec, task_ctx).await?;
    assert_eq!(1, batches.len());
    let batch = &batches[0];

    let mut vector = vec![0; 20480];
    let row_offsets =
        { write_batch_unchecked(&mut vector, 0, batch, 0, schema.clone(), WordAligned) };
    let output_batch = { read_as_batch(&vector, schema, &row_offsets, WordAligned)? };
    assert_eq!(*batch, output_batch);

    Ok(())
}

async fn get_exec(
    file_name: &str,
    projection: &Option<Vec<usize>>,
    limit: Option<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = format!("{}/{}", testdata, file_name);

    let path = Path::from_filesystem_path(filename).unwrap();

    let format = ParquetFormat::default();
    let object_store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
    let object_store_url = ObjectStoreUrl::local_filesystem();

    let meta = object_store.head(&path).await.unwrap();

    let file_schema = format
        .infer_schema(&object_store, &[meta.clone()])
        .await
        .expect("Schema inference");
    let statistics = format
        .infer_stats(&object_store, file_schema.clone(), &meta)
        .await
        .expect("Stats inference");
    let file_groups = vec![vec![meta.into()]];
    let exec = format
        .create_physical_plan(
            FileScanConfig {
                object_store_url,
                file_schema,
                file_groups,
                statistics,
                projection: projection.clone(),
                limit,
                table_partition_cols: vec![],
            },
            &[],
        )
        .await?;
    Ok(exec)
}
