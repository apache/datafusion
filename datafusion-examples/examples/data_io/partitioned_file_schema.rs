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

//! See `main.rs` for how to run it.

use arrow::array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::reader::Length;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;

/// Demonstrates how to attach a per-file Arrow schema to a [`PartitionedFile`]
/// via [`PartitionedFile::with_arrow_schema`].
///
/// By default DataFusion infers a file's physical schema by reading its
/// metadata (e.g. the Parquet footer) when the scan begins. When the schema is
/// already known, it can be supplied up front so this inference step is
/// skipped, saving an I/O round trip and metadata parse per file.
///
/// The example writes a small Parquet file with a single `Int32` column `a` and
/// reads it back three ways:
/// - without a schema, letting DataFusion infer it at query time;
/// - with the correct schema, skipping inference;
/// - with a deliberately mismatched schema (`a` typed as `Int64`), which
///   surfaces as an error since the provided schema does not match the data
///   actually stored in the file.
///
/// Note that the schema passed to [`PartitionedFile::with_arrow_schema`] must
/// describe only the columns physically stored in the file and must not include
/// partition columns.
pub async fn read_partitioned_file() -> Result<()> {
    let tmpdir = TempDir::new()?;
    let file_path = tmpdir.path().join("partitioned-file");
    let file = File::create(file_path.as_path())?;

    let file_schema =
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        file_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )?;
    let mut writer = ArrowWriter::try_new(&file, file_schema.clone(), None)?;
    writer.write(&batch)?;
    writer.finish()?;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        // Specify another field in the table which is missing from the file schema.
        // Illustrates that the table schema does not need to match the PartitionedFile schema
        // for a scan to succeed.
        Field::new("b", DataType::Float64, true),
    ]));

    // Infer file schema at query time.
    {
        let batch =
            read_file(file_path.as_path(), file.len(), table_schema.clone(), None)
                .await?;
        println!("{batch:?}");
    }

    // Provide the correct file schema to skip inferring at query time.
    {
        let batch = read_file(
            file_path.as_path(),
            file.len(),
            table_schema.clone(),
            Some(file_schema.clone()),
        )
        .await?;
        println!("{batch:?}");
    }

    // A mismatching file schema returns an error.
    {
        let mismatching_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let error = read_file(
            file_path.as_path(),
            file.len(),
            table_schema.clone(),
            Some(mismatching_schema),
        )
        .await
        .unwrap_err();
        println!("Got schema error: {error:?}");
    }

    Ok(())
}

/// Scans a single Parquet file with the given `source_schema`, optionally
/// supplying the file's Arrow schema to skip schema inference. A `None`
/// `file_schema` lets DataFusion infer the schema from the file metadata at
/// query time.
async fn read_file(
    file_path: &Path,
    file_len: u64,
    source_schema: SchemaRef,
    file_schema: Option<SchemaRef>,
) -> Result<RecordBatch> {
    let mut partitioned_file =
        PartitionedFile::new(file_path.to_string_lossy(), file_len);
    if let Some(schema) = file_schema {
        partitioned_file = partitioned_file.with_arrow_schema(schema);
    }

    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetSource::new(source_schema)),
    )
    .with_file(partitioned_file)
    .build();

    let exec = DataSourceExec::from_data_source(config);
    let mut result = exec.execute(0, Arc::new(TaskContext::default()))?;
    result.next().await.ok_or_else(|| {
        datafusion::error::DataFusionError::Internal(
            "execution produced no batches".into(),
        )
    })?
}
