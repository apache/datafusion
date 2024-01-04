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

use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Float32Array, Int64Array, StringArray};
use arrow_schema::DataType;
use datafusion::assert_batches_sorted_eq;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion_common::{Result, Statistics};
use datafusion_execution::object_store::ObjectStoreUrl;

use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tempfile::NamedTempFile;

/// Test for reading data from multiple parquet files with different schemas and coercing them into a single schema.
#[tokio::test]
async fn multi_parquet_coercion() {
    let d1: DictionaryArray<Int32Type> =
        vec![Some("one"), None, Some("three")].into_iter().collect();
    let c1: ArrayRef = Arc::new(d1);
    let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
    let c3: ArrayRef = Arc::new(Float32Array::from(vec![Some(10.0), Some(20.0), None]));

    // batch1: c1(dict), c2(int64)
    let batch1 =
        RecordBatch::try_from_iter(vec![("c1", c1), ("c2", c2.clone())]).unwrap();
    // batch2: c2(int64), c3(float32)
    let batch2 = RecordBatch::try_from_iter(vec![("c2", c2), ("c3", c3)]).unwrap();

    let (meta, _files) = store_parquet(vec![batch1, batch2]).await.unwrap();
    let file_groups = meta.into_iter().map(Into::into).collect();

    // cast c1 to utf8, c2 to int32, c3 to float64
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Float64, true),
    ]));
    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![file_groups],
            statistics: Statistics::new_unknown(&file_schema),
            file_schema,
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        },
        None,
        None,
    );

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

    let expected = [
        "+-------+----+------+",
        "| c1    | c2 | c3   |",
        "+-------+----+------+",
        "|       |    |      |",
        "|       | 1  | 10.0 |",
        "|       | 2  |      |",
        "|       | 2  | 20.0 |",
        "| one   | 1  |      |",
        "| three |    |      |",
        "+-------+----+------+",
    ];
    assert_batches_sorted_eq!(expected, &read);
}

#[tokio::test]
async fn multi_parquet_coercion_projection() {
    let d1: DictionaryArray<Int32Type> =
        vec![Some("one"), None, Some("three")].into_iter().collect();
    let c1: ArrayRef = Arc::new(d1);
    let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
    let c3: ArrayRef = Arc::new(Float32Array::from(vec![Some(10.0), Some(20.0), None]));
    let c1s = Arc::new(StringArray::from(vec![
        Some("baz"),
        Some("Boo"),
        Some("foo"),
    ]));

    // batch1: c2(int64), c1(dict)
    let batch1 =
        RecordBatch::try_from_iter(vec![("c2", c2.clone()), ("c1", c1)]).unwrap();
    // batch2: c2(int64), c1(str), c3(float32)
    let batch2 =
        RecordBatch::try_from_iter(vec![("c2", c2), ("c1", c1s), ("c3", c3)]).unwrap();

    let (meta, _files) = store_parquet(vec![batch1, batch2]).await.unwrap();
    let file_groups = meta.into_iter().map(Into::into).collect();

    // cast c1 to utf8, c2 to int32, c3 to float64
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Float64, true),
    ]));
    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![file_groups],
            statistics: Statistics::new_unknown(&file_schema),
            file_schema,
            projection: Some(vec![1, 0, 2]),
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
        },
        None,
        None,
    );

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

    let expected = [
        "+----+-------+------+",
        "| c2 | c1    | c3   |",
        "+----+-------+------+",
        "|    | foo   |      |",
        "|    | three |      |",
        "| 1  | baz   | 10.0 |",
        "| 1  | one   |      |",
        "| 2  |       |      |",
        "| 2  | Boo   | 20.0 |",
        "+----+-------+------+",
    ];
    assert_batches_sorted_eq!(expected, &read);
}

/// Writes `batches` to a temporary parquet file
pub async fn store_parquet(
    batches: Vec<RecordBatch>,
) -> Result<(Vec<ObjectMeta>, Vec<NamedTempFile>)> {
    // Each batch writes to their own file
    let files: Vec<_> = batches
        .into_iter()
        .map(|batch| {
            let mut output = NamedTempFile::new().expect("creating temp file");

            let builder = WriterProperties::builder();
            let props = builder.build();

            let mut writer =
                ArrowWriter::try_new(&mut output, batch.schema(), Some(props))
                    .expect("creating writer");

            writer.write(&batch).expect("Writing batch");
            writer.close().unwrap();
            output
        })
        .collect();

    let meta: Vec<_> = files.iter().map(local_unpartitioned_file).collect();
    Ok((meta, files))
}

/// Helper method to fetch the file size and date at given path and create a `ObjectMeta`
pub fn local_unpartitioned_file(path: impl AsRef<std::path::Path>) -> ObjectMeta {
    let location = Path::from_filesystem_path(path.as_ref()).unwrap();
    let metadata = std::fs::metadata(path).expect("Local file metadata");
    ObjectMeta {
        location,
        last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
        size: metadata.len() as usize,
        e_tag: None,
        version: None,
    }
}
