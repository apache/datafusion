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

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, DictionaryArray, Float32Array, Int64Array, ListArray};
use arrow_schema::DataType;
use datafusion::assert_batches_sorted_eq;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use datafusion_common::Statistics;
use datafusion_execution::object_store::ObjectStoreUrl;
use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tempfile::NamedTempFile;

/// Test for reading data from multiple parquet files with different schemas and coercing them into a single schema.
#[tokio::test]
async fn multi_parquet_coercion() {
    let d1: DictionaryArray<Int32Type> =
        vec![Some("one"), None, Some("three")].into_iter().collect();
    let c1: ArrayRef = Arc::new(d1);
    let c2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));
    let c3: ArrayRef = Arc::new(Float32Array::from(vec![Some(10.0), Some(20.0), None]));
    let c4 = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![Some(4), Some(5)]),
        Some(vec![Some(6)]),
    ]));

    let batch1 = RecordBatch::try_from_iter(vec![
        ("c1", c1),
        ("c2", c2.clone()),
        ("c3", c3.clone()),
    ])
    .unwrap();
    let batch2 =
        RecordBatch::try_from_iter(vec![("c2", c2), ("c3", c3), ("c4", c4)]).unwrap();

    let (meta, _files) = store_parquet(vec![batch1, batch2]).await.unwrap();
    let file_groups = meta.into_iter().map(Into::into).collect();

    let file_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Float64, true),
        Field::new("c4", DataType::Utf8, true),
    ]));
    let parquet_exec = ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![file_groups],
            file_schema,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        },
        None,
        None,
    );

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

    let expected = vec![
        "+-------+----+------+-----------+",
        "| c1    | c2 | c3   | c4        |",
        "+-------+----+------+-----------+",
        "|       |    |      | [6]       |",
        "|       | 1  | 10.0 | [1, 2, 3] |",
        "|       | 2  | 20.0 |           |",
        "|       | 2  | 20.0 | [4, 5]    |",
        "| one   | 1  | 10.0 |           |",
        "| three |    |      |           |",
        "+-------+----+------+-----------+",
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
    }
}
