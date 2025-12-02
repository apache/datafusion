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

use arrow::array::{
    types::Int32Type, ArrayRef, DictionaryArray, Float32Array, Int64Array, RecordBatch,
    StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use datafusion::test::object_store::local_unpartitioned_file;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::Result;
use datafusion_execution::object_store::ObjectStoreUrl;

use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use insta::assert_snapshot;
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
    let file_group = meta.into_iter().map(Into::into).collect();

    // cast c1 to utf8, c2 to int32, c3 to float64
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Float64, true),
    ]));
    let source = Arc::new(ParquetSource::new(file_schema.clone()));
    let conf = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file_group(file_group)
        .build();

    let parquet_exec = DataSourceExec::from_data_source(conf);

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(parquet_exec, task_ctx).await.unwrap();

    assert_snapshot!(batches_to_sort_string(&read), @r"
    +-------+----+------+
    | c1    | c2 | c3   |
    +-------+----+------+
    |       |    |      |
    |       | 1  | 10.0 |
    |       | 2  |      |
    |       | 2  | 20.0 |
    | one   | 1  |      |
    | three |    |      |
    +-------+----+------+
    ");
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
    let file_group = meta.into_iter().map(Into::into).collect();

    // cast c1 to utf8, c2 to int32, c3 to float64
    let file_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::Float64, true),
    ]));
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetSource::new(file_schema)),
    )
    .with_file_group(file_group)
    .with_projection_indices(Some(vec![1, 0, 2]))
    .unwrap()
    .build();

    let parquet_exec = DataSourceExec::from_data_source(config);

    let session_ctx = SessionContext::new();
    let task_ctx = session_ctx.task_ctx();
    let read = collect(parquet_exec, task_ctx).await.unwrap();

    assert_snapshot!(batches_to_sort_string(&read), @r"
    +----+-------+------+
    | c2 | c1    | c3   |
    +----+-------+------+
    |    | foo   |      |
    |    | three |      |
    | 1  | baz   | 10.0 |
    | 1  | one   |      |
    | 2  |       |      |
    | 2  | Boo   | 20.0 |
    +----+-------+------+
    ");
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
