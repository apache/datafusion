// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float32Array, Float64Array, Int64Array, RecordBatch,
    StringArray, types::Int32Type,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::test::object_store::local_unpartitioned_file;
use datafusion_common::test_util::batches_to_sort_string;
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::object_store::ObjectStoreUrl;

use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use insta::assert_snapshot;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;
use parquet::basic::ColumnOrder;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
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

    let (meta, _files) = store_parquet(vec![batch1, batch2]).unwrap();
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

    let (meta, _files) = store_parquet(vec![batch1, batch2]).unwrap();
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

#[tokio::test]
async fn parquet_max_with_mixed_float_schemas() {
    // Arrange
    let cast_values: ArrayRef =
        Arc::new(Float64Array::from(vec![1.00000001, 2.00000003]));
    let native_values: ArrayRef = Arc::new(Float32Array::from(vec![0.5]));
    let (_, files) = store_parquet(vec![
        RecordBatch::try_from_iter(vec![("x", cast_values)]).unwrap(),
        RecordBatch::try_from_iter(vec![("x", native_values)]).unwrap(),
    ])
    .unwrap();
    for file in &files {
        let reader = SerializedFileReader::new(file.reopen().unwrap()).unwrap();
        let metadata = reader.metadata();
        assert_eq!(
            metadata.file_metadata().column_order(0),
            ColumnOrder::IEEE_754_TOTAL_ORDER
        );
        let stats = metadata.row_group(0).column(0).statistics().unwrap();
        assert_eq!(stats.nan_count_opt(), Some(0));
        assert!(!stats.is_min_max_deprecated());
        assert!(stats.min_is_exact());
        assert!(stats.max_is_exact());
    }

    let output_schema = Schema::new(vec![Field::new("x", DataType::Float32, true)]);
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_collect_statistics(true),
    );
    let paths: Vec<_> = files
        .iter()
        .map(|file| file.path().to_str().unwrap())
        .collect();
    let df = ctx
        .read_parquet(
            paths,
            ParquetReadOptions::default()
                .file_extension("")
                .schema(&output_schema),
        )
        .await
        .unwrap();
    ctx.register_table("t", df.into_view()).unwrap();

    // Act
    let batches = ctx
        .sql("SELECT MAX(x) FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // Assert
    // The Float64 file must still contribute after its values are cast.
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Float32(Some(2.0))
    );
}

/// Writes `batches` to a temporary parquet file
pub fn store_parquet(
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
