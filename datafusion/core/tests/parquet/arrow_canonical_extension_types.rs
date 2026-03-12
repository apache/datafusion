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

use super::*;
use arrow_schema::extension::Uuid;
use parquet::arrow::ArrowWriter;
use parquet::basic::LogicalType;
use parquet::file::metadata::ParquetMetaDataReader;
use std::fs::File;
use tempfile::TempDir;

#[tokio::test]
async fn test_uuid_roundtrip() {
    let tmp_dir = TempDir::new().unwrap();

    // Create mock schema and data
    let schema = Arc::new(Schema::new(vec![
        Field::new("uuid", DataType::FixedSizeBinary(16), false)
            .with_extension_type(Uuid),
    ]));
    let uuids = Arc::new(FixedSizeBinaryArray::from(vec![b"abcdefghijklmnop"]));
    let record_batch = RecordBatch::try_new(schema.clone(), vec![uuids]).unwrap();

    // We write a Parquet file
    let table_path = tmp_dir.path().join("test.parquet");
    let file = File::create(&table_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer.write(&record_batch).unwrap();
    writer.close().unwrap();

    // We check we indeed use the UUID type in Parquet
    let parquet_metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&File::open(&table_path).unwrap())
        .unwrap();
    assert_eq!(
        parquet_metadata.file_metadata().schema().get_fields()[0]
            .get_basic_info()
            .logical_type_ref(),
        Some(&LogicalType::Uuid)
    );

    // We read the Parquet file and make sure the UUID extension type has been kept
    let data_frame = SessionContext::new()
        .read_parquet(
            vec![table_path.to_str().unwrap()],
            ParquetReadOptions::default().skip_metadata(false),
        )
        .await
        .unwrap();
    assert_eq!(*data_frame.schema().inner(), schema)
}
