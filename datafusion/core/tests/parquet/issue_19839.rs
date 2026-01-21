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

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::{SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

#[tokio::test]
async fn test_parquet_opener_without_page_index() {
    // Defines a simple schema and batch
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    // Create a temp file
    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let path = file.path().to_str().unwrap().to_string();

    // Write parquet WITHOUT page index
    // The default WriterProperties does not write page index, but we set it explicitly
    // to be robust against future changes in defaults as requested by reviewers.
    let props = WriterProperties::builder()
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build();

    let file_fs = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file_fs, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    // Setup SessionContext with PageIndex enabled
    // This triggers the ParquetOpener to try and load page index if available
    let config = SessionConfig::new().with_parquet_page_index_pruning(true);

    let ctx = SessionContext::new_with_config(config);

    // Register the table
    ctx.register_parquet("t", &path, Default::default())
        .await
        .unwrap();

    // Query the table
    // If the bug exists, this might fail because Opener tries to load PageIndex forcefully
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let batches = df.collect().await;

    // We expect this to succeed, but currently it might fail
    match batches {
        Ok(b) => {
            assert_eq!(b.len(), 1);
            assert_eq!(b[0].num_rows(), 3);
        }
        Err(e) => {
            panic!("Failed to read parquet file without page index: {}", e);
        }
    }
}
