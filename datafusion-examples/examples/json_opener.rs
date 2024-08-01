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

use std::{sync::Arc, vec};

use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    assert_batches_eq,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::{FileScanConfig, FileStream, JsonOpener},
    },
    error::Result,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};

use futures::StreamExt;
use object_store::ObjectStore;

/// This example demonstrates a scanning against an Arrow data source (JSON) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    let object_store = object_store::memory::InMemory::new();
    let path = object_store::path::Path::from("demo.json");
    let data = bytes::Bytes::from(
        r#"{"num":5,"str":"test"}
        {"num":2,"str":"hello"}
        {"num":4,"str":"foo"}"#,
    );
    object_store.put(&path, data.into()).await.unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("num", DataType::Int64, false),
        Field::new("str", DataType::Utf8, false),
    ]));

    let projected = Arc::new(schema.clone().project(&[1, 0])?);

    let opener = JsonOpener::new(
        8192,
        projected,
        FileCompressionType::UNCOMPRESSED,
        Arc::new(object_store),
    );

    let scan_config =
        FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema.clone())
            .with_projection(Some(vec![1, 0]))
            .with_limit(Some(5))
            .with_file(PartitionedFile::new(path.to_string(), 10));

    let result =
        FileStream::new(&scan_config, 0, opener, &ExecutionPlanMetricsSet::new())
            .unwrap()
            .map(|b| b.unwrap())
            .collect::<Vec<_>>()
            .await;
    assert_batches_eq!(
        &[
            "+-------+-----+",
            "| str   | num |",
            "+-------+-----+",
            "| test  | 5   |",
            "| hello | 2   |",
            "| foo   | 4   |",
            "+-------+-----+",
        ],
        &result
    );
    Ok(())
}
