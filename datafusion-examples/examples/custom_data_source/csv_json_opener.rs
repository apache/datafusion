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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::CsvOptions;
use datafusion::{
    assert_batches_eq,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::{CsvSource, FileSource, FileStream, JsonOpener, JsonSource},
    },
    error::Result,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};

use datafusion::datasource::physical_plan::FileScanConfigBuilder;
use datafusion_examples::utils::datasets::ExampleDataset;
use futures::StreamExt;
use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};

/// This example demonstrates using the low level [`FileStream`] / [`FileOpener`] APIs to directly
/// read data from (CSV/JSON) into Arrow RecordBatches.
///
/// If you want to query data in CSV or JSON files, see the [`dataframe.rs`] and [`sql_query.rs`] examples
pub async fn csv_json_opener() -> Result<()> {
    csv_opener().await?;
    json_opener().await?;
    Ok(())
}

async fn csv_opener() -> Result<()> {
    let object_store = Arc::new(LocalFileSystem::new());

    let dataset = ExampleDataset::Cars;
    let csv_path = dataset.path();
    let schema = dataset.schema();

    let options = CsvOptions {
        has_header: Some(true),
        delimiter: b',',
        quote: b'"',
        ..Default::default()
    };

    let source = CsvSource::new(Arc::clone(&schema))
        .with_csv_options(options)
        .with_comment(Some(b'#'))
        .with_batch_size(8192);

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_projection_indices(Some(vec![0, 1]))?
            .with_limit(Some(5))
            .with_file(PartitionedFile::new(csv_path.display().to_string(), 10))
            .build();

    let opener =
        scan_config
            .file_source()
            .create_file_opener(object_store, &scan_config, 0)?;

    let mut result = vec![];
    let mut stream =
        FileStream::new(&scan_config, 0, opener, &ExecutionPlanMetricsSet::new())?;
    while let Some(batch) = stream.next().await.transpose()? {
        result.push(batch);
    }
    assert_batches_eq!(
        &[
            "+-----+-------+",
            "| car | speed |",
            "+-----+-------+",
            "| red | 20.0  |",
            "| red | 20.3  |",
            "| red | 21.4  |",
            "| red | 21.5  |",
            "| red | 19.0  |",
            "+-----+-------+",
        ],
        &result
    );
    Ok(())
}

async fn json_opener() -> Result<()> {
    let object_store = InMemory::new();
    let path = object_store::path::Path::from("demo.json");
    let data = bytes::Bytes::from(
        r#"{"num":5,"str":"test"}
        {"num":2,"str":"hello"}
        {"num":4,"str":"foo"}"#,
    );

    object_store.put(&path, data.into()).await?;

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
        true,
    );

    let scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(JsonSource::new(schema)),
    )
    .with_projection_indices(Some(vec![1, 0]))?
    .with_limit(Some(5))
    .with_file(PartitionedFile::new(path.to_string(), 10))
    .build();

    let mut stream = FileStream::new(
        &scan_config,
        0,
        Arc::new(opener),
        &ExecutionPlanMetricsSet::new(),
    )?;
    let mut result = vec![];
    while let Some(batch) = stream.next().await.transpose()? {
        result.push(batch);
    }
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
