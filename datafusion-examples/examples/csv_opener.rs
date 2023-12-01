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

use datafusion::common::Statistics;
use datafusion::{
    assert_batches_eq,
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::{CsvConfig, CsvOpener, FileScanConfig, FileStream},
    },
    error::Result,
    physical_plan::metrics::ExecutionPlanMetricsSet,
    test_util::aggr_test_schema,
};

use futures::StreamExt;
use object_store::local::LocalFileSystem;

/// This example demonstrates a scanning against an Arrow data source (CSV) and
/// fetching results
#[tokio::main]
async fn main() -> Result<()> {
    let object_store = Arc::new(LocalFileSystem::new());
    let schema = aggr_test_schema();

    let config = CsvConfig::new(
        32768,
        schema.clone(),
        Some(vec![12, 0]),
        true,
        b',',
        b'"',
        object_store,
    );

    let opener = CsvOpener::new(Arc::new(config), FileCompressionType::UNCOMPRESSED);

    let testdata = datafusion::test_util::arrow_test_data();
    let path = format!("{testdata}/csv/aggregate_test_100.csv");

    let path = std::path::Path::new(&path).canonicalize()?;

    let scan_config = FileScanConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_schema: schema.clone(),
        file_groups: vec![vec![PartitionedFile::new(path.display().to_string(), 10)]],
        statistics: Statistics::new_unknown(&schema),
        projection: Some(vec![12, 0]),
        limit: Some(5),
        table_partition_cols: vec![],
        output_ordering: vec![],
        infinite_source: false,
    };

    let result =
        FileStream::new(&scan_config, 0, opener, &ExecutionPlanMetricsSet::new())
            .unwrap()
            .map(|b| b.unwrap())
            .collect::<Vec<_>>()
            .await;
    assert_batches_eq!(
        &[
            "+--------------------------------+----+",
            "| c13                            | c1 |",
            "+--------------------------------+----+",
            "| 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW | c  |",
            "| C2GT5KVyOPZpgKVl110TyZO0NcJ434 | d  |",
            "| AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz | b  |",
            "| 0keZ5G8BffGwgF2RwQD59TFzMStxCB | a  |",
            "| Ig1QcuKsjHXkproePdERo2w0mYzIqd | b  |",
            "+--------------------------------+----+",
        ],
        &result
    );
    Ok(())
}
