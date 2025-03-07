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

pub mod file_format;
pub mod source;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_datasource::{
    file::FileSource, file_scan_config::FileScanConfig, PartitionedFile,
};
use datafusion_execution::object_store::ObjectStoreUrl;
pub use file_format::*;

/// Returns a [`FileScanConfig`] for given `file_groups`
pub fn partitioned_csv_config(
    schema: SchemaRef,
    file_groups: Vec<Vec<PartitionedFile>>,
    file_source: Arc<dyn FileSource>,
) -> FileScanConfig {
    FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema, file_source)
        .with_file_groups(file_groups)
}
