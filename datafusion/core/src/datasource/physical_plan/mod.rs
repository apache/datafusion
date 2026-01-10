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

//! Execution plans that read file formats

pub mod arrow;
pub mod csv;
pub mod json;

#[cfg(feature = "parquet")]
pub mod parquet;

#[cfg(feature = "avro")]
pub mod avro;

#[cfg(feature = "avro")]
pub use avro::AvroSource;

#[cfg(feature = "parquet")]
pub use datafusion_datasource_parquet::source::ParquetSource;
#[cfg(feature = "parquet")]
pub use datafusion_datasource_parquet::{ParquetFileMetrics, ParquetFileReaderFactory};

pub use json::{JsonOpener, JsonSource};

pub use arrow::{ArrowOpener, ArrowSource};
pub use csv::{CsvOpener, CsvSource};
pub use datafusion_datasource::file::FileSource;
pub use datafusion_datasource::file_groups::FileGroup;
pub use datafusion_datasource::file_groups::FileGroupPartitioner;
pub use datafusion_datasource::file_scan_config::{
    FileScanConfig, FileScanConfigBuilder, wrap_partition_type_in_dict,
    wrap_partition_value_in_dict,
};
pub use datafusion_datasource::file_sink_config::*;

pub use datafusion_datasource::file_stream::{
    FileOpenFuture, FileOpener, FileStream, OnError,
};
