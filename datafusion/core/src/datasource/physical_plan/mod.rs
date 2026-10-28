// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
    FileOpenFuture, FileOpener, FileStream, FileStreamBuilder, OnError,
};
