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

//! User facing options for the file formats readers

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};

use crate::datasource::file_format::avro::DEFAULT_AVRO_EXTENSION;
use crate::datasource::file_format::csv::DEFAULT_CSV_EXTENSION;
use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::file_format::json::DEFAULT_JSON_EXTENSION;
use crate::datasource::file_format::parquet::DEFAULT_PARQUET_EXTENSION;
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::datasource::{
    file_format::{
        avro::AvroFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat,
    },
    listing::ListingOptions,
};

use super::context::SessionConfig;

/// Options that control the reading of CSV files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct CsvReadOptions<'a> {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::CSV.get_ext().as_str()`.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// File compression type
    pub file_compression_type: FileCompressionType,
}

impl<'a> Default for CsvReadOptions<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> CsvReadOptions<'a> {
    /// Create a CSV read option with default presets
    pub fn new() -> Self {
        Self {
            has_header: true,
            schema: None,
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORD,
            delimiter: b',',
            file_extension: DEFAULT_CSV_EXTENSION,
            table_partition_cols: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }

    /// Configure has_header setting
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Specify the file extension for CSV file selection
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }

    /// Configure delimiter setting with Option, None value will be ignored
    pub fn delimiter_option(mut self, delimiter: Option<u8>) -> Self {
        if let Some(d) = delimiter {
            self.delimiter = d;
        }
        self
    }

    /// Specify schema to use for CSV read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }

    /// Configure file compression type
    pub fn file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = CsvFormat::default()
            .with_has_header(self.has_header)
            .with_delimiter(self.delimiter)
            .with_schema_infer_max_rec(Some(self.schema_infer_max_records))
            .with_file_compression_type(self.file_compression_type.to_owned());

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(target_partitions)
            .with_table_partition_cols(self.table_partition_cols.clone())
    }
}

/// Options that control the reading of Parquet files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct ParquetReadOptions<'a> {
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".parquet".
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Should the parquet reader use the predicate to prune row groups?
    /// If None, uses value in SessionConfig
    pub parquet_pruning: Option<bool>,
    /// Should the parquet reader to skip any metadata that may be in
    /// the file Schema? This can help avoid schema conflicts due to
    /// metadata.
    ///
    /// If None specified, uses value in SessionConfig
    pub skip_metadata: Option<bool>,
}

impl<'a> Default for ParquetReadOptions<'a> {
    fn default() -> Self {
        Self {
            file_extension: DEFAULT_PARQUET_EXTENSION,
            table_partition_cols: vec![],
            parquet_pruning: None,
            skip_metadata: None,
        }
    }
}

impl<'a> ParquetReadOptions<'a> {
    /// Specify parquet_pruning
    pub fn parquet_pruning(mut self, parquet_pruning: bool) -> Self {
        self.parquet_pruning = Some(parquet_pruning);
        self
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.  Defaults to true.
    pub fn skip_metadata(mut self, skip_metadata: bool) -> Self {
        self.skip_metadata = Some(skip_metadata);
        self
    }

    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let file_format = ParquetFormat::new(config.config_options())
            .with_enable_pruning(self.parquet_pruning)
            .with_skip_metadata(self.skip_metadata);

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
    }
}

/// Options that control the reading of AVRO files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct AvroReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::AVRO.get_ext().as_str()`.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
}

impl<'a> Default for AvroReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: DEFAULT_AVRO_EXTENSION,
            table_partition_cols: vec![],
        }
    }
}

impl<'a> AvroReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = AvroFormat::default();

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(target_partitions)
            .with_table_partition_cols(self.table_partition_cols.clone())
    }
}

/// Options that control the reading of Line-delimited JSON files (NDJson)
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct NdJsonReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,
    /// Max number of rows to read from JSON files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::JSON.get_ext().as_str()`.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// File compression type
    pub file_compression_type: FileCompressionType,
}

impl<'a> Default for NdJsonReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORD,
            file_extension: DEFAULT_JSON_EXTENSION,
            table_partition_cols: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

impl<'a> NdJsonReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Specify file_extension
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }

    /// Specify file_compression_type
    pub fn file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = JsonFormat::default()
            .with_file_compression_type(self.file_compression_type.to_owned());

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(target_partitions)
            .with_table_partition_cols(self.table_partition_cols.clone())
    }
}
