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

//! DataSource and FileSource trait implementations

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

use crate::datasource::physical_plan::{FileOpener, FileScanConfig};

use arrow_schema::SchemaRef;
use datafusion_common::Statistics;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::DisplayFormatType;

use object_store::ObjectStore;

/// Common behaviors that every file format needs to implement.
pub trait FileSource: Send + Sync {
    /// Creates a `dyn FileOpener` based on given parameters
    fn create_file_opener(
        &self,
        object_store: datafusion_common::Result<Arc<dyn ObjectStore>>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> datafusion_common::Result<Arc<dyn FileOpener>>;
    /// Any
    fn as_any(&self) -> &dyn Any;
    /// Initialize new type with batch size configuration
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource>;
    /// Initialize new instance with a new schema
    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource>;
    /// Initialize new instance with projection information
    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource>;
    /// Initialize new instance with projected statistics
    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource>;
    /// Return execution plan metrics
    fn metrics(&self) -> &ExecutionPlanMetricsSet;
    /// Return projected statistics
    fn statistics(&self) -> datafusion_common::Result<Statistics>;
    /// Returns the file type such as Arrow, Avro, Parquet, ...
    fn file_type(&self) -> FileType;
    /// Format FileType specific information
    fn fmt_extra(&self, _t: DisplayFormatType, _f: &mut Formatter) -> fmt::Result {
        Ok(())
    }
}

/// Determines file types
pub enum FileType {
    /// Arrow File
    Arrow,
    /// Avro File
    Avro,
    /// CSV File
    Csv,
    /// JSON File
    Json,
    /// Parquet File
    Parquet,
}

impl FileType {
    pub(crate) fn to_str(&self) -> &str {
        match self {
            FileType::Arrow => "arrow",
            FileType::Avro => "avro",
            FileType::Csv => "csv",
            FileType::Json => "json",
            FileType::Parquet => "parquet",
        }
    }

    /// Is the file type avro?
    pub fn is_avro(&self) -> bool {
        matches!(self, FileType::Avro)
    }

    /// Is the file type csv?
    pub fn is_csv(&self) -> bool {
        matches!(self, FileType::Csv)
    }

    /// Is the file type parquet?
    pub fn is_parquet(&self) -> bool {
        matches!(self, FileType::Parquet)
    }
}
