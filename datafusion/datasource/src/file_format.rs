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

//! Module containing helper methods for the various file formats
//! See write.rs for write related helper methods

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::file::FileSource;
use crate::file_compression_type::FileCompressionType;
use crate::file_scan_config::FileScanConfig;
use crate::file_sink_config::FileSinkConfig;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{internal_err, not_impl_err, GetExt, Result, Statistics};
use datafusion_expr::Expr;
use datafusion_physical_expr::{LexRequirement, PhysicalExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_session::Session;

use async_trait::async_trait;
use object_store::{ObjectMeta, ObjectStore};

/// Default max records to scan to infer the schema
pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1000;

/// This trait abstracts all the file format specific implementations
/// from the [`TableProvider`]. This helps code re-utilization across
/// providers that support the same file formats.
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
#[async_trait]
pub trait FileFormat: Send + Sync + fmt::Debug {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String;

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String>;

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Take a list of files and the configuration to convert it to the
    /// appropriate writer executor according to this file format.
    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Writer not implemented for this format")
    }

    /// Check if the specified file format has support for pushing down the provided filters within
    /// the given schemas. Added initially to support the Parquet file format's ability to do this.
    fn supports_filters_pushdown(
        &self,
        _file_schema: &Schema,
        _table_schema: &Schema,
        _filters: &[&Expr],
    ) -> Result<FilePushdownSupport> {
        Ok(FilePushdownSupport::NoSupport)
    }

    /// Return the related FileSource such as `CsvSource`, `JsonSource`, etc.
    fn file_source(&self) -> Arc<dyn FileSource>;
}

/// An enum to distinguish between different states when determining if certain filters can be
/// pushed down to file scanning
#[derive(Debug, PartialEq)]
pub enum FilePushdownSupport {
    /// The file format/system being asked does not support any sort of pushdown. This should be
    /// used even if the file format theoretically supports some sort of pushdown, but it's not
    /// enabled or implemented yet.
    NoSupport,
    /// The file format/system being asked *does* support pushdown, but it can't make it work for
    /// the provided filter/expression
    NotSupportedForFilter,
    /// The file format/system being asked *does* support pushdown and *can* make it work for the
    /// provided filter/expression
    Supported,
}

/// Factory for creating [`FileFormat`] instances based on session and command level options
///
/// Users can provide their own `FileFormatFactory` to support arbitrary file formats
pub trait FileFormatFactory: Sync + Send + GetExt + fmt::Debug {
    /// Initialize a [FileFormat] and configure based on session and command level options
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>>;

    /// Initialize a [FileFormat] with all options set to default values
    fn default(&self) -> Arc<dyn FileFormat>;

    /// Returns the table source as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}

/// A container of [FileFormatFactory] which also implements [FileType].
/// This enables converting a dyn FileFormat to a dyn FileType.
/// The former trait is a superset of the latter trait, which includes execution time
/// relevant methods. [FileType] is only used in logical planning and only implements
/// the subset of methods required during logical planning.
#[derive(Debug)]
pub struct DefaultFileType {
    file_format_factory: Arc<dyn FileFormatFactory>,
}

impl DefaultFileType {
    /// Constructs a [DefaultFileType] wrapper from a [FileFormatFactory]
    pub fn new(file_format_factory: Arc<dyn FileFormatFactory>) -> Self {
        Self {
            file_format_factory,
        }
    }

    /// get a reference to the inner [FileFormatFactory] struct
    pub fn as_format_factory(&self) -> &Arc<dyn FileFormatFactory> {
        &self.file_format_factory
    }
}

impl FileType for DefaultFileType {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl fmt::Display for DefaultFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.file_format_factory)
    }
}

impl GetExt for DefaultFileType {
    fn get_ext(&self) -> String {
        self.file_format_factory.get_ext()
    }
}

/// Converts a [FileFormatFactory] to a [FileType]
pub fn format_as_file_type(
    file_format_factory: Arc<dyn FileFormatFactory>,
) -> Arc<dyn FileType> {
    Arc::new(DefaultFileType {
        file_format_factory,
    })
}

/// Converts a [FileType] to a [FileFormatFactory].
/// Returns an error if the [FileType] cannot be
/// downcasted to a [DefaultFileType].
pub fn file_type_to_format(
    file_type: &Arc<dyn FileType>,
) -> Result<Arc<dyn FileFormatFactory>> {
    match file_type
        .as_ref()
        .as_any()
        .downcast_ref::<DefaultFileType>()
    {
        Some(source) => Ok(Arc::clone(&source.file_format_factory)),
        _ => internal_err!("FileType was not DefaultFileType"),
    }
}
