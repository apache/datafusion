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

/// Default max records to scan to infer the schema
pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1000;

pub mod arrow;
pub mod avro;
pub mod csv;
pub mod json;
pub mod options;
#[cfg(feature = "parquet")]
pub mod parquet;
pub use datafusion_catalog_listing::file_compression_type;
pub use datafusion_catalog_listing::write;

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt::{self, Debug, Display};
use std::sync::Arc;
use std::task::Poll;

use crate::arrow::array::RecordBatch;
use crate::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use crate::arrow::error::ArrowError;
use crate::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use crate::error::Result;
use crate::physical_plan::{ExecutionPlan, Statistics};

use datafusion_catalog::Session;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{internal_err, not_impl_err, GetExt};
use datafusion_expr::Expr;
use datafusion_physical_expr::PhysicalExpr;

use crate::datasource::data_source::FileSource;
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use file_compression_type::FileCompressionType;
use futures::stream::BoxStream;
use futures::{ready, Stream, StreamExt};
use object_store::{ObjectMeta, ObjectStore};

/// Factory for creating [`FileFormat`] instances based on session and command level options
///
/// Users can provide their own `FileFormatFactory` to support arbitrary file formats
pub trait FileFormatFactory: Sync + Send + GetExt + Debug {
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

/// This trait abstracts all the file format specific implementations
/// from the [`TableProvider`]. This helps code re-utilization across
/// providers that support the same file formats.
///
/// [`TableProvider`]: crate::catalog::TableProvider
#[async_trait]
pub trait FileFormat: Send + Sync + Debug {
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

/// Possible outputs of a [`BatchDeserializer`].
#[derive(Debug, PartialEq)]
pub enum DeserializerOutput {
    /// A successfully deserialized [`RecordBatch`].
    RecordBatch(RecordBatch),
    /// The deserializer requires more data to make progress.
    RequiresMoreData,
    /// The input data has been exhausted.
    InputExhausted,
}

/// Trait defining a scheme for deserializing byte streams into structured data.
/// Implementors of this trait are responsible for converting raw bytes into
/// `RecordBatch` objects.
pub trait BatchDeserializer<T>: Send + Debug {
    /// Feeds a message for deserialization, updating the internal state of
    /// this `BatchDeserializer`. Note that one can call this function multiple
    /// times before calling `next`, which will queue multiple messages for
    /// deserialization. Returns the number of bytes consumed.
    fn digest(&mut self, message: T) -> usize;

    /// Attempts to deserialize any pending messages and returns a
    /// `DeserializerOutput` to indicate progress.
    fn next(&mut self) -> Result<DeserializerOutput, ArrowError>;

    /// Informs the deserializer that no more messages will be provided for
    /// deserialization.
    fn finish(&mut self);
}

/// A general interface for decoders such as [`arrow::json::reader::Decoder`] and
/// [`arrow::csv::reader::Decoder`]. Defines an interface similar to
/// [`Decoder::decode`] and [`Decoder::flush`] methods, but also includes
/// a method to check if the decoder can flush early. Intended to be used in
/// conjunction with [`DecoderDeserializer`].
///
/// [`arrow::json::reader::Decoder`]: ::arrow::json::reader::Decoder
/// [`arrow::csv::reader::Decoder`]: ::arrow::csv::reader::Decoder
/// [`Decoder::decode`]: ::arrow::json::reader::Decoder::decode
/// [`Decoder::flush`]: ::arrow::json::reader::Decoder::flush
pub(crate) trait Decoder: Send + Debug {
    /// See [`arrow::json::reader::Decoder::decode`].
    ///
    /// [`arrow::json::reader::Decoder::decode`]: ::arrow::json::reader::Decoder::decode
    fn decode(&mut self, buf: &[u8]) -> Result<usize, ArrowError>;

    /// See [`arrow::json::reader::Decoder::flush`].
    ///
    /// [`arrow::json::reader::Decoder::flush`]: ::arrow::json::reader::Decoder::flush
    fn flush(&mut self) -> Result<Option<RecordBatch>, ArrowError>;

    /// Whether the decoder can flush early in its current state.
    fn can_flush_early(&self) -> bool;
}

impl<T: Decoder> Debug for DecoderDeserializer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Deserializer")
            .field("buffered_queue", &self.buffered_queue)
            .field("finalized", &self.finalized)
            .finish()
    }
}

impl<T: Decoder> BatchDeserializer<Bytes> for DecoderDeserializer<T> {
    fn digest(&mut self, message: Bytes) -> usize {
        if message.is_empty() {
            return 0;
        }

        let consumed = message.len();
        self.buffered_queue.push_back(message);
        consumed
    }

    fn next(&mut self) -> Result<DeserializerOutput, ArrowError> {
        while let Some(buffered) = self.buffered_queue.front_mut() {
            let decoded = self.decoder.decode(buffered)?;
            buffered.advance(decoded);

            if buffered.is_empty() {
                self.buffered_queue.pop_front();
            }

            // Flush when the stream ends or batch size is reached
            // Certain implementations can flush early
            if decoded == 0 || self.decoder.can_flush_early() {
                return match self.decoder.flush() {
                    Ok(Some(batch)) => Ok(DeserializerOutput::RecordBatch(batch)),
                    Ok(None) => continue,
                    Err(e) => Err(e),
                };
            }
        }
        if self.finalized {
            Ok(DeserializerOutput::InputExhausted)
        } else {
            Ok(DeserializerOutput::RequiresMoreData)
        }
    }

    fn finish(&mut self) {
        self.finalized = true;
        // Ensure the decoder is flushed:
        self.buffered_queue.push_back(Bytes::new());
    }
}

/// A generic, decoder-based deserialization scheme for processing encoded data.
///
/// This struct is responsible for converting a stream of bytes, which represent
/// encoded data, into a stream of `RecordBatch` objects, following the specified
/// schema and formatting options. It also handles any buffering necessary to satisfy
/// the `Decoder` interface.
pub(crate) struct DecoderDeserializer<T: Decoder> {
    /// The underlying decoder used for deserialization
    pub(crate) decoder: T,
    /// The buffer used to store the remaining bytes to be decoded
    pub(crate) buffered_queue: VecDeque<Bytes>,
    /// Whether the input stream has been fully consumed
    pub(crate) finalized: bool,
}

impl<T: Decoder> DecoderDeserializer<T> {
    /// Creates a new `DecoderDeserializer` with the provided decoder.
    pub(crate) fn new(decoder: T) -> Self {
        DecoderDeserializer {
            decoder,
            buffered_queue: VecDeque::new(),
            finalized: false,
        }
    }
}

/// Deserializes a stream of bytes into a stream of [`RecordBatch`] objects using the
/// provided deserializer.
///
/// Returns a boxed stream of `Result<RecordBatch, ArrowError>`. The stream yields [`RecordBatch`]
/// objects as they are produced by the deserializer, or an [`ArrowError`] if an error
/// occurs while polling the input or deserializing.
pub(crate) fn deserialize_stream<'a>(
    mut input: impl Stream<Item = Result<Bytes>> + Unpin + Send + 'a,
    mut deserializer: impl BatchDeserializer<Bytes> + 'a,
) -> BoxStream<'a, Result<RecordBatch, ArrowError>> {
    futures::stream::poll_fn(move |cx| loop {
        match ready!(input.poll_next_unpin(cx)).transpose()? {
            Some(b) => _ = deserializer.digest(b),
            None => deserializer.finish(),
        };

        return match deserializer.next()? {
            DeserializerOutput::RecordBatch(rb) => Poll::Ready(Some(Ok(rb))),
            DeserializerOutput::InputExhausted => Poll::Ready(None),
            DeserializerOutput::RequiresMoreData => continue,
        };
    })
    .boxed()
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

impl Display for DefaultFileType {
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

/// Create a new field with the specified data type, copying the other
/// properties from the input field
fn field_with_new_type(field: &FieldRef, new_type: DataType) -> FieldRef {
    Arc::new(field.as_ref().clone().with_data_type(new_type))
}

/// Transform a schema to use view types for Utf8 and Binary
///
/// See [parquet::ParquetFormat::force_view_types] for details
pub fn transform_schema_to_view(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                field_with_new_type(field, DataType::Utf8View)
            }
            DataType::Binary | DataType::LargeBinary => {
                field_with_new_type(field, DataType::BinaryView)
            }
            _ => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(transformed_fields, schema.metadata.clone())
}

/// Coerces the file schema if the table schema uses a view type.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn coerce_file_schema_to_view_type(
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<Schema> {
    let mut transform = false;
    let table_fields: HashMap<_, _> = table_schema
        .fields
        .iter()
        .map(|f| {
            let dt = f.data_type();
            if dt.equals_datatype(&DataType::Utf8View)
                || dt.equals_datatype(&DataType::BinaryView)
            {
                transform = true;
            }
            (f.name(), dt)
        })
        .collect();

    if !transform {
        return None;
    }

    let transformed_fields: Vec<Arc<Field>> = file_schema
        .fields
        .iter()
        .map(
            |field| match (table_fields.get(field.name()), field.data_type()) {
                (Some(DataType::Utf8View), DataType::Utf8 | DataType::LargeUtf8) => {
                    field_with_new_type(field, DataType::Utf8View)
                }
                (
                    Some(DataType::BinaryView),
                    DataType::Binary | DataType::LargeBinary,
                ) => field_with_new_type(field, DataType::BinaryView),
                _ => Arc::clone(field),
            },
        )
        .collect();

    Some(Schema::new_with_metadata(
        transformed_fields,
        file_schema.metadata.clone(),
    ))
}

/// Transform a schema so that any binary types are strings
pub fn transform_binary_to_string(schema: &Schema) -> Schema {
    let transformed_fields: Vec<Arc<Field>> = schema
        .fields
        .iter()
        .map(|field| match field.data_type() {
            DataType::Binary => field_with_new_type(field, DataType::Utf8),
            DataType::LargeBinary => field_with_new_type(field, DataType::LargeUtf8),
            DataType::BinaryView => field_with_new_type(field, DataType::Utf8View),
            _ => Arc::clone(field),
        })
        .collect();
    Schema::new_with_metadata(transformed_fields, schema.metadata.clone())
}

/// If the table schema uses a string type, coerce the file schema to use a string type.
///
/// See [parquet::ParquetFormat::binary_as_string] for details
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn coerce_file_schema_to_string_type(
    table_schema: &Schema,
    file_schema: &Schema,
) -> Option<Schema> {
    let mut transform = false;
    let table_fields: HashMap<_, _> = table_schema
        .fields
        .iter()
        .map(|f| (f.name(), f.data_type()))
        .collect();
    let transformed_fields: Vec<Arc<Field>> = file_schema
        .fields
        .iter()
        .map(
            |field| match (table_fields.get(field.name()), field.data_type()) {
                // table schema uses string type, coerce the file schema to use string type
                (
                    Some(DataType::Utf8),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                ) => {
                    transform = true;
                    field_with_new_type(field, DataType::Utf8)
                }
                // table schema uses large string type, coerce the file schema to use large string type
                (
                    Some(DataType::LargeUtf8),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                ) => {
                    transform = true;
                    field_with_new_type(field, DataType::LargeUtf8)
                }
                // table schema uses string view type, coerce the file schema to use view type
                (
                    Some(DataType::Utf8View),
                    DataType::Binary | DataType::LargeBinary | DataType::BinaryView,
                ) => {
                    transform = true;
                    field_with_new_type(field, DataType::Utf8View)
                }
                _ => Arc::clone(field),
            },
        )
        .collect();

    if !transform {
        None
    } else {
        Some(Schema::new_with_metadata(
            transformed_fields,
            file_schema.metadata.clone(),
        ))
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::ops::Range;
    use std::sync::Mutex;

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::test::object_store::local_unpartitioned_file;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{
        Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
        PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };

    pub async fn scan_format(
        state: &dyn Session,
        format: &dyn FileFormat,
        store_root: &str,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let store = Arc::new(LocalFileSystem::new()) as _;
        let meta = local_unpartitioned_file(format!("{store_root}/{file_name}"));

        let file_schema = format
            .infer_schema(state, &store, std::slice::from_ref(&meta))
            .await?;

        let statistics = format
            .infer_stats(state, &store, file_schema.clone(), &meta)
            .await?;

        let file_groups = vec![vec![PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
            metadata_size_hint: None,
        }]];

        let exec = format
            .create_physical_plan(
                state,
                FileScanConfig::new(
                    ObjectStoreUrl::local_filesystem(),
                    file_schema,
                    format.file_source(),
                )
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .with_projection(projection)
                .with_limit(limit),
                None,
            )
            .await?;
        Ok(exec)
    }

    /// Mock ObjectStore to provide an variable stream of bytes on get
    /// Able to keep track of how many iterations of the provided bytes were repeated
    #[derive(Debug)]
    pub struct VariableStream {
        bytes_to_repeat: Bytes,
        max_iterations: usize,
        iterations_detected: Arc<Mutex<usize>>,
    }

    impl Display for VariableStream {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "VariableStream")
        }
    }

    #[async_trait]
    impl ObjectStore for VariableStream {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
            let bytes = self.bytes_to_repeat.clone();
            let range = 0..bytes.len() * self.max_iterations;
            let arc = self.iterations_detected.clone();
            let stream = futures::stream::repeat_with(move || {
                let arc_inner = arc.clone();
                *arc_inner.lock().unwrap() += 1;
                Ok(bytes.clone())
            })
            .take(self.max_iterations)
            .boxed();

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: Default::default(),
                    size: range.end,
                    e_tag: None,
                    version: None,
                },
                range: Default::default(),
                attributes: Attributes::default(),
            })
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _opts: GetOptions,
        ) -> object_store::Result<GetResult> {
            unimplemented!()
        }

        async fn get_ranges(
            &self,
            _location: &Path,
            _ranges: &[Range<usize>],
        ) -> object_store::Result<Vec<Bytes>> {
            unimplemented!()
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(
            &self,
            _from: &Path,
            _to: &Path,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    impl VariableStream {
        pub fn new(bytes_to_repeat: Bytes, max_iterations: usize) -> Self {
            Self {
                bytes_to_repeat,
                max_iterations,
                iterations_detected: Arc::new(Mutex::new(0)),
            }
        }

        pub fn get_iterations_detected(&self) -> usize {
            *self.iterations_detected.lock().unwrap()
        }
    }
}
