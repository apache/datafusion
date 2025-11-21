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

//! Execution plan for reading Arrow IPC files
//!
//! # Naming Note
//!
//! The naming in this module can be confusing:
//! - `ArrowFileOpener` handles the Arrow IPC **file format**
//!   (with footer, supports parallel reading)
//! - `ArrowStreamFileOpener` handles the Arrow IPC **stream format**
//!   (without footer, sequential only)
//! - `ArrowSource` is the unified `FileSource` implementation that uses either opener
//!   depending on the format specified at construction
//!
//! Despite the name "ArrowStreamFileOpener", it still reads from files - the "Stream"
//! refers to the Arrow IPC stream format, not streaming I/O. Both formats can be stored
//! in files on disk or object storage.

use std::sync::Arc;
use std::{any::Any, io::Cursor};

use datafusion_datasource::schema_adapter::{
    DefaultSchemaAdapterFactory, SchemaAdapterFactory,
};
use datafusion_datasource::{as_file_source, TableSchema};

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::{FileDecoder, FileReader, StreamReader};
use datafusion_common::error::Result;
use datafusion_common::exec_datafusion_err;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::ProjectionExprs;

use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::file_stream::FileOpener;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore};

/// Enum indicating which Arrow IPC format to use
#[derive(Clone, Copy, Debug)]
enum ArrowFormat {
    /// Arrow IPC file format (with footer, supports parallel reading)
    File,
    /// Arrow IPC stream format (without footer, sequential only)
    Stream,
}

/// `FileOpener` for Arrow IPC stream format. Supports only sequential reading.
pub(crate) struct ArrowStreamFileOpener {
    object_store: Arc<dyn ObjectStore>,
    projection: Option<Vec<usize>>,
    projected_schema: Option<SchemaRef>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl FileOpener for ArrowStreamFileOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        if partitioned_file.range.is_some() {
            return Err(exec_datafusion_err!(
                "ArrowStreamFileOpener does not support range-based reading"
            ));
        }
        let object_store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        let projected_schema = self.projected_schema.clone();
        let schema_adapter_factory = self.schema_adapter_factory.clone();

        Ok(Box::pin(async move {
            let r = object_store
                .get(&partitioned_file.object_meta.location)
                .await?;

            let stream = match r.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(file, _) => futures::stream::iter(
                    StreamReader::try_new(file.try_clone()?, projection.clone())?,
                )
                .map(|r| r.map_err(Into::into))
                .boxed(),
                GetResultPayload::Stream(_) => {
                    let bytes = r.bytes().await?;
                    let cursor = Cursor::new(bytes);
                    futures::stream::iter(StreamReader::try_new(
                        cursor,
                        projection.clone(),
                    )?)
                    .map(|r| r.map_err(Into::into))
                    .boxed()
                }
            };

            // If we have a schema adapter factory and projected schema, use them to normalize the schema
            if let (Some(factory), Some(proj_schema)) =
                (schema_adapter_factory, projected_schema)
            {
                Ok(stream
                    .and_then(move |batch| {
                        let factory = Arc::clone(&factory);
                        let proj_schema = Arc::clone(&proj_schema);
                        async move {
                            let schema_adapter =
                                factory.create_with_projected_schema(proj_schema);
                            let (schema_mapper, _) =
                                schema_adapter.map_schema(batch.schema().as_ref())?;
                            schema_mapper.map_batch(batch)
                        }
                    })
                    .boxed())
            } else {
                Ok(stream)
            }
        }))
    }
}

/// `FileOpener` for Arrow IPC file format. Supports range-based parallel reading.
pub(crate) struct ArrowFileOpener {
    object_store: Arc<dyn ObjectStore>,
    projection: Option<Vec<usize>>,
    projected_schema: Option<SchemaRef>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl FileOpener for ArrowFileOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        let projected_schema = self.projected_schema.clone();
        let schema_adapter_factory = self.schema_adapter_factory.clone();

        Ok(Box::pin(async move {
            let range = partitioned_file.range.clone();
            match range {
                None => {
                    let r = object_store
                        .get(&partitioned_file.object_meta.location)
                        .await?;
                    let stream = match r.payload {
                        #[cfg(not(target_arch = "wasm32"))]
                        GetResultPayload::File(file, _) => futures::stream::iter(
                            FileReader::try_new(file.try_clone()?, projection.clone())?,
                        )
                        .map(|r| r.map_err(Into::into))
                        .boxed(),
                        GetResultPayload::Stream(_) => {
                            let bytes = r.bytes().await?;
                            let cursor = Cursor::new(bytes);
                            futures::stream::iter(FileReader::try_new(
                                cursor,
                                projection.clone(),
                            )?)
                            .map(|r| r.map_err(Into::into))
                            .boxed()
                        }
                    };

                    // Apply schema adaptation if available
                    if let (Some(factory), Some(proj_schema)) =
                        (schema_adapter_factory, projected_schema)
                    {
                        Ok(stream
                            .and_then(move |batch| {
                                let factory = Arc::clone(&factory);
                                let proj_schema = Arc::clone(&proj_schema);
                                async move {
                                    let schema_adapter =
                                        factory.create_with_projected_schema(proj_schema);
                                    let (schema_mapper, _) = schema_adapter
                                        .map_schema(batch.schema().as_ref())?;
                                    schema_mapper.map_batch(batch)
                                }
                            })
                            .boxed())
                    } else {
                        Ok(stream)
                    }
                }
                Some(range) => {
                    // range is not none, the file maybe split into multiple parts to scan in parallel
                    // get footer_len firstly
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10)),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(&partitioned_file.object_meta.location, get_option)
                        .await?;
                    let footer_len_buf = get_result.bytes().await?;
                    let footer_len = arrow_ipc::reader::read_footer_length(
                        footer_len_buf[..].try_into().unwrap(),
                    )?;
                    // read footer according to footer_len
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10 + (footer_len as u64))),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(&partitioned_file.object_meta.location, get_option)
                        .await?;
                    let footer_buf = get_result.bytes().await?;
                    let footer = arrow_ipc::root_as_footer(
                        footer_buf[..footer_len].try_into().unwrap(),
                    )
                    .map_err(|err| {
                        exec_datafusion_err!("Unable to get root as footer: {err:?}")
                    })?;
                    // build decoder according to footer & projection
                    let schema =
                        arrow_ipc::convert::fb_to_schema(footer.schema().unwrap());
                    let mut decoder = FileDecoder::new(schema.into(), footer.version());
                    if let Some(projection) = projection {
                        decoder = decoder.with_projection(projection);
                    }
                    let dict_ranges = footer
                        .dictionaries()
                        .iter()
                        .flatten()
                        .map(|block| {
                            let block_len =
                                block.bodyLength() as u64 + block.metaDataLength() as u64;
                            let block_offset = block.offset() as u64;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();
                    let dict_results = object_store
                        .get_ranges(&partitioned_file.object_meta.location, &dict_ranges)
                        .await?;
                    for (dict_block, dict_result) in
                        footer.dictionaries().iter().flatten().zip(dict_results)
                    {
                        decoder
                            .read_dictionary(dict_block, &Buffer::from(dict_result))?;
                    }

                    // filter recordbatches according to range
                    let recordbatches = footer
                        .recordBatches()
                        .iter()
                        .flatten()
                        .filter(|block| {
                            let block_offset = block.offset() as u64;
                            block_offset >= range.start as u64
                                && block_offset < range.end as u64
                        })
                        .copied()
                        .collect_vec();

                    let recordbatch_ranges = recordbatches
                        .iter()
                        .map(|block| {
                            let block_len =
                                block.bodyLength() as u64 + block.metaDataLength() as u64;
                            let block_offset = block.offset() as u64;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();

                    let recordbatch_results = object_store
                        .get_ranges(
                            &partitioned_file.object_meta.location,
                            &recordbatch_ranges,
                        )
                        .await?;

                    let stream = futures::stream::iter(
                        recordbatches
                            .into_iter()
                            .zip(recordbatch_results)
                            .filter_map(move |(block, data)| {
                                decoder
                                    .read_record_batch(&block, &Buffer::from(data))
                                    .transpose()
                            }),
                    )
                    .map(|r| r.map_err(Into::into))
                    .boxed();

                    // Apply schema adaptation if available
                    if let (Some(factory), Some(proj_schema)) =
                        (schema_adapter_factory, projected_schema)
                    {
                        Ok(stream
                            .and_then(move |batch| {
                                let factory = Arc::clone(&factory);
                                let proj_schema = Arc::clone(&proj_schema);
                                async move {
                                    let schema_adapter =
                                        factory.create_with_projected_schema(proj_schema);
                                    let (schema_mapper, projection) = schema_adapter
                                        .map_schema(batch.schema().as_ref())?;
                                    let batch = batch.project(&projection)?;
                                    schema_mapper.map_batch(batch)
                                }
                            })
                            .boxed())
                    } else {
                        Ok(stream)
                    }
                }
            }
        }))
    }
}

/// `FileSource` for both Arrow IPC file and stream formats
#[derive(Clone)]
pub struct ArrowSource {
    format: ArrowFormat,
    metrics: ExecutionPlanMetricsSet,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
    projection: SplitProjection,
    table_schema: TableSchema,
}

impl ArrowSource {
    /// Creates an [`ArrowSource`] for file format
    pub fn new_file_source(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            format: ArrowFormat::File,
            metrics: ExecutionPlanMetricsSet::new(),
            schema_adapter_factory: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
        }
    }

    /// Creates an [`ArrowSource`] for stream format
    pub fn new_stream_file_source(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            format: ArrowFormat::Stream,
            metrics: ExecutionPlanMetricsSet::new(),
            schema_adapter_factory: None,
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
        }
    }
}

impl FileSource for ArrowSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let split_projection = self.projection.clone();
        // For schema adaptation, we only use the file schema (not partition columns)
        let projected_file_schema = SchemaRef::from(
            self.table_schema
                .file_schema()
                .project(&split_projection.file_indices)?,
        );

        // Use provided schema adapter factory, or default to DefaultSchemaAdapterFactory
        // This ensures schema normalization (removing metadata differences) happens during execution
        let schema_adapter_factory = self
            .schema_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultSchemaAdapterFactory));

        let opener: Arc<dyn FileOpener> = match self.format {
            ArrowFormat::File => Arc::new(ArrowFileOpener {
                object_store,
                projection: Some(split_projection.file_indices.clone()),
                projected_schema: Some(Arc::clone(&projected_file_schema)),
                schema_adapter_factory: Some(schema_adapter_factory),
            }),
            ArrowFormat::Stream => Arc::new(ArrowStreamFileOpener {
                object_store,
                projection: Some(split_projection.file_indices.clone()),
                projected_schema: Some(projected_file_schema),
                schema_adapter_factory: Some(schema_adapter_factory),
            }),
        };
        ProjectionOpener::try_new(
            split_projection,
            opener,
            self.table_schema.file_schema(),
        )
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        match self.format {
            ArrowFormat::File => "arrow",
            ArrowFormat::Stream => "arrow_stream",
        }
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        match self.format {
            ArrowFormat::Stream => {
                // The Arrow IPC stream format doesn't support range-based parallel reading
                // because it lacks a footer with the information that would be needed to
                // make range-based parallel reading practical. Without the data in the
                // footer you would either need to read the the entire file and record the
                // offsets of the record batches and dictionaries, essentially recreating
                // the footer's contents, or else each partition would need to read the
                // entire file up to the correct offset which is a lot of duplicate I/O.
                // We're opting to avoid that entirely by only acting on a single partition
                // and reading sequentially.
                Ok(None)
            }
            ArrowFormat::File => {
                // Use the default trait implementation logic for file format
                use datafusion_datasource::file_groups::FileGroupPartitioner;

                if config.file_compression_type.is_compressed()
                    || config.new_lines_in_values
                {
                    return Ok(None);
                }

                let repartitioned_file_groups_option = FileGroupPartitioner::new()
                    .with_target_partitions(target_partitions)
                    .with_repartition_file_min_size(repartition_file_min_size)
                    .with_preserve_order_within_groups(output_ordering.is_some())
                    .repartition_file_groups(&config.file_groups);

                if let Some(repartitioned_file_groups) = repartitioned_file_groups_option
                {
                    let mut source = config.clone();
                    source.file_groups = repartitioned_file_groups;
                    return Ok(Some(source));
                }
                Ok(None)
            }
        }
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        source.projection = SplitProjection::new(
            self.table_schema().file_schema(),
            &source.projection.source.try_merge(projection)?,
        );
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }
}

/// `FileOpener` wrapper for both Arrow IPC file and stream formats
pub struct ArrowOpener {
    pub inner: Arc<dyn FileOpener>,
}

impl FileOpener for ArrowOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        self.inner.open(partitioned_file)
    }
}

impl ArrowOpener {
    /// Creates a new [`ArrowOpener`]
    pub fn new(inner: Arc<dyn FileOpener>) -> Self {
        Self { inner }
    }

    pub fn new_file_opener(
        object_store: Arc<dyn ObjectStore>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            inner: Arc::new(ArrowFileOpener {
                object_store,
                projection,
                projected_schema: None,
                schema_adapter_factory: None,
            }),
        }
    }

    pub fn new_stream_file_opener(
        object_store: Arc<dyn ObjectStore>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            inner: Arc::new(ArrowStreamFileOpener {
                object_store,
                projection,
                projected_schema: None,
                schema_adapter_factory: None,
            }),
        }
    }
}

impl From<ArrowSource> for Arc<dyn FileSource> {
    fn from(source: ArrowSource) -> Self {
        as_file_source(source)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Read};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_ipc::reader::{FileReader, StreamReader};
    use bytes::Bytes;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn test_file_opener_without_ranges() -> Result<()> {
        for filename in ["example.arrow", "example_stream.arrow"] {
            let path = format!("tests/data/{filename}");
            let path_str = path.as_str();
            let mut file = File::open(path_str)?;
            let file_size = file.metadata()?.len();

            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;
            let bytes = Bytes::from(buffer);

            let object_store = Arc::new(InMemory::new());
            let partitioned_file = PartitionedFile::new(filename, file_size);
            object_store
                .put(&partitioned_file.object_meta.location, bytes.into())
                .await?;

            let schema = match FileReader::try_new(File::open(path_str)?, None) {
                Ok(reader) => reader.schema(),
                Err(_) => StreamReader::try_new(File::open(path_str)?, None)?.schema(),
            };

            let source: Arc<dyn FileSource> = if filename.contains("stream") {
                Arc::new(ArrowSource::new_stream_file_source(schema))
            } else {
                Arc::new(ArrowSource::new_file_source(schema))
            };

            let scan_config = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                source.clone(),
            )
            .build();

            let file_opener = source.create_file_opener(object_store, &scan_config, 0)?;
            let mut stream = file_opener.open(partitioned_file)?.await?;

            assert!(stream.next().await.is_some());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_file_opener_with_ranges() -> Result<()> {
        let filename = "example.arrow";
        let path = format!("tests/data/{filename}");
        let path_str = path.as_str();
        let mut file = File::open(path_str)?;
        let file_size = file.metadata()?.len();

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let bytes = Bytes::from(buffer);

        let object_store = Arc::new(InMemory::new());
        let partitioned_file = PartitionedFile::new_with_range(
            filename.into(),
            file_size,
            0,
            (file_size - 1) as i64,
        );
        object_store
            .put(&partitioned_file.object_meta.location, bytes.into())
            .await?;

        let schema = FileReader::try_new(File::open(path_str)?, None)?.schema();

        let source = Arc::new(ArrowSource::new_file_source(schema));

        let scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            source.clone(),
        )
        .build();

        let file_opener = source.create_file_opener(object_store, &scan_config, 0)?;
        let mut stream = file_opener.open(partitioned_file)?.await?;

        assert!(stream.next().await.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_opener_errors_with_ranges() -> Result<()> {
        let filename = "example_stream.arrow";
        let path = format!("tests/data/{filename}");
        let path_str = path.as_str();
        let mut file = File::open(path_str)?;
        let file_size = file.metadata()?.len();

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let bytes = Bytes::from(buffer);

        let object_store = Arc::new(InMemory::new());
        let partitioned_file = PartitionedFile::new_with_range(
            filename.into(),
            file_size,
            0,
            (file_size - 1) as i64,
        );
        object_store
            .put(&partitioned_file.object_meta.location, bytes.into())
            .await?;

        let schema = StreamReader::try_new(File::open(path_str)?, None)?.schema();

        let source = Arc::new(ArrowSource::new_stream_file_source(schema));

        let scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            source.clone(),
        )
        .build();

        let file_opener = source.create_file_opener(object_store, &scan_config, 0)?;
        let result = file_opener.open(partitioned_file);
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_arrow_stream_repartitioning_not_supported() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("f0", DataType::Int64, false)]));
        let source = ArrowSource::new_stream_file_source(schema);

        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .build();

        for target_partitions in [2, 4, 8, 16] {
            let result =
                source.repartitioned(target_partitions, 1024 * 1024, None, &config)?;

            assert!(
                result.is_none(),
                "Stream format should not support repartitioning with {target_partitions} partitions",
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_stream_opener_with_projection() -> Result<()> {
        let filename = "example_stream.arrow";
        let path = format!("tests/data/{filename}");
        let path_str = path.as_str();
        let mut file = File::open(path_str)?;
        let file_size = file.metadata()?.len();

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let bytes = Bytes::from(buffer);

        let object_store = Arc::new(InMemory::new());
        let partitioned_file = PartitionedFile::new(filename, file_size);
        object_store
            .put(&partitioned_file.object_meta.location, bytes.into())
            .await?;

        let opener = ArrowStreamFileOpener {
            object_store,
            projection: Some(vec![0]), // just the first column
            projected_schema: None,
            schema_adapter_factory: None,
        };

        let mut stream = opener.open(partitioned_file)?.await?;

        if let Some(batch) = stream.next().await {
            let batch = batch?;
            assert_eq!(
                batch.num_columns(),
                1,
                "Projection should result in 1 column"
            );
        } else {
            panic!("Expected at least one batch");
        }

        Ok(())
    }
}
