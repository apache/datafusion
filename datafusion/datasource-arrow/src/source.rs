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
//! - `ArrowFileSource` / `ArrowFileOpener` handle the Arrow IPC **file format**
//!   (with footer, supports parallel reading)
//! - `ArrowStreamFileSource` / `ArrowStreamFileOpener` handle the Arrow IPC **stream format**
//!   (without footer, sequential only)
//!
//! Despite the name "ArrowStreamFileSource", it still reads from files - the "Stream"
//! refers to the Arrow IPC stream format, not streaming I/O. Both formats can be stored
//! in files on disk or object storage.

use std::sync::Arc;
use std::{any::Any, io::Cursor};

use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::{as_file_source, TableSchema};

use arrow::buffer::Buffer;
use arrow::ipc::reader::{FileDecoder, FileReader, StreamReader};
use datafusion_common::error::Result;
use datafusion_common::{exec_datafusion_err, Statistics};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;

use datafusion_datasource::file_stream::FileOpenFuture;
use datafusion_datasource::file_stream::FileOpener;
use futures::StreamExt;
use itertools::Itertools;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore};

/// `FileSource` for Arrow IPC file format. Supports range-based parallel reading.
#[derive(Clone)]
pub(crate) struct ArrowFileSource {
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl ArrowFileSource {
    /// Initialize an ArrowFileSource with the provided schema
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            table_schema: table_schema.into(),
            metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            schema_adapter_factory: None,
        }
    }
}

impl From<ArrowFileSource> for Arc<dyn FileSource> {
    fn from(source: ArrowFileSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for ArrowFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(ArrowFileOpener {
            object_store,
            projection: base_config.file_column_projection_indices(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        "arrow"
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
}

/// `FileSource` for Arrow IPC stream format. Supports only sequential reading.
#[derive(Clone)]
pub(crate) struct ArrowStreamFileSource {
    table_schema: TableSchema,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl ArrowStreamFileSource {
    /// Initialize an ArrowStreamFileSource with the provided schema
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            table_schema: table_schema.into(),
            metrics: ExecutionPlanMetricsSet::new(),
            projected_statistics: None,
            schema_adapter_factory: None,
        }
    }
}

impl From<ArrowStreamFileSource> for Arc<dyn FileSource> {
    fn from(source: ArrowStreamFileSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for ArrowStreamFileSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(ArrowStreamFileOpener {
            object_store,
            projection: base_config.file_column_projection_indices(),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self { ..self.clone() })
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
        _config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
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

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        let statistics = &self.projected_statistics;
        Ok(statistics
            .clone()
            .expect("projected_statistics must be set"))
    }

    fn file_type(&self) -> &str {
        "arrow_stream"
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

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }
}

/// `FileOpener` for Arrow IPC stream format. Supports only sequential reading.
pub(crate) struct ArrowStreamFileOpener {
    object_store: Arc<dyn ObjectStore>,
    projection: Option<Vec<usize>>,
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
        Ok(Box::pin(async move {
            let r = object_store
                .get(&partitioned_file.object_meta.location)
                .await?;
            match r.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(file, _) => Ok(futures::stream::iter(
                    StreamReader::try_new(file.try_clone()?, projection.clone())?,
                )
                .map(|r| r.map_err(Into::into))
                .boxed()),
                GetResultPayload::Stream(_) => {
                    let bytes = r.bytes().await?;
                    let cursor = Cursor::new(bytes);
                    Ok(futures::stream::iter(StreamReader::try_new(
                        cursor,
                        projection.clone(),
                    )?)
                    .map(|r| r.map_err(Into::into))
                    .boxed())
                }
            }
        }))
    }
}

/// `FileOpener` for Arrow IPC file format. Supports range-based parallel reading.
pub(crate) struct ArrowFileOpener {
    object_store: Arc<dyn ObjectStore>,
    projection: Option<Vec<usize>>,
}

impl FileOpener for ArrowFileOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        Ok(Box::pin(async move {
            let range = partitioned_file.range.clone();
            match range {
                None => {
                    let r = object_store
                        .get(&partitioned_file.object_meta.location)
                        .await?;
                    match r.payload {
                        #[cfg(not(target_arch = "wasm32"))]
                        GetResultPayload::File(file, _) => Ok(futures::stream::iter(
                            FileReader::try_new(file.try_clone()?, projection.clone())?,
                        )
                        .map(|r| r.map_err(Into::into))
                        .boxed()),
                        GetResultPayload::Stream(_) => {
                            let bytes = r.bytes().await?;
                            let cursor = Cursor::new(bytes);
                            Ok(futures::stream::iter(FileReader::try_new(
                                cursor,
                                projection.clone(),
                            )?)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                        }
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

                    Ok(futures::stream::iter(
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
                    .boxed())
                }
            }
        }))
    }
}

/// `FileSource` wrapper for both Arrow IPC file and stream formats
#[derive(Clone)]
pub struct ArrowSource {
    pub(crate) inner: Arc<dyn FileSource>,
}

impl ArrowSource {
    /// Creates a new [`ArrowSource`]
    pub fn new(inner: Arc<dyn FileSource>) -> Self {
        Self { inner }
    }

    /// Creates an [`ArrowSource`] for file format
    pub fn new_file_source(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            inner: Arc::new(ArrowFileSource::new(table_schema)),
        }
    }

    /// Creates an [`ArrowSource`] for stream format
    pub fn new_stream_file_source(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            inner: Arc::new(ArrowStreamFileSource::new(table_schema)),
        }
    }
}

impl FileSource for ArrowSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        self.inner
            .create_file_opener(object_store, base_config, partition)
    }

    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_batch_size(batch_size),
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_projection(config),
        })
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner: self.inner.with_statistics(statistics),
        })
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            inner: self
                .inner
                .with_schema_adapter_factory(schema_adapter_factory)?,
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.inner.schema_adapter_factory()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
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
                Arc::new(ArrowStreamFileSource::new(schema))
            } else {
                Arc::new(ArrowFileSource::new(schema))
            };

            let scan_config = FileScanConfigBuilder::new(
                ObjectStoreUrl::local_filesystem(),
                source.clone(),
            )
            .build();

            let file_opener = source.create_file_opener(object_store, &scan_config, 0);
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

        let source = Arc::new(ArrowFileSource::new(schema));

        let scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            source.clone(),
        )
        .build();

        let file_opener = source.create_file_opener(object_store, &scan_config, 0);
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

        let source = Arc::new(ArrowStreamFileSource::new(schema));

        let scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            source.clone(),
        )
        .build();

        let file_opener = source.create_file_opener(object_store, &scan_config, 0);
        let result = file_opener.open(partitioned_file);
        assert!(result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_arrow_stream_repartitioning_not_supported() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("f0", DataType::Int64, false)]));
        let source = ArrowStreamFileSource::new(schema);

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
