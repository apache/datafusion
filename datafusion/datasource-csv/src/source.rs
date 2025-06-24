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

//! Execution plan for reading CSV files

use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use std::any::Any;
use std::fmt;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::task::Poll;

use datafusion_datasource::decoder::{deserialize_stream, DecoderDeserializer};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_meta::FileMeta;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{
    as_file_source, calculate_range, FileRange, ListingTableUrl, RangeCalculation,
};

use arrow::csv;
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::{
    DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
};

use crate::file_format::CsvDecoder;
use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::{GetOptions, GetResultPayload, ObjectStore};
use tokio::io::AsyncWriteExt;

/// A Config for [`CsvOpener`]
///
/// # Example: create a `DataSourceExec` for CSV
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::Schema;
/// # use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
/// # use datafusion_datasource::PartitionedFile;
/// # use datafusion_datasource_csv::source::CsvSource;
/// # use datafusion_execution::object_store::ObjectStoreUrl;
/// # use datafusion_datasource::source::DataSourceExec;
///
/// # let object_store_url = ObjectStoreUrl::local_filesystem();
/// # let file_schema = Arc::new(Schema::empty());
///
/// let source = Arc::new(CsvSource::new(
///         true,
///         b',',
///         b'"',
///     )
///     .with_terminator(Some(b'#')
/// ));
/// // Create a DataSourceExec for reading the first 100MB of `file1.csv`
/// let config = FileScanConfigBuilder::new(object_store_url, file_schema, source)
///     .with_file(PartitionedFile::new("file1.csv", 100*1024*1024))
///     .with_newlines_in_values(true) // The file contains newlines in values;
///     .build();
/// let exec = (DataSourceExec::from_data_source(config));
/// ```
#[derive(Debug, Clone, Default)]
pub struct CsvSource {
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    file_projection: Option<Vec<usize>>,
    pub(crate) has_header: bool,
    delimiter: u8,
    quote: u8,
    terminator: Option<u8>,
    escape: Option<u8>,
    comment: Option<u8>,
    metrics: ExecutionPlanMetricsSet,
    projected_statistics: Option<Statistics>,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl CsvSource {
    /// Returns a [`CsvSource`]
    pub fn new(has_header: bool, delimiter: u8, quote: u8) -> Self {
        Self {
            has_header,
            delimiter,
            quote,
            ..Self::default()
        }
    }

    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The quote character
    pub fn quote(&self) -> u8 {
        self.quote
    }

    /// The line terminator
    pub fn terminator(&self) -> Option<u8> {
        self.terminator
    }

    /// Lines beginning with this byte are ignored.
    pub fn comment(&self) -> Option<u8> {
        self.comment
    }

    /// The escape character
    pub fn escape(&self) -> Option<u8> {
        self.escape
    }

    /// Initialize a CsvSource with escape
    pub fn with_escape(&self, escape: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.escape = escape;
        conf
    }

    /// Initialize a CsvSource with terminator
    pub fn with_terminator(&self, terminator: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.terminator = terminator;
        conf
    }

    /// Initialize a CsvSource with comment
    pub fn with_comment(&self, comment: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.comment = comment;
        conf
    }
}

impl CsvSource {
    fn open<R: Read>(&self, reader: R) -> Result<csv::Reader<R>> {
        Ok(self.builder().build(reader)?)
    }

    fn builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new(Arc::clone(
            self.file_schema
                .as_ref()
                .expect("Schema must be set before initializing builder"),
        ))
        .with_delimiter(self.delimiter)
        .with_batch_size(
            self.batch_size
                .expect("Batch size must be set before initializing builder"),
        )
        .with_header(self.has_header)
        .with_quote(self.quote);
        if let Some(terminator) = self.terminator {
            builder = builder.with_terminator(terminator);
        }
        if let Some(proj) = &self.file_projection {
            builder = builder.with_projection(proj.clone());
        }
        if let Some(escape) = self.escape {
            builder = builder.with_escape(escape)
        }
        if let Some(comment) = self.comment {
            builder = builder.with_comment(comment);
        }

        builder
    }
}

/// A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]
pub struct CsvOpener {
    config: Arc<CsvSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl CsvOpener {
    /// Returns a [`CsvOpener`]
    pub fn new(
        config: Arc<CsvSource>,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            config,
            file_compression_type,
            object_store,
        }
    }
}

impl From<CsvSource> for Arc<dyn FileSource> {
    fn from(source: CsvSource) -> Self {
        as_file_source(source)
    }
}

impl FileSource for CsvSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(CsvOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_schema = Some(schema);
        Arc::new(conf)
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.projected_statistics = Some(statistics);
        Arc::new(conf)
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.file_projection = config.file_column_projection_indices();
        Arc::new(conf)
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
        "csv"
    }
    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, ", has_header={}", self.has_header)
            }
            DisplayFormatType::TreeRender => Ok(()),
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
}

impl FileOpener for CsvOpener {
    /// Open a partitioned CSV file.
    ///
    /// If `file_meta.range` is `None`, the entire file is opened.
    /// If `file_meta.range` is `Some(FileRange {start, end})`, this signifies that the partition
    /// corresponds to the byte range [start, end) within the file.
    ///
    /// Note: `start` or `end` might be in the middle of some lines. In such cases, the following rules
    /// are applied to determine which lines to read:
    /// 1. The first line of the partition is the line in which the index of the first character >= `start`.
    /// 2. The last line of the partition is the line in which the byte at position `end - 1` resides.
    ///
    /// Examples:
    /// Consider the following partitions enclosed by braces `{}`:
    ///
    /// {A,1,2,3,4,5,6,7,8,9\n
    ///  A,1,2,3,4,5,6,7,8,9\n}
    ///  A,1,2,3,4,5,6,7,8,9\n
    ///  The lines read would be: [0, 1]
    ///
    ///  A,{1,2,3,4,5,6,7,8,9\n
    ///  A,1,2,3,4,5,6,7,8,9\n
    ///  A},1,2,3,4,5,6,7,8,9\n
    ///  The lines read would be: [1, 2]
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        // `self.config.has_header` controls whether to skip reading the 1st line header
        // If the .csv file is read in parallel and this `CsvOpener` is only reading some middle
        // partition, then don't skip first line
        let mut csv_has_header = self.config.has_header;
        if let Some(FileRange { start, .. }) = file_meta.range {
            if start != 0 {
                csv_has_header = false;
            }
        }

        let config = CsvSource {
            has_header: csv_has_header,
            ..(*self.config).clone()
        };

        let file_compression_type = self.file_compression_type.to_owned();

        if file_meta.range.is_some() {
            assert!(
                !file_compression_type.is_compressed(),
                "Reading compressed .csv in parallel is not supported"
            );
        }

        let store = Arc::clone(&self.object_store);
        let terminator = self.config.terminator;

        Ok(Box::pin(async move {
            // Current partition contains bytes [start_byte, end_byte) (might contain incomplete lines at boundaries)

            let calculated_range =
                calculate_range(&file_meta, &store, terminator).await?;

            let range = match calculated_range {
                RangeCalculation::Range(None) => None,
                RangeCalculation::Range(Some(range)) => Some(range.into()),
                RangeCalculation::TerminateEarly => {
                    return Ok(
                        futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed()
                    )
                }
            };

            let options = GetOptions {
                range,
                ..Default::default()
            };

            let result = store.get_opts(file_meta.location(), options).await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(mut file, _) => {
                    let is_whole_file_scanned = file_meta.range.is_none();
                    let decoder = if is_whole_file_scanned {
                        // Don't seek if no range as breaks FIFO files
                        file_compression_type.convert_read(file)?
                    } else {
                        file.seek(SeekFrom::Start(result.range.start as _))?;
                        file_compression_type.convert_read(
                            file.take((result.range.end - result.range.start) as u64),
                        )?
                    };

                    Ok(futures::stream::iter(config.open(decoder)?).boxed())
                }
                GetResultPayload::Stream(s) => {
                    let decoder = config.builder().build_decoder();
                    let s = s.map_err(DataFusionError::from);
                    let input = file_compression_type.convert_stream(s.boxed())?.fuse();

                    Ok(deserialize_stream(
                        input,
                        DecoderDeserializer::new(CsvDecoder::new(decoder)),
                    ))
                }
            }
        }))
    }
}

pub async fn plan_to_csv(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let writer_buffer_size = task_ctx
        .session_config()
        .options()
        .execution
        .objectstore_writer_buffer_size;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let storeref = Arc::clone(&store);
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.csv", parsed.prefix());
        let file = object_store::path::Path::parse(filename)?;

        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut buf_writer =
                BufWriter::with_capacity(storeref, file.clone(), writer_buffer_size);
            let mut buffer = Vec::with_capacity(1024);
            //only write headers on first iteration
            let mut write_headers = true;
            while let Some(batch) = stream.next().await.transpose()? {
                let mut writer = csv::WriterBuilder::new()
                    .with_header(write_headers)
                    .build(buffer);
                writer.write(&batch)?;
                buffer = writer.into_inner();
                buf_writer.write_all(&buffer).await?;
                buffer.clear();
                //prevent writing headers more than once
                write_headers = false;
            }
            buf_writer.shutdown().await.map_err(DataFusionError::from)
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?, // propagate DataFusion error
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}
