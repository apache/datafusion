// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Execution plan for reading CSV files

use datafusion_datasource::boundary_stream::AlignedBoundaryStream;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_physical_plan::projection::ProjectionExprs;
use std::fmt;
use std::io::Read;
use std::ops::Range;
use std::sync::Arc;

use datafusion_datasource::decoder::{DecoderDeserializer, deserialize_stream};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::{
    FileRange, ListingTableUrl, PartitionedFile, TableSchema, as_file_source,
};

use arrow::array::RecordBatch;
use arrow::csv;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion_common::config::CsvOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, exec_datafusion_err};
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_expr_adapter::{
    BatchAdapter, BatchAdapterFactory, DefaultPhysicalExprAdapterFactory,
    PhysicalExprAdapterFactory,
};
use datafusion_physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion_physical_plan::{
    DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
};

use crate::file_format::CsvDecoder;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::{GetOptions, GetResultPayload, ObjectStore, ObjectStoreExt};
use regex::Regex;
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
/// # use datafusion_common::config::CsvOptions;
///
/// # let object_store_url = ObjectStoreUrl::local_filesystem();
/// # let file_schema = Arc::new(Schema::empty());
///
/// let options = CsvOptions {
///     has_header: Some(true),
///     delimiter: b',',
///     quote: b'"',
///     newlines_in_values: Some(true), // The file contains newlines in values
///     ..Default::default()
/// };
/// let source = Arc::new(CsvSource::new(file_schema.clone())
///     .with_csv_options(options)
///     .with_terminator(Some(b'#'))
/// );
/// // Create a DataSourceExec for reading the first 100MB of `file1.csv`
/// let config = FileScanConfigBuilder::new(object_store_url, source)
///     .with_file(PartitionedFile::new("file1.csv", 100*1024*1024))
///     .build();
/// let exec = (DataSourceExec::from_data_source(config));
/// ```
#[derive(Debug, Clone)]
pub struct CsvSource {
    options: CsvOptions,
    batch_size: Option<usize>,
    table_schema: TableSchema,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
}

impl CsvSource {
    /// Returns a [`CsvSource`]
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            options: CsvOptions::default(),
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Sets the CSV options
    pub fn with_csv_options(mut self, options: CsvOptions) -> Self {
        self.options = options;
        self
    }

    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.options.has_header.unwrap_or(true)
    }

    // true if rows length support truncate
    pub fn truncate_rows(&self) -> bool {
        self.options.truncated_rows.unwrap_or(false)
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.options.delimiter
    }

    /// The quote character
    pub fn quote(&self) -> u8 {
        self.options.quote
    }

    /// The line terminator
    pub fn terminator(&self) -> Option<u8> {
        self.options.terminator
    }

    /// Lines beginning with this byte are ignored.
    pub fn comment(&self) -> Option<u8> {
        self.options.comment
    }

    /// The escape character
    pub fn escape(&self) -> Option<u8> {
        self.options.escape
    }

    /// Regex for fields that should be read as NULL
    pub fn null_regex(&self) -> Option<&str> {
        self.options.null_regex.as_deref()
    }

    /// Initialize a CsvSource with escape
    pub fn with_escape(&self, escape: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.options.escape = escape;
        conf
    }

    /// Initialize a CsvSource with terminator
    pub fn with_terminator(&self, terminator: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.options.terminator = terminator;
        conf
    }

    /// Initialize a CsvSource with comment
    pub fn with_comment(&self, comment: Option<u8>) -> Self {
        let mut conf = self.clone();
        conf.options.comment = comment;
        conf
    }

    /// Whether to support truncate rows when read csv file
    pub fn with_truncate_rows(&self, truncate_rows: bool) -> Self {
        let mut conf = self.clone();
        conf.options.truncated_rows = Some(truncate_rows);
        conf
    }

    /// Whether values may contain newline characters
    #[deprecated(
        since = "56.0.0",
        note = "No replacement getter exists. Retain CsvOptions and read its newlines_in_values field with unwrap_or(false)."
    )]
    pub fn newlines_in_values(&self) -> bool {
        self.options.newlines_in_values.unwrap_or(false)
    }

    /// true if file columns are matched to the table schema by header name
    pub fn match_columns_by_header(&self) -> bool {
        self.options.match_columns_by_header.unwrap_or(false) && self.has_header()
    }

    /// Whether to match file columns to the table schema by header name
    pub fn with_match_columns_by_header(&self, match_by_header: bool) -> Self {
        let mut conf = self.clone();
        conf.options.match_columns_by_header = Some(match_by_header);
        conf
    }
}

impl CsvSource {
    fn open<R: Read>(
        &self,
        reader: R,
        mapping: Option<&HeaderMapping>,
    ) -> Result<csv::Reader<R>> {
        Ok(self.builder(mapping)?.build(reader)?)
    }

    fn builder(&self, mapping: Option<&HeaderMapping>) -> Result<csv::ReaderBuilder> {
        let (schema, projection) = match mapping {
            Some(mapping) => (
                Arc::clone(&mapping.physical_schema),
                mapping.physical_projection.clone(),
            ),
            None => (
                Arc::clone(self.table_schema.file_schema()),
                self.projection.file_indices.clone(),
            ),
        };
        let mut builder = csv::ReaderBuilder::new(schema)
            .with_delimiter(self.delimiter())
            .with_batch_size(
                self.batch_size
                    .expect("Batch size must be set before initializing builder"),
            )
            .with_header(self.has_header())
            .with_quote(self.quote())
            .with_truncated_rows(self.truncate_rows());
        if let Some(terminator) = self.terminator() {
            builder = builder.with_terminator(terminator);
        }
        builder = builder.with_projection(projection);
        if let Some(escape) = self.escape() {
            builder = builder.with_escape(escape)
        }
        if let Some(comment) = self.comment() {
            builder = builder.with_comment(comment);
        }
        if let Some(null_regex) = self.null_regex() {
            let regex = Regex::new(null_regex).map_err(|e| {
                exec_datafusion_err!("Unable to parse CSV null regex '{null_regex}': {e}")
            })?;
            builder = builder.with_null_regex(regex);
        }

        Ok(builder)
    }

    fn parse_header(&self, line: &[u8]) -> Result<Vec<String>> {
        let mut format = csv::reader::Format::default()
            .with_header(true)
            .with_delimiter(self.delimiter())
            .with_quote(self.quote());
        if let Some(terminator) = self.terminator() {
            format = format.with_terminator(terminator);
        }
        if let Some(escape) = self.escape() {
            format = format.with_escape(escape);
        }
        if let Some(comment) = self.comment() {
            format = format.with_comment(comment);
        }
        let (schema, _) = format.infer_schema(line, Some(0))?;
        Ok(schema.fields().iter().map(|f| f.name().clone()).collect())
    }

    /// Builds the mapping for a file whose header row is `line`, or `None` if
    /// the file can be read positionally.
    fn header_mapping(
        &self,
        line: &[u8],
        expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Result<Option<Arc<HeaderMapping>>> {
        if line.is_empty() {
            return Ok(None);
        }
        let header = self.parse_header(line)?;
        HeaderMapping::try_new(
            &header,
            self.table_schema.file_schema(),
            &self.projection.file_indices,
            expr_adapter_factory,
        )
        .map(|mapping| mapping.map(Arc::new))
    }
}

/// Maps the columns of a single CSV file onto the table's file schema by
/// header name.
///
/// Only the header columns that are declared in the table are decoded; the
/// decoded batch is then adapted to the projected file schema with the scan's
/// [`PhysicalExprAdapterFactory`], which is the same mechanism Parquet uses
/// to fill missing columns and reorder by name.
#[derive(Debug)]
struct HeaderMapping {
    /// One field per header column, typed from the table schema where the
    /// name is declared there
    physical_schema: SchemaRef,
    /// Header positions to decode, in the order of the table projection
    physical_projection: Vec<usize>,
    adapter: BatchAdapter,
}

impl HeaderMapping {
    fn try_new(
        header: &[String],
        file_schema: &Schema,
        file_indices: &[usize],
        expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Result<Option<Self>> {
        let matches_positionally = header.len() == file_schema.fields().len()
            && header
                .iter()
                .zip(file_schema.fields())
                .all(|(name, field)| name == field.name());
        if matches_positionally {
            return Ok(None);
        }

        let physical_fields: Vec<Field> = header
            .iter()
            .map(|name| match file_schema.field_with_name(name) {
                Ok(field) => field.clone(),
                Err(_) => Field::new(name, DataType::Utf8, true),
            })
            .collect();

        let physical_projection: Vec<usize> = file_indices
            .iter()
            .filter_map(|&index| {
                let name = file_schema.field(index).name();
                header.iter().position(|column| column == name)
            })
            .collect();

        let physical_schema = Arc::new(Schema::new(physical_fields));
        let decoded_schema = Arc::new(physical_schema.project(&physical_projection)?);
        let target_schema = Arc::new(file_schema.project(file_indices)?);
        let adapter = BatchAdapterFactory::new(target_schema)
            .with_adapter_factory(expr_adapter_factory)
            .make_adapter(&decoded_schema)?;

        Ok(Some(Self {
            physical_schema,
            physical_projection,
            adapter,
        }))
    }

    fn remap(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        self.adapter.adapt_batch(batch)
    }
}

/// Returns the byte range of the header row in `buf`: the first line that is
/// not a comment line, matching how the CSV decoder skips comments. Returns
/// `None` when that line is not yet complete and more input may follow.
fn find_header_line(
    buf: &[u8],
    terminator: u8,
    comment: Option<u8>,
    eof: bool,
) -> Option<Range<usize>> {
    let is_comment = |line: &[u8]| comment.is_some_and(|c| line.first() == Some(&c));
    let mut start = 0;
    loop {
        let rest = &buf[start..];
        match rest.iter().position(|b| *b == terminator) {
            Some(end) if is_comment(&rest[..end]) => start += end + 1,
            Some(end) => return Some(start..start + end),
            None if !eof => return None,
            None if is_comment(rest) => return Some(buf.len()..buf.len()),
            None => return Some(start..buf.len()),
        }
    }
}

/// Reads the header row of `input`, returning it together with a stream that
/// still yields every byte of `input`.
async fn peek_header_line(
    mut input: BoxStream<'static, Result<Bytes>>,
    terminator: u8,
    comment: Option<u8>,
) -> Result<(Vec<u8>, BoxStream<'static, Result<Bytes>>)> {
    let mut prefix = Vec::new();
    let mut buffered = Vec::new();
    loop {
        let chunk = input.next().await.transpose()?;
        let eof = chunk.is_none();
        if let Some(chunk) = chunk {
            prefix.extend_from_slice(&chunk);
            buffered.push(Ok(chunk));
        }
        if let Some(header) = find_header_line(&prefix, terminator, comment, eof) {
            let header = prefix[header].to_vec();
            let buffered = futures::stream::iter(buffered);
            let stream = if eof {
                buffered.boxed()
            } else {
                buffered.chain(input).boxed()
            };
            return Ok((header, stream));
        }
    }
}

/// Reads the header row of an uncompressed file, growing the requested range
/// until it is complete. Also returns the byte offset where the header starts.
async fn fetch_header_line(
    store: &dyn ObjectStore,
    location: &Path,
    file_size: u64,
    terminator: u8,
    comment: Option<u8>,
) -> Result<(Vec<u8>, u64)> {
    let mut len = file_size.min(64 * 1024);
    loop {
        let bytes = store.get_range(location, 0..len).await?;
        let eof = len >= file_size;
        if let Some(header) = find_header_line(&bytes, terminator, comment, eof) {
            return Ok((bytes[header.clone()].to_vec(), header.start as u64));
        }
        len = (len * 2).min(file_size);
    }
}

fn remap_stream(
    stream: BoxStream<'static, Result<RecordBatch>>,
    mapping: Option<Arc<HeaderMapping>>,
) -> BoxStream<'static, Result<RecordBatch>> {
    match mapping {
        Some(mapping) => stream.map(move |batch| mapping.remap(&batch?)).boxed(),
        None => stream,
    }
}

/// A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]
pub struct CsvOpener {
    config: Arc<CsvSource>,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
    partition_index: usize,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
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
            partition_index: 0,
            expr_adapter_factory: Arc::new(DefaultPhysicalExprAdapterFactory),
        }
    }

    /// Set the [`PhysicalExprAdapterFactory`] used to adapt files whose
    /// columns are matched to the table schema by header name
    pub fn with_expr_adapter_factory(
        mut self,
        expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Self {
        self.expr_adapter_factory = expr_adapter_factory;
        self
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
        partition_index: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        if self.options.match_columns_by_header == Some(true) && !self.has_header() {
            log::warn!(
                "CSV option match_columns_by_header is ignored because has_header is false; columns are matched by position"
            );
        }
        let expr_adapter_factory = base_config
            .expr_adapter_factory
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory) as _);
        let mut opener = Arc::new(CsvOpener {
            config: Arc::new(self.clone()),
            file_compression_type: base_config.file_compression_type,
            object_store,
            partition_index,
            expr_adapter_factory,
        }) as Arc<dyn FileOpener>;
        opener = ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::clone(&opener),
            self.table_schema.file_schema(),
        )?;
        Ok(opener)
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "csv"
    }

    fn supports_repartitioning(&self) -> bool {
        // Cannot repartition if values may contain newlines, as record
        // boundaries cannot be determined by byte offset alone
        !self.options.newlines_in_values.unwrap_or(false)
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, ", has_header={}", self.has_header())?;
                if self.match_columns_by_header() {
                    write!(f, ", match_columns_by_header=true")?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        datafusion_physical_plan::apply_expression_roots(self.projection.source.iter(), f)
    }

    /// Emit a `CsvScan` node wrapping the shared base config and CSV options.
    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        base: &FileScanConfig,
        ctx: &datafusion_physical_plan::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        use protobuf::physical_plan_node::PhysicalPlanType;

        let node = protobuf::CsvScanExecNode {
            base_conf: Some(base.try_to_proto(ctx)?),
            has_header: self.has_header(),
            delimiter: proto_byte_to_string(self.delimiter(), "delimiter")?,
            quote: proto_byte_to_string(self.quote(), "quote")?,
            optional_escape: self
                .escape()
                .map(|escape| {
                    Ok::<_, DataFusionError>(
                        protobuf::csv_scan_exec_node::OptionalEscape::Escape(
                            proto_byte_to_string(escape, "escape")?,
                        ),
                    )
                })
                .transpose()?,
            optional_comment: self
                .comment()
                .map(|comment| {
                    Ok::<_, DataFusionError>(
                        protobuf::csv_scan_exec_node::OptionalComment::Comment(
                            proto_byte_to_string(comment, "comment")?,
                        ),
                    )
                })
                .transpose()?,
            newlines_in_values: self.options.newlines_in_values.unwrap_or(false),
            truncate_rows: self.truncate_rows(),
            terminator: self.terminator().map(|terminator| vec![terminator]),
            match_columns_by_header: self.match_columns_by_header(),
        };
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::CsvScan(node)),
        }))
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
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        // `self.config.has_header` controls whether to skip reading the 1st line header
        // If the .csv file is read in parallel and this `CsvOpener` is only reading some middle
        // partition, then don't skip first line
        let match_by_header = self.config.match_columns_by_header();
        let expr_adapter_factory = Arc::clone(&self.expr_adapter_factory);
        let mut csv_has_header = self.config.has_header();
        if let Some(FileRange { start, .. }) = partitioned_file.range
            && start != 0
        {
            csv_has_header = false;
        }

        let mut config = (*self.config).clone();
        config.options.has_header = Some(csv_has_header);
        config.options.truncated_rows = Some(config.truncate_rows());

        let file_compression_type = self.file_compression_type.to_owned();

        if partitioned_file.range.is_some() {
            assert!(
                !file_compression_type.is_compressed(),
                "Reading compressed .csv in parallel is not supported"
            );
        }

        let store = Arc::clone(&self.object_store);
        let terminator = self.config.terminator();
        let line_end = terminator.unwrap_or(b'\n');
        let comment = self.config.comment();

        let baseline_metrics =
            BaselineMetrics::new(&self.config.metrics, self.partition_index);

        Ok(Box::pin(async move {
            // Current partition contains bytes [start_byte, end_byte) (might contain incomplete lines at boundaries)
            let file_size = partitioned_file.object_meta.size;
            let location = partitioned_file.object_meta.location;

            if let Some(file_range) = partitioned_file.range.as_ref() {
                let raw_start: u64 = file_range.start.try_into().map_err(|_| {
                    exec_datafusion_err!(
                        "Expected start range to fit in u64, got {}",
                        file_range.start
                    )
                })?;
                let raw_end: u64 = file_range.end.try_into().map_err(|_| {
                    exec_datafusion_err!(
                        "Expected end range to fit in u64, got {}",
                        file_range.end
                    )
                })?;

                // A partition may start mid-file, so the header is fetched
                // separately. Every byte-range partition of a file repeats this
                // small request; cache per file if that becomes costly.
                let mapping = if match_by_header {
                    let (header, header_start) = fetch_header_line(
                        store.as_ref(),
                        &location,
                        file_size,
                        line_end,
                        comment,
                    )
                    .await?;
                    // A partition starting within leading comment lines owns
                    // the header row, so it has to skip it.
                    if raw_start <= header_start {
                        config.options.has_header = Some(true);
                    }
                    config.header_mapping(&header, Arc::clone(&expr_adapter_factory))?
                } else {
                    None
                };

                let aligned_stream = AlignedBoundaryStream::new(
                    Arc::clone(&store),
                    location.clone(),
                    raw_start,
                    raw_end,
                    file_size,
                    line_end,
                )
                .await?
                .map_err(DataFusionError::from);

                let decoder = config.builder(mapping.as_deref())?.build_decoder();
                let input = file_compression_type
                    .convert_stream(aligned_stream.boxed())?
                    .fuse();
                let stream = deserialize_stream(
                    input,
                    DecoderDeserializer::new(CsvDecoder::new(decoder)),
                );
                return Ok(remap_stream(stream.map_err(Into::into).boxed(), mapping));
            }

            // No range specified — read the entire file
            let options = GetOptions::default();
            let result = store.get_opts(&location, options).await?;

            match result.payload {
                #[cfg(not(target_arch = "wasm32"))]
                GetResultPayload::File(file, _) => {
                    use std::io::{BufRead, BufReader, Cursor};

                    let decoder = file_compression_type.convert_read(file)?;
                    let (decoder, mapping): (Box<dyn Read + Send>, _) = if match_by_header
                    {
                        let mut decoder = BufReader::new(decoder);
                        let mut prefix = Vec::new();
                        let header = loop {
                            let eof = decoder.read_until(line_end, &mut prefix)? == 0;
                            if let Some(header) =
                                find_header_line(&prefix, line_end, comment, eof)
                            {
                                break prefix[header].to_vec();
                            }
                        };
                        let mapping = config
                            .header_mapping(&header, Arc::clone(&expr_adapter_factory))?;
                        (Box::new(Cursor::new(prefix).chain(decoder)), mapping)
                    } else {
                        (decoder, None)
                    };
                    let mut reader = config.open(decoder, mapping.as_deref())?;

                    // Use std::iter::from_fn to wrap execution of iterator's next() method.
                    let iterator = std::iter::from_fn(move || {
                        let mut timer = baseline_metrics.elapsed_compute().timer();
                        let result = reader.next();
                        timer.stop();
                        result
                    });

                    let stream = futures::stream::iter(iterator)
                        .map(|r| r.map_err(Into::into))
                        .boxed();
                    Ok(remap_stream(stream, mapping))
                }
                GetResultPayload::Stream(s) => {
                    let s = s.map_err(DataFusionError::from);
                    let mut input = file_compression_type.convert_stream(s.boxed())?;
                    let mut mapping = None;
                    if match_by_header {
                        let (header, rest) =
                            peek_header_line(input, line_end, comment).await?;
                        mapping = config
                            .header_mapping(&header, Arc::clone(&expr_adapter_factory))?;
                        input = rest;
                    }

                    let decoder = config.builder(mapping.as_deref())?.build_decoder();
                    let stream = deserialize_stream(
                        input.fuse(),
                        DecoderDeserializer::new(CsvDecoder::new(decoder)),
                    );
                    Ok(remap_stream(stream.map_err(Into::into).boxed(), mapping))
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
        let file = Path::parse(filename)?;

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

#[cfg(feature = "proto")]
fn proto_byte_to_string(b: u8, description: &str) -> Result<String> {
    let bytes = &[b];
    let s = std::str::from_utf8(bytes).map_err(|_| {
        datafusion_common::internal_datafusion_err!(
            "Invalid CSV {description}: can not represent {bytes:0x?} as utf8"
        )
    })?;
    Ok(s.to_owned())
}

#[cfg(feature = "proto")]
fn proto_str_to_byte(s: &str, description: &str) -> Result<u8> {
    datafusion_common::assert_eq_or_internal_err!(
        s.len(),
        1,
        "Invalid CSV {description}: expected single character, got {s}"
    );
    Ok(s.as_bytes()[0])
}

#[cfg(feature = "proto")]
fn proto_bytes_to_byte(bytes: &[u8], description: &str) -> Result<u8> {
    let [byte] = bytes else {
        return datafusion_common::internal_err!(
            "Invalid CSV {description}: expected exactly one byte, got {}",
            bytes.len()
        );
    };
    Ok(*byte)
}

#[cfg(feature = "proto")]
impl CsvSource {
    /// Reconstructs a `DataSourceExec` from a protobuf `CsvScan`.
    ///
    /// Payloads without a terminator use the default newline terminator.
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &datafusion_physical_plan::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_common::config::CsvOptions;
        use datafusion_datasource::file_scan_config::FileScanConfig;
        use datafusion_datasource::source::DataSourceExec;
        use datafusion_proto_models::protobuf;

        let Some(protobuf::physical_plan_node::PhysicalPlanType::CsvScan(scan)) =
            &node.physical_plan_type
        else {
            return datafusion_common::internal_err!("PhysicalPlanNode is not a CsvScan");
        };

        let base_conf = scan.base_conf.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "CsvScanExecNode is missing required field 'base_conf'"
            )
        })?;

        let escape = match &scan.optional_escape {
            Some(protobuf::csv_scan_exec_node::OptionalEscape::Escape(escape)) => {
                Some(proto_str_to_byte(escape, "escape")?)
            }
            None => None,
        };
        let comment = match &scan.optional_comment {
            Some(protobuf::csv_scan_exec_node::OptionalComment::Comment(comment)) => {
                Some(proto_str_to_byte(comment, "comment")?)
            }
            None => None,
        };
        let terminator = scan
            .terminator
            .as_deref()
            .map(|terminator| proto_bytes_to_byte(terminator, "terminator"))
            .transpose()?;

        let table_schema = FileScanConfig::parse_table_schema_from_proto(base_conf)?;

        let csv_options = CsvOptions {
            has_header: Some(scan.has_header),
            delimiter: proto_str_to_byte(&scan.delimiter, "delimiter")?,
            quote: proto_str_to_byte(&scan.quote, "quote")?,
            newlines_in_values: Some(scan.newlines_in_values),
            truncated_rows: Some(scan.truncate_rows),
            match_columns_by_header: Some(scan.match_columns_by_header),
            ..Default::default()
        };
        let source = Arc::new(
            CsvSource::new(table_schema)
                .with_csv_options(csv_options)
                .with_escape(escape)
                .with_comment(comment)
                .with_terminator(terminator),
        );

        let conf = FileScanConfig::try_from_proto(base_conf, ctx, source)?;
        Ok(DataSourceExec::from_data_source(conf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

    fn csv_source(schema: SchemaRef, null_regex: Option<&str>) -> CsvSource {
        let options = CsvOptions {
            has_header: Some(true),
            null_regex: null_regex.map(str::to_string),
            ..CsvOptions::default()
        };
        let mut source = CsvSource::new(schema).with_csv_options(options);
        source.batch_size = Some(1024);
        source
    }

    #[test]
    fn null_regex_nulls_matching_string_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let source = csv_source(schema, Some("^(NULL|N/A)$"));

        let data = "id,name\n1,alice\n2,N/A\n3,carol\n";
        let batch = source
            .open(data.as_bytes(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(1), "N/A should be read as NULL");
        assert_eq!(names.value(2), "carol");
    }

    #[test]
    fn null_regex_nulls_matching_values_in_numeric_columns() {
        // Without the regex this fails to parse rather than producing NULL,
        // which is the case null_regex exists for.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ]));
        let source = csv_source(schema, Some("^(NULL|N/A)$"));

        let data = "id,value\n1,10\n2,N/A\n3,30\n";
        let batch = source
            .open(data.as_bytes(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert!(values.is_null(1), "N/A should be read as NULL");
        assert_eq!(values.value(2), 30);
    }

    #[test]
    fn without_null_regex_the_placeholder_is_read_verbatim() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let source = csv_source(schema, None);

        let data = "id,name\n1,alice\n2,N/A\n3,carol\n";
        let batch = source
            .open(data.as_bytes(), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(1), "N/A");
    }

    #[test]
    fn invalid_null_regex_is_reported_as_an_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let source = csv_source(schema, Some("("));

        let err = source.open(b"id\n1\n".as_slice(), None).unwrap_err();
        assert!(
            err.to_string().contains("Unable to parse CSV null regex"),
            "unexpected error: {err}"
        );
    }

    use arrow::array::{Int32Array, RecordBatchOptions};

    fn table_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Int32, true),
        ])
    }

    fn header(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn adapter_factory() -> Arc<dyn PhysicalExprAdapterFactory> {
        Arc::new(DefaultPhysicalExprAdapterFactory)
    }

    #[test]
    fn header_mapping_skipped_when_header_matches() -> Result<()> {
        let mapping = HeaderMapping::try_new(
            &header(&["a", "b", "c"]),
            &table_schema(),
            &[0, 2],
            adapter_factory(),
        )?;
        assert!(mapping.is_none());
        Ok(())
    }

    #[test]
    fn header_mapping_by_name() -> Result<()> {
        let mapping = HeaderMapping::try_new(
            &header(&["c", "extra", "a"]),
            &table_schema(),
            &[0, 1, 2],
            adapter_factory(),
        )?
        .unwrap();

        let physical_types: Vec<_> = mapping
            .physical_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        assert_eq!(
            physical_types,
            vec![DataType::Int32, DataType::Utf8, DataType::Int32]
        );
        assert_eq!(mapping.physical_projection, vec![2, 0]);

        let decoded_schema = mapping.physical_schema.project(&[2, 0])?;
        let decoded = RecordBatch::try_new(
            Arc::new(decoded_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )?;
        let remapped = mapping.remap(&decoded)?;
        assert_eq!(remapped.schema().as_ref(), &table_schema());
        assert_eq!(remapped.column(1).null_count(), 2);
        assert_eq!(
            remapped.column(0).as_ref(),
            &Int32Array::from(vec![1, 2]) as &dyn Array
        );
        assert_eq!(
            remapped.column(2).as_ref(),
            &Int32Array::from(vec![10, 20]) as &dyn Array
        );
        Ok(())
    }

    #[test]
    fn header_mapping_only_missing_columns() -> Result<()> {
        let mapping = HeaderMapping::try_new(
            &header(&["a", "c"]),
            &table_schema(),
            &[1],
            adapter_factory(),
        )?
        .unwrap();
        assert!(mapping.physical_projection.is_empty());

        let decoded = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(3)),
        )?;
        let remapped = mapping.remap(&decoded)?;
        assert_eq!(remapped.num_rows(), 3);
        assert_eq!(remapped.column(0).null_count(), 3);
        Ok(())
    }

    #[test]
    fn header_mapping_missing_non_nullable_column() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, false),
        ]);
        let err =
            HeaderMapping::try_new(&header(&["a"]), &schema, &[0, 1], adapter_factory())
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("Non-nullable column 'b' is missing from the physical schema"),
            "{err}"
        );
    }

    #[test]
    fn parse_header_handles_quotes() -> Result<()> {
        let source = CsvSource::new(Arc::new(table_schema()));
        assert_eq!(
            source.parse_header(b"\"a,1\",b\r")?,
            vec!["a,1".to_string(), "b".to_string()]
        );
        Ok(())
    }

    #[test]
    fn find_header_line_skips_comments() {
        let comment = Some(b'#');
        assert_eq!(
            find_header_line(b"# c1\n#c2\na,b\n1,2\n", b'\n', comment, false),
            Some(9..12)
        );
        assert_eq!(
            find_header_line(b"#c\na,b\n", b'\n', None, false),
            Some(0..2)
        );
        assert_eq!(find_header_line(b"# c1\na,", b'\n', comment, false), None);
        assert_eq!(
            find_header_line(b"# c1\na,b", b'\n', comment, true),
            Some(5..8)
        );
        assert_eq!(
            find_header_line(b"# c1\n# c2", b'\n', comment, true),
            Some(9..9)
        );
        assert_eq!(find_header_line(b"", b'\n', comment, true), Some(0..0));
    }

    #[test]
    fn match_columns_by_header_requires_header() {
        let source = CsvSource::new(Arc::new(table_schema()))
            .with_csv_options(CsvOptions::default().with_has_header(false))
            .with_match_columns_by_header(true);
        assert!(!source.match_columns_by_header());
    }
}
