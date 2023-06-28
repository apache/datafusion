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

use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::listing::FileRange;
use crate::datasource::physical_plan::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::datasource::physical_plan::FileMeta;
use crate::error::Result;
use crate::physical_plan::common::AbortOnDropSingle;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::physical_plan::{
    ordering_equivalence_properties_helper, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::csv;
use arrow::datatypes::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, OrderingEquivalenceProperties};

use super::FileScanConfig;

use bytes::{Buf, Bytes};
use futures::ready;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::{GetOptions, GetResult, ObjectStore};
use std::any::Any;
use std::fs;
use std::io::Cursor;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::task::Poll;
use tokio::task::{self, JoinHandle};

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    has_header: bool,
    delimiter: u8,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Compression type of the file associated with CsvExec
    pub file_compression_type: FileCompressionType,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided base and specific configurations
    pub fn new(
        base_config: FileScanConfig,
        has_header: bool,
        delimiter: u8,
        file_compression_type: FileCompressionType,
    ) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            has_header,
            delimiter,
            metrics: ExecutionPlanMetricsSet::new(),
            file_compression_type,
        }
    }

    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }
    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// Redistribute files across partitions according to their size
    /// See comments on `repartition_file_groups()` for more detail.
    pub fn get_repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
    ) -> Option<Self> {
        let repartitioned_file_groups_option = FileScanConfig::repartition_file_groups(
            self.base_config.file_groups.clone(),
            target_partitions,
            repartition_file_min_size,
        );

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let mut new_plan = self.clone();
            new_plan.base_config.file_groups = repartitioned_file_groups;
            return Some(new_plan);
        }
        None
    }
}

impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn unbounded_output(&self, _: &[bool]) -> Result<bool> {
        Ok(self.base_config().infinite_source)
    }

    /// See comments on `impl ExecutionPlan for ParquetExec`: output order can't be
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        ordering_equivalence_properties_helper(
            self.schema(),
            &self.projected_output_ordering,
        )
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let config = Arc::new(CsvConfig {
            batch_size: context.session_config().batch_size(),
            file_schema: Arc::clone(&self.base_config.file_schema),
            file_projection: self.base_config.file_column_projection_indices(),
            has_header: self.has_header,
            delimiter: self.delimiter,
            object_store,
        });

        let opener = CsvOpener {
            config,
            file_compression_type: self.file_compression_type.to_owned(),
        };
        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "CsvExec: ")?;
        self.base_config.fmt_as(t, f)?;
        write!(f, ", has_header={}", self.has_header)
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A Config for [`CsvOpener`]
#[derive(Debug, Clone)]
pub struct CsvConfig {
    batch_size: usize,
    file_schema: SchemaRef,
    file_projection: Option<Vec<usize>>,
    has_header: bool,
    delimiter: u8,
    object_store: Arc<dyn ObjectStore>,
}

impl CsvConfig {
    /// Returns a [`CsvConfig`]
    pub fn new(
        batch_size: usize,
        file_schema: SchemaRef,
        file_projection: Option<Vec<usize>>,
        has_header: bool,
        delimiter: u8,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            file_schema,
            file_projection,
            has_header,
            delimiter,
            object_store,
        }
    }
}

impl CsvConfig {
    fn open<R: std::io::Read>(&self, reader: R) -> Result<csv::Reader<R>> {
        let mut builder = csv::ReaderBuilder::new(self.file_schema.clone())
            .has_header(self.has_header)
            .with_delimiter(self.delimiter)
            .with_batch_size(self.batch_size);

        if let Some(p) = &self.file_projection {
            builder = builder.with_projection(p.clone());
        }

        Ok(builder.build(reader)?)
    }

    fn builder(&self) -> csv::ReaderBuilder {
        let mut builder = csv::ReaderBuilder::new(self.file_schema.clone())
            .with_delimiter(self.delimiter)
            .with_batch_size(self.batch_size)
            .has_header(self.has_header);

        if let Some(proj) = &self.file_projection {
            builder = builder.with_projection(proj.clone());
        }

        builder
    }
}

/// A [`FileOpener`] that opens a CSV file and yields a [`FileOpenFuture`]
pub struct CsvOpener {
    config: Arc<CsvConfig>,
    file_compression_type: FileCompressionType,
}

impl CsvOpener {
    /// Returns a [`CsvOpener`]
    pub fn new(
        config: Arc<CsvConfig>,
        file_compression_type: FileCompressionType,
    ) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

/// Returns the position of the first newline in the byte stream, or the total length if no newline is found.
fn find_first_newline_bytes<R: std::io::Read>(reader: &mut R) -> Result<usize> {
    let mut buffer = [0; 1];
    let mut index = 0;

    loop {
        let result = reader.read(&mut buffer);
        match result {
            Ok(n) => {
                if n == 0 {
                    return Ok(index); // End of file, no newline found
                }
                if buffer[0] == b'\n' {
                    return Ok(index);
                }
                index += 1;
            }
            Err(e) => {
                return Err(DataFusionError::IoError(e));
            }
        }
    }
}

/// Returns the offset of the first newline in the object store range [start, end), or the end offset if no newline is found.
async fn find_first_newline(
    object_store: &Arc<dyn ObjectStore>,
    location: &object_store::path::Path,
    start_byte: usize,
    end_byte: usize,
) -> Result<usize> {
    let options = GetOptions {
        range: Some(Range {
            start: start_byte,
            end: end_byte,
        }),
        ..Default::default()
    };

    let offset = match object_store.get_opts(location, options).await? {
        GetResult::File(_, _) => {
            // Range currently is ignored for GetResult::File(...)
            let get_range_end_result = object_store
                .get_range(
                    location,
                    Range {
                        start: start_byte,
                        end: end_byte,
                    },
                )
                .await;
            let mut decoder_tail = Cursor::new(get_range_end_result?);
            find_first_newline_bytes(&mut decoder_tail)?
        }
        GetResult::Stream(s) => {
            let mut input = s.map_err(DataFusionError::from);
            let mut buffered = Bytes::new();

            let future_index = async move {
                let mut index = 0;

                loop {
                    if buffered.is_empty() {
                        match input.next().await {
                            Some(Ok(b)) => buffered = b,
                            Some(Err(e)) => return Err(e),
                            None => return Ok(index),
                        };
                    }

                    for byte in &buffered {
                        if *byte == b'\n' {
                            return Ok(index);
                        }
                        index += 1;
                    }

                    buffered.advance(buffered.len());
                }
            };
            future_index.await?
        }
    };
    Ok(offset)
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

        let config = CsvConfig {
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

        Ok(Box::pin(async move {
            let file_size = file_meta.object_meta.size;
            // Current partition contains bytes [start_byte, end_byte) (might contain incomplete lines at boundaries)
            let (start_byte, end_byte) = match file_meta.range {
                None => (0, file_size),
                Some(FileRange { start, end }) => {
                    let (start, end) = (start as usize, end as usize);
                    // Partition byte range is [start, end), the boundary might be in the middle of
                    // some line. Need to find out the exact line boundaries.
                    let start_delta = if start != 0 {
                        find_first_newline(
                            &config.object_store,
                            file_meta.location(),
                            start - 1,
                            file_size,
                        )
                        .await?
                    } else {
                        0
                    };
                    let end_delta = if end != file_size {
                        find_first_newline(
                            &config.object_store,
                            file_meta.location(),
                            end - 1,
                            file_size,
                        )
                        .await?
                    } else {
                        0
                    };
                    (start + start_delta, end + end_delta)
                }
            };

            // For special case: If `Range` has equal `start` and `end`, object store will fetch
            // the whole file
            let localfs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            let is_localfs = localfs.type_id() == config.object_store.type_id();
            if start_byte == end_byte && !is_localfs {
                return Ok(futures::stream::poll_fn(move |_| Poll::Ready(None)).boxed());
            }

            let options = GetOptions {
                range: Some(Range {
                    start: start_byte,
                    end: end_byte,
                }),
                ..Default::default()
            };

            match config
                .object_store
                .get_opts(file_meta.location(), options)
                .await?
            {
                GetResult::File(file, _) => {
                    let is_whole_file_scanned = file_meta.range.is_none();
                    let decoder = if is_whole_file_scanned {
                        // For special case: `get_range()` will interpret `start` and `end` as the
                        // byte range after decompression for compressed files
                        file_compression_type.convert_read(file)?
                    } else {
                        // Range currently is ignored for GetResult::File(...)
                        let bytes = Cursor::new(
                            config
                                .object_store
                                .get_range(
                                    file_meta.location(),
                                    Range {
                                        start: start_byte,
                                        end: end_byte,
                                    },
                                )
                                .await?,
                        );
                        file_compression_type.convert_read(bytes)?
                    };

                    Ok(futures::stream::iter(config.open(decoder)?).boxed())
                }
                GetResult::Stream(s) => {
                    let mut decoder = config.builder().build_decoder();
                    let s = s.map_err(DataFusionError::from);
                    let mut input =
                        file_compression_type.convert_stream(s.boxed())?.fuse();
                    let mut buffered = Bytes::new();

                    let s = futures::stream::poll_fn(move |cx| {
                        loop {
                            if buffered.is_empty() {
                                match ready!(input.poll_next_unpin(cx)) {
                                    Some(Ok(b)) => buffered = b,
                                    Some(Err(e)) => {
                                        return Poll::Ready(Some(Err(e.into())))
                                    }
                                    None => {}
                                };
                            }
                            let decoded = match decoder.decode(buffered.as_ref()) {
                                // Note: the decoder needs to be called with an empty
                                // array to delimt the final record
                                Ok(0) => break,
                                Ok(decoded) => decoded,
                                Err(e) => return Poll::Ready(Some(Err(e))),
                            };
                            buffered.advance(decoded);
                        }

                        Poll::Ready(decoder.flush().transpose())
                    });
                    Ok(s.boxed())
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
    // create directory to contain the CSV files (one per partition)
    let fs_path = Path::new(path);
    if let Err(e) = fs::create_dir(fs_path) {
        return Err(DataFusionError::Execution(format!(
            "Could not create directory {path}: {e:?}"
        )));
    }

    let mut tasks = vec![];
    for i in 0..plan.output_partitioning().partition_count() {
        let plan = plan.clone();
        let filename = format!("part-{i}.csv");
        let path = fs_path.join(filename);
        let file = fs::File::create(path)?;
        let mut writer = csv::Writer::new(file);
        let stream = plan.execute(i, task_ctx.clone())?;

        let handle: JoinHandle<Result<()>> = task::spawn(async move {
            stream
                .map(|batch| writer.write(&batch?))
                .try_collect()
                .await
                .map_err(DataFusionError::from)
        });
        tasks.push(AbortOnDropSingle::new(handle));
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .try_for_each(|result| {
            result.map_err(|e| DataFusionError::Execution(format!("{e}")))?
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::file_format::file_type::FileType;
    use crate::datasource::physical_plan::chunked_store::ChunkedStore;
    use crate::prelude::*;
    use crate::test::{partitioned_csv_config, partitioned_file_groups};
    use crate::test_util::{aggr_test_schema_with_missing_col, arrow_test_data};
    use crate::{scalar::ScalarValue, test_util::aggr_test_schema};
    use arrow::datatypes::*;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use rstest::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;
    use url::Url;

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn csv_exec_with_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            FileType::CSV,
            file_compression_type.to_owned(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups)?;
        config.projection = Some(vec![0, 2, 4]);

        let csv = CsvExec::new(config, true, b',', file_compression_type.to_owned());
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = vec![
            "+----+-----+------------+",
            "| c1 | c3  | c5         |",
            "+----+-----+------------+",
            "| c  | 1   | 2033001162 |",
            "| d  | -40 | 706441268  |",
            "| b  | 29  | 994303988  |",
            "| a  | -85 | 1171968280 |",
            "| b  | -82 | 1824882165 |",
            "+----+-----+------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn csv_exec_with_mixed_order_projection(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            FileType::CSV,
            file_compression_type.to_owned(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups)?;
        config.projection = Some(vec![4, 0, 2]);

        let csv = CsvExec::new(config, true, b',', file_compression_type.to_owned());
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(3, csv.projected_schema.fields().len());
        assert_eq!(3, csv.schema().fields().len());

        let mut stream = csv.execute(0, task_ctx)?;
        let batch = stream.next().await.unwrap()?;
        assert_eq!(3, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = vec![
            "+------------+----+-----+",
            "| c5         | c1 | c3  |",
            "+------------+----+-----+",
            "| 2033001162 | c  | 1   |",
            "| 706441268  | d  | -40 |",
            "| 994303988  | b  | 29  |",
            "| 1171968280 | a  | -85 |",
            "| 1824882165 | b  | -82 |",
            "+------------+----+-----+",
        ];

        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn csv_exec_with_limit(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            FileType::CSV,
            file_compression_type.to_owned(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups)?;
        config.limit = Some(5);

        let csv = CsvExec::new(config, true, b',', file_compression_type.to_owned());
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(13, csv.projected_schema.fields().len());
        assert_eq!(13, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(13, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        let expected = vec![
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c1 | c2 | c3  | c4     | c5         | c6                   | c7  | c8    | c9         | c10                  | c11         | c12                 | c13                            |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
            "| c  | 2  | 1   | 18109  | 2033001162 | -6513304855495910254 | 25  | 43062 | 1491205016 | 5863949479783605708  | 0.110830784 | 0.9294097332465232  | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |",
            "| d  | 5  | -40 | 22614  | 706441268  | -7542719935673075327 | 155 | 14337 | 3373581039 | 11720144131976083864 | 0.69632107  | 0.3114712539863804  | C2GT5KVyOPZpgKVl110TyZO0NcJ434 |",
            "| b  | 1  | 29  | -18218 | 994303988  | 5983957848665088916  | 204 | 9489  | 3275293996 | 14857091259186476033 | 0.53840446  | 0.17909035118828576 | AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz |",
            "| a  | 1  | -85 | -15154 | 1171968280 | 1919439543497968449  | 77  | 52286 | 774637006  | 12101411955859039553 | 0.12285209  | 0.6864391962767343  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB |",
            "| b  | 5  | -82 | 22080  | 1824882165 | 7373730676428214987  | 208 | 34331 | 3342719438 | 3330177516592499461  | 0.82634634  | 0.40975383525297016 | Ig1QcuKsjHXkproePdERo2w0mYzIqd |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn csv_exec_with_missing_column(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema_with_missing_col();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            FileType::CSV,
            file_compression_type.to_owned(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups)?;
        config.limit = Some(5);

        let csv = CsvExec::new(config, true, b',', file_compression_type.to_owned());
        assert_eq!(14, csv.base_config.file_schema.fields().len());
        assert_eq!(14, csv.projected_schema.fields().len());
        assert_eq!(14, csv.schema().fields().len());

        // errors due to https://github.com/apache/arrow-datafusion/issues/4918
        let mut it = csv.execute(0, task_ctx)?;
        let err = it.next().await.unwrap().unwrap_err().to_string();
        assert_eq!(
            err,
            "Arrow error: Csv error: incorrect number of fields for line 1, expected 14 got 13"
        );
        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn csv_exec_with_partition(
        file_compression_type: FileCompressionType,
    ) -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            FileType::CSV,
            file_compression_type.to_owned(),
        )?;

        let mut config = partitioned_csv_config(file_schema, file_groups)?;

        // Add partition columns
        config.table_partition_cols = vec![("date".to_owned(), DataType::Utf8)];
        config.file_groups[0][0].partition_values =
            vec![ScalarValue::Utf8(Some("2021-10-26".to_owned()))];

        // We should be able to project on the partition column
        // Which is supposed to be after the file fields
        config.projection = Some(vec![0, config.file_schema.fields().len()]);

        // we don't have `/date=xx/` in the path but that is ok because
        // partitions are resolved during scan anyway
        let csv = CsvExec::new(config, true, b',', file_compression_type.to_owned());
        assert_eq!(13, csv.base_config.file_schema.fields().len());
        assert_eq!(2, csv.projected_schema.fields().len());
        assert_eq!(2, csv.schema().fields().len());

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(2, batch.num_columns());
        assert_eq!(100, batch.num_rows());

        // slice of the first 5 lines
        let expected = vec![
            "+----+------------+",
            "| c1 | date       |",
            "+----+------------+",
            "| c  | 2021-10-26 |",
            "| d  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "| a  | 2021-10-26 |",
            "| b  | 2021-10-26 |",
            "+----+------------+",
        ];
        crate::assert_batches_eq!(expected, &[batch.slice(0, 5)]);

        let metrics = csv.metrics().expect("doesn't found metrics");
        let time_elapsed_processing = get_value(&metrics, "time_elapsed_processing");
        assert!(
            time_elapsed_processing > 0,
            "Expected time_elapsed_processing greater than 0",
        );
        Ok(())
    }

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{partition}.{file_extension}");
            let file_path = tmp_dir.path().join(filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    async fn test_additional_stores(
        file_compression_type: FileCompressionType,
        store: Arc<dyn ObjectStore>,
    ) {
        let ctx = SessionContext::new();
        let url = Url::parse("file://").unwrap();
        ctx.runtime_env().register_object_store(&url, store.clone());

        let task_ctx = ctx.task_ctx();

        let file_schema = aggr_test_schema();
        let path = format!("{}/csv", arrow_test_data());
        let filename = "aggregate_test_100.csv";

        let file_groups = partitioned_file_groups(
            path.as_str(),
            filename,
            1,
            FileType::CSV,
            file_compression_type.to_owned(),
        )
        .unwrap();

        let config = partitioned_csv_config(file_schema, file_groups).unwrap();
        let csv = CsvExec::new(config, true, b',', file_compression_type.to_owned());

        let it = csv.execute(0, task_ctx).unwrap();
        let batches: Vec<_> = it.try_collect().await.unwrap();

        let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();

        assert_eq!(total_rows, 100);
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ),
        case(FileCompressionType::ZSTD)
    )]
    #[tokio::test]
    async fn test_chunked_csv(
        file_compression_type: FileCompressionType,
        #[values(10, 20, 30, 40)] chunk_size: usize,
    ) {
        test_additional_stores(
            file_compression_type,
            Arc::new(ChunkedStore::new(
                Arc::new(LocalFileSystem::new()),
                chunk_size,
            )),
        )
        .await;
    }

    #[tokio::test]
    async fn test_no_trailing_delimiter() {
        let session_ctx = SessionContext::new();
        let store = object_store::memory::InMemory::new();

        let data = bytes::Bytes::from("a,b\n1,2\n3,4");
        let path = object_store::path::Path::from("a.csv");
        store.put(&path, data).await.unwrap();

        let url = Url::parse("memory://").unwrap();
        session_ctx
            .runtime_env()
            .register_object_store(&url, Arc::new(store));

        let df = session_ctx
            .read_csv("memory:///", CsvReadOptions::new())
            .await
            .unwrap();

        let result = df.collect().await.unwrap();

        let expected = vec![
            "+---+---+",
            "| a | b |",
            "+---+---+",
            "| 1 | 2 |",
            "| 3 | 4 |",
            "+---+---+",
        ];

        crate::assert_batches_eq!(expected, &result);
    }

    #[tokio::test]
    async fn write_csv_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/data/corrupt.csv", options).await?;
        let tmp_dir = TempDir::new()?;
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let e = df
            .write_csv(&out_dir)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!("Arrow error: Parser error: Error while parsing value d for column 0 at line 4", format!("{e}"));
        Ok(())
    }

    #[tokio::test]
    async fn write_csv_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let ctx =
            SessionContext::with_config(SessionConfig::new().with_target_partitions(8));

        let schema = populate_csv_partitions(&tmp_dir, 8, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        df.write_csv(&out_dir).await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let ctx = SessionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // register each partition as well as the top level dir
        let csv_read_option = CsvReadOptions::new().schema(&schema);
        ctx.register_csv(
            "part0",
            &format!("{out_dir}/part-0.csv"),
            csv_read_option.clone(),
        )
        .await?;
        ctx.register_csv("allparts", &out_dir, csv_read_option)
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 80);

        Ok(())
    }

    fn get_value(metrics: &MetricsSet, metric_name: &str) -> usize {
        match metrics.sum_by_name(metric_name) {
            Some(v) => v.as_usize(),
            _ => {
                panic!(
                    "Expected metric not found. Looking for '{metric_name}' in\n\n{metrics:#?}"
                );
            }
        }
    }
}
