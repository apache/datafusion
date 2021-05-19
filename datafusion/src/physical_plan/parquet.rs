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

//! Execution plan for reading Parquet files

use std::any::Any;
use std::fmt;
use std::fs::File;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::{
    error::{DataFusionError, Result},
    logical_plan::Expr,
    physical_optimizer::pruning::PruningPredicateBuilder,
    physical_plan::{
        common, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream,
    },
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use parquet::file::reader::{FileReader, SerializedFileReader};

use fmt::Debug;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task,
};
use tokio_stream::wrappers::ReceiverStream;

use crate::datasource::datasource::{ColumnStatistics, Statistics};
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};

/// Execution plan for scanning one or more Parquet partitions
#[derive(Debug, Clone)]
pub struct ParquetExec {
    /// Parquet partitions to read
    partitions: Vec<ParquetPartition>,
    /// Schema after projection is applied
    schema: SchemaRef,
    /// Projection for which columns to load
    projection: Vec<usize>,
    /// Batch size
    batch_size: usize,
    /// Statistics for the data set (sum of statistics for all partitions)
    statistics: Statistics,
    /// Optional predicate builder
    predicate_builder: Option<PruningPredicateBuilder>,
    /// Optional limit of the number of rows
    limit: Option<usize>,
}

/// Represents one partition of a Parquet data set and this currently means one Parquet file.
///
/// In the future it would be good to support subsets of files based on ranges of row groups
/// so that we can better parallelize reads of large files across available cores (see
/// [ARROW-10995](https://issues.apache.org/jira/browse/ARROW-10995)).
///
/// We may also want to support reading Parquet files that are partitioned based on a key and
/// in this case we would want this partition struct to represent multiple files for a given
/// partition key (see [ARROW-11019](https://issues.apache.org/jira/browse/ARROW-11019)).
#[derive(Debug, Clone)]
pub struct ParquetPartition {
    /// The Parquet filename for this partition
    pub filenames: Vec<String>,
    /// Statistics for this partition
    pub statistics: Statistics,
}

impl ParquetExec {
    /// Create a new Parquet reader execution plan based on the specified Parquet filename or
    /// directory containing Parquet files
    pub fn try_from_path(
        path: &str,
        projection: Option<Vec<usize>>,
        predicate: Option<Expr>,
        batch_size: usize,
        max_concurrency: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        // build a list of filenames from the specified path, which could be a single file or
        // a directory containing one or more parquet files
        let filenames = common::build_file_list(path, ".parquet")?;
        if filenames.is_empty() {
            Err(DataFusionError::Plan(format!(
                "No Parquet files found at path {}",
                path
            )))
        } else {
            let filenames = filenames
                .iter()
                .map(|filename| filename.as_str())
                .collect::<Vec<&str>>();
            Self::try_from_files(
                &filenames,
                projection,
                predicate,
                batch_size,
                max_concurrency,
                limit,
            )
        }
    }

    /// Create a new Parquet reader execution plan based on the specified list of Parquet
    /// files
    pub fn try_from_files(
        filenames: &[&str],
        projection: Option<Vec<usize>>,
        predicate: Option<Expr>,
        batch_size: usize,
        max_concurrency: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        // build a list of Parquet partitions with statistics and gather all unique schemas
        // used in this data set
        let mut schemas: Vec<Schema> = vec![];
        let mut partitions = Vec::with_capacity(max_concurrency);
        let filenames: Vec<String> = filenames.iter().map(|s| s.to_string()).collect();
        let chunks = split_files(&filenames, max_concurrency);
        let mut num_rows = 0;
        let mut total_byte_size = 0;
        let mut null_counts = Vec::new();
        let mut limit_exhausted = false;
        for chunk in chunks {
            let mut filenames: Vec<String> =
                chunk.iter().map(|x| x.to_string()).collect();
            let mut total_files = 0;
            for filename in &filenames {
                total_files += 1;
                let file = File::open(filename)?;
                let file_reader = Arc::new(SerializedFileReader::new(file)?);
                let mut arrow_reader = ParquetFileArrowReader::new(file_reader);
                let meta_data = arrow_reader.get_metadata();
                // collect all the unique schemas in this data set
                let schema = arrow_reader.get_schema()?;
                let num_fields = schema.fields().len();
                if schemas.is_empty() || schema != schemas[0] {
                    schemas.push(schema);
                    null_counts = vec![0; num_fields]
                }
                for row_group_meta in meta_data.row_groups() {
                    num_rows += row_group_meta.num_rows();
                    total_byte_size += row_group_meta.total_byte_size();

                    // Currently assumes every Parquet file has same schema
                    // https://issues.apache.org/jira/browse/ARROW-11017
                    let columns_null_counts = row_group_meta
                        .columns()
                        .iter()
                        .flat_map(|c| c.statistics().map(|stats| stats.null_count()));

                    for (i, cnt) in columns_null_counts.enumerate() {
                        null_counts[i] += cnt
                    }
                    if limit.map(|x| num_rows >= x as i64).unwrap_or(false) {
                        limit_exhausted = true;
                        break;
                    }
                }
            }

            let column_stats = null_counts
                .iter()
                .map(|null_count| ColumnStatistics {
                    null_count: Some(*null_count as usize),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                })
                .collect();

            let statistics = Statistics {
                num_rows: Some(num_rows as usize),
                total_byte_size: Some(total_byte_size as usize),
                column_statistics: Some(column_stats),
            };
            // remove files that are not needed in case of limit
            filenames.truncate(total_files);
            partitions.push(ParquetPartition {
                filenames,
                statistics,
            });
            if limit_exhausted {
                break;
            }
        }

        // we currently get the schema information from the first file rather than do
        // schema merging and this is a limitation.
        // See https://issues.apache.org/jira/browse/ARROW-11017
        if schemas.len() > 1 {
            return Err(DataFusionError::Plan(format!(
                "The Parquet files have {} different schemas and DataFusion does \
                not yet support schema merging",
                schemas.len()
            )));
        }
        let schema = schemas[0].clone();
        let predicate_builder = predicate.and_then(|predicate_expr| {
            PruningPredicateBuilder::try_new(&predicate_expr, schema.clone()).ok()
        });

        Ok(Self::new(
            partitions,
            schema,
            projection,
            predicate_builder,
            batch_size,
            limit,
        ))
    }

    /// Create a new Parquet reader execution plan with provided partitions and schema
    pub fn new(
        partitions: Vec<ParquetPartition>,
        schema: Schema,
        projection: Option<Vec<usize>>,
        predicate_builder: Option<PruningPredicateBuilder>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        let projected_schema = Schema::new(
            projection
                .iter()
                .map(|i| schema.field(*i).clone())
                .collect(),
        );

        // sum the statistics
        let mut num_rows: Option<usize> = None;
        let mut total_byte_size: Option<usize> = None;
        let mut null_counts: Vec<usize> = vec![0; schema.fields().len()];
        let mut has_null_counts = false;
        for part in &partitions {
            if let Some(n) = part.statistics.num_rows {
                num_rows = Some(num_rows.unwrap_or(0) + n)
            }
            if let Some(n) = part.statistics.total_byte_size {
                total_byte_size = Some(total_byte_size.unwrap_or(0) + n)
            }
            if let Some(x) = &part.statistics.column_statistics {
                let part_nulls: Vec<Option<usize>> =
                    x.iter().map(|c| c.null_count).collect();
                has_null_counts = true;

                for &i in projection.iter() {
                    null_counts[i] = part_nulls[i].unwrap_or(0);
                }
            }
        }
        let column_stats = if has_null_counts {
            Some(
                null_counts
                    .iter()
                    .map(|null_count| ColumnStatistics {
                        null_count: Some(*null_count),
                        distinct_count: None,
                        max_value: None,
                        min_value: None,
                    })
                    .collect(),
            )
        } else {
            None
        };

        let statistics = Statistics {
            num_rows,
            total_byte_size,
            column_statistics: column_stats,
        };
        Self {
            partitions,
            schema: Arc::new(projected_schema),
            projection,
            predicate_builder,
            batch_size,
            statistics,
            limit,
        }
    }

    /// Parquet partitions to read
    pub fn partitions(&self) -> &[ParquetPartition] {
        &self.partitions
    }

    /// Projection for which columns to load
    pub fn projection(&self) -> &[usize] {
        &self.projection
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Statistics for the data set (sum of statistics for all partitions)
    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

impl ParquetPartition {
    /// Create a new parquet partition
    pub fn new(filenames: Vec<String>, statistics: Statistics) -> Self {
        Self {
            filenames,
            statistics,
        }
    }

    /// The Parquet filename for this partition
    pub fn filenames(&self) -> &[String] {
        &self.filenames
    }

    /// Statistics for this partition
    pub fn statistics(&self) -> &Statistics {
        &self.statistics
    }
}

#[async_trait]
impl ExecutionPlan for ParquetExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        // because the parquet implementation is not thread-safe, it is necessary to execute
        // on a thread and communicate with channels
        let (response_tx, response_rx): (
            Sender<ArrowResult<RecordBatch>>,
            Receiver<ArrowResult<RecordBatch>>,
        ) = channel(2);

        let filenames = self.partitions[partition].filenames.clone();
        let projection = self.projection.clone();
        let predicate_builder = self.predicate_builder.clone();
        let batch_size = self.batch_size;
        let limit = self.limit;

        task::spawn_blocking(move || {
            if let Err(e) = read_files(
                &filenames,
                &projection,
                &predicate_builder,
                batch_size,
                response_tx,
                limit,
            ) {
                println!("Parquet reader thread terminated due to error: {:?}", e);
            }
        });

        Ok(Box::pin(ParquetStream {
            schema: self.schema.clone(),
            inner: ReceiverStream::new(response_rx),
        }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let files: Vec<_> = self
                    .partitions
                    .iter()
                    .map(|pp| pp.filenames.iter())
                    .flatten()
                    .map(|s| s.as_str())
                    .collect();

                write!(
                    f,
                    "ParquetExec: batch_size={}, limit={:?}, partitions=[{}]",
                    self.batch_size,
                    self.limit,
                    files.join(", ")
                )
            }
        }
    }
}

fn send_result(
    response_tx: &Sender<ArrowResult<RecordBatch>>,
    result: ArrowResult<RecordBatch>,
) -> Result<()> {
    // Note this function is running on its own blockng tokio thread so blocking here is ok.
    response_tx
        .blocking_send(result)
        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
    Ok(())
}

fn read_files(
    filenames: &[String],
    projection: &[usize],
    predicate_builder: &Option<PruningPredicateBuilder>,
    batch_size: usize,
    response_tx: Sender<ArrowResult<RecordBatch>>,
    limit: Option<usize>,
) -> Result<()> {
    let mut total_rows = 0;
    'outer: for filename in filenames {
        let file = File::open(&filename)?;
        let mut file_reader = SerializedFileReader::new(file)?;
        if let Some(predicate_builder) = predicate_builder {
            let row_group_predicate = predicate_builder
                .build_pruning_predicate(file_reader.metadata().row_groups());
            file_reader.filter_row_groups(&row_group_predicate);
        }
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));
        let mut batch_reader = arrow_reader
            .get_record_reader_by_columns(projection.to_owned(), batch_size)?;
        loop {
            match batch_reader.next() {
                Some(Ok(batch)) => {
                    //println!("ParquetExec got new batch from {}", filename);
                    total_rows += batch.num_rows();
                    send_result(&response_tx, Ok(batch))?;
                    if limit.map(|l| total_rows >= l).unwrap_or(false) {
                        break 'outer;
                    }
                }
                None => {
                    break;
                }
                Some(Err(e)) => {
                    let err_msg = format!(
                        "Error reading batch from {}: {}",
                        filename,
                        e.to_string()
                    );
                    // send error to operator
                    send_result(
                        &response_tx,
                        Err(ArrowError::ParquetError(err_msg.clone())),
                    )?;
                    // terminate thread with error
                    return Err(DataFusionError::Execution(err_msg));
                }
            }
        }
    }

    // finished reading files (dropping response_tx will close
    // channel)
    Ok(())
}

fn split_files(filenames: &[String], n: usize) -> Vec<&[String]> {
    let mut chunk_size = filenames.len() / n;
    if filenames.len() % n > 0 {
        chunk_size += 1;
    }
    filenames.chunks(chunk_size).collect()
}

struct ParquetStream {
    schema: SchemaRef,
    inner: ReceiverStream<ArrowResult<RecordBatch>>,
}

impl Stream for ParquetStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for ParquetStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use futures::StreamExt;
    use parquet::{
        basic::Type as PhysicalType,
        file::{metadata::RowGroupMetaData, statistics::Statistics as ParquetStatistics},
        schema::types::SchemaDescPtr,
    };

    #[test]
    fn test_split_files() {
        let filenames = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        let chunks = split_files(&filenames, 1);
        assert_eq!(1, chunks.len());
        assert_eq!(5, chunks[0].len());

        let chunks = split_files(&filenames, 2);
        assert_eq!(2, chunks.len());
        assert_eq!(3, chunks[0].len());
        assert_eq!(2, chunks[1].len());

        let chunks = split_files(&filenames, 5);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = split_files(&filenames, 123);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());
    }

    #[tokio::test]
    async fn test() -> Result<()> {
        let testdata = arrow::util::test_util::parquet_test_data();
        let filename = format!("{}/alltypes_plain.parquet", testdata);
        let parquet_exec = ParquetExec::try_from_path(
            &filename,
            Some(vec![0, 1, 2]),
            None,
            1024,
            4,
            None,
        )?;
        assert_eq!(parquet_exec.output_partitioning().partition_count(), 1);

        let mut results = parquet_exec.execute(0).await?;
        let batch = results.next().await.unwrap()?;

        assert_eq!(8, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        let schema = batch.schema();
        let field_names: Vec<&str> =
            schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(vec!["id", "bool_col", "tinyint_col"], field_names);

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        let batch = results.next().await;
        assert!(batch.is_none());

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_simple_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let predicate_builder = PruningPredicateBuilder::try_new(&expr, schema)?;

        let schema_descr = get_test_schema_descr(vec![("c1", PhysicalType::INT32)]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(1), Some(10), None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_pruning_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        assert_eq!(row_group_filter, vec![false, true]);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_missing_stats() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // int > 1 => c1_max > 1
        let expr = col("c1").gt(lit(15));
        let schema = Schema::new(vec![Field::new("c1", DataType::Int32, false)]);
        let predicate_builder = PruningPredicateBuilder::try_new(&expr, schema)?;

        let schema_descr = get_test_schema_descr(vec![("c1", PhysicalType::INT32)]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(None, None, None, 0, false)],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![ParquetStatistics::int32(Some(11), Some(20), None, 0, false)],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_pruning_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // missing statistics for first row group mean that the result from the predicate expression
        // is null / undefined so the first row group can't be filtered out
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_partial_expr() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with partially supported expression
        // int > 1 and int % 2 => c1_max > 1 and true
        let expr = col("c1").gt(lit(15)).and(col("c2").modulus(lit(2)));
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let predicate_builder = PruningPredicateBuilder::try_new(&expr, schema.clone())?;

        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32),
            ("c2", PhysicalType::INT32),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
            ],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_pruning_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // the first row group is still filtered out because the predicate expression can be partially evaluated
        // when conditions are joined using AND
        assert_eq!(row_group_filter, vec![false, true]);

        // if conditions in predicate are joined with OR and an unsupported expression is used
        // this bypasses the entire predicate expression and no row groups are filtered out
        let expr = col("c1").gt(lit(15)).or(col("c2").modulus(lit(2)));
        let predicate_builder = PruningPredicateBuilder::try_new(&expr, schema)?;
        let row_group_predicate =
            predicate_builder.build_pruning_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    #[test]
    fn row_group_predicate_builder_unsupported_type() -> Result<()> {
        use crate::logical_plan::{col, lit};
        // test row group predicate with unsupported statistics type (boolean)
        // where a null array is generated for some statistics columns
        // int > 1 and bool = true => c1_max > 1 and null
        let expr = col("c1").gt(lit(15)).and(col("c2").eq(lit(true)));
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Boolean, false),
        ]);
        let predicate_builder = PruningPredicateBuilder::try_new(&expr, schema)?;

        let schema_descr = get_test_schema_descr(vec![
            ("c1", PhysicalType::INT32),
            ("c2", PhysicalType::BOOLEAN),
        ]);
        let rgm1 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(1), Some(10), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let rgm2 = get_row_group_meta_data(
            &schema_descr,
            vec![
                ParquetStatistics::int32(Some(11), Some(20), None, 0, false),
                ParquetStatistics::boolean(Some(false), Some(true), None, 0, false),
            ],
        );
        let row_group_metadata = vec![rgm1, rgm2];
        let row_group_predicate =
            predicate_builder.build_pruning_predicate(&row_group_metadata);
        let row_group_filter = row_group_metadata
            .iter()
            .enumerate()
            .map(|(i, g)| row_group_predicate(g, i))
            .collect::<Vec<_>>();
        // no row group is filtered out because the predicate expression can't be evaluated
        // when a null array is generated for a statistics column,
        // because the null values propagate to the end result, making the predicate result undefined
        assert_eq!(row_group_filter, vec![true, true]);

        Ok(())
    }

    fn get_row_group_meta_data(
        schema_descr: &SchemaDescPtr,
        column_statistics: Vec<ParquetStatistics>,
    ) -> RowGroupMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        let mut columns = vec![];
        for (i, s) in column_statistics.iter().enumerate() {
            let column = ColumnChunkMetaData::builder(schema_descr.column(i))
                .set_statistics(s.clone())
                .build()
                .unwrap();
            columns.push(column);
        }
        RowGroupMetaData::builder(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(2000)
            .set_column_metadata(columns)
            .build()
            .unwrap()
    }

    fn get_test_schema_descr(fields: Vec<(&str, PhysicalType)>) -> SchemaDescPtr {
        use parquet::schema::types::{SchemaDescriptor, Type as SchemaType};
        let mut schema_fields = fields
            .iter()
            .map(|(n, t)| {
                Arc::new(SchemaType::primitive_type_builder(n, *t).build().unwrap())
            })
            .collect::<Vec<_>>();
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(&mut schema_fields)
            .build()
            .unwrap();

        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }
}
