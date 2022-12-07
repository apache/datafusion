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
use crate::error::{DataFusionError, Result};
use crate::execution::context::{SessionState, TaskContext};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::file_format::delimited_stream::newline_delimited_stream;
use crate::physical_plan::file_format::file_stream::{
    FileOpenFuture, FileOpener, FileStream,
};
use crate::physical_plan::file_format::FileMeta;
use crate::physical_plan::metrics::ExecutionPlanMetricsSet;
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::csv;
use arrow::datatypes::SchemaRef;

use bytes::Buf;

use futures::{StreamExt, TryStreamExt};
use object_store::{GetResult, ObjectStore};
use std::any::Any;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};

use super::{get_output_ordering, FileScanConfig};

/// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    has_header: bool,
    delimiter: u8,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    file_compression_type: FileCompressionType,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided base and specific configurations
    pub fn new(
        base_config: FileScanConfig,
        has_header: bool,
        delimiter: u8,
        file_compression_type: FileCompressionType,
    ) -> Self {
        let (projected_schema, projected_statistics) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
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

    /// See comments on `impl ExecutionPlan for ParquetExec`: output order can't be
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        get_output_ordering(&self.base_config)
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
        let config = Arc::new(CsvConfig {
            batch_size: context.session_config().batch_size(),
            file_schema: Arc::clone(&self.base_config.file_schema),
            file_projection: self.base_config.file_column_projection_indices(),
            has_header: self.has_header,
            delimiter: self.delimiter,
        });

        let opener = CsvOpener {
            config,
            file_compression_type: self.file_compression_type.to_owned(),
        };
        let stream = FileStream::new(
            &self.base_config,
            partition,
            context,
            opener,
            self.metrics.clone(),
        )?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "CsvExec: files={}, has_header={}, limit={:?}, projection={}",
                    super::FileGroupsDisplay(&self.base_config.file_groups),
                    self.has_header,
                    self.base_config.limit,
                    super::ProjectSchemaDisplay(&self.projected_schema),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }
}

#[derive(Debug, Clone)]
struct CsvConfig {
    batch_size: usize,
    file_schema: SchemaRef,
    file_projection: Option<Vec<usize>>,
    has_header: bool,
    delimiter: u8,
}

impl CsvConfig {
    fn open<R: std::io::Read>(&self, reader: R, first_chunk: bool) -> csv::Reader<R> {
        let datetime_format = None;
        csv::Reader::new(
            reader,
            Arc::clone(&self.file_schema),
            self.has_header && first_chunk,
            Some(self.delimiter),
            self.batch_size,
            None,
            self.file_projection.clone(),
            datetime_format,
        )
    }
}

struct CsvOpener {
    config: Arc<CsvConfig>,
    file_compression_type: FileCompressionType,
}

impl FileOpener for CsvOpener {
    fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        file_meta: FileMeta,
    ) -> Result<FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.to_owned();
        Ok(Box::pin(async move {
            match store.get(file_meta.location()).await? {
                GetResult::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    Ok(futures::stream::iter(config.open(decoder, true)).boxed())
                }
                GetResult::Stream(s) => {
                    let mut first_chunk = true;
                    let s = s.map_err(Into::<DataFusionError>::into);
                    let decoder = file_compression_type.convert_stream(s)?;
                    Ok(newline_delimited_stream(decoder)
                        .map_ok(move |bytes| {
                            let reader = config.open(bytes.reader(), first_chunk);
                            first_chunk = false;
                            futures::stream::iter(reader)
                        })
                        .try_flatten()
                        .boxed())
                }
            }
        }))
    }
}

pub async fn plan_to_csv(
    state: &SessionState,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the CSV files (one per partition)
    let fs_path = Path::new(path);
    match fs::create_dir(fs_path) {
        Ok(()) => {
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let plan = plan.clone();
                let filename = format!("part-{}.csv", i);
                let path = fs_path.join(filename);
                let file = fs::File::create(path)?;
                let mut writer = csv::Writer::new(file);
                let task_ctx = Arc::new(TaskContext::from(state));
                let stream = plan.execute(i, task_ctx)?;
                let handle: JoinHandle<Result<()>> = task::spawn(async move {
                    stream
                        .map(|batch| writer.write(&batch?))
                        .try_collect()
                        .await
                        .map_err(DataFusionError::from)
                });
                tasks.push(handle);
            }
            futures::future::join_all(tasks)
                .await
                .into_iter()
                .try_for_each(|result| {
                    result.map_err(|e| DataFusionError::Execution(format!("{}", e)))?
                })?;
            Ok(())
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Could not create directory {}: {:?}",
            path, e
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::file_format::file_type::FileType;
    use crate::physical_plan::file_format::chunked_store::ChunkedStore;
    use crate::physical_plan::file_format::partition_type_wrap;
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

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ)
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
        case(FileCompressionType::XZ)
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
        case(FileCompressionType::XZ)
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

        let mut it = csv.execute(0, task_ctx)?;
        let batch = it.next().await.unwrap()?;
        assert_eq!(14, batch.num_columns());
        assert_eq!(5, batch.num_rows());

        let expected = vec![
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+-------------+",
            "| c1 | c2 | c3  | c4     | c5         | c6                   | c7  | c8    | c9         | c10                  | c11         | c12                 | c13                            | missing_col |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+-------------+",
            "| c  | 2  | 1   | 18109  | 2033001162 | -6513304855495910254 | 25  | 43062 | 1491205016 | 5863949479783605708  | 0.110830784 | 0.9294097332465232  | 6WfVFBVGJSQb7FhA7E0lBwdvjfZnSW |             |",
            "| d  | 5  | -40 | 22614  | 706441268  | -7542719935673075327 | 155 | 14337 | 3373581039 | 11720144131976083864 | 0.69632107  | 0.3114712539863804  | C2GT5KVyOPZpgKVl110TyZO0NcJ434 |             |",
            "| b  | 1  | 29  | -18218 | 994303988  | 5983957848665088916  | 204 | 9489  | 3275293996 | 14857091259186476033 | 0.53840446  | 0.17909035118828576 | AyYVExXK6AR2qUTxNZ7qRHQOVGMLcz |             |",
            "| a  | 1  | -85 | -15154 | 1171968280 | 1919439543497968449  | 77  | 52286 | 774637006  | 12101411955859039553 | 0.12285209  | 0.6864391962767343  | 0keZ5G8BffGwgF2RwQD59TFzMStxCB |             |",
            "| b  | 5  | -82 | 22080  | 1824882165 | 7373730676428214987  | 208 | 34331 | 3342719438 | 3330177516592499461  | 0.82634634  | 0.40975383525297016 | Ig1QcuKsjHXkproePdERo2w0mYzIqd |             |",
            "+----+----+-----+--------+------------+----------------------+-----+-------+------------+----------------------+-------------+---------------------+--------------------------------+-------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);

        Ok(())
    }

    #[rstest(
        file_compression_type,
        case(FileCompressionType::UNCOMPRESSED),
        case(FileCompressionType::GZIP),
        case(FileCompressionType::BZIP2),
        case(FileCompressionType::XZ)
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
        config.table_partition_cols =
            vec![("date".to_owned(), partition_type_wrap(DataType::Utf8))];
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
            let filename = format!("partition-{}.{}", partition, file_extension);
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
        ctx.runtime_env()
            .register_object_store("file", "", store.clone());

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
        case(FileCompressionType::XZ)
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
    async fn write_csv_results_error_handling() -> Result<()> {
        let ctx = SessionContext::new();
        let options = CsvReadOptions::default()
            .schema_infer_max_records(2)
            .has_header(true);
        let df = ctx.read_csv("tests/csv/corrupt.csv", options).await?;
        let tmp_dir = TempDir::new()?;
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let e = df
            .write_csv(&out_dir)
            .await
            .expect_err("should fail because input file does not match inferred schema");
        assert_eq!("Arrow error: Parser error: Error while parsing value d for column 0 at line 4", format!("{}", e));
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
            &format!("{}/part-0.csv", out_dir),
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
}
