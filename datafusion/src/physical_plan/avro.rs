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

//! Execution plan for reading line-delimited Avro files
use async_trait::async_trait;
use futures::Stream;

use super::{common, source::Source, ExecutionPlan, Partitioning, RecordBatchStream};
use crate::avro::infer_avro_schema_from_reader;
use crate::error::{DataFusionError, Result};
use arrow::{
    datatypes::{Schema, SchemaRef},
    error::Result as ArrowResult,
    record_batch::RecordBatch,
};
use std::fs::File;
use std::{any::Any, io::Seek};
use std::{
    io::Read,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// Line-delimited Avro read options
#[derive(Clone)]
pub struct AvroReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".avro".
    pub file_extension: &'a str,
}

impl<'a> Default for AvroReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: ".avro",
        }
    }
}

trait SeekRead: Read + Seek {}

impl<T: Seek + Read> SeekRead for T {}
/// Execution plan for scanning Avro data source
#[derive(Debug)]
pub struct AvroExec {
    source: Source<Box<dyn SeekRead + Send + Sync>>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    file_extension: String,
    batch_size: usize,
    limit: Option<usize>,
}

impl AvroExec {
    /// Create a new execution plan for reading from a path
    pub fn try_new(
        path: &str,
        options: AvroReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let file_extension = options.file_extension.to_string();

        let filenames = common::build_file_list(path, &file_extension)?;

        if filenames.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "No files found at {path} with file extension {file_extension}",
                path = path,
                file_extension = file_extension.as_str()
            )));
        }

        let schema = match options.schema {
            Some(s) => s,
            None => Arc::new(AvroExec::try_infer_schema(filenames.as_slice())?),
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        };

        Ok(Self {
            source: Source::PartitionedFiles {
                path: path.to_string(),
                filenames,
            },
            schema,
            projected_schema,
            file_extension,
            projection,
            batch_size,
            limit,
        })
    }
    /// Create a new execution plan for reading from a reader
    pub fn try_new_from_reader(
        reader: impl Read + Seek + Send + Sync + 'static,
        options: AvroReadOptions,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s,
            None => {
                return Err(DataFusionError::Execution(
                    "The schema must be provided in options when reading from a reader"
                        .to_string(),
                ));
            }
        };

        let projected_schema = match &projection {
            None => schema.clone(),
            Some(p) => Arc::new(Schema::new(
                p.iter().map(|i| schema.field(*i).clone()).collect(),
            )),
        };

        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            file_extension: String::new(),
            projection,
            projected_schema,
            batch_size,
            limit,
        })
    }

    /// Path to directory containing partitioned CSV files with the same schema
    pub fn path(&self) -> &str {
        self.source.path()
    }

    /// The individual files under path
    pub fn filenames(&self) -> &[String] {
        self.source.filenames()
    }

    /// File extension
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }

    /// Get the schema of the CSV file
    pub fn file_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Optional projection for which columns to load
    pub fn projection(&self) -> Option<&Vec<usize>> {
        self.projection.as_ref()
    }

    /// Batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Limit
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Infer schema for given Avro dataset
    pub fn try_infer_schema(filenames: &[String]) -> Result<Schema> {
        let mut schemas = Vec::new();
        for filename in filenames {
            let mut file = File::open(filename)?;
            let schema = infer_avro_schema_from_reader(&mut file)?;
            schemas.push(schema);
        }

        Ok(Schema::try_merge(schemas)?)
    }
}

#[async_trait]
impl ExecutionPlan for AvroExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(match &self.source {
            Source::PartitionedFiles { filenames, .. } => filenames.len(),
            Source::Reader(_) => 1,
        })
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        } else if let Source::PartitionedFiles { filenames, path } = &self.source {
            Ok(Arc::new(Self {
                source: Source::PartitionedFiles {
                    filenames: filenames.clone(),
                    path: path.clone(),
                },
                schema: self.schema.clone(),
                projection: self.projection.clone(),
                projected_schema: self.projected_schema.clone(),
                batch_size: self.batch_size,
                limit: self.limit,
                file_extension: self.file_extension.clone(),
            }))
        } else {
            Err(DataFusionError::Internal(
                "AvroExec with reader source cannot be used with `with_new_children`"
                    .to_string(),
            ))
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<super::SendableRecordBatchStream> {
        let mut builder = crate::avro::ReaderBuilder::new()
            .with_schema(self.schema.clone())
            .with_batch_size(self.batch_size);
        if let Some(proj) = &self.projection {
            builder = builder.with_projection(
                proj.iter()
                    .map(|col_idx| self.schema.field(*col_idx).name())
                    .cloned()
                    .collect(),
            );
        }
        match &self.source {
            Source::PartitionedFiles { filenames, .. } => {
                let file = File::open(&filenames[partition])?;

                Ok(Box::pin(AvroStream::new(builder.build(file)?, self.limit)))
            }
            Source::Reader(rdr) => {
                if partition != 0 {
                    Err(DataFusionError::Internal(
                        "Only partition 0 is valid when CSV comes from a reader"
                            .to_string(),
                    ))
                } else if let Some(rdr) = rdr.lock().unwrap().take() {
                    Ok(Box::pin(AvroStream::new(builder.build(rdr)?, self.limit)))
                } else {
                    Err(DataFusionError::Execution(
                        "Error reading CSV: Data can only be read a single time when the source is a reader"
                            .to_string(),
                    ))
                }
            }
        }
    }
}

struct AvroStream<'a, R: Read> {
    reader: crate::avro::Reader<'a, R>,
    remain: Option<usize>,
}

impl<'a, R: Read> AvroStream<'a, R> {
    fn new(reader: crate::avro::Reader<'a, R>, limit: Option<usize>) -> Self {
        Self {
            reader,
            remain: limit,
        }
    }
}

impl<R: Read + Unpin> Stream for AvroStream<'_, R> {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(remain) = self.remain.as_mut() {
            if *remain < 1 {
                return Poll::Ready(None);
            }
        }

        Poll::Ready(match self.reader.next() {
            Ok(Some(item)) => {
                if let Some(remain) = self.remain.as_mut() {
                    if *remain >= item.num_rows() {
                        *remain -= item.num_rows();
                        Some(Ok(item))
                    } else {
                        let len = *remain;
                        *remain = 0;
                        Some(Ok(RecordBatch::try_new(
                            item.schema(),
                            item.columns()
                                .iter()
                                .map(|column| column.slice(0, len))
                                .collect(),
                        )?))
                    }
                } else {
                    Some(Ok(item))
                }
            }
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
    }
}

impl<R: Read + Unpin> RecordBatchStream for AvroStream<'_, R> {
    fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::avro::AvroFile;
    use crate::datasource::TableProvider;
    use crate::logical_plan::combine_filters;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampNanosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;

    fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/{}", testdata, name);
        let table = AvroFile::try_new(&filename, AvroReadOptions::default())?;
        Ok(Arc::new(table))
    }

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = None;
        let exec = table.scan(&projection, 2, &[], None)?;
        let stream = exec.execute(0).await?;

        let _ = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        // test metadata
        assert_eq!(table.statistics().num_rows, Some(8));
        assert_eq!(table.statistics().total_byte_size, Some(671));

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;

        let x: Vec<String> = table
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        let y = x.join("\n");
        assert_eq!(
            "id: Int32\n\
             bool_col: Boolean\n\
             tinyint_col: Int32\n\
             smallint_col: Int32\n\
             int_col: Int32\n\
             bigint_col: Int64\n\
             float_col: Float32\n\
             double_col: Float64\n\
             date_string_col: Binary\n\
             string_col: Binary\n\
             timestamp_col: Timestamp(Microsecond, None)",
            y
        );

        let projection = None;
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![1]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[true, false, true, false, true, false, true, false]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_i32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![0]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![10]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000000, 1235865660000000000, 1238544000000000000, 1238544060000000000, 1233446400000000000, 1233446460000000000, 1230768000000000000, 1230768060000000000]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![6]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 1.1, 0.0, 1.1, 0.0, 1.1, 0.0, 1.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_f64_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![7]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            "[0.0, 10.1, 0.0, 10.1, 0.0, 10.1, 0.0, 10.1]",
            format!("{:?}", values)
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_binary_alltypes_plain_parquet() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![9]);
        let batch = get_first_batch(table, &projection).await?;

        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..batch.num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );

        Ok(())
    }

    async fn get_first_batch(
        table: Arc<dyn TableProvider>,
        projection: &Option<Vec<usize>>,
    ) -> Result<RecordBatch> {
        let exec = table.scan(projection, 1024, &[], None)?;
        let mut it = exec.execute(0).await?;
        it.next()
            .await
            .expect("should have received at least one batch")
            .map_err(|e| e.into())
    }

    #[test]
    fn combine_zero_filters() {
        let result = combine_filters(&[]);
        assert_eq!(result, None);
    }

    #[test]
    fn combine_one_filter() {
        use crate::logical_plan::{binary_expr, col, lit, Operator};
        let filter = binary_expr(col("c1"), Operator::Lt, lit(1));
        let result = combine_filters(&[filter.clone()]);
        assert_eq!(result, Some(filter));
    }

    #[test]
    fn combine_multiple_filters() {
        use crate::logical_plan::{and, binary_expr, col, lit, Operator};
        let filter1 = binary_expr(col("c1"), Operator::Lt, lit(1));
        let filter2 = binary_expr(col("c2"), Operator::Lt, lit(2));
        let filter3 = binary_expr(col("c3"), Operator::Lt, lit(3));
        let result =
            combine_filters(&[filter1.clone(), filter2.clone(), filter3.clone()]);
        assert_eq!(result, Some(and(and(filter1, filter2), filter3)));
    }
}
