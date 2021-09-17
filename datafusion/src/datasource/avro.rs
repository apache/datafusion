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

//! Line-delimited Avro data source
//!
//! This data source allows Line-delimited Avro records or files to be used as input for queries.
//!

use std::{
    any::Any,
    io::{Read, Seek},
    sync::{Arc, Mutex},
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use crate::physical_plan::avro::{AvroExec, AvroReadOptions};
use crate::{
    datasource::{Source, TableProvider},
    error::{DataFusionError, Result},
    physical_plan::{common, ExecutionPlan},
};

trait SeekRead: Read + Seek {}

impl<T: Seek + Read> SeekRead for T {}

/// Represents a  line-delimited Avro file with a provided schema
pub struct AvroFile {
    source: Source<Box<dyn SeekRead + Send + Sync + 'static>>,
    schema: SchemaRef,
    file_extension: String,
}

impl AvroFile {
    /// Attempt to initialize a `AvroFile` from a path. The schema can be read automatically.
    pub fn try_new(path: &str, options: AvroReadOptions) -> Result<Self> {
        let schema = if let Some(schema) = options.schema {
            schema
        } else {
            let filenames =
                common::build_checked_file_list(path, options.file_extension)?;
            Arc::new(AvroExec::try_read_schema(&filenames)?)
        };

        Ok(Self {
            source: Source::Path(path.to_string()),
            schema,
            file_extension: options.file_extension.to_string(),
        })
    }

    /// Attempt to initialize a `AvroFile` from a reader. The schema MUST be provided in options
    pub fn try_new_from_reader<R: Read + Seek + Send + Sync + 'static>(
        reader: R,
        options: AvroReadOptions,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s,
            None => {
                return Err(DataFusionError::Execution(
                    "Schema must be provided to CsvRead".to_string(),
                ));
            }
        };
        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            file_extension: String::new(),
        })
    }

    /// Attempt to initialize an AvroFile from a reader impls Seek. The schema can be read automatically.
    pub fn try_new_from_reader_schema<R: Read + Seek + Send + Sync + 'static>(
        mut reader: R,
        options: AvroReadOptions,
    ) -> Result<Self> {
        let schema = {
            if let Some(schema) = options.schema {
                schema
            } else {
                Arc::new(crate::avro_to_arrow::read_avro_schema_from_reader(
                    &mut reader,
                )?)
            }
        };

        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            file_extension: String::new(),
        })
    }

    /// Get the path for Avro file(s) represented by this AvroFile instance
    pub fn path(&self) -> &str {
        match &self.source {
            Source::Reader(_) => "",
            Source::Path(path) => path,
        }
    }

    /// Get the file extension for the Avro file(s) represented by this AvroFile instance
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }
}

#[async_trait]
impl TableProvider for AvroFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[crate::logical_plan::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let opts = AvroReadOptions {
            schema: Some(self.schema.clone()),
            file_extension: self.file_extension.as_str(),
        };
        let batch_size = limit
            .map(|l| std::cmp::min(l, batch_size))
            .unwrap_or(batch_size);

        let exec = match &self.source {
            Source::Reader(maybe_reader) => {
                if let Some(rdr) = maybe_reader.lock().unwrap().take() {
                    AvroExec::try_new_from_reader(
                        rdr,
                        opts,
                        projection.clone(),
                        batch_size,
                        limit,
                    )?
                } else {
                    return Err(DataFusionError::Execution(
                        "You can only read once if the data comes from a reader"
                            .to_string(),
                    ));
                }
            }
            Source::Path(p) => {
                AvroExec::try_from_path(p, opts, projection.clone(), batch_size, limit)?
            }
        };
        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
#[cfg(feature = "avro")]
mod tests {
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampMicrosecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = None;
        let exec = table.scan(&projection, 2, &[], None).await?;
        let stream = exec.execute(0).await?;

        let _ = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        Ok(())
    }

    #[cfg(feature = "avro")]
    #[tokio::test]
    async fn read_alltypes_plain_avro() -> Result<()> {
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
        let expected =  vec![
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
            "| id | bool_col | tinyint_col | smallint_col | int_col | bigint_col | float_col | double_col | date_string_col  | string_col | timestamp_col       |",
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
            "| 4  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30332f30312f3039 | 30         | 2009-03-01 00:00:00 |",
            "| 5  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30332f30312f3039 | 31         | 2009-03-01 00:01:00 |",
            "| 6  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30342f30312f3039 | 30         | 2009-04-01 00:00:00 |",
            "| 7  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30342f30312f3039 | 31         | 2009-04-01 00:01:00 |",
            "| 2  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30322f30312f3039 | 30         | 2009-02-01 00:00:00 |",
            "| 3  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30322f30312f3039 | 31         | 2009-02-01 00:01:00 |",
            "| 0  | true     | 0           | 0            | 0       | 0          | 0         | 0          | 30312f30312f3039 | 30         | 2009-01-01 00:00:00 |",
            "| 1  | false    | 1           | 1            | 1       | 10         | 1.1       | 10.1       | 30312f30312f3039 | 31         | 2009-01-01 00:01:00 |",
            "+----+----------+-------------+--------------+---------+------------+-----------+------------+------------------+------------+---------------------+",
        ];

        crate::assert_batches_eq!(expected, &[batch]);
        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_avro() -> Result<()> {
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
    async fn read_i32_alltypes_plain_avro() -> Result<()> {
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
    async fn read_i96_alltypes_plain_avro() -> Result<()> {
        let table = load_table("alltypes_plain.avro")?;
        let projection = Some(vec![10]);
        let batch = get_first_batch(table, &projection).await?;
        assert_eq!(1, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batch.num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000, 1235865660000000, 1238544000000000, 1238544060000000, 1233446400000000, 1233446460000000, 1230768000000000, 1230768060000000]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_avro() -> Result<()> {
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
    async fn read_f64_alltypes_plain_avro() -> Result<()> {
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
    async fn read_binary_alltypes_plain_avro() -> Result<()> {
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

    fn load_table(name: &str) -> Result<Arc<dyn TableProvider>> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/{}", testdata, name);
        let table = AvroFile::try_new(&filename, AvroReadOptions::default())?;
        Ok(Arc::new(table))
    }

    async fn get_first_batch(
        table: Arc<dyn TableProvider>,
        projection: &Option<Vec<usize>>,
    ) -> Result<RecordBatch> {
        let exec = table.scan(projection, 1024, &[], None).await?;
        let mut it = exec.execute(0).await?;
        it.next()
            .await
            .expect("should have received at least one batch")
            .map_err(|e| e.into())
    }
}
