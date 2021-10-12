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

//! Apache Avro format abstractions

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::{self, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;

use super::FileFormat;
use super::PartitionedFile;
use crate::avro_to_arrow::read_avro_schema_from_reader;
use crate::datasource::object_store::{ObjectStoreRegistry, SizedFile, SizedFileStream};
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::AvroExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// Avro `FileFormat` implementation.
pub struct AvroFormat {
    /// Object store registry
    pub object_store_registry: Arc<ObjectStoreRegistry>,
}

impl Default for AvroFormat {
    fn default() -> Self {
        Self {
            object_store_registry: Arc::new(ObjectStoreRegistry::new()),
        }
    }
}

impl AvroFormat {
    /// Create Parquet with the given object store and default values
    pub fn new(object_store_registry: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            object_store_registry,
        }
    }
}

#[async_trait]
impl FileFormat for AvroFormat {
    async fn infer_schema(&self, mut file_stream: SizedFileStream) -> Result<SchemaRef> {
        let mut schemas = vec![];
        while let Some(fmeta_res) = file_stream.next().await {
            let fmeta = fmeta_res?;
            let mut reader = self
                .object_store_registry
                .get_by_uri(&fmeta.path)?
                .file_reader(fmeta)?
                .reader()?;
            let schema = read_avro_schema_from_reader(&mut reader)?;
            schemas.push(schema);
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(&self, _path: SizedFile) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        schema: SchemaRef,
        files: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = AvroExec::new(
            Arc::clone(&self.object_store_registry),
            // flattening this for now because CsvExec does not support partitioning yet
            files.into_iter().flatten().collect(),
            statistics,
            schema,
            projection.clone(),
            batch_size,
            limit,
        );
        Ok(Arc::new(exec))
    }

    fn object_store_registry(&self) -> &Arc<ObjectStoreRegistry> {
        &self.object_store_registry
    }
}

#[cfg(test)]
#[cfg(feature = "avro")]
mod tests {
    use crate::{
        datasource::object_store::local::{local_sized_file, local_sized_file_stream},
        physical_plan::collect,
    };

    use super::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array,
        TimestampMicrosecondArray,
    };
    use futures::StreamExt;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let projection = None;
        let exec = get_exec("alltypes_plain.avro", &projection, 2, None).await?;
        let stream = exec.execute(0).await?;

        let tt_batches = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(11, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 4 /* 8/2 */);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let projection = None;
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, Some(1)).await?;
        let batches = collect(exec).await?;
        assert_eq!(1, batches.len());
        assert_eq!(11, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn read_alltypes_plain_avro() -> Result<()> {
        let projection = None;
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "id: Int32",
                "bool_col: Boolean",
                "tinyint_col: Int32",
                "smallint_col: Int32",
                "int_col: Int32",
                "bigint_col: Int64",
                "float_col: Float32",
                "double_col: Float64",
                "date_string_col: Binary",
                "string_col: Binary",
                "timestamp_col: Timestamp(Microsecond, None)",
            ],
            x
        );

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);

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

        crate::assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn read_bool_alltypes_plain_avro() -> Result<()> {
        let projection = Some(vec![1]);
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let mut values: Vec<bool> = vec![];
        for i in 0..batches[0].num_rows() {
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
        let projection = Some(vec![0]);
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut values: Vec<i32> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[4, 5, 6, 7, 2, 3, 0, 1]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_i96_alltypes_plain_avro() -> Result<()> {
        let projection = Some(vec![10]);
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!("[1235865600000000, 1235865660000000, 1238544000000000, 1238544060000000, 1233446400000000, 1233446460000000, 1230768000000000, 1230768060000000]", format!("{:?}", values));

        Ok(())
    }

    #[tokio::test]
    async fn read_f32_alltypes_plain_avro() -> Result<()> {
        let projection = Some(vec![6]);
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        let mut values: Vec<f32> = vec![];
        for i in 0..batches[0].num_rows() {
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
        let projection = Some(vec![7]);
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut values: Vec<f64> = vec![];
        for i in 0..batches[0].num_rows() {
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
        let projection = Some(vec![9]);
        let exec = get_exec("alltypes_plain.avro", &projection, 1024, None).await?;

        let batches = collect(exec).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(8, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(std::str::from_utf8(array.value(i)).unwrap());
        }

        assert_eq!(
            "[\"0\", \"1\", \"0\", \"1\", \"0\", \"1\", \"0\", \"1\"]",
            format!("{:?}", values)
        );

        Ok(())
    }

    async fn get_exec(
        file_name: &str,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/{}", testdata, file_name);
        let format = AvroFormat::default();
        let schema = format
            .infer_schema(local_sized_file_stream(vec![filename.clone()]))
            .await
            .expect("Schema inference");
        let stats = format
            .infer_stats(local_sized_file(filename.clone()))
            .await
            .expect("Stats inference");
        let files = vec![vec![PartitionedFile {
            file: local_sized_file(filename.to_owned()),
        }]];
        let exec = format
            .create_physical_plan(
                schema,
                files,
                stats,
                projection,
                batch_size,
                &[],
                limit,
            )
            .await?;
        Ok(exec)
    }
}

#[cfg(test)]
#[cfg(not(feature = "avro"))]
mod tests {
    use super::*;

    use crate::datasource::object_store::local::local_sized_file_stream;
    use crate::error::DataFusionError;

    #[tokio::test]
    async fn test() -> Result<()> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/alltypes_plain.avro", testdata);
        let schema_result = AvroFormat::default()
            .infer_schema(local_sized_file_stream(vec![filename]))
            .await;
        assert!(matches!(
            schema_result,
            Err(DataFusionError::NotImplemented(msg))
            if msg == *"cannot read avro schema without the 'avro' feature enabled"
        ));

        Ok(())
    }
}
