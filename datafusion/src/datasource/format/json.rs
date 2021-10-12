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

//! Line delimited JSON format abstractions

use std::io::BufReader;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::reader::ValueIter;
use async_trait::async_trait;
use futures::StreamExt;
use std::fs::File;

use super::{FileFormat, StringStream};
use crate::datasource::PartitionedFile;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::format::NdJsonExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// New line delimited JSON `FileFormat` implementation.
pub struct JsonFormat {
    /// If no schema was provided for the table, it will be
    /// infered from the data itself, this limits the number
    /// of lines used in the process.
    pub schema_infer_max_rec: Option<usize>,
}

#[async_trait]
impl FileFormat for JsonFormat {
    async fn infer_schema(&self, mut paths: StringStream) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);
        while let Some(file) = paths.next().await {
            let file = File::open(file)?;
            let mut reader = BufReader::new(file);
            let iter = ValueIter::new(&mut reader, None);
            let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
                let should_take = records_to_read > 0;
                records_to_read -= 1;
                should_take
            }))?;
            if records_to_read == 0 {
                break;
            }
            schemas.push(schema);
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(&self, _path: &str) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_executor(
        &self,
        schema: SchemaRef,
        files: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = NdJsonExec::try_new(
            // flattening this for now because NdJsonExec does not support partitioning yet
            files.into_iter().flatten().map(|f| f.path).collect(),
            statistics,
            schema,
            projection.clone(),
            batch_size,
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;

    use super::*;
    use crate::{datasource::format::string_stream, physical_plan::collect};

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let projection = None;
        let exec = get_exec(&projection, 2).await?;
        let stream = exec.execute(0).await?;

        let tt_rows: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(4, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_rows, 6 /* 12/2 */);

        // test metadata
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let projection = None;
        let exec = get_exec(&projection, 1024).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean", "d: Utf8",], x);

        Ok(())
    }

    #[tokio::test]
    async fn read_int_column() -> Result<()> {
        let projection = Some(vec![0]);
        let exec = get_exec(&projection, 1024).await?;

        let batches = collect(exec).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(12, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut values: Vec<i64> = vec![];
        for i in 0..batches[0].num_rows() {
            values.push(array.value(i));
        }

        assert_eq!(
            vec![1, -10, 2, 1, 7, 1, 1, 5, 1, 1, 1, 100000000000000],
            values
        );

        Ok(())
    }

    async fn get_exec(
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = "tests/jsons/2.json";
        let format = JsonFormat {
            schema_infer_max_rec: Some(1000),
        };
        let schema = format
            .infer_schema(string_stream(vec![filename.to_owned()]))
            .await
            .expect("Schema inference");
        let stats = format.infer_stats(filename).await.expect("Stats inference");
        let files = vec![vec![PartitionedFile {
            path: filename.to_owned(),
            statistics: stats.clone(),
        }]];
        let exec = format
            .create_executor(schema, files, stats, projection, batch_size, &[], None)
            .await?;
        Ok(exec)
    }
}
