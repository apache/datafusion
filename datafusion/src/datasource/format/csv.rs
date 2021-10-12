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

//! CSV format abstractions

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::{self, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;
use std::fs::File;

use super::{FileFormat, StringStream};
use crate::datasource::PartitionedFile;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::format::CsvExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// Character Separated Value `FileFormat` implementation.
pub struct CsvFormat {
    /// Set true to indicate that the first line is a header.
    pub has_header: bool,
    /// The character seprating values within a row.
    pub delimiter: u8,
    /// If no schema was provided for the table, it will be
    /// infered from the data itself, this limits the number
    /// of lines used in the process.
    pub schema_infer_max_rec: Option<usize>,
}

#[async_trait]
impl FileFormat for CsvFormat {
    async fn infer_schema(&self, mut paths: StringStream) -> Result<SchemaRef> {
        let mut schemas = vec![];
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(std::usize::MAX);

        while let Some(fname) = paths.next().await {
            let (schema, records_read) = arrow::csv::reader::infer_file_schema(
                &mut File::open(fname)?,
                self.delimiter,
                Some(records_to_read),
                self.has_header,
            )?;
            if records_read == 0 {
                continue;
            }
            schemas.push(schema.clone());
            records_to_read -= records_read;
            if records_to_read == 0 {
                break;
            }
        }

        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
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
        let exec = CsvExec::try_new(
            // flattening this for now because CsvExec does not support partitioning yet
            files.into_iter().flatten().map(|f| f.path).collect(),
            statistics,
            schema,
            self.has_header,
            self.delimiter,
            projection.clone(),
            batch_size,
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;

    use super::*;
    use crate::{datasource::format::string_stream, physical_plan::collect};

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        // skip column 9 that overflows the automaticly discovered column type of i64 (u64 would work)
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12]);
        let exec = get_exec("aggregate_test_100.csv", &projection, 2).await?;
        let stream = exec.execute(0).await?;

        let tt_rows: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(12, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_rows, 50 /* 100/2 */);

        // test metadata
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let projection = None;
        let exec = get_exec("aggregate_test_100.csv", &projection, 1024).await?;

        let x: Vec<String> = exec
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect();
        assert_eq!(
            vec![
                "c1: Utf8",
                "c2: Int64",
                "c3: Int64",
                "c4: Int64",
                "c5: Int64",
                "c6: Int64",
                "c7: Int64",
                "c8: Int64",
                "c9: Int64",
                "c10: Int64",
                "c11: Float64",
                "c12: Float64",
                "c13: Utf8"
            ],
            x
        );

        Ok(())
    }

    #[tokio::test]
    async fn read_char_column() -> Result<()> {
        let projection = Some(vec![0]);
        let exec = get_exec("aggregate_test_100.csv", &projection, 1024).await?;

        let batches = collect(exec).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(100, batches[0].num_rows());

        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let mut values: Vec<&str> = vec![];
        for i in 0..5 {
            values.push(array.value(i));
        }

        assert_eq!(vec!["c", "d", "b", "a", "b"], values);

        Ok(())
    }

    async fn get_exec(
        file_name: &str,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/csv/{}", testdata, file_name);
        let table = CsvFormat {
            has_header: true,
            schema_infer_max_rec: Some(1000),
            delimiter: b',',
        };
        let schema = table
            .infer_schema(string_stream(vec![filename.clone()]))
            .await
            .expect("Schema inference");
        let stats = table.infer_stats(&filename).await.expect("Stats inference");
        let files = vec![vec![PartitionedFile {
            path: filename,
            statistics: stats.clone(),
        }]];
        let exec = table
            .create_executor(schema, files, stats, projection, batch_size, &[], None)
            .await?;
        Ok(exec)
    }
}
