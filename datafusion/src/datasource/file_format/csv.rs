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

use super::FileFormat;
use super::PartitionedFile;
use crate::datasource::object_store::{ObjectStoreRegistry, SizedFile, SizedFileStream};
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::CsvExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// Character Separated Value `FileFormat` implementation.
pub struct CsvFormat {
    has_header: bool,
    delimiter: u8,
    schema_infer_max_rec: Option<usize>,
    object_store_registry: Arc<ObjectStoreRegistry>,
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            object_store_registry: Arc::new(ObjectStoreRegistry::new()),
            schema_infer_max_rec: None,
            has_header: true,
            delimiter: b',',
        }
    }
}

impl CsvFormat {
    /// Create Parquet with the given object store and default values
    pub fn new(object_store_registry: Arc<ObjectStoreRegistry>) -> Self {
        Self {
            object_store_registry,
            ..Default::default()
        }
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `None` (no limit)
    pub fn with_schema_infer_max_rec(&mut self, max_rec: Option<usize>) -> &mut Self {
        self.schema_infer_max_rec = max_rec;
        self
    }

    /// Set true to indicate that the first line is a header.
    /// - default to true
    pub fn with_has_header(&mut self, has_header: bool) -> &mut Self {
        self.has_header = has_header;
        self
    }

    /// The character separating values within a row.
    /// - default to ','
    pub fn with_delimiter(&mut self, delimiter: u8) -> &mut Self {
        self.delimiter = delimiter;
        self
    }
}

#[async_trait]
impl FileFormat for CsvFormat {
    async fn infer_schema(&self, mut file_stream: SizedFileStream) -> Result<SchemaRef> {
        let mut schemas = vec![];
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(std::usize::MAX);

        while let Some(fmeta_res) = file_stream.next().await {
            let fmeta = fmeta_res?;
            let fsize = fmeta.size as usize;
            let object_store = self.object_store_registry.get_by_uri(&fmeta.path)?;

            let obj_reader = object_store.file_reader(fmeta)?;
            let mut reader = obj_reader.chunk_reader(0, fsize)?;
            let (schema, records_read) = arrow::csv::reader::infer_reader_schema(
                &mut reader,
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
        let exec = CsvExec::new(
            Arc::clone(&self.object_store_registry),
            // flattening this for now because CsvExec does not support partitioning yet
            files.into_iter().flatten().collect(),
            statistics,
            schema,
            self.has_header,
            self.delimiter,
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
mod tests {
    use arrow::array::StringArray;

    use super::*;
    use crate::{
        datasource::object_store::local::{local_sized_file, local_sized_file_stream},
        physical_plan::collect,
    };

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
        let format = CsvFormat::default();
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
            .create_physical_plan(schema, files, stats, projection, batch_size, &[], None)
            .await?;
        Ok(exec)
    }
}
