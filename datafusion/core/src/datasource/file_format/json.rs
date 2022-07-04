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

use std::any::Any;
use std::io::BufReader;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::reader::ValueIter;
use async_trait::async_trait;
use object_store::{GetResult, ObjectMeta, ObjectStore};

use super::FileFormat;
use super::FileScanConfig;
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::NdJsonExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// The default file extension of json files
pub const DEFAULT_JSON_EXTENSION: &str = ".json";
/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug)]
pub struct JsonFormat {
    schema_infer_max_rec: Option<usize>,
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_rec: Some(DEFAULT_SCHEMA_INFER_MAX_RECORD),
        }
    }
}

impl JsonFormat {
    /// Set a limit in terms of records to scan to infer the schema
    /// - defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: Option<usize>) -> Self {
        self.schema_infer_max_rec = max_rec;
        self
    }
}

#[async_trait]
impl FileFormat for JsonFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);
        for object in objects {
            let mut take_while = || {
                let should_take = records_to_read > 0;
                if should_take {
                    records_to_read -= 1;
                }
                should_take
            };

            let schema = match store.get(&object.location).await? {
                GetResult::File(file, _) => {
                    let mut reader = BufReader::new(file);
                    let iter = ValueIter::new(&mut reader, None);
                    infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                }
                r @ GetResult::Stream(_) => {
                    let data = r.bytes().await?;
                    let mut reader = BufReader::new(data.as_ref());
                    let iter = ValueIter::new(&mut reader, None);
                    infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                }
            };

            schemas.push(schema);
            if records_to_read == 0 {
                break;
            }
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = NdJsonExec::new(conf);
        Ok(Arc::new(exec))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use arrow::array::Int64Array;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::physical_plan::collect;
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let ctx = SessionContext::with_config(config);
        let projection = None;
        let exec = get_exec(projection, None).await?;
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx)?;

        let tt_batches: i32 = stream
            .map(|batch| {
                let batch = batch.unwrap();
                assert_eq!(4, batch.num_columns());
                assert_eq!(2, batch.num_rows());
            })
            .fold(0, |acc, _| async move { acc + 1i32 })
            .await;

        assert_eq!(tt_batches, 6 /* 12/2 */);

        // test metadata
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = None;
        let exec = get_exec(projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let projection = None;
        let exec = get_exec(projection, None).await?;

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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(projection, None).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

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
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = "tests/jsons/2.json";
        let format = JsonFormat::default();
        scan_format(&format, ".", filename, projection, limit).await
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let store = Arc::new(LocalFileSystem::new()) as _;
        let filename = "tests/jsons/schema_infer_limit.json";
        let format = JsonFormat::default().with_schema_infer_max_rec(Some(3));

        let file_schema = format
            .infer_schema(&store, &[local_unpartitioned_file(filename)])
            .await
            .expect("Schema inference");

        let fields = file_schema
            .fields()
            .iter()
            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
            .collect::<Vec<_>>();
        assert_eq!(vec!["a: Int64", "b: Float64", "c: Boolean"], fields);
    }
}
