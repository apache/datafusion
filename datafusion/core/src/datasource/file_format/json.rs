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
use std::fmt;
use std::fmt::Debug;
use std::io::BufReader;
use std::sync::Arc;

use super::{FileFormat, FileScanConfig};
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::reader::ValueIter;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Buf;

use bytes::Bytes;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::PhysicalSortRequirement;
use datafusion_physical_plan::ExecutionPlan;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

use crate::datasource::physical_plan::FileGroupDisplay;
use crate::physical_plan::insert::DataSink;
use crate::physical_plan::insert::FileSinkExec;
use crate::physical_plan::SendableRecordBatchStream;
use crate::physical_plan::{DisplayAs, DisplayFormatType, Statistics};

use super::write::orchestration::{stateless_append_all, stateless_multipart_put};

use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::write::{BatchSerializer, FileWriterMode};
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::datasource::physical_plan::{FileSinkConfig, NdJsonExec};
use crate::error::Result;
use crate::execution::context::SessionState;

use datafusion_common::{not_impl_err, DataFusionError, FileType};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::metrics::MetricsSet;

/// New line delimited JSON `FileFormat` implementation.
#[derive(Debug)]
pub struct JsonFormat {
    schema_infer_max_rec: Option<usize>,
    file_compression_type: FileCompressionType,
}

impl Default for JsonFormat {
    fn default() -> Self {
        Self {
            schema_infer_max_rec: Some(DEFAULT_SCHEMA_INFER_MAX_RECORD),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
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

    /// Set a `FileCompressionType` of JSON
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
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
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);
        let file_compression_type = self.file_compression_type.to_owned();
        for object in objects {
            let mut take_while = || {
                let should_take = records_to_read > 0;
                if should_take {
                    records_to_read -= 1;
                }
                should_take
            };

            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let mut reader = BufReader::new(decoder);
                    let iter = ValueIter::new(&mut reader, None);
                    infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                }
                GetResultPayload::Stream(_) => {
                    let data = r.bytes().await?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let mut reader = BufReader::new(decoder);
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
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = NdJsonExec::new(conf, self.file_compression_type.to_owned());
        Ok(Arc::new(exec))
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        conf: FileSinkConfig,
        order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return not_impl_err!("Overwrites are not implemented yet for Json");
        }

        if self.file_compression_type != FileCompressionType::UNCOMPRESSED {
            return not_impl_err!("Inserting compressed JSON is not implemented yet.");
        }
        let sink_schema = conf.output_schema().clone();
        let sink = Arc::new(JsonSink::new(conf));

        Ok(Arc::new(FileSinkExec::new(
            input,
            sink,
            sink_schema,
            order_requirements,
        )) as _)
    }

    fn file_type(&self) -> FileType {
        FileType::JSON
    }
}

impl Default for JsonSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Define a struct for serializing Json records to a stream
pub struct JsonSerializer {
    // Inner buffer for avoiding reallocation
    buffer: Vec<u8>,
}

impl JsonSerializer {
    /// Constructor for the JsonSerializer object
    pub fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(4096),
        }
    }
}

#[async_trait]
impl BatchSerializer for JsonSerializer {
    async fn serialize(&mut self, batch: RecordBatch) -> Result<Bytes> {
        let mut writer = json::LineDelimitedWriter::new(&mut self.buffer);
        writer.write(&batch)?;
        //drop(writer);
        Ok(Bytes::from(self.buffer.drain(..).collect::<Vec<u8>>()))
    }

    fn duplicate(&mut self) -> Result<Box<dyn BatchSerializer>> {
        Ok(Box::new(JsonSerializer::new()))
    }
}

/// Implements [`DataSink`] for writing to a Json file.
pub struct JsonSink {
    /// Config options for writing data
    config: FileSinkConfig,
}

impl Debug for JsonSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonSink").finish()
    }
}

impl DisplayAs for JsonSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "JsonSink(writer_mode={:?}, file_groups=",
                    self.config.writer_mode
                )?;
                FileGroupDisplay(&self.config.file_groups).fmt_as(t, f)?;
                write!(f, ")")
            }
        }
    }
}

impl JsonSink {
    /// Create from config.
    pub fn new(config: FileSinkConfig) -> Self {
        Self { config }
    }

    /// Retrieve the inner [`FileSinkConfig`].
    pub fn config(&self) -> &FileSinkConfig {
        &self.config
    }

    async fn append_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        if !self.config.table_partition_cols.is_empty() {
            return Err(DataFusionError::NotImplemented("Inserting in append mode to hive style partitioned tables is not supported".into()));
        }

        let writer_options = self.config.file_type_writer_options.try_into_json()?;
        let compression = &writer_options.compression;

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;
        let file_groups = &self.config.file_groups;

        let get_serializer = move |_| {
            let serializer: Box<dyn BatchSerializer> = Box::new(JsonSerializer::new());
            serializer
        };

        stateless_append_all(
            data,
            context,
            object_store,
            file_groups,
            self.config.unbounded_input,
            (*compression).into(),
            Box::new(get_serializer),
        )
        .await
    }

    async fn multipartput_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let writer_options = self.config.file_type_writer_options.try_into_json()?;
        let compression = &writer_options.compression;

        let get_serializer = move || {
            let serializer: Box<dyn BatchSerializer> = Box::new(JsonSerializer::new());
            serializer
        };

        stateless_multipart_put(
            data,
            context,
            "json".into(),
            Box::new(get_serializer),
            &self.config,
            (*compression).into(),
        )
        .await
    }
}

#[async_trait]
impl DataSink for JsonSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        match self.config.writer_mode {
            FileWriterMode::Append => {
                let total_count = self.append_all(data, context).await?;
                Ok(total_count)
            }
            FileWriterMode::PutMultipart => {
                let total_count = self.multipartput_all(data, context).await?;
                Ok(total_count)
            }
            FileWriterMode::Put => {
                return not_impl_err!("FileWriterMode::Put is not supported yet!")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use datafusion_common::cast::as_int64_array;
    use datafusion_common::stats::Precision;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::physical_plan::collect;
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::new_with_config(config);
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, projection, None).await?;
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
        assert_eq!(exec.statistics()?.num_rows, Precision::Absent);
        assert_eq!(exec.statistics()?.total_byte_size, Precision::Absent);

        Ok(())
    }

    #[tokio::test]
    async fn read_limit() -> Result<()> {
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = None;
        let exec = get_exec(&state, projection, Some(1)).await?;
        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        assert_eq!(4, batches[0].num_columns());
        assert_eq!(1, batches[0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn infer_schema() -> Result<()> {
        let projection = None;
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();
        let exec = get_exec(&state, projection, None).await?;

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
        let state = session_ctx.state();
        let task_ctx = state.task_ctx();
        let projection = Some(vec![0]);
        let exec = get_exec(&state, projection, None).await?;

        let batches = collect(exec, task_ctx).await.expect("Collect batches");

        assert_eq!(1, batches.len());
        assert_eq!(1, batches[0].num_columns());
        assert_eq!(12, batches[0].num_rows());

        let array = as_int64_array(batches[0].column(0))?;
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
        state: &SessionState,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filename = "tests/data/2.json";
        let format = JsonFormat::default();
        scan_format(state, &format, ".", filename, projection, limit).await
    }

    #[tokio::test]
    async fn infer_schema_with_limit() {
        let session = SessionContext::new();
        let ctx = session.state();
        let store = Arc::new(LocalFileSystem::new()) as _;
        let filename = "tests/data/schema_infer_limit.json";
        let format = JsonFormat::default().with_schema_infer_max_rec(Some(3));

        let file_schema = format
            .infer_schema(&ctx, &store, &[local_unpartitioned_file(filename)])
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
