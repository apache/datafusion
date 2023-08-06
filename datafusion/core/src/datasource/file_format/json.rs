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

use bytes::Bytes;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use futures::StreamExt;
use rand::distributions::Alphanumeric;
use rand::distributions::DistString;
use std::fmt;
use std::fmt::Debug;
use std::io::BufReader;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::json;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::reader::ValueIter;
use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::Buf;

use datafusion_physical_expr::PhysicalExpr;
use object_store::{GetResult, ObjectMeta, ObjectStore};

use crate::datasource::physical_plan::FileGroupDisplay;
use crate::datasource::physical_plan::FileMeta;
use crate::physical_plan::insert::DataSink;
use crate::physical_plan::insert::InsertExec;
use crate::physical_plan::SendableRecordBatchStream;
use crate::physical_plan::{DisplayAs, DisplayFormatType, Statistics};

use super::AbortMode;
use super::AbortableWrite;
use super::AsyncPutWriter;
use super::BatchSerializer;
use super::FileFormat;
use super::FileScanConfig;
use super::FileWriterMode;
use super::MultiPart;
use crate::datasource::file_format::file_type::FileCompressionType;
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::datasource::physical_plan::FileSinkConfig;
use crate::datasource::physical_plan::NdJsonExec;
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::ExecutionPlan;

/// The default file extension of json files
pub const DEFAULT_JSON_EXTENSION: &str = ".json";
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

            let schema = match store.get(&object.location).await? {
                GetResult::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let mut reader = BufReader::new(decoder);
                    let iter = ValueIter::new(&mut reader, None);
                    infer_json_schema_from_iterator(iter.take_while(|_| take_while()))?
                }
                r @ GetResult::Stream(_) => {
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
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
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
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if conf.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented yet for Json".into(),
            ));
        }

        if self.file_compression_type != FileCompressionType::UNCOMPRESSED {
            return Err(DataFusionError::NotImplemented(
                "Inserting compressed JSON is not implemented yet.".into(),
            ));
        }
        let sink_schema = conf.output_schema().clone();
        let sink = Arc::new(JsonSink::new(conf, self.file_compression_type.clone()));

        Ok(Arc::new(InsertExec::new(input, sink, sink_schema)) as _)
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
}

async fn check_for_errors<T, W: AsyncWrite + Unpin + Send>(
    result: Result<T>,
    writers: &mut [AbortableWrite<W>],
) -> Result<T> {
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            // Abort all writers before returning the error:
            for writer in writers {
                let mut abort_future = writer.abort_writer();
                if let Ok(abort_future) = &mut abort_future {
                    let _ = abort_future.await;
                }
                // Ignore errors that occur during abortion,
                // We do try to abort all writers before returning error.
            }
            // After aborting writers return original error.
            Err(e)
        }
    }
}

/// Implements [`DataSink`] for writing to a Json file.
struct JsonSink {
    /// Config options for writing data
    config: FileSinkConfig,
    file_compression_type: FileCompressionType,
}

impl Debug for JsonSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonSink")
            .field("file_compression_type", &self.file_compression_type)
            .finish()
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
    fn new(config: FileSinkConfig, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }

    // Create a write for Json files
    async fn create_writer(
        &self,
        file_meta: FileMeta,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<AbortableWrite<Box<dyn AsyncWrite + Send + Unpin>>> {
        let object = &file_meta.object_meta;
        match self.config.writer_mode {
            // If the mode is append, call the store's append method and return wrapped in
            // a boxed trait object.
            FileWriterMode::Append => {
                let writer = object_store
                    .append(&object.location)
                    .await
                    .map_err(DataFusionError::ObjectStore)?;
                let writer = AbortableWrite::new(
                    self.file_compression_type.convert_async_writer(writer)?,
                    AbortMode::Append,
                );
                Ok(writer)
            }
            // If the mode is put, create a new AsyncPut writer and return it wrapped in
            // a boxed trait object
            FileWriterMode::Put => {
                let writer = Box::new(AsyncPutWriter::new(object.clone(), object_store));
                let writer = AbortableWrite::new(
                    self.file_compression_type.convert_async_writer(writer)?,
                    AbortMode::Put,
                );
                Ok(writer)
            }
            // If the mode is put multipart, call the store's put_multipart method and
            // return the writer wrapped in a boxed trait object.
            FileWriterMode::PutMultipart => {
                let (multipart_id, writer) = object_store
                    .put_multipart(&object.location)
                    .await
                    .map_err(DataFusionError::ObjectStore)?;
                Ok(AbortableWrite::new(
                    self.file_compression_type.convert_async_writer(writer)?,
                    AbortMode::MultiPart(MultiPart::new(
                        object_store,
                        multipart_id,
                        object.location.clone(),
                    )),
                ))
            }
        }
    }
}

#[async_trait]
impl DataSink for JsonSink {
    async fn write_all(
        &self,
        mut data: Vec<SendableRecordBatchStream>,
        context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = data.len();

        let object_store = context
            .runtime_env()
            .object_store(&self.config.object_store_url)?;

        // Construct serializer and writer for each file group
        let mut serializers = vec![];
        let mut writers = vec![];
        match self.config.writer_mode {
            FileWriterMode::Append => {
                for file_group in &self.config.file_groups {
                    let serializer = JsonSerializer::new();
                    serializers.push(serializer);

                    let file = file_group.clone();
                    let writer = self
                        .create_writer(
                            file.object_meta.clone().into(),
                            object_store.clone(),
                        )
                        .await?;
                    writers.push(writer);
                }
            }
            FileWriterMode::Put => {
                return Err(DataFusionError::NotImplemented(
                    "Put Mode is not implemented for Json Sink yet".into(),
                ))
            }
            FileWriterMode::PutMultipart => {
                //currently assuming only 1 partition path (i.e. not hive style partitioning on a column)
                let base_path = &self.config.table_paths[0];
                //uniquely identify this batch of files with a random string, to prevent collisions overwriting files
                let write_id = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
                for part_idx in 0..num_partitions {
                    let serializer = JsonSerializer::new();
                    serializers.push(serializer);
                    let file_path = base_path
                        .prefix()
                        .child(format!("/{}_{}.json", write_id, part_idx));
                    let object_meta = ObjectMeta {
                        location: file_path,
                        last_modified: chrono::offset::Utc::now(),
                        size: 0,
                        e_tag: None,
                    };
                    let writer = self
                        .create_writer(object_meta.into(), object_store.clone())
                        .await?;
                    writers.push(writer);
                }
            }
        }

        let mut row_count = 0;
        // Map errors to DatafusionError.
        let err_converter =
            |_| DataFusionError::Internal("Unexpected FileSink Error".to_string());
        // TODO parallelize serialization accross partitions and batches within partitions
        // see: https://github.com/apache/arrow-datafusion/issues/7079
        for idx in 0..num_partitions {
            while let Some(maybe_batch) = data[idx].next().await {
                // Write data to files in a round robin fashion:
                let serializer = &mut serializers[idx];
                let batch = check_for_errors(maybe_batch, &mut writers).await?;
                row_count += batch.num_rows();
                let bytes =
                    check_for_errors(serializer.serialize(batch).await, &mut writers)
                        .await?;
                let writer = &mut writers[idx];
                check_for_errors(
                    writer.write_all(&bytes).await.map_err(err_converter),
                    &mut writers,
                )
                .await?;
            }
        }
        // Perform cleanup:
        let n_writers = writers.len();
        for idx in 0..n_writers {
            check_for_errors(
                writers[idx].shutdown().await.map_err(err_converter),
                &mut writers,
            )
            .await?;
        }
        Ok(row_count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::scan_format;
    use datafusion_common::cast::as_int64_array;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::physical_plan::collect;
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::object_store::local_unpartitioned_file;

    #[tokio::test]
    async fn read_small_batches() -> Result<()> {
        let config = SessionConfig::new().with_batch_size(2);
        let session_ctx = SessionContext::with_config(config);
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
        assert_eq!(exec.statistics().num_rows, None);
        assert_eq!(exec.statistics().total_byte_size, None);

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
