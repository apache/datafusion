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

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::SchemaRef;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_catalog::streaming::StreamingTable;
use datafusion_common::config::ConfigNonZeroUsize;
use datafusion_common::parquet_config::DFParquetCompression;
use datafusion_common::{Result, assert_contains};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use futures::stream;
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    memory::InMemory, path::Path,
};
use rstest::rstest;
use tempfile::TempDir;
use url::Url;

#[rstest]
#[case(None)]
#[case(Some(1_000_000))]
#[tokio::test]
async fn parallel_write_returns_column_error_before_input_ends(
    #[case] max_bytes: Option<usize>,
) -> Result<()> {
    let pool = Arc::new(GreedyMemoryPool::new(128 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let mut config = SessionConfig::new()
        .with_batch_size(128)
        .with_target_partitions(1);
    let options = &mut config.options_mut().execution;
    options.minimum_parallel_output_files = ConfigNonZeroUsize::try_new(1)?;
    options.parquet.max_row_group_size = usize::MAX;
    options.parquet.maximum_buffered_record_batches_per_stream = 1;
    if let Some(limit) = max_bytes {
        config.options_mut().set(
            "datafusion.execution.parquet.max_row_group_bytes",
            &limit.to_string(),
        )?;
    }
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..128)) as _),
        (
            "payload",
            Arc::new(StringArray::from_iter_values(
                (0..128).map(|i| format!("{i}{}", "x".repeat(8192))),
            )) as _,
        ),
    ])?;
    let source =
        StreamingTable::try_new(batch.schema(), vec![Arc::new(RepeatingBatch(batch))])?;
    let output = TempDir::new()?;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        ctx.read_table(Arc::new(source))?.write_parquet(
            output.path().to_str().unwrap(),
            DataFrameWriteOptions::new(),
            None,
        ),
    )
    .await
    .expect("the writer kept consuming input after a column failed")
    .expect_err("the column encoder should exceed the memory limit");
    assert_contains!(error.to_string(), "ParquetSink(ArrowColumnWriter)");
    wait_for_memory_release(pool.as_ref()).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_write_releases_memory_after_storage_error() -> Result<()> {
    let pool = Arc::new(GreedyMemoryPool::new(128 * 1024 * 1024));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let mut config = SessionConfig::new()
        .with_batch_size(128)
        .with_target_partitions(1);
    let options = &mut config.options_mut().execution;
    options.minimum_parallel_output_files = ConfigNonZeroUsize::try_new(1)?;
    options.objectstore_writer_buffer_size = 64 * 1024;
    options.parquet.allow_single_file_parallelism = true;
    options.parquet.max_row_group_size = usize::MAX;
    options.parquet.maximum_parallel_row_group_writers = 1;
    options.parquet.maximum_buffered_record_batches_per_stream = 1;
    options.parquet.compression = Some(DFParquetCompression::Uncompressed);
    config
        .options_mut()
        .set("datafusion.execution.parquet.max_row_group_bytes", "65536")?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_object_store(
        &Url::parse("test://fail-write").unwrap(),
        Arc::new(FailingWriteStore {
            inner: InMemory::new(),
            pool: pool.clone(),
        }),
    );
    // A row group exceeds the file writer's flush buffer without compression.
    // Endless input keeps encoding active when the storage write fails.
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from_iter_values(0..128)) as _),
        (
            "payload",
            Arc::new(StringArray::from_iter_values(
                (0..128).map(|i| format!("{i}{}", "x".repeat(16 * 1024))),
            )) as _,
        ),
    ])?;
    let source =
        StreamingTable::try_new(batch.schema(), vec![Arc::new(RepeatingBatch(batch))])?;
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        ctx.read_table(Arc::new(source))?.write_parquet(
            "test://fail-write/output/",
            DataFrameWriteOptions::new(),
            None,
        ),
    )
    .await
    .expect("parallel write hung after a storage failure")
    .expect_err("the storage write should fail");
    assert_contains!(error.to_string(), "injected Parquet storage write failure");
    wait_for_memory_release(pool.as_ref()).await;
    Ok(())
}

async fn wait_for_memory_release(pool: &dyn MemoryPool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed writer retained memory");
}

/// Keep input available so the test cannot pass by reporting the error at EOF.
#[derive(Debug)]
struct RepeatingBatch(RecordBatch);

impl PartitionStream for RepeatingBatch {
    fn schema(&self) -> &SchemaRef {
        self.0.schema_ref()
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let batch = self.0.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            batch.schema(),
            stream::repeat_with(move || Ok(batch.clone())),
        ))
    }
}

/// Fail from BufWriter's AsyncWrite path when it starts a multipart upload.
#[derive(Debug)]
struct FailingWriteStore {
    inner: InMemory,
    pool: Arc<dyn MemoryPool>,
}

impl std::fmt::Display for FailingWriteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingWriteStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingWriteStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        unreachable!("the write must fail during streaming, before shutdown")
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // Let the dispatcher and column workers run while the write is pending.
        tokio::task::yield_now().await;
        assert!(self.pool.reserved() > 0, "no active writer reservations");
        Err(object_store::Error::Generic {
            store: "FailingWriteStore",
            source: "injected Parquet storage write failure".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
