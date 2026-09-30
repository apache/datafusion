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
use datafusion_common::{Result, assert_contains};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_plan::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::streaming::PartitionStream;
use futures::stream;
use rstest::rstest;
use tempfile::TempDir;

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
    tokio::time::timeout(Duration::from_secs(10), async {
        while pool.reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("failed writer retained memory");
    Ok(())
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
