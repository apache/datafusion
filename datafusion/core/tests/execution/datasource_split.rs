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

use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{common::collect, ExecutionPlan};
use std::sync::Arc;

/// Helper function to create a memory source with the given batch size and collect all batches
async fn create_and_collect_batches(
    batch_size: usize,
) -> datafusion_common::Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let array = Int32Array::from_iter_values(0..batch_size as i32);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])?;
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    collect(stream).await
}

/// Helper function to create a memory source with multiple batches and collect all results
async fn create_and_collect_multiple_batches(
    input_batches: Vec<RecordBatch>,
) -> datafusion_common::Result<Vec<RecordBatch>> {
    let schema = input_batches[0].schema();
    let exec = MemorySourceConfig::try_new_exec(&[input_batches], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    collect(stream).await
}

#[tokio::test]
async fn datasource_splits_large_batches() -> datafusion_common::Result<()> {
    let batch_size = 20000;
    let batches = create_and_collect_batches(batch_size).await?;

    assert!(batches.len() > 1);
    let max = batches.iter().map(|b| b.num_rows()).max().unwrap();
    assert!(
        max <= datafusion_execution::config::SessionConfig::new()
            .options()
            .execution
            .batch_size
    );
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, batch_size);
    Ok(())
}

#[tokio::test]
async fn datasource_exact_batch_size_no_split() -> datafusion_common::Result<()> {
    let session_config = datafusion_execution::config::SessionConfig::new();
    let configured_batch_size = session_config.options().execution.batch_size;

    let batches = create_and_collect_batches(configured_batch_size).await?;

    // Should not split when exactly equal to batch_size
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), configured_batch_size);
    Ok(())
}

#[tokio::test]
async fn datasource_small_batch_no_split() -> datafusion_common::Result<()> {
    // Test with batch smaller than the batch size (8192)
    let small_batch_size = 512; // Less than 8192

    let batches = create_and_collect_batches(small_batch_size).await?;

    // Should not split small batches below the batch size
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), small_batch_size);
    Ok(())
}

#[tokio::test]
async fn datasource_empty_batch_clean_termination() -> datafusion_common::Result<()> {
    let batches = create_and_collect_batches(0).await?;

    // Empty batch should result in one empty batch
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 0);
    Ok(())
}

#[tokio::test]
async fn datasource_multiple_empty_batches() -> datafusion_common::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let empty_array = Int32Array::from_iter_values(std::iter::empty::<i32>());
    let empty_batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(empty_array) as ArrayRef])?;

    // Create multiple empty batches
    let input_batches = vec![empty_batch.clone(), empty_batch.clone(), empty_batch];
    let batches = create_and_collect_multiple_batches(input_batches).await?;

    // Should preserve empty batches without issues
    assert_eq!(batches.len(), 3);
    for batch in &batches {
        assert_eq!(batch.num_rows(), 0);
    }
    Ok(())
}
