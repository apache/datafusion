use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

#[tokio::test]
async fn datasource_splits_large_batches() -> datafusion_common::Result<()> {
    let batch_size = 20000;
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let array = Int32Array::from_iter_values(0..batch_size as i32);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])?;
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    let batches = collect(stream).await?;
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

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let array = Int32Array::from_iter_values(0..configured_batch_size as i32);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])?;
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    let batches = collect(stream).await?;

    // Should not split when exactly equal to batch_size
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), configured_batch_size);
    Ok(())
}

#[tokio::test]
async fn datasource_small_batch_no_split() -> datafusion_common::Result<()> {
    // Test with batch smaller than the split threshold (1024)
    let small_batch_size = 512; // Less than 1024

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let array = Int32Array::from_iter_values(0..small_batch_size as i32);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])?;
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    let batches = collect(stream).await?;

    // Should not split small batches below the threshold
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), small_batch_size);
    Ok(())
}

#[tokio::test]
async fn datasource_empty_batch_clean_termination() -> datafusion_common::Result<()> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let array = Int32Array::from_iter_values(std::iter::empty::<i32>());
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as ArrayRef])?;
    let exec = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    let batches = collect(stream).await?;

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
    let batches_input = vec![empty_batch.clone(), empty_batch.clone(), empty_batch];
    let exec = MemorySourceConfig::try_new_exec(&[batches_input], schema, None)?;
    let ctx = Arc::new(TaskContext::default());
    let stream = exec.execute(0, ctx)?;
    let batches = collect(stream).await?;

    // Should preserve empty batches without issues
    assert_eq!(batches.len(), 3);
    for batch in &batches {
        assert_eq!(batch.num_rows(), 0);
    }
    Ok(())
}
