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
