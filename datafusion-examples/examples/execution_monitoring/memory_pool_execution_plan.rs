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

//! See `main.rs` for how to run it.
//!
//! This example demonstrates how to implement custom ExecutionPlans that properly
//! use memory tracking through TrackConsumersPool.
//!
//! This shows the pattern for implementing memory-aware operators that:
//! - Register memory consumers with the pool
//! - Reserve memory before allocating
//! - Handle memory pressure by spilling to disk
//! - Release memory when done

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::record_batch;
use datafusion::common::{exec_datafusion_err, internal_err};
use datafusion::datasource::{DefaultTableSource, memory::MemTable};
use datafusion::error::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::*;
use futures::stream::{StreamExt, TryStreamExt};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Shows how to implement memory-aware ExecutionPlan with memory reservation and spilling
pub async fn memory_pool_execution_plan() -> Result<()> {
    println!("=== DataFusion ExecutionPlan Memory Tracking Example ===\n");

    // Set up a runtime with memory tracking
    // Set a low memory limit to trigger spilling on the second batch
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(15000, 1.0) // Allow only enough for 1 batch at once
        .build_arc()?;

    let config = SessionConfig::new().with_coalesce_batches(false);
    let ctx = SessionContext::new_with_config_rt(config, runtime.clone());

    // Create smaller batches to ensure we get multiple RecordBatches from the scan
    // Make each batch smaller than the memory limit to force multiple batches
    let batch1 = record_batch!(
        ("id", Int32, vec![1; 800]),
        ("name", Utf8, vec!["Alice"; 800])
    )?;

    let batch2 = record_batch!(
        ("id", Int32, vec![2; 800]),
        ("name", Utf8, vec!["Bob"; 800])
    )?;

    let batch3 = record_batch!(
        ("id", Int32, vec![3; 800]),
        ("name", Utf8, vec!["Charlie"; 800])
    )?;

    let batch4 = record_batch!(
        ("id", Int32, vec![4; 800]),
        ("name", Utf8, vec!["David"; 800])
    )?;

    let schema = batch1.schema();

    // Create a single MemTable with all batches in one partition to preserve order but ensure streaming
    let mem_table = Arc::new(MemTable::try_new(
        Arc::clone(&schema),
        vec![vec![batch1, batch2, batch3, batch4]], // Single partition with multiple batches
    )?);

    // Build logical plan with a single scan that will yield multiple batches
    let table_source = Arc::new(DefaultTableSource::new(mem_table));
    let logical_plan =
        LogicalPlanBuilder::scan("multi_batch_table", table_source, None)?.build()?;

    // Convert to physical plan
    let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

    println!("Example: Custom Memory-Aware BufferingExecutionPlan");
    println!("---------------------------------------------------");

    // Wrap our input plan with our custom BufferingExecutionPlan
    let buffering_plan = Arc::new(BufferingExecutionPlan::new(schema, physical_plan));

    // Create a task context from our runtime
    let task_ctx = Arc::new(TaskContext::default().with_runtime(runtime));

    // Execute the plan directly to demonstrate memory tracking
    println!("Executing BufferingExecutionPlan with memory tracking...");
    println!("Memory limit: 15000 bytes - should trigger spill on later batches\n");

    let stream = buffering_plan.execute(0, task_ctx.clone())?;
    let _results: Vec<RecordBatch> = stream.try_collect().await?;

    println!("\nSuccessfully executed BufferingExecutionPlan!");

    println!("\nThe BufferingExecutionPlan processed 4 input batches and");
    println!("demonstrated memory tracking with spilling behavior when the");
    println!("memory limit was exceeded by later batches.");
    println!("Check the console output above to see the spill messages.");

    Ok(())
}

/// Example of an external batch bufferer that uses memory reservation.
///
/// It's a simple example which spills all existing data to disk
/// whenever the memory limit is reached.
struct ExternalBatchBufferer {
    buffer: Vec<u8>,
    reservation: MemoryReservation,
    spill_count: usize,
}

impl ExternalBatchBufferer {
    fn new(reservation: MemoryReservation) -> Self {
        Self {
            buffer: Vec::new(),
            reservation,
            spill_count: 0,
        }
    }

    #[expect(clippy::needless_pass_by_value)]
    fn add_batch(&mut self, batch_data: Vec<u8>) -> Result<()> {
        let additional_memory = batch_data.len();

        // Try to reserve memory before allocating
        if self.reservation.try_grow(additional_memory).is_err() {
            // Memory limit reached - handle by spilling
            println!(
                "Memory limit reached, spilling {} bytes to disk",
                self.buffer.len()
            );
            self.spill_to_disk()?;

            // Try again after spilling
            self.reservation.try_grow(additional_memory)?;
        }

        self.buffer.extend_from_slice(&batch_data);
        println!(
            "Added batch of {} bytes, total buffered: {} bytes",
            additional_memory,
            self.buffer.len()
        );
        Ok(())
    }

    fn spill_to_disk(&mut self) -> Result<()> {
        // Simulate writing buffer to disk
        self.spill_count += 1;
        println!(
            "Spill #{}: Writing {} bytes to disk",
            self.spill_count,
            self.buffer.len()
        );

        // Free the memory after spilling
        let freed_bytes = self.buffer.len();
        self.buffer.clear();
        self.reservation.shrink(freed_bytes);

        Ok(())
    }

    fn finish(&mut self) -> Vec<u8> {
        let result = std::mem::take(&mut self.buffer);
        // Free the memory when done
        self.reservation.free();
        println!("Finished processing, released {} bytes", result.len());
        result
    }
}

/// Example of an ExecutionPlan that uses the ExternalBatchBufferer.
#[derive(Debug)]
struct BufferingExecutionPlan {
    schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl BufferingExecutionPlan {
    fn new(schema: SchemaRef, input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input.properties().clone();

        Self {
            schema,
            input,
            properties,
        }
    }
}

impl DisplayAs for BufferingExecutionPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BufferingExecutionPlan")
    }
}

impl ExecutionPlan for BufferingExecutionPlan {
    fn name(&self) -> &'static str {
        "BufferingExecutionPlan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(BufferingExecutionPlan::new(
                self.schema.clone(),
                children[0].clone(),
            )))
        } else {
            internal_err!("BufferingExecutionPlan must have exactly one child")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Register memory consumer with the context's memory pool
        let reservation = MemoryConsumer::new("MyExternalBatchBufferer")
            .with_can_spill(true)
            .register(context.memory_pool());

        // Incoming stream of batches
        let mut input_stream = self.input.execute(partition, context)?;

        // Process the stream and collect all batches
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(async move {
                let mut operator = ExternalBatchBufferer::new(reservation);

                while let Some(batch) = input_stream.next().await {
                    let batch = batch?;

                    // Convert RecordBatch to bytes for this example
                    let batch_data = vec![1u8; batch.get_array_memory_size()];

                    operator.add_batch(batch_data)?;
                }

                // Finish processing and get results
                let _final_result = operator.finish();
                // In a real implementation, you would convert final_result back to RecordBatches

                // Since this is a simplified example, return an empty batch
                // In a real implementation, you would create a batch stream from the processed results
                record_batch!(("id", Int32, vec![5]), ("name", Utf8, vec!["Eve"]))
                    .map_err(|e| {
                        exec_datafusion_err!("Failed to create final RecordBatch: {e}")
                    })
            }),
        )))
    }
}
