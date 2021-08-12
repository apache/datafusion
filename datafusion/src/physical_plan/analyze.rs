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

//! Defines the ANALYZE operator

use std::sync::Arc;
use std::{any::Any, time::Instant};

use crate::{
    error::{DataFusionError, Result},
    physical_plan::{display::DisplayableExecutionPlan, Partitioning},
    physical_plan::{DisplayFormatType, ExecutionPlan},
};
use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::StreamExt;

use super::{stream::RecordBatchReceiverStream, Distribution, SendableRecordBatchStream};
use async_trait::async_trait;

/// `EXPLAIN ANALYZE` execution plan operator. This operator runs its input,
/// discards the results, and then prints out an annotated plan with metrics
#[derive(Debug, Clone)]
pub struct AnalyzeExec {
    /// control how much extra to print
    verbose: bool,
    /// The input plan (the plan being analyzed)
    input: Arc<dyn ExecutionPlan>,
    /// The output schema for RecordBatches of this exec node
    schema: SchemaRef,
}

impl AnalyzeExec {
    /// Create a new AnalyzeExec
    pub fn new(verbose: bool, input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        AnalyzeExec {
            verbose,
            input,
            schema,
        }
    }
}

#[async_trait]
impl ExecutionPlan for AnalyzeExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    /// Specifies we want the input as a single stream
    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn with_new_children(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Arc::new(Self::new(
                self.verbose,
                children.pop().unwrap(),
                self.schema.clone(),
            )))
        } else {
            Err(DataFusionError::Internal(format!(
                "Invalid child count for AnalyzeExec. Expected 1 got {}",
                children.len()
            )))
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "AnalyzeExec invalid partition. Expected 0, got {}",
                partition
            )));
        }

        // should be ensured by `SinglePartition`  above
        let input_partitions = self.input.output_partitioning().partition_count();
        if input_partitions != 1 {
            return Err(DataFusionError::Internal(format!(
                "AnalyzeExec invalid number of input partitions. Expected 1, got {}",
                input_partitions
            )));
        }

        let (tx, rx) = tokio::sync::mpsc::channel(input_partitions);

        let captured_input = self.input.clone();
        let mut input_stream = captured_input.execute(0).await?;
        let captured_schema = self.schema.clone();
        let verbose = self.verbose;

        // Task reads batches the input and when complete produce a
        // RecordBatch with a report that is written to `tx` when done
        tokio::task::spawn(async move {
            let start = Instant::now();
            let mut total_rows = 0;

            // Note the code below ignores errors sending on tx. An
            // error sending means the plan is being torn down and
            // nothing is left that will handle the error (aka no one
            // will hear us scream)
            while let Some(b) = input_stream.next().await {
                match b {
                    Ok(batch) => {
                        total_rows += batch.num_rows();
                    }
                    b @ Err(_) => {
                        // try and pass on errors from input
                        if tx.send(b).await.is_err() {
                            // receiver hung up, stop executing (no
                            // one will look at any further results we
                            // send)
                            return;
                        }
                    }
                }
            }
            let end = Instant::now();

            let mut type_builder = StringBuilder::new(1);
            let mut plan_builder = StringBuilder::new(1);

            // TODO use some sort of enum rather than strings?
            type_builder.append_value("Plan with Metrics").unwrap();

            let annotated_plan =
                DisplayableExecutionPlan::with_metrics(captured_input.as_ref())
                    .indent()
                    .to_string();
            plan_builder.append_value(annotated_plan).unwrap();

            // Verbose output
            // TODO make this more sophisticated
            if verbose {
                type_builder.append_value("Output Rows").unwrap();
                plan_builder.append_value(total_rows.to_string()).unwrap();

                type_builder.append_value("Duration").unwrap();
                plan_builder
                    .append_value(format!("{:?}", end - start))
                    .unwrap();
            }

            let maybe_batch = RecordBatch::try_new(
                captured_schema,
                vec![
                    Arc::new(type_builder.finish()),
                    Arc::new(plan_builder.finish()),
                ],
            );
            // again ignore error
            tx.send(maybe_batch).await.ok();
        });

        Ok(RecordBatchReceiverStream::create(&self.schema, rx))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "AnalyzeExec verbose={}", self.verbose)
            }
        }
    }
}
