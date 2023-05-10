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

//! Execution plan for copying data to DataSinks

use super::expressions::PhysicalSortExpr;
use super::{
    DisplayFormatType, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_array::UInt64Array;
use arrow_schema::{Schema, DataType, Field};
use datafusion_common::DataFusionError;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use std::any::Any;
use std::sync::Arc;

use crate::execution::context::TaskContext;
use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::Distribution;

/// The DataSink implements writing streams of [`RecordBatch`]es to
/// partitioned destinations
pub trait DataSink: std::fmt::Debug + std::fmt::Display + Send + Sync {

    /// How does this sink want its input distributed?
    fn required_input_distribution(&self) -> Distribution;

    /// return a future which writes a RecordBatchStream to a particular partition
    /// and return the number of rows written
    fn write_stream(&self, partition: usize, input: SendableRecordBatchStream) -> BoxFuture<Result<u64>>;

}


/// Execution plan for writing batches to DataSink
///
/// The output is a single RecordBatch with a single UInt64 column
/// representing the total number of rows written
#[derive(Debug)]
pub struct CopyExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// The output
    sink: Arc<dyn DataSink>,
    /// Schema describing the structure of the data.
    schema: SchemaRef,
}

impl ExecutionPlan for CopyExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        // single output partition
        Partitioning::UnknownPartitioning(1)
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        // don't automatically repartition this
        false
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![self.sink.required_input_distribution()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CopyExec::new(
            children[0].clone(),
            self.sink.clone(),
        )))
    }

    /// Execute the plan and return a stream of record batches for the specified partition.
    /// Depending on the number of input partitions and MemTable partitions, it will choose
    /// either a less lock acquiring or a locked implementation.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        assert_eq!(partition, 0);

        // Launch tasks for all input's partitions and flatten them together
        let partition_count = self.input.output_partitioning().partition_count();

        // fire up tasks that will run in parallel.
        let sink = self.sink.clone();
        let input = self.input.clone();

        // make a future which will run all the input streams in
        // parallel and produce a single result
        let future = async move {
            let mut input_futures = Vec::with_capacity(partition_count);
            for i in 0..partition_count {
                let input_stream = input.execute(i, context.clone())?;
                input_futures.push(sink.write_stream(i, input_stream));
            }

            let counts: Vec<_> = futures::stream::iter(input_futures)
            // run all the streams in parallel
                .buffer_unordered(partition_count)
                .try_collect()
                .await?;

            // sum them all up and make a record batch
            let total_size = counts.iter().sum::<u64>();

            let batch = RecordBatch::try_from_iter(vec![
                ("total_rows", Arc::new(UInt64Array::from_iter_values(vec![total_size])) as _)
            ])?;
            Ok(batch) as Result<RecordBatch, DataFusionError>
        };

        let stream = futures::stream::once(future).boxed();


        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "CopyExec: sink={}, input_partition={}",
                    self.sink,
                    self.input.output_partitioning().partition_count()
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl CopyExec {
    /// Create a new execution plan for reading in-memory record batches
    /// and sending them to the DataSink
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn DataSink>,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("total_rows", DataType::UInt64, false),
        ]));

        Self {
            input,
            sink,
            schema,
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
