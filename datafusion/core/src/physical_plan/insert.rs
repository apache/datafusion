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

//! Execution plan for writing data to [`DataSink`]s

use super::expressions::PhysicalSortExpr;
use super::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use core::fmt;
use datafusion_common::Result;
use datafusion_physical_expr::PhysicalSortRequirement;
use futures::StreamExt;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::Arc;

use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::Distribution;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;

/// `DataSink` implements writing streams of [`RecordBatch`]es to
/// user defined destinations.
///
/// The `Display` impl is used to format the sink for explain plan
/// output.
#[async_trait]
pub trait DataSink: Display + Debug + Send + Sync {
    // TODO add desired input ordering
    // How does this sink want its input ordered?

    /// Writes the data to the sink, returns the number of values written
    ///
    /// This method will be called exactly once during each DML
    /// statement. Thus prior to return, the sink should do any commit
    /// or rollback required.
    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64>;
}

/// Execution plan for writing record batches to a [`DataSink`]
///
/// Returns a single row with the number of values written
pub struct InsertExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// Sink to which to write
    sink: Arc<dyn DataSink>,
    /// Schema of the sink for validating the input data
    sink_schema: SchemaRef,
    /// Schema describing the structure of the output data.
    count_schema: SchemaRef,
}

impl fmt::Debug for InsertExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InsertExec schema: {:?}", self.count_schema)
    }
}

impl InsertExec {
    /// Create a plan to write to `sink`
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn DataSink>,
        sink_schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            sink,
            sink_schema,
            count_schema: make_count_schema(),
        }
    }
}

impl ExecutionPlan for InsertExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.count_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        // Require that the InsertExec gets the data in the order the
        // input produced it (otherwise the optimizer may chose to reorder
        // the input which could result in unintended / poor UX)
        //
        // More rationale:
        // https://github.com/apache/arrow-datafusion/pull/6354#discussion_r1195284178
        vec![self
            .input
            .output_ordering()
            .map(PhysicalSortRequirement::from_sort_exprs)]
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
        Ok(Arc::new(Self {
            input: children[0].clone(),
            sink: self.sink.clone(),
            sink_schema: self.sink_schema.clone(),
            count_schema: self.count_schema.clone(),
        }))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                format!("Invalid requested partition {partition}. InsertExec requires a single input partition."
                )));
        }

        // Execute each of our own input's partitions and pass them to the sink
        let input_partition_count = self.input.output_partitioning().partition_count();
        if input_partition_count != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid input partition count {input_partition_count}. \
                         InsertExec needs only a single partition."
            )));
        }

        let sink_schema = self.sink_schema.clone();
        let data = Box::pin(RecordBatchStreamAdapter::new(
            self.sink_schema.clone(),
            self.input
                .execute(0, context.clone())?
                .map(move |batch| check_batch(batch?, &sink_schema)),
        ));

        let count_schema = self.count_schema.clone();
        let sink = self.sink.clone();

        let stream = futures::stream::once(async move {
            sink.write_all(data, &context).await.map(make_count_batch)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "InsertExec: sink={}", self.sink)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

/// Create a output record batch with a count
///
/// ```text
/// +-------+,
/// | count |,
/// +-------+,
/// | 6     |,
/// +-------+,
/// ```
fn make_count_batch(count: u64) -> RecordBatch {
    let array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;

    RecordBatch::try_from_iter_with_nullable(vec![("count", array, false)]).unwrap()
}

fn make_count_schema() -> SchemaRef {
    // define a schema.
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn check_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "Invalid batch column count {} expected {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }

    // Check NOT NULL constraints
    for (i, field) in schema.fields().iter().enumerate() {
        if !field.is_nullable() && batch.column(i).null_count() > 0 {
            return Err(DataFusionError::Execution(format!(
                "Invalid batch column at '{}' has null but schema specifies non-nullable",
                i
            )));
        }
    }

    Ok(batch)
}
