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
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
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
use std::fmt::Debug;
use std::sync::Arc;

use crate::metrics::MetricsSet;
use crate::stream::RecordBatchStreamAdapter;
use datafusion_common::{exec_err, internal_err, DataFusionError};
use datafusion_execution::TaskContext;

/// `DataSink` implements writing streams of [`RecordBatch`]es to
/// user defined destinations.
///
/// The `Display` impl is used to format the sink for explain plan
/// output.
#[async_trait]
pub trait DataSink: DisplayAs + Debug + Send + Sync {
    /// Returns the data sink as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Return a snapshot of the [MetricsSet] for this
    /// [DataSink].
    ///
    /// See [ExecutionPlan::metrics()] for more details
    fn metrics(&self) -> Option<MetricsSet>;

    // TODO add desired input ordering
    // How does this sink want its input ordered?

    /// Writes the data to the sink, returns the number of values written
    ///
    /// This method will be called exactly once during each DML
    /// statement. Thus prior to return, the sink should do any commit
    /// or rollback required.
    async fn write_all(
        &self,
        data: Vec<SendableRecordBatchStream>,
        context: &Arc<TaskContext>,
    ) -> Result<u64>;
}

/// Execution plan for writing record batches to a [`DataSink`]
///
/// Returns a single row with the number of values written
pub struct FileSinkExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// Sink to which to write
    sink: Arc<dyn DataSink>,
    /// Schema of the sink for validating the input data
    sink_schema: SchemaRef,
    /// Schema describing the structure of the output data.
    count_schema: SchemaRef,
    /// Optional required sort order for output data.
    sort_order: Option<Vec<PhysicalSortRequirement>>,
}

impl fmt::Debug for FileSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileSinkExec schema: {:?}", self.count_schema)
    }
}

impl FileSinkExec {
    /// Create a plan to write to `sink`
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn DataSink>,
        sink_schema: SchemaRef,
        sort_order: Option<Vec<PhysicalSortRequirement>>,
    ) -> Self {
        Self {
            input,
            sink,
            sink_schema,
            count_schema: make_count_schema(),
            sort_order,
        }
    }

    fn execute_input_stream(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        debug_assert_eq!(
            self.sink_schema.fields().len(),
            self.input.schema().fields().len()
        );

        // Find input columns that may violate the not null constraint.
        let risky_columns: Vec<_> = self
            .sink_schema
            .fields()
            .iter()
            .zip(self.input.schema().fields().iter())
            .enumerate()
            .filter_map(|(i, (sink_field, input_field))| {
                if !sink_field.is_nullable() && input_field.is_nullable() {
                    Some(i)
                } else {
                    None
                }
            })
            .collect();

        if risky_columns.is_empty() {
            Ok(input_stream)
        } else {
            // Check not null constraint on the input stream
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.sink_schema.clone(),
                input_stream
                    .map(move |batch| check_not_null_contraits(batch?, &risky_columns)),
            )))
        }
    }

    fn execute_all_input_streams(
        &self,
        context: Arc<TaskContext>,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let n_input_parts = self.input.output_partitioning().partition_count();
        let mut streams = Vec::with_capacity(n_input_parts);
        for part in 0..n_input_parts {
            streams.push(self.execute_input_stream(part, context.clone())?);
        }
        Ok(streams)
    }

    /// Returns insert sink
    pub fn sink(&self) -> &dyn DataSink {
        self.sink.as_ref()
    }

    /// Returns the metrics of the underlying [DataSink]
    pub fn metrics(&self) -> Option<MetricsSet> {
        self.sink.metrics()
    }
}

impl DisplayAs for FileSinkExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InsertExec: sink=")?;
                self.sink.fmt_as(t, f)
            }
        }
    }
}

impl ExecutionPlan for FileSinkExec {
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Incoming number of partitions is taken to be the
        // number of files the query is required to write out.
        // The optimizer should not change this number.
        // Parrallelism is handled within the appropriate DataSink
        vec![false]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        // The input order is either exlicitly set (such as by a ListingTable),
        // or require that the [FileSinkExec] gets the data in the order the
        // input produced it (otherwise the optimizer may chose to reorder
        // the input which could result in unintended / poor UX)
        //
        // More rationale:
        // https://github.com/apache/arrow-datafusion/pull/6354#discussion_r1195284178
        match &self.sort_order {
            Some(requirements) => vec![Some(requirements.clone())],
            None => vec![self
                .input
                .output_ordering()
                .map(PhysicalSortRequirement::from_sort_exprs)],
        }
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
            sort_order: self.sort_order.clone(),
        }))
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(_children[0])
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("FileSinkExec can only be called on partition 0!");
        }
        let data = self.execute_all_input_streams(context.clone())?;

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

fn check_not_null_contraits(
    batch: RecordBatch,
    column_indices: &Vec<usize>,
) -> Result<RecordBatch> {
    for &index in column_indices {
        if batch.num_columns() <= index {
            return exec_err!(
                "Invalid batch column count {} expected > {}",
                batch.num_columns(),
                index
            );
        }

        if batch.column(index).null_count() > 0 {
            return exec_err!(
                "Invalid batch column at '{}' has null but schema specifies non-nullable",
                index
            );
        }
    }

    Ok(batch)
}
