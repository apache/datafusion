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

use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;

use super::{
    execute_input_stream, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties, SendableRecordBatchStream,
};
use crate::metrics::MetricsSet;
use crate::stream::RecordBatchStreamAdapter;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{internal_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{Distribution, EquivalenceProperties};

use async_trait::async_trait;
use datafusion_physical_expr_common::sort_expr::LexRequirement;
use futures::StreamExt;

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
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> Result<u64>;
}

#[deprecated(since = "38.0.0", note = "Use [`DataSinkExec`] instead")]
pub type FileSinkExec = DataSinkExec;

/// Execution plan for writing record batches to a [`DataSink`]
///
/// Returns a single row with the number of values written
pub struct DataSinkExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// Sink to which to write
    sink: Arc<dyn DataSink>,
    /// Schema of the sink for validating the input data
    sink_schema: SchemaRef,
    /// Schema describing the structure of the output data.
    count_schema: SchemaRef,
    /// Optional required sort order for output data.
    sort_order: Option<LexRequirement>,
    cache: PlanProperties,
}

impl fmt::Debug for DataSinkExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DataSinkExec schema: {:?}", self.count_schema)
    }
}

impl DataSinkExec {
    /// Create a plan to write to `sink`
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sink: Arc<dyn DataSink>,
        sink_schema: SchemaRef,
        sort_order: Option<LexRequirement>,
    ) -> Self {
        let count_schema = make_count_schema();
        let cache = Self::create_schema(&input, count_schema);
        Self {
            input,
            sink,
            sink_schema,
            count_schema: make_count_schema(),
            sort_order,
            cache,
        }
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Returns insert sink
    pub fn sink(&self) -> &dyn DataSink {
        self.sink.as_ref()
    }

    /// Optional sort order for output data
    pub fn sort_order(&self) -> &Option<LexRequirement> {
        &self.sort_order
    }

    fn create_schema(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            input.execution_mode(),
        )
    }
}

impl DisplayAs for DataSinkExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "DataSinkExec: sink=")?;
                self.sink.fmt_as(t, f)
            }
        }
    }
}

impl ExecutionPlan for DataSinkExec {
    fn name(&self) -> &'static str {
        "DataSinkExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time.
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // DataSink is responsible for dynamically partitioning its
        // own input at execution time, and so requires a single input partition.
        vec![Distribution::SinglePartition; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        // The required input ordering is set externally (e.g. by a `ListingTable`).
        // Otherwise, there is no specific requirement (i.e. `sort_expr` is `None`).
        vec![self.sort_order.as_ref().cloned()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Maintains ordering in the sense that the written file will reflect
        // the ordering of the input. For more context, see:
        //
        // https://github.com/apache/datafusion/pull/6354#discussion_r1195284178
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.sink),
            Arc::clone(&self.sink_schema),
            self.sort_order.clone(),
        )))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("DataSinkExec can only be called on partition 0!");
        }
        let data = execute_input_stream(
            Arc::clone(&self.input),
            Arc::clone(&self.sink_schema),
            0,
            Arc::clone(&context),
        )?;

        let count_schema = Arc::clone(&self.count_schema);
        let sink = Arc::clone(&self.sink);

        let stream = futures::stream::once(async move {
            sink.write_all(data, &context).await.map(make_count_batch)
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }

    /// Returns the metrics of the underlying [DataSink]
    fn metrics(&self) -> Option<MetricsSet> {
        self.sink.metrics()
    }

    fn with_node_id(
        self: Arc<Self>,
        _node_id: usize,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let mut new_plan = DataSinkExec::new(
            self.input.clone(),
            self.sink.clone(),
            self.sink_schema.clone(),
            self.sort_order.clone(),
        );
        let new_props = new_plan.cache.clone().with_node_id(_node_id);
        new_plan.cache = new_props;
        Ok(Some(Arc::new(new_plan)))
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
