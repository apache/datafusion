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

//! Execution plan for window functions

use crate::error::{DataFusionError, Result};
use crate::logical_plan::window_frames::{WindowFrame, WindowFrameUnits};
use crate::physical_plan::{
    aggregates, common,
    expressions::{Literal, NthValue, PhysicalSortExpr, RowNumber},
    type_coercion::coerce,
    window_functions::signature_for_built_in,
    window_functions::BuiltInWindowFunctionExpr,
    window_functions::{BuiltInWindowFunction, WindowFunction},
    Accumulator, AggregateExpr, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
    RecordBatchStream, SendableRecordBatchStream, WindowExpr,
};
use arrow::compute::concat;
use arrow::{
    array::ArrayRef,
    datatypes::{Field, Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::stream::Stream;
use futures::Future;
use pin_project_lite::pin_project;
use std::any::Any;
use std::convert::TryInto;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Window execution plan
#[derive(Debug)]
pub struct WindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Schema before the window
    input_schema: SchemaRef,
}

/// Create a physical expression for window function
pub fn create_window_expr(
    fun: &WindowFunction,
    name: String,
    args: &[Arc<dyn PhysicalExpr>],
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &[PhysicalSortExpr],
    window_frame: Option<WindowFrame>,
    input_schema: &Schema,
) -> Result<Arc<dyn WindowExpr>> {
    Ok(match fun {
        WindowFunction::AggregateFunction(fun) => Arc::new(AggregateWindowExpr {
            aggregate: aggregates::create_aggregate_expr(
                fun,
                false,
                args,
                input_schema,
                name,
            )?,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }),
        WindowFunction::BuiltInWindowFunction(fun) => Arc::new(BuiltInWindowExpr {
            window: create_built_in_window_expr(fun, args, input_schema, name)?,
            partition_by: partition_by.to_vec(),
            order_by: order_by.to_vec(),
            window_frame,
        }),
    })
}

fn create_built_in_window_expr(
    fun: &BuiltInWindowFunction,
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn BuiltInWindowFunctionExpr>> {
    match fun {
        BuiltInWindowFunction::RowNumber => Ok(Arc::new(RowNumber::new(name))),
        BuiltInWindowFunction::NthValue => {
            let coerced_args = coerce(args, input_schema, &signature_for_built_in(fun))?;
            let arg = coerced_args[0].clone();
            let n = coerced_args[1]
                .as_any()
                .downcast_ref::<Literal>()
                .unwrap()
                .value();
            let n: i64 = n
                .clone()
                .try_into()
                .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
            let n: u32 = n as u32;
            let data_type = args[0].data_type(input_schema)?;
            Ok(Arc::new(NthValue::nth_value(name, arg, data_type, n)?))
        }
        BuiltInWindowFunction::FirstValue => {
            let arg =
                coerce(args, input_schema, &signature_for_built_in(fun))?[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Ok(Arc::new(NthValue::first_value(name, arg, data_type)))
        }
        BuiltInWindowFunction::LastValue => {
            let arg =
                coerce(args, input_schema, &signature_for_built_in(fun))?[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Ok(Arc::new(NthValue::last_value(name, arg, data_type)))
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "Window function with {:?} not yet implemented",
            fun
        ))),
    }
}

/// A window expr that takes the form of a built in window function
#[derive(Debug)]
pub struct BuiltInWindowExpr {
    window: Arc<dyn BuiltInWindowFunctionExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<WindowFrame>,
}

impl WindowExpr for BuiltInWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.window.name()
    }

    fn field(&self) -> Result<Field> {
        self.window.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.window.expressions()
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // FIXME, for now we assume all the rows belong to the same partition, which will not be the
        // case when partition_by is supported, in which case we'll parallelize the calls.
        // See https://github.com/apache/arrow-datafusion/issues/299
        let values = self.evaluate_args(batch)?;
        self.window.evaluate(batch.num_rows(), &values)
    }
}

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    order_by: Vec<PhysicalSortExpr>,
    window_frame: Option<WindowFrame>,
}

impl AggregateWindowExpr {
    /// the aggregate window function operates based on window frame, and by default the mode is
    /// "range".
    fn evaluation_mode(&self) -> WindowFrameUnits {
        self.window_frame.unwrap_or_default().units
    }

    /// create a new accumulator based on the underlying aggregation function
    fn create_accumulator(&self) -> Result<AggregateWindowAccumulator> {
        let accumulator = self.aggregate.create_accumulator()?;
        Ok(AggregateWindowAccumulator { accumulator })
    }

    /// peer based evaluation based on the fact that batch is pre-sorted given the sort columns
    /// and then per partition point we'll evaluate the peer group (e.g. SUM or MAX gives the same
    /// results for peers) and concatenate the results.
    fn peer_based_evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let sort_partition_points = self.evaluate_sort_partition_points(batch)?;
        let mut window_accumulators = self.create_accumulator()?;
        let values = self.evaluate_args(batch)?;
        let results = sort_partition_points
            .iter()
            .map(|peer_range| window_accumulators.scan_peers(&values, peer_range))
            .collect::<Result<Vec<_>>>()?;
        let results = results.iter().map(|i| i.as_ref()).collect::<Vec<_>>();
        concat(&results).map_err(DataFusionError::ArrowError)
    }

    fn group_based_evaluate(&self, _batch: &RecordBatch) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(format!(
            "Group based evaluation for {} is not yet implemented",
            self.name()
        )))
    }

    fn row_based_evaluate(&self, _batch: &RecordBatch) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(format!(
            "Row based evaluation for {} is not yet implemented",
            self.name()
        )))
    }
}

impl WindowExpr for AggregateWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.aggregate.name()
    }

    fn field(&self) -> Result<Field> {
        self.aggregate.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.aggregate.expressions()
    }

    fn partition_by(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.partition_by
    }

    fn order_by(&self) -> &[PhysicalSortExpr] {
        &self.order_by
    }

    /// evaluate the window function values against the batch
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        // FIXME, for now we assume all the rows belong to the same partition, which will not be the
        // case when partition_by is supported, in which case we'll parallelize the calls.
        // See https://github.com/apache/arrow-datafusion/issues/299
        match self.evaluation_mode() {
            WindowFrameUnits::Range => self.peer_based_evaluate(batch),
            WindowFrameUnits::Rows => self.row_based_evaluate(batch),
            WindowFrameUnits::Groups => self.group_based_evaluate(batch),
        }
    }
}

/// Aggregate window accumulator utilizes the accumulator from aggregation and do a accumulative sum
/// across evaluation arguments based on peer equivalences.
#[derive(Debug)]
struct AggregateWindowAccumulator {
    accumulator: Box<dyn Accumulator>,
}

impl AggregateWindowAccumulator {
    /// scan one peer group of values (as arguments to window function) given by the value_range
    /// and return evaluation result that are of the same number of rows.
    fn scan_peers(
        &mut self,
        values: &[ArrayRef],
        value_range: &Range<usize>,
    ) -> Result<ArrayRef> {
        if value_range.is_empty() {
            return Err(DataFusionError::Internal(
                "Value range cannot be empty".to_owned(),
            ));
        }
        let len = value_range.end - value_range.start;
        let values = values
            .iter()
            .map(|v| v.slice(value_range.start, len))
            .collect::<Vec<_>>();
        self.accumulator.update_batch(&values)?;
        let value = self.accumulator.evaluate()?;
        Ok(value.to_array_of_size(len))
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(input_schema.fields().len() + window_expr.len());
    for expr in window_expr {
        fields.push(expr.field()?);
    }
    fields.extend_from_slice(input_schema.fields());
    Ok(Schema::new(fields))
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);
        Ok(WindowAggExec {
            input,
            window_expr,
            schema,
            input_schema,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any window functions are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }
}

#[async_trait]
impl ExecutionPlan for WindowAggExec {
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

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::SinglePartition
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(WindowAggExec::try_new(
                self.window_expr.clone(),
                children[0].clone(),
                self.input_schema.clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "WindowAggExec wrong number of children".to_owned(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "WindowAggExec invalid partition {}",
                partition
            )));
        }

        // window needs to operate on a single partition currently
        if 1 != self.input.output_partitioning().partition_count() {
            return Err(DataFusionError::Internal(
                "WindowAggExec requires a single input partition".to_owned(),
            ));
        }

        let input = self.input.execute(partition).await?;

        let stream = Box::pin(WindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
        ));
        Ok(stream)
    }
}

pin_project! {
    /// stream for window aggregation plan
    pub struct WindowAggStream {
        schema: SchemaRef,
        #[pin]
        output: futures::channel::oneshot::Receiver<ArrowResult<RecordBatch>>,
        finished: bool,
    }
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: Vec<Arc<dyn WindowExpr>>,
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    // FIXME, for now we assume all the rows belong to the same partition, which will not be the
    // case when partition_by is supported, in which case we'll parallelize the calls.
    // See https://github.com/apache/arrow-datafusion/issues/299
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
    ) -> Self {
        let (tx, rx) = futures::channel::oneshot::channel();
        let schema_clone = schema.clone();
        tokio::spawn(async move {
            let schema = schema_clone.clone();
            let result = WindowAggStream::process(input, window_expr, schema).await;
            tx.send(result)
        });

        Self {
            output: rx,
            finished: false,
            schema,
        }
    }

    async fn process(
        input: SendableRecordBatchStream,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        schema: SchemaRef,
    ) -> ArrowResult<RecordBatch> {
        let input_schema = input.schema();
        let batches = common::collect(input)
            .await
            .map_err(DataFusionError::into_arrow_external_error)?;
        let batch = common::combine_batches(&batches, input_schema.clone())?;
        if let Some(batch) = batch {
            // calculate window cols
            let mut columns = compute_window_aggregates(window_expr, &batch)
                .map_err(DataFusionError::into_arrow_external_error)?;
            // combine with the original cols
            // note the setup of window aggregates is that they newly calculated window
            // expressions are always prepended to the columns
            columns.extend_from_slice(batch.columns());
            RecordBatch::try_new(schema, columns)
        } else {
            Ok(RecordBatch::new_empty(schema))
        }
    }
}

impl Stream for WindowAggStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // is the output ready?
        let this = self.project();
        let output_poll = this.output.poll(cx);

        match output_poll {
            Poll::Ready(result) => {
                *this.finished = true;
                // check for error in receiving channel and unwrap actual result
                let result = match result {
                    Err(e) => Some(Err(ArrowError::ExternalError(Box::new(e)))), // error receiving
                    Ok(result) => Some(result),
                };
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::aggregates::AggregateFunction;
    use crate::physical_plan::collect;
    use crate::physical_plan::csv::{CsvExec, CsvReadOptions};
    use crate::physical_plan::expressions::col;
    use crate::test;
    use arrow::array::*;

    fn create_test_schema(partitions: usize) -> Result<(Arc<CsvExec>, SchemaRef)> {
        let schema = test::aggr_test_schema();
        let path = test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;
        let csv = CsvExec::try_new(
            &path,
            CsvReadOptions::new().schema(&schema),
            None,
            1024,
            None,
        )?;

        let input = Arc::new(csv);
        Ok((input, schema))
    }

    #[tokio::test]
    async fn window_function_input_partition() -> Result<()> {
        let (input, schema) = create_test_schema(4)?;

        let window_exec = Arc::new(WindowAggExec::try_new(
            vec![create_window_expr(
                &WindowFunction::AggregateFunction(AggregateFunction::Count),
                "count".to_owned(),
                &[col("c3")],
                &[],
                &[],
                Some(WindowFrame::default()),
                schema.as_ref(),
            )?],
            input,
            schema.clone(),
        )?);

        let result = collect(window_exec).await;

        assert!(result.is_err());
        if let Some(DataFusionError::Internal(msg)) = result.err() {
            assert_eq!(
                msg,
                "WindowAggExec requires a single input partition".to_owned()
            );
        } else {
            unreachable!("Expect an internal error to happen");
        }
        Ok(())
    }

    #[tokio::test]
    async fn window_function() -> Result<()> {
        let (input, schema) = create_test_schema(1)?;

        let window_exec = Arc::new(WindowAggExec::try_new(
            vec![
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Count),
                    "count".to_owned(),
                    &[col("c3")],
                    &[],
                    &[],
                    Some(WindowFrame::default()),
                    schema.as_ref(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Max),
                    "max".to_owned(),
                    &[col("c3")],
                    &[],
                    &[],
                    Some(WindowFrame::default()),
                    schema.as_ref(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Min),
                    "min".to_owned(),
                    &[col("c3")],
                    &[],
                    &[],
                    Some(WindowFrame::default()),
                    schema.as_ref(),
                )?,
            ],
            input,
            schema.clone(),
        )?);

        let result: Vec<RecordBatch> = collect(window_exec).await?;
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        // c3 is small int

        let count: &UInt64Array = as_primitive_array(&columns[0]);
        assert_eq!(count.value(0), 100);
        assert_eq!(count.value(99), 100);

        let max: &Int8Array = as_primitive_array(&columns[1]);
        assert_eq!(max.value(0), 125);
        assert_eq!(max.value(99), 125);

        let min: &Int8Array = as_primitive_array(&columns[2]);
        assert_eq!(min.value(0), -117);
        assert_eq!(min.value(99), -117);

        Ok(())
    }
}
