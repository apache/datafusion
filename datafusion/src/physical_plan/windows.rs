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
use crate::physical_plan::{
    aggregates, common,
    expressions::{Literal, NthValue, RowNumber},
    type_coercion::coerce,
    window_functions::signature_for_built_in,
    window_functions::BuiltInWindowFunctionExpr,
    window_functions::{BuiltInWindowFunction, WindowFunction},
    Accumulator, AggregateExpr, Distribution, ExecutionPlan, Partitioning, PhysicalExpr,
    RecordBatchStream, SendableRecordBatchStream, WindowAccumulator, WindowExpr,
};
use crate::scalar::ScalarValue;
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
    args: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: String,
) -> Result<Arc<dyn WindowExpr>> {
    match fun {
        WindowFunction::AggregateFunction(fun) => Ok(Arc::new(AggregateWindowExpr {
            aggregate: aggregates::create_aggregate_expr(
                fun,
                false,
                args,
                input_schema,
                name,
            )?,
        })),
        WindowFunction::BuiltInWindowFunction(fun) => Ok(Arc::new(BuiltInWindowExpr {
            window: create_built_in_window_expr(fun, args, input_schema, name)?,
        })),
    }
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
}

impl WindowExpr for BuiltInWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.window.name()
    }

    fn field(&self) -> Result<Field> {
        self.window.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.window.expressions()
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        self.window.create_accumulator()
    }
}

/// A window expr that takes the form of an aggregate function
#[derive(Debug)]
pub struct AggregateWindowExpr {
    aggregate: Arc<dyn AggregateExpr>,
}

#[derive(Debug)]
struct AggregateWindowAccumulator {
    accumulator: Box<dyn Accumulator>,
}

impl WindowAccumulator for AggregateWindowAccumulator {
    fn scan(&mut self, values: &[ScalarValue]) -> Result<Option<ScalarValue>> {
        self.accumulator.update(values)?;
        Ok(None)
    }

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<Option<ScalarValue>> {
        Ok(Some(self.accumulator.evaluate()?))
    }
}

impl WindowExpr for AggregateWindowExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.aggregate.name()
    }

    fn field(&self) -> Result<Field> {
        self.aggregate.field()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.aggregate.expressions()
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        let accumulator = self.aggregate.create_accumulator()?;
        Ok(Box::new(AggregateWindowAccumulator { accumulator }))
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

type WindowAccumulatorItem = Box<dyn WindowAccumulator>;

fn window_expressions(
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Vec<Vec<Arc<dyn PhysicalExpr>>>> {
    Ok(window_expr
        .iter()
        .map(|expr| expr.expressions())
        .collect::<Vec<_>>())
}

fn window_aggregate_batch(
    batch: &RecordBatch,
    window_accumulators: &mut [WindowAccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<Vec<Option<ArrayRef>>> {
    window_accumulators
        .iter_mut()
        .zip(expressions)
        .map(|(window_acc, expr)| {
            let values = &expr
                .iter()
                .map(|e| e.evaluate(&batch))
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;
            window_acc.scan_batch(batch.num_rows(), values)
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to one window expr
fn finalize_window_aggregation(
    window_accumulators: &[WindowAccumulatorItem],
) -> Result<Vec<Option<ScalarValue>>> {
    window_accumulators
        .iter()
        .map(|window_accumulator| window_accumulator.evaluate())
        .collect::<Result<Vec<_>>>()
}

fn create_window_accumulators(
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Vec<WindowAccumulatorItem>> {
    window_expr
        .iter()
        .map(|expr| expr.create_accumulator())
        .collect::<Result<Vec<_>>>()
}

/// Compute the window aggregate columns
///
/// 1. get a list of window accumulators
/// 2. evaluate the args
/// 3. scan args with window functions
/// 4. concat with final aggregations
///
/// FIXME so far this fn does not support:
/// 1. partition by
/// 2. order by
/// 3. window frame
///
/// which will require further work:
/// 1. inter-partition order by using vec partition-point (https://github.com/apache/arrow-datafusion/issues/360)
/// 2. inter-partition parallelism using one-shot channel (https://github.com/apache/arrow-datafusion/issues/299)
/// 3. convert aggregation based window functions to be self-contain so that: (https://github.com/apache/arrow-datafusion/issues/361)
///    a. some can be grow-only window-accumulating
///    b. some can be grow-and-shrink window-accumulating
///    c. some can be based on segment tree
fn compute_window_aggregates(
    window_expr: Vec<Arc<dyn WindowExpr>>,
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    let mut window_accumulators = create_window_accumulators(&window_expr)?;
    let expressions = Arc::new(window_expressions(&window_expr)?);
    let num_rows = batch.num_rows();
    let window_aggregates =
        window_aggregate_batch(batch, &mut window_accumulators, &expressions)?;
    let final_aggregates = finalize_window_aggregation(&window_accumulators)?;

    // both must equal to window_expr.len()
    if window_aggregates.len() != final_aggregates.len() {
        return Err(DataFusionError::Internal(
            "Impossibly got len mismatch".to_owned(),
        ));
    }

    window_aggregates
        .iter()
        .zip(final_aggregates)
        .map(|(wa, fa)| {
            Ok(match (wa, fa) {
                (None, Some(fa)) => fa.to_array_of_size(num_rows),
                (Some(wa), None) if wa.len() == num_rows => wa.clone(),
                _ => {
                    return Err(DataFusionError::Execution(
                        "Invalid window function behavior".to_owned(),
                    ))
                }
            })
        })
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
                &[col("c3")],
                schema.as_ref(),
                "count".to_owned(),
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
                    &[col("c3")],
                    schema.as_ref(),
                    "count".to_owned(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Max),
                    &[col("c3")],
                    schema.as_ref(),
                    "max".to_owned(),
                )?,
                create_window_expr(
                    &WindowFunction::AggregateFunction(AggregateFunction::Min),
                    &[col("c3")],
                    schema.as_ref(),
                    "min".to_owned(),
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
