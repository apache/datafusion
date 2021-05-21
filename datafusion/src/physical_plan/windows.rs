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
    aggregates,
    expressions::{FirstValue, LastValue, Literal, NthValue, RowNumber},
    type_coercion::coerce,
    window_functions::{signature_for_built_in, BuiltInWindowFunction, WindowFunction},
    Accumulator, AggregateExpr, BuiltInWindowFunctionExpr, Distribution, ExecutionPlan,
    Partitioning, PhysicalExpr, RecordBatchStream, SendableRecordBatchStream,
    WindowAccumulator, WindowExpr,
};
use crate::scalar::ScalarValue;
use arrow::compute::concat;
use arrow::{
    array::ArrayRef,
    datatypes::{Field, Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use futures::Future;
use pin_project_lite::pin_project;
use std::any::Any;
use std::convert::TryInto;
use std::iter;
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
            Ok(Arc::new(NthValue::try_new(arg, name, n, data_type)?))
        }
        BuiltInWindowFunction::FirstValue => {
            let arg =
                coerce(args, input_schema, &signature_for_built_in(fun))?[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Ok(Arc::new(FirstValue::new(arg, name, data_type)))
        }
        BuiltInWindowFunction::LastValue => {
            let arg =
                coerce(args, input_schema, &signature_for_built_in(fun))?[0].clone();
            let data_type = args[0].data_type(input_schema)?;
            Ok(Arc::new(LastValue::new(arg, name, data_type)))
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
) -> Result<Vec<Option<Vec<ScalarValue>>>> {
    // 1.1 iterate accumulators and respective expressions together
    // 1.2 evaluate expressions
    // 1.3 update / merge window accumulators with the expressions' values

    // 1.1
    window_accumulators
        .iter_mut()
        .zip(expressions)
        .map(|(window_acc, expr)| {
            // 1.2
            let values = &expr
                .iter()
                .map(|e| e.evaluate(batch))
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            window_acc.scan_batch(batch.num_rows(), values)
        })
        .into_iter()
        .collect::<Result<Vec<_>>>()
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final) or states (mode = Partial)
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

async fn compute_window_aggregate(
    schema: SchemaRef,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    mut input: SendableRecordBatchStream,
) -> ArrowResult<RecordBatch> {
    let mut window_accumulators = create_window_accumulators(&window_expr)
        .map_err(DataFusionError::into_arrow_external_error)?;

    let expressions = window_expressions(&window_expr)
        .map_err(DataFusionError::into_arrow_external_error)?;

    let expressions = Arc::new(expressions);

    // TODO each element shall have some size hint
    let mut accumulator: Vec<Vec<ScalarValue>> =
        iter::repeat(vec![]).take(window_expr.len()).collect();

    let mut original_batches: Vec<RecordBatch> = vec![];

    let mut total_num_rows = 0;

    while let Some(batch) = input.next().await {
        let batch = batch?;
        total_num_rows += batch.num_rows();
        original_batches.push(batch.clone());

        let batch_aggregated =
            window_aggregate_batch(&batch, &mut window_accumulators, &expressions)
                .map_err(DataFusionError::into_arrow_external_error)?;
        accumulator.iter_mut().zip(batch_aggregated).for_each(
            |(acc_for_window, window_batch)| {
                if let Some(data) = window_batch {
                    acc_for_window.extend(data);
                }
            },
        );
    }

    let aggregated_mapped = finalize_window_aggregation(&window_accumulators)
        .map_err(DataFusionError::into_arrow_external_error)?;

    let mut columns: Vec<ArrayRef> = accumulator
        .iter()
        .zip(aggregated_mapped)
        .map(|(acc, agg)| match (acc, agg) {
            // either accumulator values or the aggregated values are non-empty, but not both
            (acc, Some(scalar_value)) if acc.is_empty() => {
                Ok(scalar_value.to_array_of_size(total_num_rows))
            }
            (acc, None) if !acc.is_empty() => ScalarValue::iter_to_array(acc),
            _ => Err(DataFusionError::Execution(
                "Invalid window function behavior".to_owned(),
            )),
        })
        .collect::<Result<Vec<ArrayRef>>>()
        .map_err(DataFusionError::into_arrow_external_error)?;

    for i in 0..(schema.fields().len() - window_expr.len()) {
        let col = concat(
            &original_batches
                .iter()
                .map(|batch| batch.column(i).as_ref())
                .collect::<Vec<_>>(),
        )?;
        columns.push(col);
    }

    RecordBatch::try_new(schema.clone(), columns)
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
            let result = compute_window_aggregate(schema_clone, window_expr, input).await;
            tx.send(result)
        });

        Self {
            output: rx,
            finished: false,
            schema,
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
    // use super::*;

    // /// some mock data to test windows
    // fn some_data() -> (Arc<Schema>, Vec<RecordBatch>) {
    //     // define a schema.
    //     let schema = Arc::new(Schema::new(vec![
    //         Field::new("a", DataType::UInt32, false),
    //         Field::new("b", DataType::Float64, false),
    //     ]));

    //     // define data.
    //     (
    //         schema.clone(),
    //         vec![
    //             RecordBatch::try_new(
    //                 schema.clone(),
    //                 vec![
    //                     Arc::new(UInt32Array::from(vec![2, 3, 4, 4])),
    //                     Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
    //                 ],
    //             )
    //             .unwrap(),
    //             RecordBatch::try_new(
    //                 schema,
    //                 vec![
    //                     Arc::new(UInt32Array::from(vec![2, 3, 3, 4])),
    //                     Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
    //                 ],
    //             )
    //             .unwrap(),
    //         ],
    //     )
    // }

    // #[tokio::test]
    // async fn window_function() -> Result<()> {
    //     let input: Arc<dyn ExecutionPlan> = unimplemented!();
    //     let input_schema = input.schema();
    //     let window_expr = vec![];
    //     WindowAggExec::try_new(window_expr, input, input_schema);
    // }
}
