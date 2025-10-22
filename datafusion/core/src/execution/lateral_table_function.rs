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

//! Execution plan for LATERAL table functions

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_catalog::TableFunction;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_expr::{ColumnarValue, Expr};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use futures::stream::Stream;
use futures::{ready, FutureExt, StreamExt};

use crate::execution::session_state::SessionState;

/// Execution plan for LATERAL table functions
///
/// This operator evaluates a table function for each row from the input,
/// allowing the table function to reference columns from the outer query.
#[derive(Debug, Clone)]
pub struct LateralTableFunctionExec {
    /// Input execution plan (the "outer" table)
    input: Arc<dyn ExecutionPlan>,
    /// Name of the table function to call
    function_name: String,
    /// The table function instance
    table_function: Arc<TableFunction>,
    /// Physical expressions for table function arguments
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Complete output schema (input columns + table function columns).
    /// Used when creating the final output batches that combine both.
    schema: SchemaRef,
    /// Table function output schema only (excludes input columns).
    /// Used when concatenating batches from the table function before
    /// combining them with input columns.
    table_function_schema: SchemaRef,
    /// Session state for accessing catalog and scanning
    session_state: Arc<SessionState>,
    /// Cached plan properties (partitioning, ordering, equivalences, etc.).
    /// Computed once during construction to avoid expensive recalculation
    /// during query optimization.
    cache: PlanProperties,
}

impl LateralTableFunctionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        function_name: String,
        table_function: Arc<TableFunction>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: SchemaRef,
        table_function_schema: SchemaRef,
        session_state: Arc<SessionState>,
    ) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));
        Self {
            input,
            function_name,
            table_function,
            args,
            schema,
            table_function_schema,
            session_state,
            cache,
        }
    }

    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for LateralTableFunctionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LateralTableFunctionExec: function={}(..)",
                    self.function_name
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "LateralTableFunction({})", self.function_name)
            }
        }
    }
}

impl ExecutionPlan for LateralTableFunctionExec {
    fn name(&self) -> &str {
        "LateralTableFunctionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "LateralTableFunctionExec expects exactly one child, got {}",
                children.len()
            );
        }

        Ok(Arc::new(LateralTableFunctionExec::new(
            Arc::clone(&children[0]),
            self.function_name.clone(),
            Arc::clone(&self.table_function),
            self.args.clone(),
            Arc::clone(&self.schema),
            Arc::clone(&self.table_function_schema),
            Arc::clone(&self.session_state),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;

        Ok(Box::pin(LateralTableFunctionStream {
            input: input_stream,
            table_function: Arc::clone(&self.table_function),
            args: self.args.clone(),
            table_function_schema: Arc::clone(&self.table_function_schema),
            schema: Arc::clone(&self.schema),
            session_state: Arc::clone(&self.session_state),
            context,
            state: ProcessingState::ReadingInput,
        }))
    }
}

enum ProcessingState {
    ReadingInput,
    ProcessingRow {
        input_batch: RecordBatch,
        row_idx: usize,
        output_batches: Vec<RecordBatch>,
    },
    ScanningTableFunction {
        input_batch: RecordBatch,
        row_idx: usize,
        output_batches: Vec<RecordBatch>,
        scan_future: BoxFuture<'static, Result<Arc<dyn ExecutionPlan>>>,
    },
    ReadingTableStream {
        input_batch: RecordBatch,
        row_idx: usize,
        output_batches: Vec<RecordBatch>,
        table_stream: SendableRecordBatchStream,
        table_batches: Vec<RecordBatch>,
    },
}

struct LateralTableFunctionStream {
    input: SendableRecordBatchStream,
    table_function: Arc<TableFunction>,
    args: Vec<Arc<dyn PhysicalExpr>>,
    table_function_schema: SchemaRef,
    schema: SchemaRef,
    session_state: Arc<SessionState>,
    context: Arc<TaskContext>,
    state: ProcessingState,
}

impl Stream for LateralTableFunctionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let state = std::mem::replace(
                &mut self.state,
                ProcessingState::ReadingInput,
            );

            match state {
                ProcessingState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            if batch.num_rows() == 0 {
                                return Poll::Ready(Some(Ok(RecordBatch::new_empty(
                                    Arc::clone(&self.schema),
                                ))));
                            }
                            self.state = ProcessingState::ProcessingRow {
                                input_batch: batch,
                                row_idx: 0,
                                output_batches: Vec::new(),
                            };
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => return Poll::Ready(None),
                    }
                }

                ProcessingState::ProcessingRow {
                    input_batch,
                    row_idx,
                    output_batches,
                } => {
                    if row_idx >= input_batch.num_rows() {
                        let result = self.combine_output_batches(&output_batches)?;
                        return Poll::Ready(Some(Ok(result)));
                    }

                    let arg_values = match self.evaluate_args(&input_batch, row_idx) {
                        Ok(args) => args,
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    let table_provider =
                        match self.table_function.function().call(&arg_values) {
                            Ok(provider) => provider,
                            Err(e) => {
                                return Poll::Ready(Some(Err(e)));
                            }
                        };

                    let session_state = Arc::clone(&self.session_state);
                    let scan_future = async move {
                        table_provider
                            .scan(session_state.as_ref(), None, &[], None)
                            .await
                    }
                    .boxed();

                    self.state = ProcessingState::ScanningTableFunction {
                        input_batch,
                        row_idx,
                        output_batches,
                        scan_future,
                    };
                }

                ProcessingState::ScanningTableFunction {
                    input_batch,
                    row_idx,
                    output_batches,
                    mut scan_future,
                } => {
                    let table_exec = ready!(scan_future.poll_unpin(cx));
                    let table_exec = match table_exec {
                        Ok(exec) => exec,
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    let table_stream = match table_exec.execute(0, Arc::clone(&self.context))
                    {
                        Ok(stream) => stream,
                        Err(e) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                    };

                    self.state = ProcessingState::ReadingTableStream {
                        input_batch,
                        row_idx,
                        output_batches,
                        table_stream,
                        table_batches: Vec::new(),
                    };
                }

                ProcessingState::ReadingTableStream {
                    input_batch,
                    row_idx,
                    mut output_batches,
                    mut table_stream,
                    mut table_batches,
                } => {
                    match ready!(table_stream.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            table_batches.push(batch);
                            self.state = ProcessingState::ReadingTableStream {
                                input_batch,
                                row_idx,
                                output_batches,
                                table_stream,
                                table_batches,
                            };
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            if !table_batches.is_empty() {
                                match self.combine_row_with_table_output(
                                    &input_batch,
                                    row_idx,
                                    &table_batches,
                                ) {
                                    Ok(output_batch) => {
                                        output_batches.push(output_batch);
                                    }
                                    Err(e) => {
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                }
                            }

                            self.state = ProcessingState::ProcessingRow {
                                input_batch,
                                row_idx: row_idx + 1,
                                output_batches,
                            };
                        }
                    }
                }
            }
        }
    }
}

impl LateralTableFunctionStream {
    fn evaluate_args(
        &self,
        input_batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<Vec<Expr>> {
        self.args
            .iter()
            .map(|arg_expr| {
                let columnar_value = arg_expr.evaluate(input_batch)?;
                match columnar_value {
                    ColumnarValue::Scalar(scalar) => Ok(Expr::Literal(scalar, None)),
                    ColumnarValue::Array(array) => {
                        let scalar = ScalarValue::try_from_array(&array, row_idx)?;
                        Ok(Expr::Literal(scalar, None))
                    }
                }
            })
            .collect::<Result<Vec<_>>>()
    }

    fn combine_row_with_table_output(
        &self,
        input_batch: &RecordBatch,
        row_idx: usize,
        table_batches: &[RecordBatch],
    ) -> Result<RecordBatch> {
        let combined_table_batch = if table_batches.len() == 1 {
            table_batches[0].clone()
        } else {
            concat_batches(&self.table_function_schema, table_batches)?
        };

        let input_row_arrays: Vec<ArrayRef> = input_batch
            .columns()
            .iter()
            .map(|col| {
                let scalar = ScalarValue::try_from_array(col, row_idx)?;
                scalar.to_array_of_size(combined_table_batch.num_rows())
            })
            .collect::<Result<Vec<_>>>()?;

        let mut output_columns = input_row_arrays;
        output_columns.extend(combined_table_batch.columns().iter().cloned());

        RecordBatch::try_new(Arc::clone(&self.schema), output_columns)
            .map_err(Into::into)
    }

    fn combine_output_batches(
        &self,
        output_batches: &[RecordBatch],
    ) -> Result<RecordBatch> {
        if output_batches.is_empty() {
            Ok(RecordBatch::new_empty(Arc::clone(&self.schema)))
        } else if output_batches.len() == 1 {
            Ok(output_batches[0].clone())
        } else {
            concat_batches(&self.schema, output_batches).map_err(Into::into)
        }
    }
}

impl RecordBatchStream for LateralTableFunctionStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
