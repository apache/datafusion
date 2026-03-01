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

use crate::coalesce::LimitedBatchCoalescer;
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    check_if_same_properties,
};
use arrow::array::RecordBatch;
use arrow_schema::{Field, Fields, Schema, SchemaRef};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_physical_expr::expressions::Column;

use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::async_scalar_function::AsyncFuncExpr;
use datafusion_physical_expr::equivalence::ProjectionMapping;

use datafusion_physical_expr_common::metrics::{BaselineMetrics, RecordOutput};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use futures::Stream;
use futures::stream::StreamExt;
use log::trace;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// This structure evaluates a set of async expressions on a record
/// batch producing a new record batch
///
/// The schema of the output of the AsyncFuncExec is:
/// Input columns followed by one column for each async expression
#[derive(Debug, Clone)]
pub struct AsyncFuncExec {
    /// The async expressions to evaluate
    async_exprs: Vec<Arc<AsyncFuncExpr>>,
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl AsyncFuncExec {
    pub fn try_new(
        async_exprs: Vec<Arc<AsyncFuncExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let mut current_fields = input.schema().fields().to_vec();
        let mut async_fields = Vec::with_capacity(async_exprs.len());
        for async_expr in &async_exprs {
            let current_schema = Schema::new(current_fields.clone());
            let field = async_expr.field(&current_schema)?;
            current_fields.push(Arc::new(field.clone()));
            async_fields.push(field);
        }

        // compute the output schema: input schema then async expressions
        let fields: Fields = input
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(async_fields.into_iter().map(Arc::new))
            .collect();

        let schema = Arc::new(Schema::new(fields));

        // Only include expressions that map to input columns in the ProjectionMapping
        // Expressions referencing newly created async columns cannot be verified against input schema
        let input_len = input.schema().fields().len();
        let mut tuples = Vec::new();
        for expr in &async_exprs {
            let mut refers_to_new_cols = false;
            expr.func.apply(&mut |e: &Arc<dyn PhysicalExpr>| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    refers_to_new_cols |= col.index() >= input_len;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;

            if !refers_to_new_cols {
                tuples.push((Arc::clone(&expr.func), expr.name().to_string()));
            }
        }

        let async_expr_mapping = ProjectionMapping::try_new(tuples, &input.schema())?;
        let cache =
            AsyncFuncExec::compute_properties(&input, schema, &async_expr_mapping)?;
        Ok(Self {
            input,
            async_exprs,
            cache: Arc::new(cache),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// This function creates the cache object that stores the plan properties
    /// such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        async_expr_mapping: &ProjectionMapping,
    ) -> Result<PlanProperties> {
        Ok(PlanProperties::new(
            input
                .equivalence_properties()
                .project(async_expr_mapping, schema),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }

    pub fn async_exprs(&self) -> &[Arc<AsyncFuncExpr>] {
        &self.async_exprs
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
    }
}

impl DisplayAs for AsyncFuncExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let expr: Vec<String> = self
            .async_exprs
            .iter()
            .map(|async_expr| async_expr.to_string())
            .collect();
        let exprs = expr.join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AsyncFuncExec: async_expr=[{exprs}]")
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format=async_expr")?;
                writeln!(f, "async_expr={exprs}")?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for AsyncFuncExec {
    fn name(&self) -> &str {
        "async_func"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq_or_internal_err!(
            children.len(),
            1,
            "AsyncFuncExec wrong number of children"
        );
        check_if_same_properties!(self, children);
        Ok(Arc::new(AsyncFuncExec::try_new(
            self.async_exprs.clone(),
            children.swap_remove(0),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start AsyncFuncExpr::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        // first execute the input stream
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;

        // TODO: Track `elapsed_compute` in `BaselineMetrics`
        // Issue: <https://github.com/apache/datafusion/issues/19658>
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        // now, for each record batch, evaluate the async expressions and add the columns to the result
        let async_exprs_captured = Arc::new(self.async_exprs.clone());
        let schema_captured = self.schema();
        let config_options_ref = Arc::clone(context.session_config().options());

        let coalesced_input_stream = CoalesceInputStream {
            input_stream,
            batch_coalescer: LimitedBatchCoalescer::new(
                Arc::clone(&self.input.schema()),
                config_options_ref.execution.batch_size,
                None,
            ),
        };

        let stream_with_async_functions = coalesced_input_stream.then(move |batch| {
            // need to clone *again* to capture the async_exprs and schema in the
            // stream and satisfy lifetime requirements.
            let async_exprs_captured = Arc::clone(&async_exprs_captured);
            let schema_captured = Arc::clone(&schema_captured);
            let config_options = Arc::clone(&config_options_ref);
            let baseline_metrics_captured = baseline_metrics.clone();

            async move {
                let batch = batch?;
                // append the result of evaluating the async expressions to the output
                // We must evaluate them in order, adding the results to the batch
                // so that subsequent async expressions can access the results of previous ones
                let mut output_arrays = Vec::with_capacity(async_exprs_captured.len());
                let input_columns = batch.columns().len();

                for (i, async_expr) in async_exprs_captured.iter().enumerate() {
                    // Create a batch with the input columns and the async columns evaluated so far
                    // We need to construct a schema for this intermediate batch
                    let current_schema_fields: Vec<_> = schema_captured
                        .fields()
                        .iter()
                        .take(input_columns + i)
                        .cloned()
                        .collect();
                    let current_schema = Arc::new(Schema::new(current_schema_fields));

                    let mut current_columns = batch.columns().to_vec();
                    current_columns.extend_from_slice(&output_arrays);

                    let current_batch =
                        RecordBatch::try_new(current_schema, current_columns)?;

                    let output = async_expr
                        .invoke_with_args(&current_batch, Arc::clone(&config_options))
                        .await?;
                    output_arrays.push(output.to_array(batch.num_rows())?);
                }

                let mut final_columns = batch.columns().to_vec();
                final_columns.extend(output_arrays);

                let batch = RecordBatch::try_new(schema_captured, final_columns)?;

                Ok(batch.record_output(&baseline_metrics_captured))
            }
        });

        // Adapt the stream with the output schema
        let adapter =
            RecordBatchStreamAdapter::new(self.schema(), stream_with_async_functions);
        Ok(Box::pin(adapter))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct CoalesceInputStream {
    input_stream: Pin<Box<dyn RecordBatchStream + Send>>,
    batch_coalescer: LimitedBatchCoalescer,
}

impl Stream for CoalesceInputStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut completed = false;

        loop {
            if let Some(batch) = self.batch_coalescer.next_completed_batch() {
                return Poll::Ready(Some(Ok(batch)));
            }

            if completed {
                return Poll::Ready(None);
            }

            match ready!(self.input_stream.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if let Err(err) = self.batch_coalescer.push_batch(batch) {
                        return Poll::Ready(Some(Err(err)));
                    }
                }
                Some(err) => {
                    return Poll::Ready(Some(err));
                }
                None => {
                    completed = true;
                    if let Err(err) = self.batch_coalescer.finish() {
                        return Poll::Ready(Some(Err(err)));
                    }
                }
            }
        }
    }
}

const ASYNC_FN_PREFIX: &str = "__async_fn_";

/// Maps async_expressions to new columns
///
/// The output of the async functions are appended, in order, to the end of the input schema
#[derive(Debug)]
pub struct AsyncMapper {
    /// the number of columns in the input plan
    /// used to generate the output column names.
    /// the first async expr is `__async_fn_0`, the second is `__async_fn_1`, etc
    num_input_columns: usize,
    /// the expressions to map
    pub async_exprs: Vec<Arc<AsyncFuncExpr>>,
    /// the output fields of the async expressions
    output_fields: Vec<Field>,
}

impl AsyncMapper {
    pub fn new(num_input_columns: usize) -> Self {
        Self {
            num_input_columns,
            async_exprs: Vec::new(),
            output_fields: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.async_exprs.is_empty()
    }

    pub fn next_column_name(&self) -> String {
        format!("{}{}", ASYNC_FN_PREFIX, self.async_exprs.len())
    }

    /// Finds any references to async functions in the expression and adds them to the map
    /// AND rewrites the expression to use the mapped columns.
    pub fn find_and_map(
        &mut self,
        physical_expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let transformed = Arc::clone(physical_expr).transform_up(|expr| {
            if let Some(scalar_func_expr) =
                expr.as_any().downcast_ref::<ScalarFunctionExpr>()
                && scalar_func_expr.fun().as_async().is_some()
            {
                let next_name = self.next_column_name();

                // Construct extended schema including previously mapped async fields
                let mut current_fields = schema.fields().to_vec();
                current_fields.extend(
                    self.output_fields
                        .iter()
                        .map(|f: &Field| Arc::new(f.clone())),
                );
                let current_schema = Schema::new(current_fields);

                // We use the expression with its children already transformed
                let async_expr = Arc::new(AsyncFuncExpr::try_new(
                    next_name.clone(),
                    Arc::clone(&expr),
                    &current_schema,
                )?);

                // Store the output field for subsequent expressions
                self.output_fields.push(async_expr.field(&current_schema)?);
                self.async_exprs.push(async_expr);

                // Replace with Column
                let output_idx = self.num_input_columns + self.async_exprs.len() - 1;
                Ok(Transformed::yes(Arc::new(Column::new(
                    &next_name, output_idx,
                ))))
            } else {
                Ok(Transformed::no(expr))
            }
        })?;
        Ok(transformed.data)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{RecordBatch, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_execution::{TaskContext, config::SessionConfig};
    use futures::StreamExt;

    use crate::{ExecutionPlan, async_func::AsyncFuncExec, test::TestMemoryExec};

    #[tokio::test]
    async fn test_async_fn_with_coalescing() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6]))],
        )?;

        let batches: Vec<RecordBatch> = std::iter::repeat_n(batch, 50).collect();

        let session_config = SessionConfig::new().with_batch_size(200);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        let test_exec =
            TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let exec = AsyncFuncExec::try_new(vec![], test_exec)?;

        let mut stream = exec.execute(0, Arc::clone(&task_ctx))?;
        let batch = stream
            .next()
            .await
            .expect("expected to get a record batch")?;
        assert_eq!(200, batch.num_rows());
        let batch = stream
            .next()
            .await
            .expect("expected to get a record batch")?;
        assert_eq!(100, batch.num_rows());

        Ok(())
    }
}
