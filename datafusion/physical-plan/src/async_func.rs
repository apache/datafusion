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

use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use arrow::array::RecordBatch;
use arrow_schema::{Fields, Schema, SchemaRef};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{assert_eq_or_internal_err, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::async_scalar_function::AsyncFuncExpr;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use futures::stream::StreamExt;
use log::trace;
use std::any::Any;
use std::sync::Arc;

/// This structure evaluates a set of async expressions on a record
/// batch producing a new record batch
///
/// The schema of the output of the AsyncFuncExec is:
/// Input columns followed by one column for each async expression
#[derive(Debug)]
pub struct AsyncFuncExec {
    /// The async expressions to evaluate
    async_exprs: Vec<Arc<AsyncFuncExpr>>,
    input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl AsyncFuncExec {
    pub fn try_new(
        async_exprs: Vec<Arc<AsyncFuncExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let async_fields = async_exprs
            .iter()
            .map(|async_expr| async_expr.field(input.schema().as_ref()))
            .collect::<Result<Vec<_>>>()?;

        // compute the output schema: input schema then async expressions
        let fields: Fields = input
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(async_fields.into_iter().map(Arc::new))
            .collect();

        let schema = Arc::new(Schema::new(fields));
        let tuples = async_exprs
            .iter()
            .map(|expr| (Arc::clone(&expr.func), expr.name().to_string()))
            .collect::<Vec<_>>();
        let async_expr_mapping = ProjectionMapping::try_new(tuples, &input.schema())?;
        let cache =
            AsyncFuncExec::compute_properties(&input, schema, &async_expr_mapping)?;
        Ok(Self {
            input,
            async_exprs,
            cache,
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
        assert_eq_or_internal_err!(
            children.len(),
            1,
            "AsyncFuncExec wrong number of children"
        );
        Ok(Arc::new(AsyncFuncExec::try_new(
            self.async_exprs.clone(),
            Arc::clone(&children[0]),
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
        // TODO figure out how to record metrics

        // first execute the input stream
        let input_stream = self.input.execute(partition, Arc::clone(&context))?;

        // now, for each record batch, evaluate the async expressions and add the columns to the result
        let async_exprs_captured = Arc::new(self.async_exprs.clone());
        let schema_captured = self.schema();
        let config_options_ref = Arc::clone(context.session_config().options());

        let stream_with_async_functions = input_stream.then(move |batch| {
            // need to clone *again* to capture the async_exprs and schema in the
            // stream and satisfy lifetime requirements.
            let async_exprs_captured = Arc::clone(&async_exprs_captured);
            let schema_captured = Arc::clone(&schema_captured);
            let config_options = Arc::clone(&config_options_ref);

            async move {
                let batch = batch?;
                // append the result of evaluating the async expressions to the output
                let mut output_arrays = batch.columns().to_vec();
                for async_expr in async_exprs_captured.iter() {
                    let output = async_expr
                        .invoke_with_args(&batch, Arc::clone(&config_options))
                        .await?;
                    output_arrays.push(output.to_array(batch.num_rows())?);
                }
                let batch = RecordBatch::try_new(schema_captured, output_arrays)?;
                Ok(batch)
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
}

impl AsyncMapper {
    pub fn new(num_input_columns: usize) -> Self {
        Self {
            num_input_columns,
            async_exprs: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.async_exprs.is_empty()
    }

    pub fn next_column_name(&self) -> String {
        format!("{}{}", ASYNC_FN_PREFIX, self.async_exprs.len())
    }

    /// Finds any references to async functions in the expression and adds them to the map
    pub fn find_references(
        &mut self,
        physical_expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> Result<()> {
        // recursively look for references to async functions
        physical_expr.apply(|expr| {
            if let Some(scalar_func_expr) =
                expr.as_any().downcast_ref::<ScalarFunctionExpr>()
            {
                if scalar_func_expr.fun().as_async().is_some() {
                    let next_name = self.next_column_name();
                    self.async_exprs.push(Arc::new(AsyncFuncExpr::try_new(
                        next_name,
                        Arc::clone(expr),
                        schema,
                    )?));
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(())
    }

    /// If the expression matches any of the async functions, return the new column
    pub fn map_expr(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Transformed<Arc<dyn PhysicalExpr>> {
        // find the first matching async function if any
        let Some(idx) =
            self.async_exprs
                .iter()
                .enumerate()
                .find_map(|(idx, async_expr)| {
                    if async_expr.func == Arc::clone(&expr) {
                        Some(idx)
                    } else {
                        None
                    }
                })
        else {
            return Transformed::no(expr);
        };
        // rewrite in terms of the output column
        Transformed::yes(self.output_column(idx))
    }

    /// return the output column for the async function at index idx
    pub fn output_column(&self, idx: usize) -> Arc<dyn PhysicalExpr> {
        let async_expr = &self.async_exprs[idx];
        let output_idx = self.num_input_columns + idx;
        Arc::new(Column::new(async_expr.name(), output_idx))
    }
}
