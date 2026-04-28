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

//! Execution plan for uncorrelated scalar subqueries.
//!
//! [`ScalarSubqueryExec`] wraps a main input plan and a set of subquery plans.
//! At execution time, it runs each subquery exactly once, extracts the scalar
//! result, and populates a shared [`ScalarSubqueryResults`] container that
//! [`ScalarSubqueryExpr`] instances hold directly and read from by index.
//!
//! [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr

use std::fmt;
use std::sync::Arc;

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, ScalarValue, Statistics, exec_err, internal_err};
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_physical_expr::PhysicalExpr;

use crate::execution_plan::{CardinalityEffect, ExecutionPlan, PlanProperties};
use crate::joins::utils::{OnceAsync, OnceFut};
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};

use futures::StreamExt;
use futures::TryStreamExt;

/// Links a scalar subquery's execution plan to its index in the shared results
/// container. The [`ScalarSubqueryExec`] that owns these links populates
/// `results[index]` at execution time, and [`ScalarSubqueryExpr`] instances
/// with the same index read from it.
///
/// [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr
#[derive(Debug, Clone)]
pub struct ScalarSubqueryLink {
    /// The physical plan for the subquery.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Index into the shared results container.
    pub index: SubqueryIndex,
}

/// Manages execution of uncorrelated scalar subqueries for a single plan
/// level.
///
/// From a query-results perspective, this node is a pass-through: it yields
/// the same batches as its main input and exists only to populate scalar
/// subquery results as a side effect before those batches are produced.
///
/// The first child node is the **main input plan**, whose batches are passed
/// through unchanged. The remaining children are **subquery plans**, each of
/// which must produce exactly zero or one row. Before any batches from the main
/// input are yielded, all subquery plans are executed and their scalar results
/// are stored in a shared [`ScalarSubqueryResults`] container owned by this
/// node. [`ScalarSubqueryExpr`] nodes embedded in the main input's expressions
/// hold the same container and read from it by index.
///
/// All subqueries are evaluated eagerly when the first output partition is
/// requested, before any rows from the main input are produced.
///
/// TODO: Consider overlapping computation of the subqueries with evaluating the
/// main query.
///
/// [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr
#[derive(Debug)]
pub struct ScalarSubqueryExec {
    /// The main input plan whose output is passed through.
    input: Arc<dyn ExecutionPlan>,
    /// Subquery plans and their result indexes.
    subqueries: Vec<ScalarSubqueryLink>,
    /// Shared one-time async computation of subquery results.
    subquery_future: Arc<OnceAsync<()>>,
    /// Shared results container; the corresponding `ScalarSubqueryExpr`
    /// nodes in the input plan hold the same underlying container.
    results: ScalarSubqueryResults,
    /// Cached plan properties (copied from input).
    cache: Arc<PlanProperties>,
}

impl ScalarSubqueryExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        subqueries: Vec<ScalarSubqueryLink>,
        results: ScalarSubqueryResults,
    ) -> Self {
        let cache = Arc::clone(input.properties());
        Self {
            input,
            subqueries,
            subquery_future: Arc::default(),
            results,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn subqueries(&self) -> &[ScalarSubqueryLink] {
        &self.subqueries
    }

    pub fn results(&self) -> &ScalarSubqueryResults {
        &self.results
    }

    /// Returns a per-child bool vec that is `true` for the main input
    /// (child 0) and `false` for every subquery child.
    fn true_for_input_only(&self) -> Vec<bool> {
        std::iter::once(true)
            .chain(std::iter::repeat_n(false, self.subqueries.len()))
            .collect()
    }
}

impl DisplayAs for ScalarSubqueryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ScalarSubqueryExec: subqueries={}",
                    self.subqueries.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for ScalarSubqueryExec {
    fn name(&self) -> &'static str {
        "ScalarSubqueryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut children = vec![&self.input];
        for sq in &self.subqueries {
            children.push(&sq.plan);
        }
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // First child is the main input, the rest are subquery plans.
        let input = children.remove(0);
        let subqueries = self
            .subqueries
            .iter()
            .zip(children)
            .map(|(sq, new_plan)| ScalarSubqueryLink {
                plan: new_plan,
                index: sq.index,
            })
            .collect();
        Ok(Arc::new(ScalarSubqueryExec::new(
            input,
            subqueries,
            self.results.clone(),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        self.results.clear();
        Ok(Arc::new(ScalarSubqueryExec {
            input: Arc::clone(&self.input),
            subqueries: self.subqueries.clone(),
            subquery_future: Arc::default(),
            results: self.results.clone(),
            cache: Arc::clone(&self.cache),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let subqueries = self.subqueries.clone();
        let results = self.results.clone();
        let subquery_ctx = Arc::clone(&context);
        let mut subquery_future = self.subquery_future.try_once(move || {
            Ok(async move { execute_subqueries(subqueries, results, subquery_ctx).await })
        })?;
        let input = Arc::clone(&self.input);
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                // Execute all subqueries exactly once, even when multiple
                // partitions call execute() concurrently.
                wait_for_subqueries(&mut subquery_future).await?;

                // Now that the subqueries have finished execution, we can
                // safely execute the main input
                input.execute(partition, context)
            })
            .try_flatten(),
        )))
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Only the main input (first child); subquery children don't contribute
        // to ordering.
        self.true_for_input_only()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Only the main input; subquery children produce at most one row, so
        // repartitioning them has no benefit.
        self.true_for_input_only()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Wait for the subquery execution future to complete.
async fn wait_for_subqueries(fut: &mut OnceFut<()>) -> Result<()> {
    std::future::poll_fn(|cx| fut.get_shared(cx)).await?;
    Ok(())
}

async fn execute_subqueries(
    subqueries: Vec<ScalarSubqueryLink>,
    results: ScalarSubqueryResults,
    context: Arc<TaskContext>,
) -> Result<()> {
    // Evaluate subqueries in parallel; wait for them all to finish evaluation
    // before returning.
    let futures = subqueries.iter().map(|sq| {
        let plan = Arc::clone(&sq.plan);
        let ctx = Arc::clone(&context);
        let results = results.clone();
        let index = sq.index;
        async move {
            let value = execute_scalar_subquery(plan, ctx).await?;
            results.set(index, value)?;
            Ok(()) as Result<()>
        }
    });
    futures::future::try_join_all(futures).await?;
    Ok(())
}

/// Execute a single subquery plan and extract the scalar value.
/// Returns NULL for 0 rows, the scalar value for exactly 1 row,
/// or an error for >1 rows.
async fn execute_scalar_subquery(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<ScalarValue> {
    let schema = plan.schema();
    if schema.fields().len() != 1 {
        // Should be enforced by the physical planner.
        return internal_err!(
            "Scalar subquery must return exactly one column, got {}",
            schema.fields().len()
        );
    }

    let mut stream = crate::execute_stream(plan, context)?;
    let mut result: Option<ScalarValue> = None;

    while let Some(batch) = stream.next().await.transpose()? {
        if batch.num_rows() == 0 {
            continue;
        }
        if result.is_some() || batch.num_rows() > 1 {
            return exec_err!("Scalar subquery returned more than one row");
        }
        result = Some(ScalarValue::try_from_array(batch.column(0), 0)?);
    }

    // 0 rows → typed NULL per SQL semantics
    match result {
        Some(v) => Ok(v),
        None => ScalarValue::try_from(schema.field(0).data_type()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{self, TestMemoryExec};
    use crate::{
        execution_plan::reset_plan_states,
        projection::{ProjectionExec, ProjectionExpr},
    };

    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::test::exec::ErrorExec;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;

    enum ExpectedSubqueryResult {
        Value(ScalarValue),
        Error(&'static str),
    }

    #[derive(Debug)]
    struct CountingExec {
        inner: Arc<dyn ExecutionPlan>,
        execute_calls: Arc<AtomicUsize>,
    }

    impl CountingExec {
        fn new(inner: Arc<dyn ExecutionPlan>, execute_calls: Arc<AtomicUsize>) -> Self {
            Self {
                inner,
                execute_calls,
            }
        }
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "CountingExec")
                }
                DisplayFormatType::TreeRender => write!(f, ""),
            }
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self::new(
                children.remove(0),
                Arc::clone(&self.execute_calls),
            )))
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.execute(partition, context)
        }
    }

    fn make_subquery_plan(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches[0].schema();
        TestMemoryExec::try_new_exec(&[batches], schema, None).unwrap()
    }

    fn int32_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    fn empty_int64_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))])
            .unwrap()
    }

    fn placeholder_input() -> Arc<dyn ExecutionPlan> {
        Arc::new(crate::placeholder_row::PlaceholderRowExec::new(
            test::aggr_test_schema(),
        ))
    }

    fn single_subquery_exec(
        input: Arc<dyn ExecutionPlan>,
        subquery_plan: Arc<dyn ExecutionPlan>,
        results: ScalarSubqueryResults,
    ) -> ScalarSubqueryExec {
        ScalarSubqueryExec::new(
            input,
            vec![ScalarSubqueryLink {
                plan: subquery_plan,
                index: SubqueryIndex::new(0),
            }],
            results,
        )
    }

    fn scalar_subquery_projection_input(
        results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ProjectionExec::try_new(
            vec![ProjectionExpr {
                expr: Arc::new(ScalarSubqueryExpr::new(
                    DataType::Int32,
                    false,
                    SubqueryIndex::new(0),
                    results,
                )),
                alias: "sq".to_string(),
            }],
            placeholder_input(),
        )?))
    }

    fn extract_single_int32_value(batches: &[RecordBatch]) -> i32 {
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.len(), 1);
        values.value(0)
    }

    #[tokio::test]
    async fn test_execute_scalar_subquery_row_count_semantics() -> Result<()> {
        for (name, plan, expected) in [
            (
                "single_row",
                make_subquery_plan(vec![int32_batch(vec![42])]),
                ExpectedSubqueryResult::Value(ScalarValue::Int32(Some(42))),
            ),
            (
                "zero_rows",
                make_subquery_plan(vec![empty_int64_batch()]),
                ExpectedSubqueryResult::Value(ScalarValue::Int64(None)),
            ),
            (
                "multiple_rows",
                make_subquery_plan(vec![int32_batch(vec![1, 2, 3])]),
                ExpectedSubqueryResult::Error("more than one row"),
            ),
        ] {
            let actual =
                execute_scalar_subquery(plan, Arc::new(TaskContext::default())).await;
            match expected {
                ExpectedSubqueryResult::Value(expected) => {
                    assert_eq!(actual?, expected, "{name}");
                }
                ExpectedSubqueryResult::Error(expected) => {
                    let err = actual.expect_err(name);
                    assert!(
                        err.to_string().contains(expected),
                        "{name}: expected error containing '{expected}', got {err}"
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_failed_subquery_is_not_retried() -> Result<()> {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let subquery_plan = Arc::new(CountingExec::new(
            Arc::new(ErrorExec::new()),
            Arc::clone(&execute_calls),
        ));
        let exec = single_subquery_exec(
            placeholder_input(),
            subquery_plan,
            ScalarSubqueryResults::new(1),
        );

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, Arc::clone(&ctx))?;
        assert!(crate::common::collect(stream).await.is_err());

        let stream = exec.execute(0, ctx)?;
        assert!(crate::common::collect(stream).await.is_err());

        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_reset_state_clears_results_and_reexecutes_subqueries() -> Result<()> {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let results = ScalarSubqueryResults::new(1);
        let subquery_plan = Arc::new(CountingExec::new(
            make_subquery_plan(vec![int32_batch(vec![42])]),
            Arc::clone(&execute_calls),
        ));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(single_subquery_exec(
            scalar_subquery_projection_input(results.clone())?,
            subquery_plan,
            results.clone(),
        ));

        let batches =
            crate::common::collect(exec.execute(0, Arc::new(TaskContext::default()))?)
                .await?;
        assert_eq!(extract_single_int32_value(&batches), 42);
        assert_eq!(
            results.get(SubqueryIndex::new(0)),
            Some(ScalarValue::Int32(Some(42)))
        );

        let reset_exec = reset_plan_states(Arc::clone(&exec))?;
        assert_eq!(results.get(SubqueryIndex::new(0)), None);

        let reset_batches = crate::common::collect(
            reset_exec.execute(0, Arc::new(TaskContext::default()))?,
        )
        .await?;
        assert_eq!(extract_single_int32_value(&reset_batches), 42);
        assert_eq!(
            results.get(SubqueryIndex::new(0)),
            Some(ScalarValue::Int32(Some(42)))
        );
        assert_eq!(execute_calls.load(Ordering::SeqCst), 2);

        Ok(())
    }
}
