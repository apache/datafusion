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

//! [`PartitionedTopKExec`]: Top-K per partition operator
//!
//! For queries like:
//! ```sql
//! SELECT *, ROW_NUMBER() OVER (PARTITION BY pk ORDER BY val) as rn
//! FROM t WHERE rn <= N
//! ```
//!
//! Instead of sorting the entire dataset, this operator maintains a
//! [`TopK`] heap per partition (reusing the existing TopK implementation)
//! and emits only the top-K rows per partition in sorted order
//! `(partition_keys, order_keys)`.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{BatchCoalescer, take_record_batch};
use arrow::datatypes::SchemaRef;
use arrow::row::{OwnedRow, RowConverter};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{HashMap, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use futures::StreamExt;
use futures::TryStreamExt;
use parking_lot::RwLock;

use crate::execution_plan::{Boundedness, EmissionType};
use crate::metrics::ExecutionPlanMetricsSet;
use crate::topk::{TopK, TopKDynamicFilters, build_sort_fields};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
};

/// Per-partition Top-K operator for window function queries.
///
/// # Background
///
/// "Top K per partition" is a common analytics pattern used for queries such as
/// "find the top 3 products by revenue for each store". The (simplified) SQL
/// for such a query might be:
///
/// ```sql
/// SELECT * FROM (
///     SELECT *, ROW_NUMBER() OVER (PARTITION BY store ORDER BY revenue DESC) as rn
///     FROM sales
/// ) WHERE rn <= 3;
/// ```
///
/// The unoptimized physical plan would be:
///
/// ```text
/// FilterExec: rn <= 3
///   BoundedWindowAggExec: ROW_NUMBER() PARTITION BY [store] ORDER BY [revenue DESC]
///     SortExec: expr=[store ASC, revenue DESC]
///       DataSourceExec
/// ```
///
/// This plan sorts the **entire** dataset (O(N log N)), computes `ROW_NUMBER`
/// for **all** rows, and then filters to keep only the top K per partition.
/// With 10M rows, 1K partitions, and K=3, it sorts all 10M rows but only
/// keeps 3K.
///
/// # Optimization
///
/// `PartitionedTopKExec` replaces the `SortExec` and the `FilterExec` is
/// removed. The optimized plan becomes:
///
/// ```text
/// BoundedWindowAggExec: ROW_NUMBER() PARTITION BY [store] ORDER BY [revenue DESC]
///   PartitionedTopKExec: fetch=3, partition=[store], order=[revenue DESC]
///     DataSourceExec
/// ```
///
/// Instead of sorting the entire dataset, this operator reads unsorted input,
/// maintains a [`TopK`] heap per distinct partition key, and emits only the
/// top-K rows per partition in sorted order `(partition_keys, order_keys)`.
///
/// Cost: O(N log K) time instead of O(N log N), and O(K × P × row_size)
/// memory where K = fetch, P = number of distinct partitions.
/// ## Why maintaining partition key order in output
/// Window functions do not require partition keys to be globally sorted, and
/// enforcing such ordering in the output can introduce unnecessary overhead.
/// However, the physical optimizer framework currently cannot express an
/// ordering that is only grouped by some keys while ordered by others. For
/// example:
///
///
/// # Example
///
/// For the query above with `fetch=3` and input:
///
/// ```text
/// store | revenue
/// ------|--------
///   A   |  100
///   B   |   50
///   A   |  200
///   B   |  150
///   A   |  300
///   A   |  400
/// ```
///
/// The operator maintains two heaps:
/// - **store=A**: keeps top-3 by revenue DESC → {400, 300, 200}, evicts 100
/// - **store=B**: keeps top-3 by revenue DESC → {150, 50} (only 2 rows)
///
/// Output (sorted by store ASC, revenue DESC):
///
/// ```text
/// store | revenue
/// ------|--------
///   A   |  400
///   A   |  300
///   A   |  200
///   B   |  150
///   B   |   50
/// ```
///
/// This is then passed to `BoundedWindowAggExec` which assigns
/// `ROW_NUMBER` 1, 2, 3 to each partition — all of which satisfy `rn <= 3`.
///
/// # Limitations
///
/// - Only activated when the window function is `ROW_NUMBER` with a
///   `PARTITION BY` clause. Global top-K (no `PARTITION BY`) is already
///   handled efficiently by `SortExec` with `fetch`.
/// - For very high cardinality partition keys (millions of distinct values),
///   both memory usage and runtime overhead can become significant. In such
///   cases, the sort-based plan is more robust. Therefore, this optimization
///   is currently controlled by a configuration flag.
#[derive(Debug, Clone)]
pub struct PartitionedTopKExec {
    /// Input execution plan (reads unsorted data)
    input: Arc<dyn ExecutionPlan>,
    /// Full sort expressions: `[partition_keys..., order_keys...]`.
    ///
    /// For `PARTITION BY store ORDER BY revenue DESC` with sort
    /// `[store ASC, revenue DESC]`, the first `partition_prefix_len`
    /// expressions are the partition keys (`[store ASC]`) and the
    /// remaining are the order-by keys (`[revenue DESC]`).
    expr: LexOrdering,
    /// Number of leading expressions in `expr` that define the partition
    /// key. For example, `PARTITION BY a, b` → `partition_prefix_len = 2`.
    partition_prefix_len: usize,
    /// Maximum number of rows to keep per partition (the K in "top-K").
    /// Derived from the filter predicate: `rn <= 3` → `fetch = 3`,
    /// `rn < 3` → `fetch = 2`.
    fetch: usize,
    /// Execution metrics
    metrics_set: ExecutionPlanMetricsSet,
    /// Cached plan properties (output ordering, partitioning, etc.)
    cache: Arc<PlanProperties>,
}

impl PartitionedTopKExec {
    /// Create a new `PartitionedTopKExec`.
    ///
    /// # Arguments
    ///
    /// * `input` - The child execution plan providing unsorted input rows.
    /// * `expr` - Full sort ordering `[partition_keys..., order_keys...]`.
    ///   For `PARTITION BY pk ORDER BY val ASC`, this would be `[pk ASC, val ASC]`.
    /// * `partition_prefix_len` - Number of leading expressions in `expr`
    ///   that form the partition key. Must be >= 1.
    /// * `fetch` - Maximum rows to retain per partition (the K in "top-K").
    ///
    /// # Example
    ///
    /// ```text
    /// // For: ROW_NUMBER() OVER (PARTITION BY store ORDER BY revenue DESC) ... WHERE rn <= 5
    /// PartitionedTopKExec::try_new(
    ///     data_source,
    ///     LexOrdering([store ASC, revenue DESC]),
    ///     1,    // partition_prefix_len: 1 partition column (store)
    ///     5,    // fetch: keep top 5 per partition
    /// )
    /// ```
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expr: LexOrdering,
        partition_prefix_len: usize,
        fetch: usize,
    ) -> Result<Self> {
        let cache = Self::compute_properties(&input, expr.clone())?;
        Ok(Self {
            input,
            expr,
            partition_prefix_len,
            fetch,
            metrics_set: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Returns the child execution plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Returns the full sort ordering `[partition_keys..., order_keys...]`.
    pub fn expr(&self) -> &LexOrdering {
        &self.expr
    }

    /// Returns the number of leading expressions in [`Self::expr`] that
    /// define the partition key.
    pub fn partition_prefix_len(&self) -> usize {
        self.partition_prefix_len
    }

    /// Returns the maximum number of rows retained per partition.
    pub fn fetch(&self) -> usize {
        self.fetch
    }

    /// Compute [`PlanProperties`] for this operator.
    ///
    /// The output is sorted by `sort_exprs` (partition keys then order keys),
    /// uses the same partitioning as the input, emits all output at once
    /// (`EmissionType::Final`), and is bounded.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
    ) -> Result<PlanProperties> {
        let mut eq_properties = input.equivalence_properties().clone();
        eq_properties.reorder(sort_exprs)?;

        Ok(PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            EmissionType::Final,
            Boundedness::Bounded,
        ))
    }
}

impl DisplayAs for PartitionedTopKExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_exprs: Vec<String> = self.expr[..self.partition_prefix_len]
                    .iter()
                    .map(|e| format!("{}", e.expr))
                    .collect();
                let order_exprs: Vec<String> = self.expr[self.partition_prefix_len..]
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect();
                write!(
                    f,
                    "PartitionedTopKExec: fetch={}, partition=[{}], order=[{}]",
                    self.fetch,
                    partition_exprs.join(", "),
                    order_exprs.join(", "),
                )
            }
            DisplayFormatType::TreeRender => {
                let partition_exprs: Vec<String> = self.expr[..self.partition_prefix_len]
                    .iter()
                    .map(|e| format!("{}", e.expr))
                    .collect();
                let order_exprs: Vec<String> = self.expr[self.partition_prefix_len..]
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect();
                writeln!(f, "fetch={}", self.fetch)?;
                writeln!(f, "partition=[{}]", partition_exprs.join(", "))?;
                writeln!(f, "order=[{}]", order_exprs.join(", "))
            }
        }
    }
}

impl ExecutionPlan for PartitionedTopKExec {
    fn name(&self) -> &'static str {
        "PartitionedTopKExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr
            [..self.partition_prefix_len]
            .iter()
            .map(|e| Arc::clone(&e.expr))
            .collect();
        vec![Distribution::HashPartitioned(partition_exprs)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(PartitionedTopKExec::try_new(
            Arc::clone(&children[0]),
            self.expr.clone(),
            self.partition_prefix_len,
            self.fetch,
        )?))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for sort_expr in &self.expr {
            tnr = tnr.visit_sibling(|| f(sort_expr.expr.as_ref()))?;
        }
        Ok(tnr)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let schema = input.schema();

        let partition_sort_fields =
            build_sort_fields(&self.expr[..self.partition_prefix_len], &schema)?;

        let partition_converter = RowConverter::new(partition_sort_fields)?;

        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr
            [..self.partition_prefix_len]
            .iter()
            .map(|e| Arc::clone(&e.expr))
            .collect();
        let order_expr: LexOrdering =
            LexOrdering::new(self.expr[self.partition_prefix_len..].iter().cloned())
                .expect("PartitionedTopKExec requires at least one order-by expression");
        let fetch = self.fetch;
        let batch_size = context.session_config().batch_size();
        let runtime = Arc::clone(&context.runtime_env());
        let metrics_set = self.metrics_set.clone();

        let stream = futures::stream::once(async move {
            do_partitioned_topk(
                input,
                schema,
                partition_converter,
                partition_exprs,
                order_expr,
                fetch,
                batch_size,
                runtime,
                metrics_set,
            )
            .await
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            stream,
        )))
    }
}

/// Create a no-op [`TopKDynamicFilters`] for a per-partition [`TopK`].
///
/// In normal `SortExec` top-K mode, dynamic filters push predicates down to
/// the data source (e.g., telling Parquet to skip rows worse than the current
/// K-th best). For per-partition heaps the data is already in memory and split
/// by partition key, so there is no data source to push filters to. We pass
/// `lit(true)` (accept everything) so the filter never rejects any row.
fn create_noop_dynamic_filter() -> Arc<RwLock<TopKDynamicFilters>> {
    Arc::new(RwLock::new(TopKDynamicFilters::new(Arc::new(
        DynamicFilterPhysicalExpr::new(vec![], lit(true)),
    ))))
}

/// Read all input, split batches by partition key, feed each sub-batch
/// to a per-partition [`TopK`], then emit results in partition-key order.
///
/// # Phases
///
/// 1. **Accumulation** — For each input batch:
///    - Evaluate partition key expressions to get partition column arrays
///    - Convert partition columns to binary [`arrow::row::Row`] format
///    - Group row indices by partition key
///    - Extract sub-batches via [`take_record_batch`] and insert into
///      the partition's [`TopK`] heap
///
/// 2. **Emission** — After all input is consumed:
///    - Sort partition keys so output is ordered by partition key
///    - For each partition in sorted order, call [`TopK::emit`] to get
///      rows sorted by order-by key
///    - Return all batches as a single stream
///
/// # Cost
///
/// - Time: O(N log K) where N = total rows, K = fetch
/// - Memory: O(K × P × row_size) where P = number of distinct partitions
#[expect(clippy::too_many_arguments)]
async fn do_partitioned_topk(
    mut input: SendableRecordBatchStream,
    schema: SchemaRef,
    partition_converter: RowConverter,
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    order_expr: LexOrdering,
    fetch: usize,
    batch_size: usize,
    runtime: Arc<datafusion_execution::runtime_env::RuntimeEnv>,
    metrics_set: ExecutionPlanMetricsSet,
) -> Result<SendableRecordBatchStream> {
    let mut partitions: HashMap<OwnedRow, TopK> = HashMap::new();
    let mut partition_counter: usize = 0;

    // Macro-like helper: create a new TopK for a partition
    macro_rules! new_topk {
        () => {{
            let id = partition_counter;
            partition_counter += 1;
            TopK::try_new(
                id,
                Arc::clone(&schema),
                vec![],
                order_expr.clone(),
                fetch,
                batch_size,
                Arc::clone(&runtime),
                &metrics_set,
                create_noop_dynamic_filter(),
            )
        }};
    }

    // ---------- Accumulation phase ----------
    while let Some(batch) = input.next().await {
        let batch = batch?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        // Evaluate partition key columns
        let pk_arrays: Vec<_> = partition_exprs
            .iter()
            .map(|e| e.evaluate(&batch).and_then(|v| v.into_array(num_rows)))
            .collect::<Result<Vec<_>>>()?;

        let pk_rows = partition_converter.convert_columns(&pk_arrays)?;

        // Group row indices by partition key
        let mut groups: HashMap<OwnedRow, Vec<u32>> = HashMap::new();
        for row_idx in 0..num_rows {
            let pk = pk_rows.row(row_idx).owned();
            groups.entry(pk).or_default().push(row_idx as u32);
        }

        // For each partition group, create a sub-batch and feed to TopK
        for (pk, indices) in groups {
            if !partitions.contains_key(&pk) {
                partitions.insert(pk.clone(), new_topk!()?);
            }
            let topk = partitions.get_mut(&pk).unwrap();
            let indices_array = UInt32Array::from(indices);
            let sub_batch = take_record_batch(&batch, &indices_array)?;
            topk.insert_batch(sub_batch)?;
        }
    }

    // ---------- Emit phase ----------
    // Sort partition keys so output is ordered by (partition_keys, order_keys).
    let mut sorted_pks: Vec<OwnedRow> = partitions.keys().cloned().collect();
    sorted_pks.sort();

    let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), batch_size);

    for pk in sorted_pks {
        if let Some(topk) = partitions.remove(&pk) {
            // TopK::emit() returns a stream of sorted batches
            let mut stream = topk.emit()?;
            while let Some(batch) = stream.next().await {
                coalescer.push_batch(batch?)?;
            }
        }
    }
    coalescer.finish_buffered_batch()?;
    let mut output_batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = coalescer.next_completed_batch() {
        output_batches.push(batch);
    }

    Ok(Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(output_batches.into_iter().map(Ok)),
    )))
}
