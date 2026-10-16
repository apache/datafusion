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
//! Instead of sorting the entire dataset, this operator delegates to a
//! per-partition top-K implementation — one variant each for `ROW_NUMBER`,
//! `RANK`, and `DENSE_RANK` — all of which keep per-partition state while
//! sharing a single [`arrow::row::RowConverter`],
//! [`MemoryReservation`](datafusion_execution::memory_pool::MemoryReservation),
//! and metrics set across all partitions, and emit only the top-K rows
//! per partition in sorted order `(partition_keys, order_keys)`.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{FieldRef, SchemaRef};
use arrow::row::SortField;
use datafusion_common::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_execution::TaskContext;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::equivalence::EquivalenceProperties;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use futures::StreamExt;
use futures::TryStreamExt;

use crate::execution_plan::{Boundedness, EmissionType};
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::topk::{
    PartitionedTopK, PartitionedTopKDenseRank, PartitionedTopKRank, build_sort_fields,
    partitioned_topk_output_schema,
};
use crate::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
};

/// Which window function `PartitionedTopKExec` is optimizing.
///
/// Different ranking functions have different per-partition retention rules:
/// - [`RowNumber`](Self::RowNumber): exactly K rows per partition.
/// - [`Rank`](Self::Rank): K rows plus any rows tied at the boundary
///   ORDER BY value (RANK semantics — `WHERE rk <= K` may keep more
///   than K rows when ties straddle the boundary).
/// - [`DenseRank`](Self::DenseRank): every row whose ORDER BY value is
///   among the K distinct-smallest ORDER BY values in the partition
///   (DENSE_RANK semantics — total kept rows is unbounded in
///   rows-per-distinct-value).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFnKind {
    /// `ROW_NUMBER()` — keep exactly K rows per partition.
    RowNumber,
    /// `RANK()` — keep K rows plus any rows tied at the boundary.
    Rank,
    /// `DENSE_RANK()` — keep every row whose ob value is among the K
    /// distinct-smallest ob values seen in the partition.
    DenseRank,
}

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
/// Instead of sorting the entire dataset, this operator reads unsorted input
/// and delegates to a per-partition top-K implementation (`PartitionedTopK`
/// for `ROW_NUMBER`, `PartitionedTopKRank` for `RANK`, and
/// `PartitionedTopKDenseRank` for `DENSE_RANK`), each maintaining
/// per-partition state while sharing a single
/// [`arrow::row::RowConverter`] /
/// [`MemoryReservation`](datafusion_execution::memory_pool::MemoryReservation)
/// across all partitions, and emits only the top-K rows per partition in
/// sorted order `(partition_keys, order_keys)`.
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
/// - Only activated when the window function is `ROW_NUMBER`, `RANK`, or
///   `DENSE_RANK` with a `PARTITION BY` clause. `RANK` and `DENSE_RANK`
///   additionally require a non-empty `ORDER BY` (with an empty `ORDER BY`
///   every row ties at rank 1 and the rewrite doesn't apply). Global top-K
///   (no `PARTITION BY`) is already handled efficiently by `SortExec` with
///   `fetch`.
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
    /// Which window function this operator is optimizing. Selects the
    /// per-partition retention policy (see [`WindowFnKind`]).
    fn_kind: WindowFnKind,
    /// When set, the operator appends this column to its output, holding each
    /// retained row's value for [`Self::fn_kind`]'s ranking function.
    ///
    /// Every policy already knows those values at emit time — the retained
    /// rows leave in `(partition_keys, order_keys)` order, and the retained set
    /// is a complete order-prefix of each partition, so `ROW_NUMBER` is the
    /// emit position, `RANK` follows from comparing adjacent ORDER BY keys, and
    /// `DENSE_RANK` is the index of the row's distinct-ORDER-BY-value group.
    /// Filling them in here lets the rewrite drop the `BoundedWindowAggExec`
    /// entirely instead of leaving it to re-derive the same numbers over the
    /// operator's output.
    ranking_field: Option<FieldRef>,
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
    /// * `fn_kind` - Which ranking window function this operator optimizes
    ///   ([`WindowFnKind::RowNumber`], [`WindowFnKind::Rank`], or
    ///   [`WindowFnKind::DenseRank`]).
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
    ///     WindowFnKind::RowNumber,
    /// )
    /// ```
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        expr: LexOrdering,
        partition_prefix_len: usize,
        fetch: usize,
        fn_kind: WindowFnKind,
    ) -> Result<Self> {
        let cache =
            Self::compute_properties(&input, expr.clone(), partition_prefix_len, None)?;
        Ok(Self {
            input,
            expr,
            partition_prefix_len,
            fetch,
            fn_kind,
            ranking_field: None,
            metrics_set: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Append `field` to the output, holding each retained row's value for
    /// [`Self::fn_kind`]'s ranking function, so the caller can drop the
    /// `BoundedWindowAggExec` that would otherwise compute it.
    ///
    /// The output schema widens by that one column, so the plan properties
    /// are recomputed under it.
    pub fn with_ranking_field(self, field: FieldRef) -> Result<Self> {
        let cache = Self::compute_properties(
            &self.input,
            self.expr.clone(),
            self.partition_prefix_len,
            Some(&field),
        )?;
        Ok(Self {
            ranking_field: Some(field),
            cache: Arc::new(cache),
            ..self
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

    /// Returns which window function this operator is optimizing.
    pub fn fn_kind(&self) -> WindowFnKind {
        self.fn_kind
    }

    /// Returns the ranking column this operator appends to its output, or
    /// `None` when it emits only its input's columns.
    pub fn ranking_field(&self) -> Option<&FieldRef> {
        self.ranking_field.as_ref()
    }

    /// Compute [`PlanProperties`] for this operator.
    ///
    /// The output is sorted by `sort_exprs` (partition keys then order keys),
    /// uses the same partitioning as the input, emits all output at once
    /// (`EmissionType::Final`), and is bounded.
    ///
    /// When a ranking column is appended, the output is *also* sorted by
    /// `[partition keys..., ranking column ASC NULLS LAST]`: all three
    /// retention policies assign ranks that are non-decreasing in emit order
    /// within a partition. `BoundedWindowAggExec` declared the same ordering
    /// (from each ranking function's `sort_options`), so declaring it here is
    /// what lets `ORDER BY <partition keys>, <ranking column>` stay
    /// sort-free after the window node is removed.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
        partition_prefix_len: usize,
        ranking_field: Option<&FieldRef>,
    ) -> Result<PlanProperties> {
        // With a ranking column appended the output schema is wider than
        // the input's, so the input's properties have to be re-hung under the
        // new schema rather than cloned (the same reason
        // `window_equivalence_properties` does this for `BoundedWindowAggExec`).
        let mut eq_properties = match ranking_field {
            None => input.equivalence_properties().clone(),
            Some(_) => {
                let output_schema =
                    partitioned_topk_output_schema(&input.schema(), ranking_field);
                EquivalenceProperties::new(output_schema)
                    .extend(input.equivalence_properties().clone())?
            }
        };
        let partition_exprs = sort_exprs[..partition_prefix_len].to_vec();
        eq_properties.reorder(sort_exprs)?;

        if let Some(field) = ranking_field {
            // The ranking column is the last one, appended by
            // `partitioned_topk_output_schema`.
            let ranking_col =
                Arc::new(Column::new(field.name(), input.schema().fields().len()))
                    as Arc<dyn PhysicalExpr>;
            let ranking_sort = PhysicalSortExpr::new(
                ranking_col,
                SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            );
            // Mirrors `add_new_ordering_expr_with_partition_by`: the ranking
            // value only ascends *within* a partition, so the ordering has to
            // be prefixed by the partition keys. `reorder` above already
            // established that prefix.
            eq_properties.add_ordering(partition_exprs.into_iter().chain([ranking_sort]));
        }

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
        let fn_label = match self.fn_kind {
            WindowFnKind::RowNumber => "row_number",
            WindowFnKind::Rank => "rank",
            WindowFnKind::DenseRank => "dense_rank",
        };
        let partition_exprs: Vec<String> = self.expr[..self.partition_prefix_len]
            .iter()
            .map(|e| format!("{}", e.expr))
            .collect();
        let order_exprs: Vec<String> = self.expr[self.partition_prefix_len..]
            .iter()
            .map(|e| format!("{e}"))
            .collect();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PartitionedTopKExec: fn={}, fetch={}, partition=[{}], order=[{}]",
                    fn_label,
                    self.fetch,
                    partition_exprs.join(", "),
                    order_exprs.join(", "),
                )?;
                if let Some(field) = &self.ranking_field {
                    write!(f, ", emit=[{}]", field.name())?;
                }
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "fn={fn_label}")?;
                writeln!(f, "fetch={}", self.fetch)?;
                writeln!(f, "partition=[{}]", partition_exprs.join(", "))?;
                writeln!(f, "order=[{}]", order_exprs.join(", "))?;
                if let Some(field) = &self.ranking_field {
                    writeln!(f, "emit=[{}]", field.name())?;
                }
            }
        }
        Ok(())
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
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> crate::InputDistributionRequirements {
        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr
            [..self.partition_prefix_len]
            .iter()
            .map(|e| Arc::clone(&e.expr))
            .collect();
        crate::InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(
            partition_exprs,
        )])
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        let mut exec = PartitionedTopKExec::try_new(
            Arc::clone(&children[0]),
            self.expr.clone(),
            self.partition_prefix_len,
            self.fetch,
            self.fn_kind,
        )?;
        // Not a `try_new` argument, so it has to be carried over by hand:
        // dropping it would narrow the schema the nodes above were planned
        // against.
        if let Some(field) = &self.ranking_field {
            exec = exec.with_ranking_field(Arc::clone(field))?;
        }
        Ok(Arc::new(exec))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        crate::apply_expression_roots(
            self.expr.iter().map(|sort_expr| &sort_expr.expr),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
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

        let partition_exprs: Vec<Arc<dyn PhysicalExpr>> = self.expr
            [..self.partition_prefix_len]
            .iter()
            .map(|e| Arc::clone(&e.expr))
            .collect();
        let order_expr: LexOrdering =
            LexOrdering::new(self.expr[self.partition_prefix_len..].iter().cloned())
                .expect("PartitionedTopKExec requires at least one order-by expression");
        let fetch = self.fetch;
        let fn_kind = self.fn_kind;
        let batch_size = context.session_config().batch_size();
        let runtime = Arc::clone(&context.runtime_env());
        let metrics_set = self.metrics_set.clone();
        let output_schema = self.schema();
        let ranked_schema = self
            .ranking_field
            .is_some()
            .then(|| Arc::clone(&output_schema));

        let stream = futures::stream::once(async move {
            do_partitioned_topk(
                partition,
                input,
                schema,
                partition_exprs,
                partition_sort_fields,
                order_expr,
                fetch,
                fn_kind,
                batch_size,
                runtime,
                metrics_set,
                ranked_schema,
            )
            .await
        })
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.clone_inner())
    }
}

/// Read all input, feed each batch into a per-partition top-K state
/// ([`PartitionedTopK`] for `ROW_NUMBER`, [`PartitionedTopKRank`] for
/// `RANK`, or [`PartitionedTopKDenseRank`] for `DENSE_RANK`), then emit
/// results ordered by `(partition_keys, order_keys)`.
///
/// # Phases
///
/// 1. **Accumulation** — forward each input `RecordBatch` to the
///    per-partition state's `insert_batch`. The `RowConverter` for
///    ORDER BY columns, the operator's `MemoryReservation`, and the
///    `TopKMetrics` are shared across all distinct partition keys for
///    this operator instance.
///
/// 2. **Emission** — `emit` drains all per-partition state in sorted
///    partition-key order. `ROW_NUMBER` and `RANK` interleave the retained
///    rows out of their shared store in `batch_size` chunks, so their output
///    needs no coalescing; `DENSE_RANK` still coalesces. For `RANK`,
///    boundary-tied rows are emitted after each partition's heap rows. For
///    `DENSE_RANK`, rows are emitted from a K-bounded map of distinct ob
///    keys, sorted ascending.
///
/// # Cost
///
/// - Time: O(N log K) where N = total rows, K = fetch
/// - Memory: O(K × P × row_size) where P = number of distinct partitions
///   plus, for RANK, the boundary ties' rows. `ROW_NUMBER` and `RANK` hold
///   their rows by reference into gathered batches, so their constant is the
///   store's compaction ratio, and one in-flight gather is pinned on top.
#[expect(clippy::too_many_arguments)]
async fn do_partitioned_topk(
    partition_id: usize,
    mut input: SendableRecordBatchStream,
    schema: SchemaRef,
    partition_exprs: Vec<Arc<dyn PhysicalExpr>>,
    partition_sort_fields: Vec<SortField>,
    order_expr: LexOrdering,
    fetch: usize,
    fn_kind: WindowFnKind,
    batch_size: usize,
    runtime: Arc<RuntimeEnv>,
    metrics_set: ExecutionPlanMetricsSet,
    ranked_schema: Option<SchemaRef>,
) -> Result<SendableRecordBatchStream> {
    match fn_kind {
        WindowFnKind::RowNumber => {
            let mut state = PartitionedTopK::try_new(
                partition_id,
                &schema,
                partition_exprs,
                partition_sort_fields,
                order_expr,
                fetch,
                batch_size,
                &runtime,
                &metrics_set,
                ranked_schema,
            )?;
            while let Some(batch) = input.next().await {
                state.insert_batch(&batch?)?;
            }
            drop(input);
            state.emit()
        }
        WindowFnKind::Rank => {
            let mut state = PartitionedTopKRank::try_new(
                partition_id,
                &schema,
                partition_exprs,
                partition_sort_fields,
                order_expr,
                fetch,
                batch_size,
                &runtime,
                &metrics_set,
                ranked_schema,
            )?;
            while let Some(batch) = input.next().await {
                state.insert_batch(&batch?)?;
            }
            drop(input);
            state.emit()
        }
        WindowFnKind::DenseRank => {
            let mut state = PartitionedTopKDenseRank::try_new(
                partition_id,
                &schema,
                partition_exprs,
                partition_sort_fields,
                order_expr,
                fetch,
                batch_size,
                &runtime,
                &metrics_set,
                ranked_schema,
            )?;
            while let Some(batch) = input.next().await {
                state.insert_batch(&batch?)?;
            }
            drop(input);
            state.emit()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::placeholder_row::PlaceholderRowExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

    fn pk_val_input() -> Result<(Arc<Schema>, Arc<dyn ExecutionPlan>)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(PlaceholderRowExec::new(Arc::clone(&schema)));
        Ok((schema, input))
    }

    fn pk_then_val(schema: &Arc<Schema>) -> Result<LexOrdering> {
        Ok(LexOrdering::new([
            PhysicalSortExpr::new_default(col("pk", schema)?),
            PhysicalSortExpr::new_default(col("val", schema)?),
        ])
        .expect("two sort expressions"))
    }

    /// Emitting the `ROW_NUMBER()` column widens the output schema by exactly
    /// that field, and leaves it alone otherwise.
    #[test]
    fn ranking_field_widens_the_output_schema() -> Result<()> {
        let (schema, input) = pk_val_input()?;
        let rn_field = Arc::new(Field::new("rn", DataType::UInt64, false));

        let without = PartitionedTopKExec::try_new(
            input,
            pk_then_val(&schema)?,
            1,
            3,
            WindowFnKind::RowNumber,
        )?;
        assert_eq!(without.schema().fields().len(), 2);

        let with = without.with_ranking_field(Arc::clone(&rn_field))?;
        assert_eq!(with.schema().fields().len(), 3);
        assert_eq!(with.schema().field(2), rn_field.as_ref());

        // The ordering the operator guarantees survives the wider schema, and
        // the appended column brings its own `[pk, rn]` ordering with it.
        let eq = with.properties().equivalence_properties();
        let pk = Arc::new(Column::new("pk", 0)) as Arc<dyn PhysicalExpr>;
        let rn = Arc::new(Column::new("rn", 2)) as Arc<dyn PhysicalExpr>;
        let asc_nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        assert!(eq.ordering_satisfy([
            PhysicalSortExpr::new(Arc::clone(&pk), asc_nulls_last),
            PhysicalSortExpr::new(Arc::clone(&rn), asc_nulls_last),
        ])?);
        // ... but only within a partition: `rn` restarts at 1 per key, so it is
        // not a global ordering on its own.
        assert!(!eq.ordering_satisfy([PhysicalSortExpr::new(rn, asc_nulls_last)])?);
        Ok(())
    }

    /// Every policy can emit its own ranking column, and each widens the
    /// output schema the same way.
    #[test]
    fn every_policy_accepts_a_ranking_field() -> Result<()> {
        let (schema, input) = pk_val_input()?;
        let field = Arc::new(Field::new("rk", DataType::UInt64, false));

        for fn_kind in [
            WindowFnKind::RowNumber,
            WindowFnKind::Rank,
            WindowFnKind::DenseRank,
        ] {
            let exec = PartitionedTopKExec::try_new(
                Arc::clone(&input),
                pk_then_val(&schema)?,
                1,
                3,
                fn_kind,
            )?
            .with_ranking_field(Arc::clone(&field))?;
            assert_eq!(
                exec.schema().field(2),
                field.as_ref(),
                "unexpected appended field for {fn_kind:?}"
            );
            assert_eq!(exec.ranking_field(), Some(&field));
        }
        Ok(())
    }

    /// The ranking field is set by `with_ranking_field` rather than passed to
    /// `try_new`, so rebuilding the operator over a new child has to carry it
    /// over. Dropping it would narrow the output schema under nodes that were
    /// planned against the wider one.
    #[test]
    fn replace_children_keeps_the_ranking_field() -> Result<()> {
        let (schema, input) = pk_val_input()?;
        let rn_field = Arc::new(Field::new("rn", DataType::UInt64, false));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            PartitionedTopKExec::try_new(
                Arc::clone(&input),
                pk_then_val(&schema)?,
                1,
                3,
                WindowFnKind::RowNumber,
            )?
            .with_ranking_field(Arc::clone(&rn_field))?,
        );

        let rebuilt = Arc::clone(&exec).replace_children(
            vec![input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        assert_eq!(rebuilt.schema(), exec.schema());
        assert_eq!(
            rebuilt
                .downcast_ref::<PartitionedTopKExec>()
                .expect("still a PartitionedTopKExec")
                .ranking_field(),
            Some(&rn_field)
        );
        // The ordering on the appended column is recomputed with it.
        let pk = Arc::new(Column::new("pk", 0)) as Arc<dyn PhysicalExpr>;
        let rn = Arc::new(Column::new("rn", 2)) as Arc<dyn PhysicalExpr>;
        let asc_nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        assert!(
            rebuilt
                .properties()
                .equivalence_properties()
                .ordering_satisfy([
                    PhysicalSortExpr::new(pk, asc_nulls_last),
                    PhysicalSortExpr::new(rn, asc_nulls_last),
                ])?
        );
        Ok(())
    }
}
