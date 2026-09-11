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

//! [`MemTable`] for querying `Vec<RecordBatch>` by DataFusion.

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::ready;
use std::sync::Arc;

use crate::TableProvider;

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch as ArrowRecordBatch, UInt64Array,
};
use arrow::compute::concat_batches;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, cast, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::error::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    Column, Constraints, DFSchema, DFSchemaRef, DataFusionError, ScalarValue, SchemaExt,
    exec_err, not_impl_err, plan_err,
};
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::dml::{
    InsertOp, MergeIntoAction, MergeIntoClause, MergeIntoClauseKind,
};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, SortExpr, TableType};
use datafusion_physical_expr::{
    LexOrdering, create_physical_expr, create_physical_sort_exprs,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PhysicalExpr, PlanProperties, ReplaceChildrenOptions, collect, collect_partitioned,
};
use datafusion_session::Session;

use async_trait::async_trait;
use futures::future::BoxFuture;
use log::debug;
use parking_lot::Mutex;
use tokio::sync::RwLock;

// backward compatibility
pub use datafusion_datasource::memory::PartitionData;

/// In-memory data source for presenting a `Vec<RecordBatch>` as a
/// data source that can be queried by DataFusion. This allows data to
/// be pre-loaded into memory and then repeatedly queried without
/// incurring additional file I/O overhead.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    // batches used to be pub(crate), but it's needed to be public for the tests
    pub batches: Vec<PartitionData>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
    /// Optional pre-known sort order(s). Must be `SortExpr`s.
    /// inserting data into this table removes the order
    pub sort_order: Arc<Mutex<Vec<Vec<SortExpr>>>>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches.
    ///
    /// Requires at least one partition. To construct an empty `MemTable`, pass
    /// `vec![vec![]]` as the `partitions` argument, this represents one partition with
    /// no batches.
    pub fn try_new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        if partitions.is_empty() {
            return plan_err!("No partitions provided, expected at least one partition");
        }

        for batches in partitions.iter().flatten() {
            let batches_schema = batches.schema();
            if !schema.contains(&batches_schema) {
                debug!(
                    "mem table schema does not contain batches schema. \
                        Target_schema: {schema:?}. Batches Schema: {batches_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        Ok(Self {
            schema,
            batches: partitions
                .into_iter()
                .map(|e| Arc::new(RwLock::new(e)))
                .collect::<Vec<_>>(),
            constraints: Constraints::default(),
            column_defaults: HashMap::new(),
            sort_order: Arc::new(Mutex::new(vec![])),
        })
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Assign column defaults
    pub fn with_column_defaults(
        mut self,
        column_defaults: HashMap<String, Expr>,
    ) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    /// Specify an optional pre-known sort order(s). Must be `SortExpr`s.
    ///
    /// If the data is not sorted by this order, DataFusion may produce
    /// incorrect results.
    ///
    /// DataFusion may take advantage of this ordering to omit sorts
    /// or use more efficient algorithms.
    ///
    /// Note that multiple sort orders are supported, if some are known to be
    /// equivalent,
    pub fn with_sort_order(self, mut sort_order: Vec<Vec<SortExpr>>) -> Self {
        std::mem::swap(self.sort_order.lock().as_mut(), &mut sort_order);
        self
    }

    /// Create a mem table by reading from another data source
    pub async fn load(
        t: Arc<dyn TableProvider>,
        output_partitions: Option<usize>,
        state: &dyn Session,
    ) -> Result<Self> {
        let schema = t.schema();
        let constraints = t.constraints().cloned().unwrap_or_default();

        let exec = t.scan(state, None, &[], None).await?;
        let data = collect_partitioned(exec, state.task_ctx()).await?;

        // Optionally repartition the collected batches.
        let data = if let Some(num_partitions) = output_partitions {
            let source = DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
                &data,
                Arc::clone(&schema),
                None,
            )?));
            let exec = RepartitionExec::try_new(
                Arc::new(source),
                Partitioning::RoundRobinBatch(num_partitions),
            )?;
            collect_partitioned(Arc::new(exec), state.task_ctx()).await?
        } else {
            data
        };

        MemTable::try_new(schema, data).map(|table| table.with_constraints(constraints))
    }
}

#[async_trait]
impl TableProvider for MemTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 [usize]>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        self.scan_boxed(state, projection, filters, limit)
    }

    /// Returns an ExecutionPlan that inserts the execution results of a given [`ExecutionPlan`] into this [`MemTable`].
    ///
    /// The [`ExecutionPlan`] must have the same schema as this [`MemTable`].
    ///
    /// # Arguments
    ///
    /// * `state` - The [`SessionState`] containing the context for executing the plan.
    /// * `input` - The [`ExecutionPlan`] to execute and insert.
    ///
    /// # Returns
    ///
    /// * A plan that returns the number of rows written.
    ///
    /// [`SessionState`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html
    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn insert_into<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.insert_into_boxed(state, input, insert_op)
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }

    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn delete_from<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        filters: Vec<Expr>,
    ) -> BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.delete_from_boxed(state, filters)
    }

    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn update<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.update_boxed(state, assignments, filters)
    }

    fn merge_into<'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.merge_into_boxed(state, source, merge_schema, on, clauses)
    }
}

impl MemTable {
    fn scan_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        projection: Option<&'a [usize]>,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.scan_inner(state, projection, filters, limit))
    }

    async fn scan_inner(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        for arc_inner_vec in self.batches.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone())
        }

        let mut source = MemorySourceConfig::try_new(
            &partitions,
            self.schema(),
            projection.map(|p| p.to_vec()),
        )?;

        let show_sizes = state.config_options().explain.show_sizes;
        source = source.with_show_sizes(show_sizes);

        // add sort information if present
        let sort_order = self.sort_order.lock();
        if !sort_order.is_empty() {
            let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;

            let eqp = state.execution_props();
            let mut file_sort_order = vec![];
            for sort_exprs in sort_order.iter() {
                let physical_exprs = create_physical_sort_exprs(
                    sort_exprs,
                    &df_schema,
                    eqp,
                    &PhysicalPlanningContext::default(),
                )?;
                file_sort_order.extend(LexOrdering::new(physical_exprs));
            }
            source = source.try_with_sort_information(file_sort_order)?;
        }

        Ok(DataSourceExec::from_data_source(source))
    }

    fn insert_into_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(ready(self.insert_into_inner(state, input, insert_op)))
    }

    fn insert_into_inner(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // If we are inserting into the table, any sort order may be messed up so reset it here
        *self.sort_order.lock() = vec![];

        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;

        if insert_op != InsertOp::Append {
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }
        let sink = MemSink::try_new(self.batches.clone(), Arc::clone(&self.schema))?;
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(sink), None)))
    }

    fn delete_from_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        filters: Vec<Expr>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.delete_from_inner(state, filters))
    }

    async fn delete_from_inner(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Early exit if table has no partitions
        if self.batches.is_empty() {
            return Ok(Arc::new(DmlResultExec::new(0)));
        }

        *self.sort_order.lock() = vec![];

        let mut total_deleted: u64 = 0;
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;

        for partition_data in &self.batches {
            let mut partition = partition_data.write().await;
            let mut new_batches = Vec::with_capacity(partition.len());

            for batch in partition.iter() {
                if batch.num_rows() == 0 {
                    continue;
                }

                // Evaluate filters - None means "match all rows"
                let filter_mask = evaluate_filters_to_mask(
                    &filters,
                    batch,
                    &df_schema,
                    state.execution_props(),
                )?;

                let (delete_count, keep_mask) = match filter_mask {
                    Some(mask) => {
                        // Count rows where mask is true (will be deleted)
                        let count = mask.iter().filter(|v| v == &Some(true)).count();
                        // Keep rows where predicate is false or NULL (SQL three-valued logic)
                        let keep: BooleanArray =
                            mask.iter().map(|v| Some(v != Some(true))).collect();
                        (count, keep)
                    }
                    None => {
                        // No filters = delete all rows
                        (
                            batch.num_rows(),
                            BooleanArray::from(vec![false; batch.num_rows()]),
                        )
                    }
                };

                total_deleted += delete_count as u64;

                let filtered_batch = filter_record_batch(batch, &keep_mask)?;
                if filtered_batch.num_rows() > 0 {
                    new_batches.push(filtered_batch);
                }
            }

            *partition = new_batches;
        }

        Ok(Arc::new(DmlResultExec::new(total_deleted)))
    }

    fn update_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.update_inner(state, assignments, filters))
    }

    async fn update_inner(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Early exit if table has no partitions
        if self.batches.is_empty() {
            return Ok(Arc::new(DmlResultExec::new(0)));
        }

        // Validate column names upfront with clear error messages
        let available_columns: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        for (column_name, _) in &assignments {
            if self.schema.field_with_name(column_name).is_err() {
                return plan_err!(
                    "UPDATE failed: column '{}' does not exist. Available columns: {}",
                    column_name,
                    available_columns.join(", ")
                );
            }
        }

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;

        // Create physical expressions for assignments upfront (outside batch loop)
        let physical_assignments: HashMap<String, Arc<dyn PhysicalExpr>> = assignments
            .iter()
            .map(|(name, expr)| {
                let physical_expr = create_physical_expr(
                    expr,
                    &df_schema,
                    state.execution_props(),
                    &PhysicalPlanningContext::default(),
                )?;
                Ok((name.clone(), physical_expr))
            })
            .collect::<Result<_>>()?;

        *self.sort_order.lock() = vec![];

        let mut total_updated: u64 = 0;

        for partition_data in &self.batches {
            let mut partition = partition_data.write().await;
            let mut new_batches = Vec::with_capacity(partition.len());

            for batch in partition.iter() {
                if batch.num_rows() == 0 {
                    continue;
                }

                // Evaluate filters - None means "match all rows"
                let filter_mask = evaluate_filters_to_mask(
                    &filters,
                    batch,
                    &df_schema,
                    state.execution_props(),
                )?;

                let (update_count, update_mask) = match filter_mask {
                    Some(mask) => {
                        // Count rows where mask is true (will be updated)
                        let count = mask.iter().filter(|v| v == &Some(true)).count();
                        // Normalize mask: only true (not NULL) triggers update
                        let normalized: BooleanArray =
                            mask.iter().map(|v| Some(v == Some(true))).collect();
                        (count, normalized)
                    }
                    None => {
                        // No filters = update all rows
                        (
                            batch.num_rows(),
                            BooleanArray::from(vec![true; batch.num_rows()]),
                        )
                    }
                };

                total_updated += update_count as u64;

                if update_count == 0 {
                    new_batches.push(batch.clone());
                    continue;
                }

                let mut new_columns: Vec<ArrayRef> =
                    Vec::with_capacity(batch.num_columns());

                for field in self.schema.fields() {
                    let column_name = field.name();
                    let original_column =
                        batch.column_by_name(column_name).ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Column '{column_name}' not found in batch"
                            ))
                        })?;

                    let new_column = if let Some(physical_expr) =
                        physical_assignments.get(column_name.as_str())
                    {
                        // Use evaluate_selection to only evaluate on matching rows.
                        // This avoids errors (e.g., divide-by-zero) on rows that won't
                        // be updated. The result is scattered back with nulls for
                        // non-matching rows, which zip() will replace with originals.
                        let new_values =
                            physical_expr.evaluate_selection(batch, &update_mask)?;
                        let new_array = new_values.into_array(batch.num_rows())?;

                        // Convert to &dyn Array which implements Datum
                        let new_arr: &dyn Array = new_array.as_ref();
                        let orig_arr: &dyn Array = original_column.as_ref();
                        zip(&update_mask, &new_arr, &orig_arr)?
                    } else {
                        Arc::clone(original_column)
                    };

                    new_columns.push(new_column);
                }

                let updated_batch =
                    ArrowRecordBatch::try_new(Arc::clone(&self.schema), new_columns)?;
                new_batches.push(updated_batch);
            }

            *partition = new_batches;
        }

        Ok(Arc::new(DmlResultExec::new(total_updated)))
    }

    fn merge_into_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.merge_into_inner(state, source, merge_schema, on, clauses))
    }

    async fn merge_into_inner(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_num_cols = self.schema.fields().len();
        let target_schema_ref = Arc::clone(&self.schema);

        // Helper to extract equi-join column indices from `on`
        let equi_keys = extract_equi_join_keys(&on, &merge_schema, target_num_cols)?;
        let target_key_indices: Vec<usize> = equi_keys.iter().map(|(t, _)| *t).collect();
        let source_key_indices: Vec<usize> = equi_keys.iter().map(|(_, s)| *s).collect();

        // Collect source batches
        let source_batches = collect(source, state.task_ctx()).await?;
        let total_source_rows: usize = source_batches.iter().map(|b| b.num_rows()).sum();
        if total_source_rows == 0 {
            return Ok(Arc::new(DmlResultExec::new(0)));
        }

        // Lock all partitions in global ascending order to avoid deadlock
        let mut partitions = Vec::with_capacity(self.batches.len());
        for p in &self.batches {
            partitions.push(p.write().await);
        }

        *self.sort_order.lock() = vec![];

        // Build target hash index across all partitions:
        // Key -> (partition_idx, batch_idx, row_idx)
        let mut target_key_map: HashMap<Vec<ScalarValue>, (usize, usize, usize)> =
            HashMap::new();
        for (p_idx, partition) in partitions.iter().enumerate() {
            for (b_idx, batch) in partition.iter().enumerate() {
                for r_idx in 0..batch.num_rows() {
                    let key = target_key_indices
                        .iter()
                        .map(|&col_idx| {
                            ScalarValue::try_from_array(batch.column(col_idx), r_idx)
                        })
                        .collect::<Result<Vec<_>>>()?;

                    // NULL in any conflict key column never conflicts
                    if key.iter().any(|v| v.is_null()) {
                        continue;
                    }

                    if target_key_map
                        .insert(key.clone(), (p_idx, b_idx, r_idx))
                        .is_some()
                    {
                        return exec_err!(
                            "Table contains duplicate rows for conflict key '{key:?}', but ON CONFLICT requires unique keys"
                        );
                    }
                }
            }
        }

        // Find WHEN MATCHED, WHEN NOT MATCHED, and WHEN NOT MATCHED BY SOURCE clauses
        let matched_clause = clauses
            .iter()
            .find(|c| c.kind == MergeIntoClauseKind::Matched);
        let not_matched_clause = clauses.iter().find(|c| {
            c.kind == MergeIntoClauseKind::NotMatched
                || c.kind == MergeIntoClauseKind::NotMatchedByTarget
        });
        let not_matched_by_source_clause = clauses
            .iter()
            .find(|c| c.kind == MergeIntoClauseKind::NotMatchedBySource);

        let has_update = matched_clause
            .is_some_and(|c| matches!(c.action, MergeIntoAction::Update(_)));
        let mut seen_incoming_keys = HashSet::new();

        let mut matched_target_rows = HashSet::new();
        let mut row_updates: HashMap<(usize, usize, usize), Vec<(String, ScalarValue)>> =
            HashMap::new();
        let mut row_deletions: HashSet<(usize, usize, usize)> = HashSet::new();
        let mut rows_to_insert: Vec<RecordBatch> = Vec::new();
        let mut affected_count: u64 = 0;

        for source_batch in &source_batches {
            if source_batch.num_rows() == 0 {
                continue;
            }

            for s_r_idx in 0..source_batch.num_rows() {
                let source_key = source_key_indices
                    .iter()
                    .map(|&col_idx| {
                        ScalarValue::try_from_array(source_batch.column(col_idx), s_r_idx)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let has_null_key = source_key.iter().any(|v| v.is_null());

                if !has_null_key {
                    if has_update {
                        if !seen_incoming_keys.insert(source_key.clone()) {
                            return exec_err!(
                                "ON CONFLICT DO UPDATE command cannot affect row a second time"
                            );
                        }
                    } else {
                        // DO NOTHING: coalesce intra-batch duplicates that don't match target
                        if !target_key_map.contains_key(&source_key)
                            && !seen_incoming_keys.insert(source_key.clone())
                        {
                            continue;
                        }
                    }
                }

                let target_match = if has_null_key {
                    None
                } else {
                    target_key_map.get(&source_key).copied()
                };

                if let Some(target_loc) = target_match {
                    let already_matched = !matched_target_rows.insert(target_loc);
                    if already_matched && matched_clause.is_some() {
                        return exec_err!(
                            "ON CONFLICT DO UPDATE command cannot affect row a second time"
                        );
                    }

                    if let Some(clause) = matched_clause {
                        let (p_idx, b_idx, r_idx) = target_loc;
                        let target_batch = &partitions[p_idx][b_idx];

                        let combined_batch = create_combined_row_batch(
                            &merge_schema,
                            target_batch,
                            r_idx,
                            source_batch,
                            s_r_idx,
                        )?;

                        let predicate_passed = match &clause.predicate {
                            Some(pred) => {
                                let phys_pred = create_physical_expr(
                                    pred,
                                    &merge_schema,
                                    state.execution_props(),
                                    &PhysicalPlanningContext::default(),
                                )?;
                                let result = phys_pred.evaluate(&combined_batch)?;
                                let arr = result.into_array(1)?;
                                let bool_arr = arr
                                    .as_any()
                                    .downcast_ref::<BooleanArray>()
                                    .ok_or_else(|| {
                                        DataFusionError::Internal(
                                            "Predicate did not evaluate to boolean"
                                                .to_string(),
                                        )
                                    })?;
                                bool_arr.value(0) && !bool_arr.is_null(0)
                            }
                            None => true,
                        };

                        if predicate_passed {
                            match &clause.action {
                                MergeIntoAction::Update(assignments) => {
                                    let mut new_vals =
                                        Vec::with_capacity(assignments.len());
                                    for (col_name, expr) in assignments {
                                        let phys_expr = create_physical_expr(
                                            expr,
                                            &merge_schema,
                                            state.execution_props(),
                                            &PhysicalPlanningContext::default(),
                                        )?;
                                        let res = phys_expr.evaluate(&combined_batch)?;
                                        let val = res.into_array(1)?;
                                        let sv = ScalarValue::try_from_array(&val, 0)?;
                                        new_vals.push((col_name.clone(), sv));
                                    }
                                    row_updates.insert(target_loc, new_vals);
                                    affected_count += 1;
                                }
                                MergeIntoAction::Delete => {
                                    row_deletions.insert(target_loc);
                                    affected_count += 1;
                                }
                                MergeIntoAction::Insert { .. } => {}
                            }
                        }
                    }
                } else if let Some(clause) = not_matched_clause
                    && let MergeIntoAction::Insert { columns, values } = &clause.action
                {
                    let not_matched_batch = create_not_matched_row_batch(
                        &merge_schema,
                        &target_schema_ref,
                        source_batch,
                        s_r_idx,
                    )?;

                    let predicate_passed = match &clause.predicate {
                        Some(pred) => {
                            let phys_pred = create_physical_expr(
                                pred,
                                &merge_schema,
                                state.execution_props(),
                                &PhysicalPlanningContext::default(),
                            )?;
                            let result = phys_pred.evaluate(&not_matched_batch)?;
                            let arr = result.into_array(1)?;
                            let bool_arr = arr
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| {
                                    DataFusionError::Internal(
                                        "Predicate did not evaluate to boolean"
                                            .to_string(),
                                    )
                                })?;
                            bool_arr.value(0) && !bool_arr.is_null(0)
                        }
                        None => true,
                    };

                    if predicate_passed {
                        let insert_col_names: Vec<String> = if columns.is_empty() {
                            target_schema_ref
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect()
                        } else {
                            columns.clone()
                        };

                        let mut evaluated_cols = HashMap::with_capacity(values.len());
                        for (col_name, expr) in insert_col_names.iter().zip(values.iter())
                        {
                            let phys_expr = create_physical_expr(
                                expr,
                                &merge_schema,
                                state.execution_props(),
                                &PhysicalPlanningContext::default(),
                            )?;
                            let res = phys_expr.evaluate(&not_matched_batch)?;
                            let arr = res.into_array(1)?;
                            evaluated_cols.insert(col_name.clone(), arr);
                        }

                        let mut row_cols = Vec::with_capacity(target_num_cols);
                        for field in target_schema_ref.fields() {
                            if let Some(arr) = evaluated_cols.remove(field.name()) {
                                let target_type = field.data_type();
                                let casted_arr = if arr.data_type() == target_type {
                                    arr
                                } else {
                                    cast(&arr, target_type)?
                                };
                                row_cols.push(casted_arr);
                            } else {
                                row_cols.push(arrow::array::new_null_array(
                                    field.data_type(),
                                    1,
                                ));
                            }
                        }

                        let projected_row = RecordBatch::try_new(
                            Arc::clone(&target_schema_ref),
                            row_cols,
                        )?;
                        rows_to_insert.push(projected_row);
                        affected_count += 1;
                    }
                }
            }
        }

        // Handle WHEN NOT MATCHED BY SOURCE clauses
        if let Some(clause) = not_matched_by_source_clause {
            for (p_idx, partition) in partitions.iter().enumerate() {
                for (b_idx, batch) in partition.iter().enumerate() {
                    for r_idx in 0..batch.num_rows() {
                        let target_loc = (p_idx, b_idx, r_idx);
                        if !matched_target_rows.contains(&target_loc) {
                            let mut combined_batch = None;
                            let predicate_passed = match &clause.predicate {
                                Some(pred) => {
                                    let cb = create_combined_row_batch_with_null_source(
                                        &merge_schema,
                                        batch,
                                        r_idx,
                                    )?;
                                    let phys_pred = create_physical_expr(
                                        pred,
                                        &merge_schema,
                                        state.execution_props(),
                                        &PhysicalPlanningContext::default(),
                                    )?;
                                    let result = phys_pred.evaluate(&cb)?;
                                    let arr = result.into_array(1)?;
                                    let bool_arr = arr
                                        .as_any()
                                        .downcast_ref::<BooleanArray>()
                                        .ok_or_else(|| {
                                            DataFusionError::Internal(
                                                "Predicate did not evaluate to boolean"
                                                    .to_string(),
                                            )
                                        })?;
                                    let passed =
                                        bool_arr.value(0) && !bool_arr.is_null(0);
                                    combined_batch = Some(cb);
                                    passed
                                }
                                None => true,
                            };

                            if predicate_passed {
                                match &clause.action {
                                    MergeIntoAction::Delete => {
                                        row_deletions.insert(target_loc);
                                        affected_count += 1;
                                    }
                                    MergeIntoAction::Update(assignments) => {
                                        let cb = match combined_batch {
                                            Some(cb) => cb,
                                            None => {
                                                create_combined_row_batch_with_null_source(
                                                    &merge_schema,
                                                    batch,
                                                    r_idx,
                                                )?
                                            }
                                        };
                                        let mut new_vals =
                                            Vec::with_capacity(assignments.len());
                                        for (col_name, expr) in assignments {
                                            let phys_expr = create_physical_expr(
                                                expr,
                                                &merge_schema,
                                                state.execution_props(),
                                                &PhysicalPlanningContext::default(),
                                            )?;
                                            let res = phys_expr.evaluate(&cb)?;
                                            let val = res.into_array(1)?;
                                            let sv =
                                                ScalarValue::try_from_array(&val, 0)?;
                                            new_vals.push((col_name.clone(), sv));
                                        }
                                        row_updates.insert(target_loc, new_vals);
                                        affected_count += 1;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }

        // Apply updates and deletions to partitions
        for (p_idx, partition) in partitions.iter_mut().enumerate() {
            let mut new_partition = Vec::with_capacity(partition.len());

            for (b_idx, batch) in partition.iter().enumerate() {
                let updates_for_batch: HashMap<usize, &Vec<(String, ScalarValue)>> =
                    row_updates
                        .iter()
                        .filter_map(|(&(p, b, r), vals)| {
                            if p == p_idx && b == b_idx {
                                Some((r, vals))
                            } else {
                                None
                            }
                        })
                        .collect();

                let deletions_for_batch: HashSet<usize> = row_deletions
                    .iter()
                    .filter_map(|&(p, b, r)| {
                        if p == p_idx && b == b_idx {
                            Some(r)
                        } else {
                            None
                        }
                    })
                    .collect();

                if updates_for_batch.is_empty() && deletions_for_batch.is_empty() {
                    new_partition.push(batch.clone());
                    continue;
                }

                let remaining_rows: Vec<usize> = (0..batch.num_rows())
                    .filter(|r| !deletions_for_batch.contains(r))
                    .collect();

                if remaining_rows.is_empty() {
                    continue;
                }

                let mut new_columns = Vec::with_capacity(batch.num_columns());
                for (col_idx, field) in target_schema_ref.fields().iter().enumerate() {
                    let col_name = field.name();
                    let orig_col = batch.column(col_idx);

                    let mut new_scalars = Vec::with_capacity(remaining_rows.len());
                    for &r_idx in &remaining_rows {
                        if let Some(vals) = updates_for_batch.get(&r_idx) {
                            if let Some((_, new_sv)) =
                                vals.iter().find(|(name, _)| name == col_name)
                            {
                                new_scalars.push(new_sv.clone());
                            } else {
                                new_scalars
                                    .push(ScalarValue::try_from_array(orig_col, r_idx)?);
                            }
                        } else {
                            new_scalars
                                .push(ScalarValue::try_from_array(orig_col, r_idx)?);
                        }
                    }
                    let new_col = ScalarValue::iter_to_array(new_scalars)?;
                    new_columns.push(new_col);
                }
                new_partition.push(ArrowRecordBatch::try_new(
                    Arc::clone(&target_schema_ref),
                    new_columns,
                )?);
            }

            **partition = new_partition;
        }

        // Append rows_to_insert to partition 0
        if !rows_to_insert.is_empty() {
            let combined_inserts = concat_batches(&target_schema_ref, &rows_to_insert)?;
            if partitions.is_empty() {
                return not_impl_err!("MemTable has no partitions");
            }
            partitions[0].push(combined_inserts);
        }

        Ok(Arc::new(DmlResultExec::new(affected_count)))
    }
}

fn create_combined_row_batch(
    merge_schema: &DFSchema,
    target_batch: &RecordBatch,
    target_row_idx: usize,
    source_batch: &RecordBatch,
    source_row_idx: usize,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(merge_schema.fields().len());
    for col in target_batch.columns() {
        columns.push(col.slice(target_row_idx, 1));
    }
    for col in source_batch.columns() {
        columns.push(col.slice(source_row_idx, 1));
    }
    let arrow_schema = Arc::new(merge_schema.as_arrow().clone());
    Ok(RecordBatch::try_new(arrow_schema, columns)?)
}

fn create_not_matched_row_batch(
    merge_schema: &DFSchema,
    target_schema: &SchemaRef,
    source_batch: &RecordBatch,
    source_row_idx: usize,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(merge_schema.fields().len());
    for field in target_schema.fields() {
        columns.push(arrow::array::new_null_array(field.data_type(), 1));
    }
    for col in source_batch.columns() {
        columns.push(col.slice(source_row_idx, 1));
    }
    let arrow_schema = Arc::new(merge_schema.as_arrow().clone());
    Ok(RecordBatch::try_new(arrow_schema, columns)?)
}

fn create_combined_row_batch_with_null_source(
    merge_schema: &DFSchema,
    target_batch: &RecordBatch,
    target_row_idx: usize,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(merge_schema.fields().len());
    for col in target_batch.columns() {
        columns.push(col.slice(target_row_idx, 1));
    }
    let target_num_cols = target_batch.num_columns();
    for field in &merge_schema.fields()[target_num_cols..] {
        columns.push(arrow::array::new_null_array(field.data_type(), 1));
    }
    let arrow_schema = Arc::new(merge_schema.as_arrow().clone());
    Ok(RecordBatch::try_new(arrow_schema, columns)?)
}

fn extract_equi_join_keys(
    on: &Expr,
    merge_schema: &DFSchema,
    target_num_cols: usize,
) -> Result<Vec<(usize, usize)>> {
    let mut pairs = Vec::new();
    collect_equi_keys(on, merge_schema, target_num_cols, &mut pairs)?;
    if pairs.is_empty() {
        return not_impl_err!(
            "MERGE INTO not supported for Base table: requires at least one equi-join condition in ON clause"
        );
    }
    Ok(pairs)
}

/// Extracts a column reference from an expression, unwrapping aliases and casts.
///
/// Note: Unwrapping `Expr::Cast` assumes type-coercion casts inserted by the DataFusion planner
/// preserve equi-join uniqueness semantics (e.g. natural type widening). A lossy or non-injective
/// cast in a general MERGE ON condition could map distinct source values to the same key.
fn extract_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(c) => Some(c),
        Expr::Alias(datafusion_expr::expr::Alias { expr: inner, .. }) => {
            extract_column(inner.as_ref())
        }
        Expr::Cast(datafusion_expr::Cast { expr: inner, .. }) => {
            extract_column(inner.as_ref())
        }
        _ => None,
    }
}

fn collect_equi_keys(
    expr: &Expr,
    merge_schema: &DFSchema,
    target_num_cols: usize,
    pairs: &mut Vec<(usize, usize)>,
) -> Result<()> {
    match expr {
        Expr::Alias(datafusion_expr::expr::Alias { expr: inner, .. }) => {
            collect_equi_keys(inner.as_ref(), merge_schema, target_num_cols, pairs)
        }
        Expr::BinaryExpr(datafusion_expr::BinaryExpr { left, op, right }) => match op {
            datafusion_expr::Operator::And => {
                collect_equi_keys(left.as_ref(), merge_schema, target_num_cols, pairs)?;
                collect_equi_keys(right.as_ref(), merge_schema, target_num_cols, pairs)?;
                Ok(())
            }
            datafusion_expr::Operator::Eq => {
                let left_col = extract_column(left.as_ref());
                let right_col = extract_column(right.as_ref());
                if let (Some(c1), Some(c2)) = (left_col, right_col) {
                    let idx1 = merge_schema.index_of_column(c1)?;
                    let idx2 = merge_schema.index_of_column(c2)?;
                    if idx1 < target_num_cols && idx2 >= target_num_cols {
                        pairs.push((idx1, idx2 - target_num_cols));
                        Ok(())
                    } else if idx2 < target_num_cols && idx1 >= target_num_cols {
                        pairs.push((idx2, idx1 - target_num_cols));
                        Ok(())
                    } else {
                        not_impl_err!(
                            "MERGE INTO not supported for Base table: ON equality condition must compare a target column with a source column: {expr}"
                        )
                    }
                } else {
                    not_impl_err!(
                        "MERGE INTO not supported for Base table: requires column equality conditions in ON clause, found: {expr}"
                    )
                }
            }
            _ => not_impl_err!(
                "MERGE INTO not supported for Base table: only supports AND and EQ in ON condition, found: {expr}"
            ),
        },
        _ => not_impl_err!(
            "MERGE INTO not supported for Base table: requires binary equality expressions in ON condition, found: {expr}"
        ),
    }
}

/// Evaluate filter expressions against a batch and return a combined boolean mask.
/// Returns None if filters is empty (meaning "match all rows").
/// The returned mask has true for rows that match the filter predicates.
fn evaluate_filters_to_mask(
    filters: &[Expr],
    batch: &RecordBatch,
    df_schema: &DFSchema,
    execution_props: &datafusion_expr::execution_props::ExecutionProps,
) -> Result<Option<BooleanArray>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut combined_mask: Option<BooleanArray> = None;

    for filter_expr in filters {
        let physical_expr = create_physical_expr(
            filter_expr,
            df_schema,
            execution_props,
            &PhysicalPlanningContext::default(),
        )?;

        let result = physical_expr.evaluate(batch)?;
        let array = result.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Filter did not evaluate to boolean".to_string(),
                )
            })?
            .clone();

        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &bool_array)?,
            None => bool_array,
        });
    }

    Ok(combined_mask)
}

/// Returns a single row with the count of affected rows.
#[derive(Debug)]
struct DmlResultExec {
    rows_affected: u64,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DmlResultExec {
    fn new(rows_affected: u64) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::UInt64,
            false,
        )]));

        let properties = PlanProperties::new(
            datafusion_physical_expr::EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            datafusion_physical_plan::execution_plan::EmissionType::Final,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            rows_affected,
            schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for DmlResultExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "DmlResultExec: rows_affected={}", self.rows_affected)
            }
        }
    }
}

impl ExecutionPlan for DmlResultExec {
    fn name(&self) -> &str {
        "DmlResultExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn replace_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
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
        _partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        // Create a single batch with the count
        let count_array = UInt64Array::from(vec![self.rows_affected]);
        let batch = ArrowRecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![Arc::new(count_array) as ArrayRef],
        )?;

        // Create a stream that yields just this one batch
        let stream = futures::stream::iter(vec![Ok(batch)]);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}
