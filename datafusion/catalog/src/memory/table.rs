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
    new_empty_array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion_common::error::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    Constraints, DFSchema, DFSchemaRef, ScalarValue, SchemaExt, not_impl_err, plan_err,
};
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::dml::{
    InsertOp, MergeIntoAction, MergeIntoClause, MergeIntoClauseKind,
};
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, ExprSchemable, SortExpr, TableType};
use datafusion_physical_expr::{
    LexOrdering, create_physical_expr, create_physical_sort_exprs,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PhysicalExpr, PlanProperties, ReplaceChildrenOptions, collect_partitioned,
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

    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
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
                            datafusion_common::DataFusionError::Internal(format!(
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
        if self.batches.is_empty() {
            return plan_err!("No partitions provided, expected at least one partition");
        }

        let source_schema = source.schema();
        let source_partitions = collect_partitioned(source, state.task_ctx()).await?;
        let source_rows = partitioned_batches_to_rows(&source_partitions)?;

        let mut target_batches = vec![];
        for partition_data in &self.batches {
            let partition = partition_data.read().await;
            target_batches.extend(partition.iter().cloned());
        }
        let target_rows = batches_to_rows(&target_batches)?;

        let target_width = self.schema.fields().len();
        let source_width = source_schema.fields().len();
        if merge_schema.fields().len() != target_width + source_width {
            return plan_err!(
                "MERGE INTO schema mismatch: expected {} target/source columns, got {}",
                target_width + source_width,
                merge_schema.fields().len()
            );
        }

        let merge_arrow_schema = Arc::new(merge_schema.as_arrow().clone());
        let on = on.cast_to(&DataType::Boolean, merge_schema.as_ref())?;
        let on = state.create_physical_expr(on, merge_schema.as_ref())?;
        let clauses = compile_merge_clauses(self, state, merge_schema.as_ref(), clauses)?;

        let null_target = null_row_for_schema(&self.schema)?;
        let null_source = null_row_for_schema(&source_schema)?;

        let mut target_matches: Vec<Option<usize>> = vec![None; target_rows.len()];
        let mut source_matched = vec![false; source_rows.len()];

        for (target_idx, target_row) in target_rows.iter().enumerate() {
            for (source_idx, source_row) in source_rows.iter().enumerate() {
                let combined = combined_row_batch(
                    Arc::clone(&merge_arrow_schema),
                    target_row,
                    source_row,
                )?;
                if evaluate_merge_predicate(&on, &combined)? {
                    if let Some(first_source_idx) = target_matches[target_idx] {
                        return plan_err!(
                            "MERGE INTO matched target row {target_idx} with more than one source row ({first_source_idx} and {source_idx})"
                        );
                    }
                    target_matches[target_idx] = Some(source_idx);
                    source_matched[source_idx] = true;
                }
            }
        }

        let default_batch = one_row_empty_batch()?;
        let mut merged_rows =
            Vec::with_capacity(target_rows.len().saturating_add(source_rows.len()));
        let mut rows_affected = 0_u64;

        for (target_idx, target_row) in target_rows.iter().enumerate() {
            let (source_row, clause_kind) =
                if let Some(source_idx) = target_matches[target_idx] {
                    (&source_rows[source_idx], MergeIntoClauseKind::Matched)
                } else {
                    (&null_source, MergeIntoClauseKind::NotMatchedBySource)
                };

            let combined = combined_row_batch(
                Arc::clone(&merge_arrow_schema),
                target_row,
                source_row,
            )?;
            let application = apply_first_merge_clause(
                &clauses,
                clause_kind,
                &combined,
                &default_batch,
                Some(target_row),
            )?;
            if application.affected {
                rows_affected += 1;
            }
            if let Some(row) = application.row {
                merged_rows.push(row);
            }
        }

        for (source_idx, source_row) in source_rows.iter().enumerate() {
            if source_matched[source_idx] {
                continue;
            }

            let combined = combined_row_batch(
                Arc::clone(&merge_arrow_schema),
                &null_target,
                source_row,
            )?;
            let application = apply_first_merge_clause(
                &clauses,
                MergeIntoClauseKind::NotMatchedByTarget,
                &combined,
                &default_batch,
                None,
            )?;
            if application.affected {
                rows_affected += 1;
            }
            if let Some(row) = application.row {
                merged_rows.push(row);
            }
        }

        let merged_batch = rows_to_batch(Arc::clone(&self.schema), &merged_rows)?;

        *self.sort_order.lock() = vec![];
        let mut wrote_first_partition = false;
        for partition_data in &self.batches {
            let mut partition = partition_data.write().await;
            if !wrote_first_partition {
                if merged_batch.num_rows() == 0 {
                    partition.clear();
                } else {
                    *partition = vec![merged_batch.clone()];
                }
                wrote_first_partition = true;
            } else {
                partition.clear();
            }
        }

        Ok(Arc::new(DmlResultExec::new(rows_affected)))
    }
}

struct CompiledMergeClause {
    kind: MergeIntoClauseKind,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    action: CompiledMergeAction,
}

enum CompiledMergeAction {
    Update(Vec<CompiledMergeAssignment>),
    Insert(Vec<CompiledInsertValue>),
    Delete,
}

struct CompiledMergeAssignment {
    target_index: usize,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

enum CompiledInsertValue {
    MergeExpr {
        data_type: DataType,
        expr: Arc<dyn PhysicalExpr>,
    },
    DefaultExpr {
        data_type: DataType,
        expr: Arc<dyn PhysicalExpr>,
    },
    Null(ScalarValue),
}

struct MergeApplication {
    row: Option<Vec<ScalarValue>>,
    affected: bool,
}

fn compile_merge_clauses(
    table: &MemTable,
    state: &dyn Session,
    merge_schema: &DFSchema,
    clauses: Vec<MergeIntoClause>,
) -> Result<Vec<CompiledMergeClause>> {
    let empty_schema = DFSchema::empty();
    clauses
        .into_iter()
        .map(|clause| {
            let predicate = clause
                .predicate
                .map(|predicate| {
                    let predicate =
                        predicate.cast_to(&DataType::Boolean, merge_schema)?;
                    state.create_physical_expr(predicate, merge_schema)
                })
                .transpose()?;

            let action = match (clause.kind.canonical(), clause.action) {
                (MergeIntoClauseKind::Matched, MergeIntoAction::Update(assignments))
                | (
                    MergeIntoClauseKind::NotMatchedBySource,
                    MergeIntoAction::Update(assignments),
                ) => CompiledMergeAction::Update(compile_merge_assignments(
                    table,
                    state,
                    merge_schema,
                    assignments,
                )?),
                (MergeIntoClauseKind::Matched, MergeIntoAction::Delete)
                | (MergeIntoClauseKind::NotMatchedBySource, MergeIntoAction::Delete) => {
                    CompiledMergeAction::Delete
                }
                (
                    MergeIntoClauseKind::NotMatchedByTarget,
                    MergeIntoAction::Insert { columns, values },
                ) => CompiledMergeAction::Insert(compile_merge_insert_values(
                    table,
                    state,
                    merge_schema,
                    &empty_schema,
                    columns,
                    values,
                )?),
                (MergeIntoClauseKind::Matched, MergeIntoAction::Insert { .. }) => {
                    return plan_err!("MERGE MATCHED INSERT is not supported");
                }
                (MergeIntoClauseKind::NotMatchedByTarget, MergeIntoAction::Update(_)) => {
                    return plan_err!("MERGE NOT MATCHED UPDATE is not supported");
                }
                (MergeIntoClauseKind::NotMatchedByTarget, MergeIntoAction::Delete) => {
                    return plan_err!("MERGE NOT MATCHED DELETE is not supported");
                }
                (
                    MergeIntoClauseKind::NotMatchedBySource,
                    MergeIntoAction::Insert { .. },
                ) => {
                    return plan_err!(
                        "MERGE NOT MATCHED BY SOURCE INSERT is not supported"
                    );
                }
                (MergeIntoClauseKind::NotMatched, _) => {
                    unreachable!("canonical() never returns NotMatched")
                }
            };

            Ok(CompiledMergeClause {
                kind: clause.kind,
                predicate,
                action,
            })
        })
        .collect()
}

fn compile_merge_assignments(
    table: &MemTable,
    state: &dyn Session,
    merge_schema: &DFSchema,
    assignments: Vec<(String, Expr)>,
) -> Result<Vec<CompiledMergeAssignment>> {
    let available_columns = table.available_column_names();
    let mut seen = HashSet::new();
    assignments
        .into_iter()
        .map(|(column, value)| {
            if !seen.insert(column.clone()) {
                return plan_err!("Duplicate column '{column}' in MERGE UPDATE");
            }
            let (target_index, field) =
                table.target_field(&column).ok_or_else(|| {
                    datafusion_common::DataFusionError::Plan(format!(
                        "MERGE UPDATE failed: column '{column}' does not exist. Available columns: {}",
                        available_columns.join(", ")
                    ))
                })?;
            let value = value.cast_to(field.data_type(), merge_schema)?;
            let expr = state.create_physical_expr(value, merge_schema)?;
            Ok(CompiledMergeAssignment {
                target_index,
                data_type: field.data_type().clone(),
                expr,
            })
        })
        .collect()
}

fn compile_merge_insert_values(
    table: &MemTable,
    state: &dyn Session,
    merge_schema: &DFSchema,
    empty_schema: &DFSchema,
    columns: Vec<String>,
    values: Vec<Expr>,
) -> Result<Vec<CompiledInsertValue>> {
    let target_width = table.schema.fields().len();
    if columns.is_empty() {
        if values.len() != target_width {
            return plan_err!(
                "MERGE INSERT has {target_width} column(s) but {} value(s)",
                values.len()
            );
        }
        return values
            .into_iter()
            .zip(table.schema.fields())
            .map(|(value, field)| {
                let value = value.cast_to(field.data_type(), merge_schema)?;
                let expr = state.create_physical_expr(value, merge_schema)?;
                Ok(CompiledInsertValue::MergeExpr {
                    data_type: field.data_type().clone(),
                    expr,
                })
            })
            .collect();
    }

    if columns.len() != values.len() {
        return plan_err!(
            "MERGE INSERT has {} column(s) but {} value(s)",
            columns.len(),
            values.len()
        );
    }

    let mut insert_values = table
        .schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(default) = table.column_defaults.get(field.name()) {
                let default = default.clone().cast_to(field.data_type(), empty_schema)?;
                let expr = state.create_physical_expr(default, empty_schema)?;
                Ok(CompiledInsertValue::DefaultExpr {
                    data_type: field.data_type().clone(),
                    expr,
                })
            } else {
                Ok(CompiledInsertValue::Null(ScalarValue::try_new_null(
                    field.data_type(),
                )?))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let available_columns = table.available_column_names();
    let mut seen = HashSet::new();
    for (column, value) in columns.into_iter().zip(values) {
        if !seen.insert(column.clone()) {
            return plan_err!("Duplicate column '{column}' in MERGE INSERT");
        }
        let (target_index, field) = table.target_field(&column).ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(format!(
                "MERGE INSERT failed: column '{column}' does not exist. Available columns: {}",
                available_columns.join(", ")
            ))
        })?;
        let value = value.cast_to(field.data_type(), merge_schema)?;
        let expr = state.create_physical_expr(value, merge_schema)?;
        insert_values[target_index] = CompiledInsertValue::MergeExpr {
            data_type: field.data_type().clone(),
            expr,
        };
    }

    Ok(insert_values)
}

impl MemTable {
    fn target_field(&self, column: &str) -> Option<(usize, &Field)> {
        self.schema
            .fields()
            .iter()
            .enumerate()
            .find_map(|(idx, field)| {
                (field.name() == column).then_some((idx, field.as_ref()))
            })
    }

    fn available_column_names(&self) -> Vec<&str> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect()
    }
}

fn apply_first_merge_clause(
    clauses: &[CompiledMergeClause],
    clause_kind: MergeIntoClauseKind,
    combined: &RecordBatch,
    default_batch: &RecordBatch,
    base_target_row: Option<&[ScalarValue]>,
) -> Result<MergeApplication> {
    for clause in clauses {
        if clause.kind.canonical() != clause_kind.canonical() {
            continue;
        }
        if let Some(predicate) = &clause.predicate
            && !evaluate_merge_predicate(predicate, combined)?
        {
            continue;
        }

        return match &clause.action {
            CompiledMergeAction::Update(assignments) => {
                let Some(base_target_row) = base_target_row else {
                    return plan_err!("MERGE UPDATE requires a target row");
                };
                let mut row = base_target_row.to_vec();
                for assignment in assignments {
                    row[assignment.target_index] = evaluate_merge_value(
                        &assignment.expr,
                        combined,
                        &assignment.data_type,
                    )?;
                }
                Ok(MergeApplication {
                    row: Some(row),
                    affected: true,
                })
            }
            CompiledMergeAction::Delete => Ok(MergeApplication {
                row: None,
                affected: true,
            }),
            CompiledMergeAction::Insert(values) => {
                let row = values
                    .iter()
                    .map(|value| match value {
                        CompiledInsertValue::MergeExpr { data_type, expr } => {
                            evaluate_merge_value(expr, combined, data_type)
                        }
                        CompiledInsertValue::DefaultExpr { data_type, expr } => {
                            evaluate_merge_value(expr, default_batch, data_type)
                        }
                        CompiledInsertValue::Null(value) => Ok(value.clone()),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(MergeApplication {
                    row: Some(row),
                    affected: true,
                })
            }
        };
    }

    Ok(MergeApplication {
        row: base_target_row.map(|row| row.to_vec()),
        affected: false,
    })
}

fn evaluate_merge_predicate(
    predicate: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<bool> {
    let array = predicate.evaluate(batch)?.into_array(batch.num_rows())?;
    let bool_array = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "MERGE predicate did not evaluate to boolean".to_string(),
            )
        })?;
    Ok(!bool_array.is_null(0) && bool_array.value(0))
}

fn evaluate_merge_value(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    data_type: &DataType,
) -> Result<ScalarValue> {
    let array = expr.evaluate(batch)?.into_array(batch.num_rows())?;
    ScalarValue::try_from_array(array.as_ref(), 0)?.cast_to(data_type)
}

fn combined_row_batch(
    schema: SchemaRef,
    target_row: &[ScalarValue],
    source_row: &[ScalarValue],
) -> Result<RecordBatch> {
    let columns = target_row
        .iter()
        .chain(source_row.iter())
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>>>()?;
    Ok(ArrowRecordBatch::try_new(schema, columns)?)
}

fn null_row_for_schema(schema: &SchemaRef) -> Result<Vec<ScalarValue>> {
    schema
        .fields()
        .iter()
        .map(|field| ScalarValue::try_new_null(field.data_type()))
        .collect()
}

fn partitioned_batches_to_rows(
    partitions: &[Vec<RecordBatch>],
) -> Result<Vec<Vec<ScalarValue>>> {
    let mut rows = vec![];
    for partition in partitions {
        rows.extend(batches_to_rows(partition)?);
    }
    Ok(rows)
}

fn batches_to_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<ScalarValue>>> {
    let mut rows = vec![];
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row = batch
                .columns()
                .iter()
                .map(|column| ScalarValue::try_from_array(column.as_ref(), row_idx))
                .collect::<Result<Vec<_>>>()?;
            rows.push(row);
        }
    }
    Ok(rows)
}

fn rows_to_batch(schema: SchemaRef, rows: &[Vec<ScalarValue>]) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column_idx, field)| {
            if rows.is_empty() {
                return Ok(new_empty_array(field.data_type()));
            }

            ScalarValue::iter_to_array(rows.iter().map(|row| row[column_idx].clone()))
        })
        .collect::<Result<Vec<ArrayRef>>>()?;
    Ok(ArrowRecordBatch::try_new(schema, columns)?)
}

fn one_row_empty_batch() -> Result<RecordBatch> {
    Ok(ArrowRecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )?)
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
                datafusion_common::DataFusionError::Internal(
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
