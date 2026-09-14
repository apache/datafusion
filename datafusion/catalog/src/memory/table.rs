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

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::ready;
use std::sync::Arc;

use crate::TableProvider;

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch as ArrowRecordBatch, UInt64Array,
};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::error::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    Constraints, DFSchema, SchemaExt, internal_err, not_impl_err, plan_err,
};
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, SortExpr, TableType};
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
        Box::pin(ready(self.plan_delete(state, filters)))
    }

    /// Build the plan of a DELETE. The rows change when the plan runs, not here.
    fn plan_delete(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Early exit if table has no partitions
        if self.batches.is_empty() {
            return Ok(self.dml_exec(vec![], vec![], MemDmlOp::Delete));
        }

        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let filters = compile_filters(filters, &df_schema, state.execution_props())?;

        Ok(self.dml_exec(self.batches.clone(), filters, MemDmlOp::Delete))
    }

    fn update_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(ready(self.plan_update(state, assignments, filters)))
    }

    /// Build the plan of an UPDATE. The rows change when the plan runs, not
    /// here. Every check of the statement stays here, so that an `EXPLAIN`
    /// still reports an invalid statement.
    fn plan_update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Early exit if table has no partitions
        if self.batches.is_empty() {
            return Ok(self.dml_exec(vec![], vec![], MemDmlOp::Update(HashMap::new())));
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
            .into_iter()
            .map(|(name, expr)| {
                let physical_expr = create_physical_expr(
                    &expr,
                    &df_schema,
                    state.execution_props(),
                    &PhysicalPlanningContext::default(),
                )?;
                Ok((name, physical_expr))
            })
            .collect::<Result<_>>()?;

        let filters = compile_filters(filters, &df_schema, state.execution_props())?;

        Ok(self.dml_exec(
            self.batches.clone(),
            filters,
            MemDmlOp::Update(physical_assignments),
        ))
    }

    /// Build the plan node that applies `op` to `partitions`.
    fn dml_exec(
        &self,
        partitions: Vec<PartitionData>,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        op: MemDmlOp,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(MemDmlExec::new(MemDmlState {
            partitions,
            table_schema: Arc::clone(&self.schema),
            sort_order: Arc::clone(&self.sort_order),
            filters,
            op,
        }))
    }
}

/// Compile the `WHERE` clause of a DELETE or an UPDATE into physical
/// expressions. An empty result means "match all rows".
fn compile_filters(
    filters: Vec<Expr>,
    df_schema: &DFSchema,
    execution_props: &datafusion_expr::execution_props::ExecutionProps,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    filters
        .into_iter()
        .map(|filter_expr| {
            create_physical_expr(
                &filter_expr,
                df_schema,
                execution_props,
                &PhysicalPlanningContext::default(),
            )
        })
        .collect()
}

/// Evaluate filter expressions against a batch and return a combined boolean mask.
/// Returns None if filters is empty (meaning "match all rows").
/// The returned mask has true for rows that match the filter predicates.
fn evaluate_filters_to_mask(
    filters: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Option<BooleanArray>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let mut combined_mask: Option<BooleanArray> = None;

    for physical_expr in filters {
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

/// The operation that a [`MemDmlExec`] applies to the rows of a [`MemTable`].
#[derive(Debug)]
enum MemDmlOp {
    /// Delete each row that the filters match.
    Delete,
    /// Assign a new value to each row that the filters match. The map holds one
    /// expression per assigned column, keyed by column name.
    Update(HashMap<String, Arc<dyn PhysicalExpr>>),
}

impl MemDmlOp {
    fn as_str(&self) -> &'static str {
        match self {
            MemDmlOp::Delete => "Delete",
            MemDmlOp::Update(_) => "Update",
        }
    }
}

/// Everything that a [`MemDmlExec`] needs in order to apply its operation.
/// Each field is a clone of a field of the [`MemTable`], so the plan changes the
/// rows of the table itself.
#[derive(Debug)]
struct MemDmlState {
    partitions: Vec<PartitionData>,
    table_schema: SchemaRef,
    sort_order: Arc<Mutex<Vec<Vec<SortExpr>>>>,
    /// The `WHERE` clause of the statement, compiled while the plan was built.
    /// An empty list means "match all rows".
    filters: Vec<Arc<dyn PhysicalExpr>>,
    op: MemDmlOp,
}

impl MemDmlState {
    /// Delete the rows that the filters match, and return the number of rows
    /// deleted.
    async fn apply_delete(&self) -> Result<u64> {
        let mut total_deleted: u64 = 0;

        for partition_data in &self.partitions {
            let mut partition = partition_data.write().await;
            let mut new_batches = Vec::with_capacity(partition.len());

            for batch in partition.iter() {
                if batch.num_rows() == 0 {
                    continue;
                }

                // Evaluate filters - None means "match all rows"
                let filter_mask = evaluate_filters_to_mask(&self.filters, batch)?;

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

        Ok(total_deleted)
    }

    /// Assign a new value to each row that the filters match, and return the
    /// number of rows updated.
    async fn apply_update(
        &self,
        physical_assignments: &HashMap<String, Arc<dyn PhysicalExpr>>,
    ) -> Result<u64> {
        let mut total_updated: u64 = 0;

        for partition_data in &self.partitions {
            let mut partition = partition_data.write().await;
            let mut new_batches = Vec::with_capacity(partition.len());

            for batch in partition.iter() {
                if batch.num_rows() == 0 {
                    continue;
                }

                // Evaluate filters - None means "match all rows"
                let filter_mask = evaluate_filters_to_mask(&self.filters, batch)?;

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

                for field in self.table_schema.fields() {
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

                let updated_batch = ArrowRecordBatch::try_new(
                    Arc::clone(&self.table_schema),
                    new_columns,
                )?;
                new_batches.push(updated_batch);
            }

            *partition = new_batches;
        }

        Ok(total_updated)
    }
}

/// Applies a DELETE or an UPDATE to a [`MemTable`], and returns a single row
/// with the count of affected rows.
///
/// The rows change in [`ExecutionPlan::execute`], not while the plan is built,
/// so an `EXPLAIN` of the statement leaves the table alone. Each run of the plan
/// applies the operation once more, as [`DataSinkExec`] does for an INSERT.
#[derive(Debug)]
struct MemDmlExec {
    state: Arc<MemDmlState>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl MemDmlExec {
    fn new(state: MemDmlState) -> Self {
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
            state: Arc::new(state),
            schema,
            properties: Arc::new(properties),
        }
    }
}

impl DisplayAs for MemDmlExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MemDmlExec: op={}", self.state.op.as_str())
            }
        }
    }
}

impl ExecutionPlan for MemDmlExec {
    fn name(&self) -> &str {
        "MemDmlExec"
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
        partition: usize,
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "MemDmlExec has one partition, but partition {partition} was requested"
            );
        }

        let state = Arc::clone(&self.state);
        let schema = Arc::clone(&self.schema);

        // Apply the operation, then emit the count as the single output row.
        let stream = futures::stream::once(async move {
            // The rows change, so any declared sort order no longer holds. The
            // guard drops at the end of this statement, before the first await.
            *state.sort_order.lock() = vec![];

            let rows_affected = match &state.op {
                MemDmlOp::Delete => state.apply_delete().await?,
                MemDmlOp::Update(assignments) => state.apply_update(assignments).await?,
            };

            let count_array = UInt64Array::from(vec![rows_affected]);
            Ok(ArrowRecordBatch::try_new(
                schema,
                vec![Arc::new(count_array) as ArrayRef],
            )?)
        });

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
