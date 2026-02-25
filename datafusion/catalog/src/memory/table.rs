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

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
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
use datafusion_common::{Constraints, DFSchema, SchemaExt, not_impl_err, plan_err};
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, SortExpr, TableType};
use datafusion_physical_expr::{
    LexOrdering, create_physical_expr, create_physical_sort_exprs,
};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, common,
};
use datafusion_session::Session;

use async_trait::async_trait;
use futures::StreamExt;
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
        let constraints = t.constraints();
        let exec = t.scan(state, None, &[], None).await?;
        let partition_count = exec.output_partitioning().partition_count();

        let mut join_set = JoinSet::new();

        for part_idx in 0..partition_count {
            let task = state.task_ctx();
            let exec = Arc::clone(&exec);
            join_set.spawn(async move {
                let stream = exec.execute(part_idx, task)?;
                common::collect(stream).await
            });
        }

        let mut data: Vec<Vec<RecordBatch>> =
            Vec::with_capacity(exec.output_partitioning().partition_count());

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(res) => data.push(res?),
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        let mut exec = DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(
            &data,
            Arc::clone(&schema),
            None,
        )?));
        if let Some(cons) = constraints {
            exec = exec.with_constraints(cons.clone());
        }

        if let Some(num_partitions) = output_partitions {
            let exec = RepartitionExec::try_new(
                Arc::new(exec),
                Partitioning::RoundRobinBatch(num_partitions),
            )?;

            // execute and collect results
            let mut output_partitions = vec![];
            for i in 0..exec.properties().output_partitioning().partition_count() {
                // execute this *output* partition and collect all batches
                let task_ctx = state.task_ctx();
                let mut stream = exec.execute(i, task_ctx)?;
                let mut batches = vec![];
                while let Some(result) = stream.next().await {
                    batches.push(result?);
                }
                output_partitions.push(batches);
            }

            return MemTable::try_new(Arc::clone(&schema), output_partitions);
        }
        MemTable::try_new(Arc::clone(&schema), data)
    }
}

#[async_trait]
impl TableProvider for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut partitions = vec![];
        for arc_inner_vec in self.batches.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone())
        }

        let mut source =
            MemorySourceConfig::try_new(&partitions, self.schema(), projection.cloned())?;

        let show_sizes = state.config_options().explain.show_sizes;
        source = source.with_show_sizes(show_sizes);

        // add sort information if present
        let sort_order = self.sort_order.lock();
        if !sort_order.is_empty() {
            let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;

            let eqp = state.execution_props();
            let mut file_sort_order = vec![];
            for sort_exprs in sort_order.iter() {
                let physical_exprs =
                    create_physical_sort_exprs(sort_exprs, &df_schema, eqp)?;
                file_sort_order.extend(LexOrdering::new(physical_exprs));
            }
            source = source.try_with_sort_information(file_sort_order)?;
        }

        Ok(DataSourceExec::from_data_source(source))
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
    async fn insert_into(
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

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }

    async fn delete_from(
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

    async fn update(
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
        let physical_assignments: HashMap<
            String,
            Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        > = assignments
            .iter()
            .map(|(name, expr)| {
                let physical_expr =
                    create_physical_expr(expr, &df_schema, state.execution_props())?;
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
        let physical_expr =
            create_physical_expr(filter_expr, df_schema, execution_props)?;

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

    fn as_any(&self) -> &dyn Any {
        self
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

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
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
}
