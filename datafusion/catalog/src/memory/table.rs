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

use arrow::array::{Array, ArrayRef, BooleanArray, UInt64Array};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, filter_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::error::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    Constraints, DFSchema, SchemaExt, internal_datafusion_err, internal_err,
    not_impl_err, plan_err,
};
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion_expr::{Expr, SortExpr, TableType};
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, create_physical_expr, create_physical_sort_exprs,
};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PhysicalExpr, PlanProperties, ReplaceChildrenOptions, apply_expression_roots,
    collect_partitioned,
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
        // Planning a `DELETE` needs no await, so the future is ready at once.
        Box::pin(ready(self.delete_from_inner(state, &filters)))
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
        // Planning an `UPDATE` needs no await, so the future is ready at once.
        Box::pin(ready(self.update_inner(state, &assignments, &filters)))
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

    /// Plan a `DELETE`. The rows change when the returned plan runs, not here,
    /// so `EXPLAIN DELETE` prints the plan and the table keeps its rows.
    fn delete_from_inner(
        &self,
        state: &dyn Session,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;
        let predicates = create_predicates(filters, &df_schema, state)?;

        Ok(Arc::new(MemDeleteExec::new(
            self.batches.clone(),
            Arc::clone(&self.sort_order),
            predicates,
        )))
    }

    /// Plan an `UPDATE`. The rows change when the returned plan runs, not here,
    /// so `EXPLAIN UPDATE` prints the plan and the table keeps its rows.
    fn update_inner(
        &self,
        state: &dyn Session,
        assignments: &[(String, Expr)],
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(Arc::clone(&self.schema))?;

        // One entry for each field of the table, in field order. A `Some` entry
        // holds the expression of the `SET` clause for that field.
        let mut set_exprs: Vec<Option<Arc<dyn PhysicalExpr>>> =
            vec![None; self.schema.fields().len()];
        for (column_name, expr) in assignments {
            let Ok(index) = self.schema.index_of(column_name) else {
                let available_columns: Vec<&str> = self
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect();
                return plan_err!(
                    "UPDATE failed: column '{}' does not exist. Available columns: {}",
                    column_name,
                    available_columns.join(", ")
                );
            };
            set_exprs[index] = Some(create_physical_expr(
                expr,
                &df_schema,
                state.execution_props(),
                &PhysicalPlanningContext::default(),
            )?);
        }

        let predicates = create_predicates(filters, &df_schema, state)?;

        Ok(Arc::new(MemUpdateExec::new(
            self.batches.clone(),
            Arc::clone(&self.sort_order),
            Arc::clone(&self.schema),
            set_exprs,
            predicates,
        )))
    }
}

/// Build one physical predicate for each expression of the `WHERE` clause.
///
/// The planner calls this, so a predicate that cannot be planned raises its
/// error while the plan is built and `EXPLAIN` reports it.
fn create_predicates(
    filters: &[Expr],
    df_schema: &DFSchema,
    state: &dyn Session,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    filters
        .iter()
        .map(|filter| {
            create_physical_expr(
                filter,
                df_schema,
                state.execution_props(),
                &PhysicalPlanningContext::default(),
            )
        })
        .collect()
}

/// Combine the predicates into one mask over the rows of `batch`. The mask is
/// true for a row that every predicate matches. `None` means there is no
/// `WHERE` clause, which matches every row.
fn evaluate_predicates(
    predicates: &[Arc<dyn PhysicalExpr>],
    batch: &RecordBatch,
) -> Result<Option<BooleanArray>> {
    let mut combined_mask: Option<BooleanArray> = None;

    for predicate in predicates {
        let result = predicate.evaluate(batch)?;
        let array = result.into_array(batch.num_rows())?;
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                internal_datafusion_err!("Filter did not evaluate to boolean")
            })?
            .clone();

        combined_mask = Some(match combined_mask {
            Some(existing) => and(&existing, &bool_array)?,
            None => bool_array,
        });
    }

    Ok(combined_mask)
}

/// Schema of the single `count` column that a DML plan emits.
fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Properties of a DML plan: one partition, one final batch.
fn dml_plan_properties(schema: &SchemaRef) -> Arc<PlanProperties> {
    Arc::new(PlanProperties::new(
        EquivalenceProperties::new(Arc::clone(schema)),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ))
}

/// A single row holding the number of rows the statement changed.
fn count_batch(schema: &SchemaRef, rows_affected: u64) -> Result<RecordBatch> {
    let count_array = UInt64Array::from(vec![rows_affected]);
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(count_array) as ArrayRef],
    )?)
}

/// Render the predicates as a comma separated list.
fn format_predicates(predicates: &[Arc<dyn PhysicalExpr>]) -> String {
    predicates
        .iter()
        .map(|predicate| predicate.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Deletes the matching rows of a [`MemTable`] when it runs, and emits the count.
///
/// The provider hook that builds this node changes no row, so `EXPLAIN DELETE`
/// prints the plan and the table keeps its rows. Each run of the plan applies
/// the delete once, as [`DataSinkExec`] does for an `INSERT`.
#[derive(Debug)]
struct MemDeleteExec {
    /// Partitions of the target table, shared with the [`MemTable`].
    partitions: Vec<PartitionData>,
    /// Declared sort order of the target table. A delete clears it.
    sort_order: Arc<Mutex<Vec<Vec<SortExpr>>>>,
    /// Predicates of the `WHERE` clause. An empty list matches every row.
    predicates: Vec<Arc<dyn PhysicalExpr>>,
    /// Single `count` column of the output.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl MemDeleteExec {
    fn new(
        partitions: Vec<PartitionData>,
        sort_order: Arc<Mutex<Vec<Vec<SortExpr>>>>,
        predicates: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let schema = dml_count_schema();
        let properties = dml_plan_properties(&schema);

        Self {
            partitions,
            sort_order,
            predicates,
            schema,
            properties,
        }
    }
}

impl DisplayAs for MemDeleteExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MemDeleteExec")?;
                if !self.predicates.is_empty() {
                    write!(f, ": predicate=[{}]", format_predicates(&self.predicates))?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for MemDeleteExec {
    fn name(&self) -> &str {
        "MemDeleteExec"
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "MemDeleteExec has one partition, but partition {partition} was requested"
            );
        }

        let partitions = self.partitions.clone();
        let sort_order = Arc::clone(&self.sort_order);
        let predicates = self.predicates.clone();
        let schema = Arc::clone(&self.schema);
        let count_schema = Arc::clone(&self.schema);

        let stream = futures::stream::once(async move {
            let rows_affected =
                delete_rows(&partitions, &sort_order, &predicates).await?;
            count_batch(&count_schema, rows_affected)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(&self.predicates, f)
    }
}

/// Delete the matching rows of every partition and return how many it removed.
async fn delete_rows(
    partitions: &[PartitionData],
    sort_order: &Mutex<Vec<Vec<SortExpr>>>,
    predicates: &[Arc<dyn PhysicalExpr>],
) -> Result<u64> {
    // The surviving rows hold no known order.
    *sort_order.lock() = vec![];

    let mut total_deleted: u64 = 0;

    for partition_data in partitions {
        let mut partition = partition_data.write().await;
        let mut new_batches = Vec::with_capacity(partition.len());

        for batch in partition.iter() {
            if batch.num_rows() == 0 {
                continue;
            }

            let (delete_count, keep_mask) = match evaluate_predicates(predicates, batch)?
            {
                Some(mask) => {
                    // Count the rows the mask selects, which are the rows to delete.
                    let count = mask.iter().filter(|v| v == &Some(true)).count();
                    // Keep the rows for which the predicate is false or NULL,
                    // which follows SQL three-valued logic.
                    let keep: BooleanArray =
                        mask.iter().map(|v| Some(v != Some(true))).collect();
                    (count, keep)
                }
                None => {
                    // No `WHERE` clause deletes every row.
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

/// Updates the matching rows of a [`MemTable`] when it runs, and emits the count.
///
/// The provider hook that builds this node changes no row, so `EXPLAIN UPDATE`
/// prints the plan and the table keeps its rows. Each run of the plan applies
/// the update once, as [`DataSinkExec`] does for an `INSERT`.
#[derive(Debug)]
struct MemUpdateExec {
    /// Partitions of the target table, shared with the [`MemTable`].
    partitions: Vec<PartitionData>,
    /// Declared sort order of the target table. An update clears it.
    sort_order: Arc<Mutex<Vec<Vec<SortExpr>>>>,
    /// Schema of the target table.
    table_schema: SchemaRef,
    /// One entry for each field of the target table, in field order. A `Some`
    /// entry holds the expression of the `SET` clause for that field.
    set_exprs: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Predicates of the `WHERE` clause. An empty list matches every row.
    predicates: Vec<Arc<dyn PhysicalExpr>>,
    /// Single `count` column of the output.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl MemUpdateExec {
    fn new(
        partitions: Vec<PartitionData>,
        sort_order: Arc<Mutex<Vec<Vec<SortExpr>>>>,
        table_schema: SchemaRef,
        set_exprs: Vec<Option<Arc<dyn PhysicalExpr>>>,
        predicates: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let schema = dml_count_schema();
        let properties = dml_plan_properties(&schema);

        Self {
            partitions,
            sort_order,
            table_schema,
            set_exprs,
            predicates,
            schema,
            properties,
        }
    }

    /// Render the `SET` clauses as `column=expression`, in field order.
    fn format_set_exprs(&self) -> String {
        self.table_schema
            .fields()
            .iter()
            .zip(&self.set_exprs)
            .filter_map(|(field, set_expr)| {
                set_expr
                    .as_ref()
                    .map(|expr| format!("{}={}", field.name(), expr))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl DisplayAs for MemUpdateExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MemUpdateExec: set=[{}]", self.format_set_exprs())?;
                if !self.predicates.is_empty() {
                    write!(f, ", predicate=[{}]", format_predicates(&self.predicates))?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for MemUpdateExec {
    fn name(&self) -> &str {
        "MemUpdateExec"
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
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "MemUpdateExec has one partition, but partition {partition} was requested"
            );
        }

        let partitions = self.partitions.clone();
        let sort_order = Arc::clone(&self.sort_order);
        let table_schema = Arc::clone(&self.table_schema);
        let set_exprs = self.set_exprs.clone();
        let predicates = self.predicates.clone();
        let schema = Arc::clone(&self.schema);
        let count_schema = Arc::clone(&self.schema);

        let stream = futures::stream::once(async move {
            let rows_affected = update_rows(
                &partitions,
                &sort_order,
                &table_schema,
                &set_exprs,
                &predicates,
            )
            .await?;
            count_batch(&count_schema, rows_affected)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        match apply_expression_roots(self.set_exprs.iter().flatten(), f)? {
            TreeNodeRecursion::Stop => Ok(TreeNodeRecursion::Stop),
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                apply_expression_roots(&self.predicates, f)
            }
        }
    }
}

/// Apply the `SET` clauses to the matching rows of every partition and return
/// how many rows changed.
async fn update_rows(
    partitions: &[PartitionData],
    sort_order: &Mutex<Vec<Vec<SortExpr>>>,
    table_schema: &SchemaRef,
    set_exprs: &[Option<Arc<dyn PhysicalExpr>>],
    predicates: &[Arc<dyn PhysicalExpr>],
) -> Result<u64> {
    // The new values hold no known order.
    *sort_order.lock() = vec![];

    let mut total_updated: u64 = 0;

    for partition_data in partitions {
        let mut partition = partition_data.write().await;
        let mut new_batches = Vec::with_capacity(partition.len());

        for batch in partition.iter() {
            if batch.num_rows() == 0 {
                continue;
            }

            let (update_count, update_mask) =
                match evaluate_predicates(predicates, batch)? {
                    Some(mask) => {
                        // Count the rows the mask selects, which are the rows to update.
                        let count = mask.iter().filter(|v| v == &Some(true)).count();
                        // Only true, never NULL, selects a row for update.
                        let normalized: BooleanArray =
                            mask.iter().map(|v| Some(v == Some(true))).collect();
                        (count, normalized)
                    }
                    None => {
                        // No `WHERE` clause updates every row.
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

            let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

            for (index, field) in table_schema.fields().iter().enumerate() {
                let column_name = field.name();
                let original_column =
                    batch.column_by_name(column_name).ok_or_else(|| {
                        internal_datafusion_err!(
                            "Column '{column_name}' not found in batch"
                        )
                    })?;

                let new_column = match set_exprs.get(index).and_then(|e| e.as_ref()) {
                    Some(set_expr) => {
                        // `evaluate_selection` evaluates the matching rows only,
                        // which keeps an error such as a divide by zero away from
                        // the rows the statement does not touch. It returns NULL
                        // for the other rows, and `zip` puts the originals back.
                        let new_values =
                            set_expr.evaluate_selection(batch, &update_mask)?;
                        let new_array = new_values.into_array(batch.num_rows())?;

                        // Convert to &dyn Array, which implements Datum
                        let new_arr: &dyn Array = new_array.as_ref();
                        let orig_arr: &dyn Array = original_column.as_ref();
                        zip(&update_mask, &new_arr, &orig_arr)?
                    }
                    None => Arc::clone(original_column),
                };

                new_columns.push(new_column);
            }

            let updated_batch =
                RecordBatch::try_new(Arc::clone(table_schema), new_columns)?;
            new_batches.push(updated_batch);
        }

        *partition = new_batches;
    }

    Ok(total_updated)
}
