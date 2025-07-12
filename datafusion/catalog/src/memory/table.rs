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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::error::Result;
use datafusion_common::{not_impl_err, plan_err, Constraints, DFSchema, SchemaExt};
use datafusion_common_runtime::JoinSet;
use datafusion_datasource::memory::{MemSink, MemorySourceConfig};
use datafusion_datasource::sink::DataSinkExec;
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{Expr, SortExpr, TableType};
use datafusion_physical_expr::{create_physical_sort_exprs, LexOrdering};
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::{
    common, ExecutionPlan, ExecutionPlanProperties, Partitioning,
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
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;

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
}
