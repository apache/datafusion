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

//! GroupJoin execution plan.
//!
//! Implements a fused hash-join + aggregation operator based on
//! "Accelerating Queries with Group-By and Join by Groupjoin"
//! (Moerkotte & Neumann, VLDB 2011).
//!
//! The operator builds a hash table on the right (build) side, then
//! for each left (probe) row, looks up matching right rows and feeds
//! them directly into group-by accumulators—avoiding materialization of
//! the potentially large intermediate join result.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use datafusion_common::hash_utils::{RandomState, create_hashes};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{NullEquality, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::StreamExt;

use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::row_hash::create_group_accumulator;
use crate::aggregates::{
    AggregateMode, AggregateOutputMode, PhysicalGroupBy, evaluate_group_by,
};
use crate::execution_plan::{CardinalityEffect, EmissionType, PlanProperties};
use crate::joins::hash_join::stream::lookup_join_hashmap;
use crate::joins::join_hash_map::{JoinHashMapU32, JoinHashMapU64};
use crate::joins::utils::{JoinHashMapType, update_hash};
use crate::joins::{JoinOn, Map, MapOffset};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    SendableRecordBatchStream,
};

/// GroupJoinExec fuses a hash join with a subsequent aggregation.
///
/// Instead of materializing the full join result and then grouping,
/// it builds a hash table on the right (build) side and, for each
/// left (probe) row, directly aggregates matching right-side values.
///
/// This is beneficial when the join has high fan-out (many right rows
/// per left row) but the final result is much smaller after aggregation.
#[derive(Debug)]
pub struct GroupJoinExec {
    /// Aggregation mode (Single or Partial)
    mode: AggregateMode,
    /// Left input (probe side) — group-by columns come from here
    left: Arc<dyn ExecutionPlan>,
    /// Right input (build side) — aggregated columns come from here
    right: Arc<dyn ExecutionPlan>,
    /// Equi-join keys: (left_expr, right_expr)
    on: JoinOn,
    /// Group-by expressions evaluated on the left (probe) input
    group_by: PhysicalGroupBy,
    /// Aggregate function expressions with arguments referencing the right (build) input
    aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
    /// Output schema (matches the original AggregateExec's output)
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cached plan properties
    cache: Arc<PlanProperties>,
}

impl GroupJoinExec {
    pub fn try_new(
        mode: AggregateMode,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        group_by_exprs: Vec<(PhysicalExprRef, String)>,
        aggr_expr: Vec<Arc<AggregateFunctionExpr>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let group_by = PhysicalGroupBy::new_single(group_by_exprs);
        let cache = Arc::new(Self::compute_properties(
            left.properties(),
            Arc::clone(&schema),
        ));

        Ok(Self {
            mode,
            left,
            right,
            on,
            group_by,
            aggr_expr,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        })
    }

    fn compute_properties(
        left_props: &PlanProperties,
        schema: SchemaRef,
    ) -> PlanProperties {
        use datafusion_physical_expr::equivalence::EquivalenceProperties;
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            left_props.output_partitioning().clone(),
            EmissionType::Final,
            left_props.boundedness,
        )
    }
}

impl DisplayAs for GroupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let on = self
                    .on
                    .iter()
                    .map(|(l, r)| format!("({l}, {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let group_by: Vec<_> = self
                    .group_by
                    .expr()
                    .iter()
                    .map(|(e, alias)| format!("{e} as {alias}"))
                    .collect();
                let aggrs: Vec<_> = self
                    .aggr_expr
                    .iter()
                    .map(|e| e.name().to_string())
                    .collect();
                write!(
                    f,
                    "GroupJoinExec: mode={:?}, on=[{}], group_by=[{}], aggr=[{}]",
                    self.mode,
                    on,
                    group_by.join(", "),
                    aggrs.join(", "),
                )
            }
            DisplayFormatType::TreeRender => {
                let on = self
                    .on
                    .iter()
                    .map(|(l, r)| format!("({l} = {r})"))
                    .collect::<Vec<_>>()
                    .join(", ");
                writeln!(f, "on={on}")?;
                let group_by: Vec<_> = self
                    .group_by
                    .expr()
                    .iter()
                    .map(|(e, alias)| format!("{e} as {alias}"))
                    .collect();
                writeln!(f, "group_by=[{}]", group_by.join(", "))?;
                let aggrs: Vec<_> = self
                    .aggr_expr
                    .iter()
                    .map(|e| e.name().to_string())
                    .collect();
                writeln!(f, "aggr=[{}]", aggrs.join(", "))?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for GroupJoinExec {
    fn name(&self) -> &'static str {
        "GroupJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for (left, right) in &self.on {
            tnr = tnr.visit_sibling(|| f(left.as_ref()))?;
            tnr = tnr.visit_sibling(|| f(right.as_ref()))?;
        }
        for (expr, _) in self.group_by.expr() {
            tnr = tnr.visit_sibling(|| f(expr.as_ref()))?;
        }
        for agg in &self.aggr_expr {
            for arg in agg.expressions() {
                tnr = tnr.visit_sibling(|| f(arg.as_ref()))?;
            }
        }
        Ok(tnr)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(GroupJoinExec::try_new(
            self.mode,
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.on.clone(),
            self.group_by.expr().to_vec(),
            self.aggr_expr.clone(),
            Arc::clone(&self.schema),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let on_right: Vec<PhysicalExprRef> =
            self.on.iter().map(|on| Arc::clone(&on.1)).collect();
        let on_left: Vec<PhysicalExprRef> =
            self.on.iter().map(|on| Arc::clone(&on.0)).collect();

        // Collect ALL right-side partitions so any left-side partitioning is safe,
        // mirroring HashJoinExec(CollectLeft) which collects the full build side.
        let right_part_count = self.right.output_partitioning().partition_count();
        let right_streams: Vec<SendableRecordBatchStream> = (0..right_part_count)
            .map(|p| self.right.execute(p, Arc::clone(&context)))
            .collect::<Result<_>>()?;

        let left_stream = self.left.execute(partition, Arc::clone(&context))?;

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let accumulators: Vec<Box<dyn GroupsAccumulator>> = self
            .aggr_expr
            .iter()
            .map(create_group_accumulator)
            .collect::<Result<_>>()?;

        let aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>> =
            self.aggr_expr.iter().map(|agg| agg.expressions()).collect();

        let group_by = self.group_by.clone();
        let mode = self.mode;
        let schema = Arc::clone(&self.schema);
        let batch_size = context.session_config().batch_size();

        let group_schema = group_by.group_schema(&self.left.schema())?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;

        let reservation =
            MemoryConsumer::new("GroupJoinExec").register(context.memory_pool());

        let stream = GroupJoinStream {
            schema: Arc::clone(&schema),
            on_left,
            on_right,
            right_streams,
            left_stream: Some(left_stream),
            mode,
            group_by,
            aggregate_arguments,
            accumulators,
            group_values,
            current_group_indices: Vec::new(),
            batch_size,
            baseline_metrics,
            state: GroupJoinStreamState::BuildRight,
            right_data: None,
            reservation,
            random_state: RandomState::with_seed(0),
            hashes_buffer: Vec::new(),
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::unfold(stream, |mut stream| async move {
                match stream.next_batch().await {
                    Ok(Some(batch)) => Some((Ok(batch), stream)),
                    Ok(None) => None,
                    Err(e) => Some((Err(e), stream)),
                }
            }),
        )))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }
}

enum GroupJoinStreamState {
    BuildRight,
    ProbeAndAggregate,
    Emit,
    Done,
}

struct RightData {
    map: Arc<Map>,
    batch: RecordBatch,
    values: Vec<ArrayRef>,
}

struct GroupJoinStream {
    schema: SchemaRef,
    on_left: Vec<PhysicalExprRef>,
    on_right: Vec<PhysicalExprRef>,

    /// All right-side partition streams (collected exhaustively to build the hash table)
    right_streams: Vec<SendableRecordBatchStream>,
    left_stream: Option<SendableRecordBatchStream>,

    mode: AggregateMode,
    group_by: PhysicalGroupBy,
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    accumulators: Vec<Box<dyn GroupsAccumulator>>,
    group_values: Box<dyn GroupValues>,
    current_group_indices: Vec<usize>,

    batch_size: usize,
    #[expect(dead_code)]
    baseline_metrics: BaselineMetrics,
    state: GroupJoinStreamState,
    right_data: Option<RightData>,
    reservation: MemoryReservation,
    random_state: RandomState,
    hashes_buffer: Vec<u64>,
}

impl GroupJoinStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            match &self.state {
                GroupJoinStreamState::BuildRight => {
                    self.build_right_side().await?;
                    self.state = GroupJoinStreamState::ProbeAndAggregate;
                }
                GroupJoinStreamState::ProbeAndAggregate => {
                    let left_stream = self.left_stream.as_mut().unwrap();
                    match left_stream.next().await {
                        Some(Ok(batch)) => {
                            self.process_left_batch(&batch)?;
                        }
                        Some(Err(e)) => return Err(e),
                        None => {
                            self.state = GroupJoinStreamState::Emit;
                        }
                    }
                }
                GroupJoinStreamState::Emit => {
                    let result = self.emit_results()?;
                    self.state = GroupJoinStreamState::Done;
                    if result.num_rows() > 0 {
                        return Ok(Some(result));
                    }
                    return Ok(None);
                }
                GroupJoinStreamState::Done => return Ok(None),
            }
        }
    }

    async fn build_right_side(&mut self) -> Result<()> {
        let right_streams = std::mem::take(&mut self.right_streams);
        if right_streams.is_empty() {
            return datafusion_common::internal_err!("Right streams already consumed");
        }

        let right_schema = right_streams[0].schema();
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut num_rows: usize = 0;

        // Drain all right-side partitions into a single batch collection.
        for mut stream in right_streams {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let batch_mem = get_record_batch_memory_size(&batch);
                self.reservation.try_grow(batch_mem)?;
                num_rows += batch.num_rows();
                batches.push(batch);
            }
        }

        if num_rows == 0 {
            self.right_data = Some(RightData {
                map: Arc::new(Map::HashMap(Box::new(JoinHashMapU32::with_capacity(0)))),
                batch: RecordBatch::new_empty(right_schema),
                values: vec![],
            });
            return Ok(());
        }

        let mut hashmap: Box<dyn JoinHashMapType> = if num_rows > u32::MAX as usize {
            Box::new(JoinHashMapU64::with_capacity(num_rows))
        } else {
            Box::new(JoinHashMapU32::with_capacity(num_rows))
        };

        let mut hash_buf = Vec::new();
        let mut offset = 0;

        for batch in batches.iter().rev() {
            hash_buf.clear();
            hash_buf.resize(batch.num_rows(), 0);
            update_hash(
                &self.on_right,
                batch,
                &mut *hashmap,
                offset,
                &self.random_state,
                &mut hash_buf,
                0,
                true,
            )?;
            offset += batch.num_rows();
        }

        let batch = compute::concat_batches(&right_schema, batches.iter().rev())?;
        let values = evaluate_expressions_to_arrays(&self.on_right, &batch)?;

        self.right_data = Some(RightData {
            map: Arc::new(Map::HashMap(hashmap)),
            batch,
            values,
        });

        Ok(())
    }

    fn process_left_batch(&mut self, left_batch: &RecordBatch) -> Result<()> {
        let right_data = self.right_data.as_ref().ok_or_else(|| {
            datafusion_common::DataFusionError::Internal(
                "Right data not built".to_string(),
            )
        })?;

        if right_data.map.is_empty() {
            return Ok(());
        }

        let left_key_values = evaluate_expressions_to_arrays(&self.on_left, left_batch)?;

        self.hashes_buffer.clear();
        self.hashes_buffer.resize(left_batch.num_rows(), 0);
        create_hashes(
            &left_key_values,
            &self.random_state,
            &mut self.hashes_buffer,
        )?;

        let mut probe_indices_buf: Vec<u32> = Vec::new();
        let mut build_indices_buf: Vec<u64> = Vec::new();
        let mut offset: Option<MapOffset> = Some((0, None));

        // Pre-evaluate group-by and aggregate expressions once per batch
        let group_by_values = evaluate_group_by(&self.group_by, left_batch)?;

        // Pre-evaluate aggregate arguments on the right batch
        let right_arg_arrays: Vec<Vec<ArrayRef>> = self
            .aggregate_arguments
            .iter()
            .map(|arg_exprs| {
                arg_exprs
                    .iter()
                    .map(|expr| {
                        expr.evaluate(&right_data.batch)
                            .and_then(|v| v.into_array(right_data.batch.num_rows()))
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<_>>()?;

        while let Some(current_offset) = offset {
            probe_indices_buf.clear();
            build_indices_buf.clear();

            let (build_indices, probe_indices, next_offset) =
                match right_data.map.as_ref() {
                    Map::HashMap(map) => lookup_join_hashmap(
                        map.as_ref(),
                        &right_data.values,
                        &left_key_values,
                        NullEquality::NullEqualsNothing,
                        &self.hashes_buffer,
                        self.batch_size,
                        current_offset,
                        &mut probe_indices_buf,
                        &mut build_indices_buf,
                    )?,
                    Map::ArrayMap(array_map) => {
                        let next = array_map.get_matched_indices_with_limit_offset(
                            &left_key_values,
                            self.batch_size,
                            current_offset,
                            &mut probe_indices_buf,
                            &mut build_indices_buf,
                        )?;
                        (
                            UInt64Array::from(std::mem::take(&mut build_indices_buf)),
                            UInt32Array::from(std::mem::take(&mut probe_indices_buf)),
                            next,
                        )
                    }
                };

            if build_indices.is_empty() {
                offset = next_offset;
                if offset.is_none() {
                    break;
                }
                continue;
            }

            for group_values_arr in &group_by_values {
                // Take group values at matched probe positions
                let matched_group_values: Vec<ArrayRef> = group_values_arr
                    .iter()
                    .map(|arr| compute::take(arr.as_ref(), &probe_indices, None))
                    .collect::<std::result::Result<_, _>>()?;

                self.group_values
                    .intern(&matched_group_values, &mut self.current_group_indices)?;
                let total_num_groups = self.group_values.len();

                // Feed matched right-side values to accumulators
                for (acc_idx, acc) in self.accumulators.iter_mut().enumerate() {
                    let values: Vec<ArrayRef> = right_arg_arrays[acc_idx]
                        .iter()
                        .map(|arr| compute::take(arr.as_ref(), &build_indices, None))
                        .collect::<std::result::Result<_, _>>()?;

                    acc.update_batch(
                        &values,
                        &self.current_group_indices,
                        None,
                        total_num_groups,
                    )?;
                }
            }

            offset = next_offset;
        }

        Ok(())
    }

    fn emit_results(&mut self) -> Result<RecordBatch> {
        let group_columns = self.group_values.emit(EmitTo::All)?;

        let agg_columns: Vec<ArrayRef> = match self.mode.output_mode() {
            AggregateOutputMode::Final => self
                .accumulators
                .iter_mut()
                .map(|acc| acc.evaluate(EmitTo::All))
                .collect::<Result<Vec<_>>>()?,
            AggregateOutputMode::Partial => self
                .accumulators
                .iter_mut()
                .flat_map(|acc| match acc.state(EmitTo::All) {
                    Ok(states) => states.into_iter().map(Ok).collect::<Vec<_>>(),
                    Err(e) => vec![Err(e)],
                })
                .collect::<Result<Vec<_>>>()?,
        };

        let mut columns = group_columns;
        columns.extend(agg_columns);

        if columns.is_empty() || columns[0].is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        Ok(RecordBatch::try_new(Arc::clone(&self.schema), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::Column;

    use crate::test::TestMemoryExec;

    fn build_task_context() -> Arc<TaskContext> {
        let runtime = RuntimeEnvBuilder::new().build_arc().unwrap();
        let config = SessionConfig::new().with_batch_size(4096);
        Arc::new(TaskContext::new(
            None,
            "test_session".to_string(),
            config,
            Default::default(),
            Default::default(),
            Default::default(),
            runtime,
        ))
    }

    #[tokio::test]
    async fn test_group_join_basic() -> Result<()> {
        // Left table: orders (order_id, customer_id)
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("customer_id", DataType::Int32, false),
        ]));
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int32Array::from(vec![10, 10, 20, 20])),
            ],
        )?;

        // Right table: line_items (order_id, amount)
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 3, 3, 3, 4])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600, 700])),
            ],
        )?;

        let left_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?);
        let right_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?);

        // GROUP BY customer_id, SUM(amount)
        // Join on order_id
        let on: JoinOn = vec![(
            Arc::new(Column::new("order_id", 0)),
            Arc::new(Column::new("order_id", 0)),
        )];

        let group_by_exprs = vec![(
            Arc::new(Column::new("customer_id", 1)) as PhysicalExprRef,
            "customer_id".to_string(),
        )];

        // SUM(amount) - amount is column 1 in the right schema
        let sum_udf = datafusion_functions_aggregate::sum::sum_udaf();
        let sum_expr = Arc::new(
            AggregateExprBuilder::new(sum_udf, vec![Arc::new(Column::new("amount", 1))])
                .schema(Arc::clone(&right_schema))
                .alias("sum_amount")
                .build()?,
        );

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int32, false),
            Field::new("sum_amount", DataType::Int64, true),
        ]));

        let group_join = GroupJoinExec::try_new(
            AggregateMode::Single,
            left_exec,
            right_exec,
            on,
            group_by_exprs,
            vec![sum_expr],
            output_schema,
        )?;

        let context = build_task_context();
        let mut stream = group_join.execute(0, context)?;

        let mut results: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.num_rows(), 2);

        // Collect results into a map for order-independent comparison
        let customer_ids = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let sums = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut result_map = std::collections::HashMap::new();
        for i in 0..result.num_rows() {
            result_map.insert(customer_ids.value(i), sums.value(i));
        }

        // Customer 10 has orders 1, 2:
        //   order 1: amounts 100 + 200 = 300
        //   order 2: amount 300
        //   total = 600
        assert_eq!(result_map[&10], 600);

        // Customer 20 has orders 3, 4:
        //   order 3: amounts 400 + 500 + 600 = 1500
        //   order 4: amount 700
        //   total = 2200
        assert_eq!(result_map[&20], 2200);

        Ok(())
    }

    #[tokio::test]
    async fn test_group_join_empty_right() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]));
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![10, 20])),
            ],
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        // Empty right side
        let right_batch = RecordBatch::new_empty(Arc::clone(&right_schema));

        let left_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?);
        let right_exec = Arc::new(TestMemoryExec::try_new(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?);

        let on: JoinOn = vec![(
            Arc::new(Column::new("key", 0)),
            Arc::new(Column::new("key", 0)),
        )];

        let group_by_exprs = vec![(
            Arc::new(Column::new("val", 1)) as PhysicalExprRef,
            "val".to_string(),
        )];

        let sum_udf = datafusion_functions_aggregate::sum::sum_udaf();
        let sum_expr = Arc::new(
            AggregateExprBuilder::new(sum_udf, vec![Arc::new(Column::new("amount", 1))])
                .schema(Arc::clone(&right_schema))
                .alias("sum_amount")
                .build()?,
        );

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Int32, false),
            Field::new("sum_amount", DataType::Int64, true),
        ]));

        let group_join = GroupJoinExec::try_new(
            AggregateMode::Single,
            left_exec,
            right_exec,
            on,
            group_by_exprs,
            vec![sum_expr],
            output_schema,
        )?;

        let context = build_task_context();
        let mut stream = group_join.execute(0, context)?;

        let mut results: Vec<RecordBatch> = Vec::new();
        while let Some(batch) = stream.next().await {
            results.push(batch?);
        }

        // INNER join with empty right → no results
        assert!(results.is_empty());

        Ok(())
    }
}
