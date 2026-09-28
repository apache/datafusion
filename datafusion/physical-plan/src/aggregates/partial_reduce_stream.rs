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

//! Partial-reduce hash aggregation stream implementation.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_datafusion_err, DataFusionError, Result};
use datafusion_execution::{async_try_stream, TaskContext, TryEmitter};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use futures::stream::{Stream, StreamExt};

use super::AggregateExec;
use super::aggregate_hash_table::{AggregateHashTable, PartialReduceMarker};
use crate::metrics::{BaselineMetrics, Count, MetricBuilder, RecordOutput, SpillMetrics};
use crate::stream::{EmptyRecordBatchStream, RecordBatchStreamAdapter};
use crate::{InputOrderMode, RecordBatchStream, SendableRecordBatchStream};

/// Hash aggregation can combine multiple partial stages before final
/// evaluation. This stream implements the partial-reduce stage.
///
/// # Example
///
/// SELECT k, AVG(v) FROM t GROUP BY k;
///
/// ## Plan
/// AggregateExec(stage=final)
/// -- RepartitionExec(hash(k))
/// ---- AggregateExec(stage=partial_reduce)
/// ------ RepartitionExec(hash(k))
/// -------- AggregateExec(stage=partial)
///
/// Note: the example plan is only intended to demonstrate this stream's semantics;
/// the default DataFusion SQL planner does not produce plans in this shape.
///
/// This stream implements the middle partial-reduce aggregation in the plan above.
///
/// The motivation is to reduce shuffling traffic in a distributed setting. See
/// <https://github.com/datafusion-contrib/datafusion-distributed/issues/360>
///
/// ## Partial-Reduce Stage Behavior
/// Input: partial aggregate state rows
/// Output: merged partial aggregate state rows
///
/// This stage is useful for tree-reduce plans. It consumes the same schema as
/// a final aggregate stage, but emits the same schema as a partial aggregate
/// stage.
///
/// # Memory Management
///
/// If the memory reservation cannot grow after aggregating an input batch, all
/// accumulated partial states are emitted immediately, and the remaining input
/// is aggregated with an empty table. This repeats until the input ends.
///
/// See [`crate::aggregates::AggregateMode::PartialReduce`] for why it's allowed
/// to emit the same group multiple times.
pub(crate) struct PartialReduceHashAggregateStream {
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing partial aggregate state rows.
    input: SendableRecordBatchStream,

    /// Target output batch size from configuration.
    batch_size: usize,

    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,

    /// Number of times accumulated states were emitted due to memory pressure.
    early_emit_count: Count,

    /// The hash table owns the lower-level state for emitting output batches.
    ///
    /// This will be None after the stream is created
    hash_table: Option<AggregateHashTable<PartialReduceMarker>>,
}

#[derive(PartialEq)]
enum HandleInputResult {
    ProcessNext,
    #[expect(clippy::upper_case_acronyms)]
    OOM,
}

impl PartialReduceHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::PartialReduce);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);
        let early_emit_count =
            MetricBuilder::new(&agg.metrics).counter("early_emit_count", partition);

        let hash_table = AggregateHashTable::<PartialReduceMarker>::new(
            agg,
            partition,
            Arc::clone(&schema),
            batch_size,
        )?;

        let reservation =
            MemoryConsumer::new(format!("PartialReduceHashAggregateStream[{partition}]"))
                // We interpret 'can spill' as 'can handle memory back pressure'.
                // This value needs to be set to true for the default memory pool implementations
                // to ensure fair application of back pressure amongst the memory consumers.
                .with_can_spill(true)
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            batch_size,
            baseline_metrics,
            reservation,
            early_emit_count,
            hash_table: Some(hash_table),
        })
    }

    fn close_input(&mut self) {
        let input_schema = self.input.schema();
        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
    }

    pub(crate) fn into_stream(self) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);

        Box::pin(RecordBatchStreamAdapter::new(schema, self.create_stream()))
    }

    /// Entry point for the partial reduce hash aggregate.
    fn create_stream(
        mut self,
    ) -> impl Stream<Item = Result<RecordBatch>> {
        async_try_stream(|mut emitter| async move {
            let mut hash_table: AggregateHashTable<PartialReduceMarker> = self.hash_table.take().expect("must have hash table");

            debug_assert!(hash_table.is_building());
            let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

            let mut last_state = HandleInputResult::ProcessNext;
            while let Some(batch) = self.input.next().await.transpose()? {
                let timer = elapsed_compute.timer();

                last_state = self.handle_input_batch(batch, &mut hash_table)?;

                match last_state {
                    HandleInputResult::ProcessNext => {}
                    HandleInputResult::OOM => {
                        let materialized_group_states = hash_table.take_state_batch()?.ok_or_else(|| {
                            internal_datafusion_err!(
                                "Partial reduce hash aggregate ran out of memory with no aggregated groups"
                            )
                        })?;

                        self.early_emit_count.add(1);
                        timer.done();
                        self.emit_on_memory_pressure(
                            materialized_group_states,
                            &mut emitter,
                            hash_table.memory_size(),
                        )
                          .await?;
                    }
                }
            }

            let timer = elapsed_compute.timer();

            self.close_input();
            hash_table.start_output()?;

            timer.done();

            self.produce_output(hash_table, emitter).await?;

            Ok(())
        })
    }


    /// Aggregate partial state batch into the hash table
    fn handle_input_batch(
        &mut self,
        batch: RecordBatch,
        hash_table: &mut AggregateHashTable<PartialReduceMarker>,
    ) -> Result<HandleInputResult> {
        debug_assert!(hash_table.is_building());
        hash_table.aggregate_batch(&batch)?;

        let resize_result = self.reservation.try_resize(hash_table.memory_size());
        match resize_result {
            Ok(()) => Ok(HandleInputResult::ProcessNext),
            Err(DataFusionError::ResourcesExhausted(_)) => Ok(HandleInputResult::OOM),
            Err(e) => Err(e),
        }
    }

    /// emit a materialized partial-state on memory pressure
    /// batch in `batch_size`(from configuration) slices
    ///
    /// # Implementation Note
    /// All accumulated states are materialized at once, and then sliced into
    /// `batch_size` output batches (in case we have enough memory to hold on them while slicing).
    /// Emit them incrementally after blocked state management is ready.
    ///
    /// Issue: <https://github.com/apache/datafusion/issues/7065>
    async fn emit_on_memory_pressure(
        &mut self,
        remaining_groups: RecordBatch,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
        hash_table_mem_size: usize,
    ) -> Result<()> {
        let remaining_groups_memory = remaining_groups.get_array_memory_size();

        // Emitting clears the aggregate table and releases its
        // accumulated memory. Update the reservation accordingly.
        // We account here for the remaining groups memory to see if we can return batch size states
        // if there is not enough memory, fallback to emit large batch
        match self
          .reservation
          .try_resize(hash_table_mem_size + remaining_groups_memory)
        {
            Ok(_) => {
                // Continue with slicing
            }
            Err(DataFusionError::ResourcesExhausted(_)) => {
                // Fail to reserve memory for the hash table + state batch while slicing so emit a huge batch

                // Try resize without holding the state batch, if it fails there is nothing we can do
                self.reservation.try_resize(hash_table_mem_size)?;

                emitter
                  .emit(remaining_groups.record_output(&self.baseline_metrics))
                  .await;

                return Ok(());
            }
            Err(e) => return Err(e),
        }

        let mut index = 0;

        while index + self.batch_size < remaining_groups.num_rows() {
            // More batch to output
            let output = remaining_groups.slice(index, index + self.batch_size);
            index += self.batch_size;

            emitter
              .emit(output.record_output(&self.baseline_metrics))
              .await;
        }

        let last_batch = remaining_groups.slice(index, remaining_groups.num_rows() - index);

        debug_assert!(last_batch.num_rows() > 0);
        debug_assert!(last_batch.num_rows() <= self.batch_size);

        // We are no longer holding on the batch while slicing, so release the memory.
        // The memory will now equal to the hash table size
        self.reservation.try_shrink(remaining_groups_memory)?;

        emitter
          .emit(remaining_groups.record_output(&self.baseline_metrics))
          .await;

        Ok(())
    }

    /// Emit merged partial aggregate state batches.
    async fn produce_output(
        &mut self,
        mut hash_table: AggregateHashTable<PartialReduceMarker>,
        mut emitter: TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<()> {
        debug_assert!(!hash_table.is_building());

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        let mut timer = elapsed_compute.timer();

        loop {
            let Some(batch) = hash_table.next_output_batch()? else {
                // Only reachable when the table held no groups at all: a
                // non-empty table always reports its last batch together with
                // the `Done` state, which the `try_resize` below already zeroes.
                self.reservation.try_resize(0)?;
                return Ok(());
            };

            debug_assert!(batch.num_rows() > 0);

            // The table hands over its groups as they are materialized and
            // reports a size of 0 once it reaches `Done`, so this releases the
            // reservation before the final batch goes downstream.
            // The output is already materialized, so a failed resize cannot
            // be acted on: keep the reservation as is and finish the output.
            let _ = self.reservation.try_resize(hash_table.memory_size());

            timer.done();
            emitter
              .emit(batch.record_output(&self.baseline_metrics))
              .await;
            timer = elapsed_compute.timer();
        };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;
    use arrow::array::{AsArray, Int32Array, Int64Array, RecordBatch};
    use arrow::datatypes::Int32Type;
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_execution::{SendableRecordBatchStream, TaskContext};
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use crate::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use crate::aggregates::hash_stream::PartialHashAggregateStream;
    use crate::ExecutionPlan;
    use crate::test::exec::BarrierExec;

    /// Builds a partial hash aggregate stream over a single input batch of
    /// `num_groups` distinct groups, running under `memory_limit` bytes.
    ///
    /// The input does not signal end-of-stream until `wait_finish` is called
    /// on the returned [`BarrierExec`], so any output produced before that can
    /// only come from the memory pressure emission path (normal output waits
    /// for all input). Skip partial aggregation is disabled for the same reason.
    fn partial_reduce_stream_under_memory_limit(
        memory_limit: usize,
        batch_size: usize,
        num_groups: usize,
    ) -> datafusion_common::Result<(
        SendableRecordBatchStream,
        Arc<BarrierExec>,
        Arc<datafusion_execution::runtime_env::RuntimeEnv>,
    )> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col_state", DataType::Int64, false),
        ]));

        let group_ids: Vec<i32> = (0..num_groups as i32).collect();
        let values: Vec<i64> = vec![1; num_groups];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        let input_partitions = vec![vec![batch]];

        let runtime = RuntimeEnvBuilder::default()
          .with_memory_limit(memory_limit, 1.0)
          .build_arc()?;

        let mut task_ctx = TaskContext::default().with_runtime(Arc::clone(&runtime));
        let session_config = task_ctx
          .session_config()
          .clone()
          .set(
              "datafusion.execution.batch_size",
              &datafusion_common::ScalarValue::UInt64(Some(batch_size as u64)),
          );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col_state", &schema)?])
              .schema(Arc::clone(&schema))
              .alias("count_value")
              .build()?,
        )];

        let input = Arc::new(
            BarrierExec::new(input_partitions, Arc::clone(&schema))
              .without_start_barrier()
              .with_finish_barrier()
              .with_log(false),
        );

        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::PartialReduce,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            Arc::clone(&schema),
        )?;

        let stream =
          PartialHashAggregateStream::new(&aggregate_exec, &task_ctx, 0)?.into_stream();

        Ok((stream, input, runtime))
    }

    #[tokio::test]
    async fn test_partial_reduce_hash_stream_accounts_held_batch_on_memory_pressure_while_slicing()
        -> datafusion_common::Result<()> {
        // When memory pressure triggers early emission, the materialized state
        // batch is held while it is sliced into `batch_size` outputs. The
        // stream must keep that held batch accounted for in its memory
        // reservation until the last slice is emitted; before the fix the
        // reservation was resized down to just the (emptied) hash table size,
        // leaving the held batch unaccounted.

        let batch_size = 1024;
        // One row per group so the state batch is emitted in 4 slices
        let num_groups = 4 * batch_size;

        // Smaller than the building hash table (so pressure triggers) but large
        // enough to hold the materialized state batch (so slicing can proceed)
        let memory_limit = 100 * 1024;
        let (mut stream, input, runtime) =
          partial_reduce_stream_under_memory_limit(memory_limit, batch_size, num_groups)?;

        // The first output batch must be a pressure-emitted slice, with the rest
        // of the materialized state batch still held by the stream
        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
          .await
          .expect(
              "did not get early emit due to OOM, this probably means that the \
                 memory limit is too high to trigger the OOM",
          )
          .expect("stream ended early")?;
        assert_eq!(first.num_rows(), batch_size);

        // The emitted slice shares buffers with the held state batch, so its
        // array memory size reflects the full held allocation
        let held_size = first.get_array_memory_size();
        let reserved = runtime.memory_pool.reserved();
        assert!(
            reserved >= held_size,
            "memory pool has {reserved} bytes reserved but the stream is \
             holding a materialized state batch of {held_size} bytes"
        );

        let second = stream.next().await.expect("stream ended early")?;
        assert_eq!(second.num_rows(), batch_size);

        // Make sure the state batch is really being sliced (and not emitted whole by the fallback path):
        // the second output must share the same underlying buffer as the first
        //
        // If you changed the code and this fail because
        // - you now deep copy `batch_size` from the full state batch, please update this assertion to something else
        // - you only take batch size from the hash table, you can remove the test
        assert_eq!(
            first
              .column(0)
              .as_primitive::<Int32Type>()
              .values()
              .inner()
              .data_ptr(),
            second
              .column(0)
              .as_primitive::<Int32Type>()
              .values()
              .inner()
              .data_ptr(),
            "both batches should be slices of the same materialized state batch"
        );

        // Let the input finish and drain the stream: no groups lost
        input.wait_finish().await;
        let mut total_rows = first.num_rows() + second.num_rows();
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert_eq!(total_rows, num_groups);

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_reduce_hash_stream_emits_whole_batch_when_held_batch_does_not_fit()
        -> datafusion_common::Result<()> {
        // When memory pressure triggers early emission but the materialized
        // state batch itself does not fit in the reservation, the stream must
        // not fail with a resources exhausted error. Instead it gives up on
        // slicing and emits the whole state batch at once.

        let batch_size = 1024;
        let num_groups = 4 * batch_size;

        // Smaller than the materialized state batch (4096 rows of Int32 group
        // keys plus Int64 counts is at least 48 KiB), so the reservation for
        // hash table  held batch fails. The emptied hash table itself is tiny
        // and still fits.
        let memory_limit = 32 * 1024;
        let (mut stream, input, runtime) =
          partial_reduce_stream_under_memory_limit(memory_limit, batch_size, num_groups)?;

        let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
          .await
          .expect(
              "did not get early emit due to OOM, this probably means that the \
                 memory limit is too high to trigger the OOM",
          )
          .expect("stream ended early")?;

        // The whole state batch is emitted at once instead of `batch_size` slices
        assert_eq!(first.num_rows(), num_groups);
        assert!(
            first.get_array_memory_size() > memory_limit,
            "test setup is wrong: the state batch fits within the memory limit, \
             so the slicing path would have been taken"
        );

        // Unlike the slicing path, the stream does not hold on to the emitted
        // batch, so it must not be accounted for in the reservation. Only the
        // (emptied) hash table remains reserved
        let emitted_size = first.get_array_memory_size();
        let reserved = runtime.memory_pool.reserved();
        assert!(
            reserved < emitted_size,
            "memory pool has {reserved} bytes reserved but the stream no longer \
             holds the emitted state batch of {emitted_size} bytes"
        );

        input.wait_finish().await;
        let mut total_rows = first.num_rows();
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert_eq!(total_rows, num_groups);

        Ok(())
    }

    #[tokio::test]
    async fn test_partial_reduce_hash_stream_releases_held_batch_after_last_slice() -> datafusion_common::Result<()>
    {
        // While the pressure-emitted state batch is sliced, the stream holds
        // the remaining groups and keeps them reserved. Once the last slice is
        // handed out nothing is held anymore, so the reservation must drop
        // back to just the (emptied) hash table before the input is resumed.

        let batch_size = 1024;
        let num_slices = 4;
        let num_groups = num_slices * batch_size;

        let memory_limit = 100 * 1024;
        let (mut stream, input, runtime) =
          partial_reduce_stream_under_memory_limit(memory_limit, batch_size, num_groups)?;

        // The input has not finished, so all of these are pressure-emitted slices
        let mut held_size = 0;
        for slice_idx in 0..num_slices {
            let slice = if slice_idx == 0 {
                tokio::time::timeout(Duration::from_secs(5), stream.next())
                  .await
                  .expect(
                      "did not get early emit due to OOM, this probably means that the \
                         memory limit is too high to trigger the OOM",
                  )
                  .expect("stream ended early")?
            } else {
                stream.next().await.expect("stream ended early")?
            };

            assert_eq!(slice.num_rows(), batch_size);

            // Every slice shares buffers with the held state batch, so this is
            // the size of the full held allocation
            held_size = slice.get_array_memory_size();
            let reserved = runtime.memory_pool.reserved();

            if slice_idx + 1 < num_slices {
                assert!(
                    reserved >= held_size,
                    "after slice {slice_idx} the stream still holds {held_size} \
                     bytes but only {reserved} bytes are reserved"
                );
            } else {
                assert!(
                    reserved < held_size,
                    "after the last slice nothing is held anymore but {reserved} \
                     bytes are still reserved (held batch was {held_size} bytes)"
                );
            }
        }
        assert!(held_size > 0);

        input.wait_finish().await;
        let mut total_rows = num_groups;
        while let Some(batch) = stream.next().await {
            total_rows += batch?.num_rows();
        }
        assert_eq!(total_rows, num_groups);

        Ok(())
    }
}
