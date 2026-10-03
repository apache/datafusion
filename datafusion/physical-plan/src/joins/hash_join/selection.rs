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

//! Private probe-side exchange. Selection messages never implement
//! `RecordBatchStream`: only the inner hash join may consume them.

use std::pin::Pin;
use std::sync::{Arc, Weak};

use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::take;
use arrow::record_batch::RecordBatch;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::{DataFusionError, Result, exec_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::repartition::REPARTITION_RANDOM_STATE;
use crate::{ExecutionPlan, ExecutionPlanProperties};

pub(super) type SelectionStream =
    Pin<Box<dyn Stream<Item = Result<SelectedBatch>> + Send>>;

/// One reservation for the shared payload, irrespective of fan-out.
#[derive(Debug)]
pub(super) struct SharedBatch {
    pub batch: RecordBatch,
    _reservation: MemoryReservation,
}

#[derive(Debug)]
pub(super) struct SelectedBatch {
    pub owner: Arc<SharedBatch>,
    pub indices: UInt32Array,
    /// Compact keys: lookup positions refer to these arrays, not the payload.
    pub values: Vec<ArrayRef>,
    _reservation: MemoryReservation,
}

type ReceiverSlot = Mutex<Option<mpsc::Receiver<Result<SelectedBatch>>>>;

struct Tasks {
    _tasks: Vec<SpawnedTask<()>>,
    receivers: Vec<Weak<ReceiverSlot>>,
}

impl Drop for Tasks {
    fn drop(&mut self) {
        // Also release queued messages for partitions never executed, even if
        // the caller retains the plan after canceling the query.
        for receiver in &self.receivers {
            if let Some(receiver) = receiver.upgrade() {
                receiver.lock().take();
            }
        }
    }
}

struct ExchangeState {
    receivers: Vec<Arc<ReceiverSlot>>,
    // Only consumers keep producers alive. Retaining the plan must not keep
    // tasks running after all consumers have been dropped.
    tasks: Weak<Tasks>,
}

#[derive(Default)]
pub(super) struct SelectionExchange {
    state: Mutex<Option<ExchangeState>>,
}

impl SelectionExchange {
    pub fn execute(
        self: &Arc<Self>,
        input: &dyn ExecutionPlan,
        keys: &[PhysicalExprRef],
        partitions: usize,
        partition: usize,
        context: &Arc<TaskContext>,
    ) -> Result<SelectionStream> {
        let (receiver, guard) = {
            let mut state = self.state.lock();
            let guard = if let Some(state) = state.as_ref() {
                state.tasks.upgrade().ok_or_else(|| {
                    DataFusionError::Execution(
                        "Selection exchange already stopped".into(),
                    )
                })?
            } else {
                let (senders, receivers): (Vec<_>, Vec<_>) =
                    (0..partitions).map(|_| mpsc::channel(1)).unzip();
                let mut tasks = Vec::new();
                for p in 0..input.output_partitioning().partition_count() {
                    let stream = input.execute(p, Arc::clone(context))?;
                    let senders = senders.clone();
                    let task = SpawnedTask::spawn(route_input(
                        stream,
                        keys.to_vec(),
                        senders.clone(),
                        Arc::clone(context),
                    ));
                    tasks.push(SpawnedTask::spawn(async move {
                        let error = match task.join().await {
                            Ok(Ok(())) => return,
                            Ok(Err(e)) => e,
                            Err(e) => DataFusionError::External(Box::new(e)),
                        };
                        let error = Arc::new(error);
                        // Send errors concurrently: an unpolled output must
                        // not prevent an active output observing the error.
                        futures::future::join_all(
                            senders
                                .iter()
                                .map(|tx| tx.send(Err(DataFusionError::from(&error)))),
                        )
                        .await;
                    }));
                }
                let receivers: Vec<_> = receivers
                    .into_iter()
                    .map(|r| Arc::new(Mutex::new(Some(r))))
                    .collect();
                let guard = Arc::new(Tasks {
                    _tasks: tasks,
                    receivers: receivers.iter().map(Arc::downgrade).collect(),
                });
                *state = Some(ExchangeState {
                    receivers,
                    tasks: Arc::downgrade(&guard),
                });
                guard
            };
            let receiver = state
                .as_mut()
                .unwrap()
                .receivers
                .get_mut(partition)
                .and_then(|receiver| receiver.lock().take())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Selection partition {partition} already consumed or out of range"
                    ))
                })?;
            (receiver, guard)
        };
        Ok(Box::pin(futures::stream::unfold(
            (receiver, guard),
            |(mut receiver, guard)| async move {
                receiver
                    .recv()
                    .await
                    .map(|batch| (batch, (receiver, guard)))
            },
        )))
    }
}

async fn route_input(
    mut input: crate::SendableRecordBatchStream,
    keys: Vec<PhysicalExprRef>,
    senders: Vec<mpsc::Sender<Result<SelectedBatch>>>,
    context: Arc<TaskContext>,
) -> Result<()> {
    let scratch =
        MemoryConsumer::new("HashJoinSelectionScratch").register(context.memory_pool());
    while let Some(batch) = input.next().await.transpose()? {
        if senders.iter().all(mpsc::Sender::is_closed) {
            break;
        }
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        if n > u32::MAX as usize {
            return exec_err!(
                "Selection exchange requires at most u32::MAX rows per batch"
            );
        }
        let reservation =
            MemoryConsumer::new("HashJoinSelectionBatch").register(context.memory_pool());
        reservation.try_grow(batch.get_array_memory_size())?;
        // Bound routing scratch before allocation (u64 hashes + u32 indices,
        // plus Vec headers). Vec growth is avoided by counting first.
        scratch.try_resize(n * 12 + senders.len() * (size_of::<Vec<u32>>() + 16))?;
        let values = evaluate_expressions_to_arrays(&keys, &batch)?;
        let mut hashes = vec![0; n];
        create_hashes(
            &values,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )?;
        let mut counts = vec![0usize; senders.len()];
        for hash in &hashes {
            counts[(*hash % senders.len() as u64) as usize] += 1;
        }
        let mut indices: Vec<Vec<u32>> =
            counts.iter().map(|&n| Vec::with_capacity(n)).collect();
        for (row, hash) in hashes.iter().enumerate() {
            indices[(*hash % senders.len() as u64) as usize].push(row as u32);
        }
        let owner = Arc::new(SharedBatch {
            batch,
            _reservation: reservation,
        });
        for (tx, rows) in senders.iter().zip(indices) {
            if rows.is_empty() || tx.is_closed() {
                continue;
            }
            let indices = UInt32Array::from(rows);
            let selected_values = values
                .iter()
                .map(|v| take(v.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let bytes = indices.get_array_memory_size()
                + selected_values
                    .iter()
                    .map(|v| v.get_array_memory_size())
                    .sum::<usize>();
            let reservation = MemoryConsumer::new("HashJoinSelectionIndices")
                .register(context.memory_pool());
            reservation.try_grow(bytes)?;
            // A full queue applies backpressure. A dropped consumer simply
            // relinquishes its partition; other consumers can still finish.
            tx.send(Ok(SelectedBatch {
                owner: Arc::clone(&owner),
                indices,
                values: selected_values,
                _reservation: reservation,
            }))
            .await
            .ok();
        }
        scratch.free();
    }
    Ok(())
}
