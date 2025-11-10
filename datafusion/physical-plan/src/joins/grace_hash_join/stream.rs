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

//! Stream implementation for Hash Join
//!
//! This module implements [`HashJoinStream`], the streaming engine for
//! [`super::HashJoinExec`]. See comments in [`HashJoinStream`] for more details.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::joins::utils::OnceFut;
use crate::{
    joins::utils::{BuildProbeJoinMetrics, ColumnIndex, JoinFilter},
    ExecutionPlan, RecordBatchStream, SendableRecordBatchStream, SpillManager,
};

use crate::empty::EmptyExec;
use crate::joins::grace_hash_join::exec::PartitionIndex;
use crate::joins::{HashJoinExec, PartitionMode};
use crate::test::TestMemoryExec;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExprRef;
use futures::{ready, Stream, StreamExt};
use tokio::sync::Mutex;

enum GraceJoinState {
    /// Waiting for the partitioning phase (Phase 1) to finish
    WaitPartitioning,

    WaitAllPartitions {
        wait_all_fut: Option<OnceFut<Vec<SpillFut>>>,
    },

    /// Currently joining partition `current`
    JoinPartition {
        current: usize,
        all_parts: Arc<Vec<SpillFut>>,
        current_stream: Option<SendableRecordBatchStream>,
        left_fut: Option<OnceFut<Vec<RecordBatch>>>,
        right_fut: Option<OnceFut<Vec<RecordBatch>>>,
    },

    Done,
}

pub struct GraceHashJoinStream {
    schema: SchemaRef,
    spill_fut: OnceFut<SpillFut>,
    spill_left: Arc<SpillManager>,
    spill_right: Arc<SpillManager>,
    on_left: Vec<PhysicalExprRef>,
    on_right: Vec<PhysicalExprRef>,
    projection: Option<Vec<usize>>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    column_indices: Vec<ColumnIndex>,
    join_metrics: Arc<BuildProbeJoinMetrics>,
    context: Arc<TaskContext>,
    accumulator: Arc<GraceAccumulator>,
    state: GraceJoinState,
}

#[derive(Debug, Clone)]
pub struct SpillFut {
    partition: usize,
    left: Vec<PartitionIndex>,
    right: Vec<PartitionIndex>,
}
impl SpillFut {
    pub(crate) fn new(
        partition: usize,
        left: Vec<PartitionIndex>,
        right: Vec<PartitionIndex>,
    ) -> Self {
        SpillFut {
            partition,
            left,
            right,
        }
    }
}

impl RecordBatchStream for GraceHashJoinStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl GraceHashJoinStream {
    pub fn new(
        schema: SchemaRef,
        spill_fut: OnceFut<SpillFut>,
        spill_left: Arc<SpillManager>,
        spill_right: Arc<SpillManager>,
        on_left: Vec<PhysicalExprRef>,
        on_right: Vec<PhysicalExprRef>,
        projection: Option<Vec<usize>>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        column_indices: Vec<ColumnIndex>,
        join_metrics: Arc<BuildProbeJoinMetrics>,
        context: Arc<TaskContext>,
        accumulator: Arc<GraceAccumulator>,
    ) -> Self {
        Self {
            schema,
            spill_fut,
            spill_left,
            spill_right,
            on_left,
            on_right,
            projection,
            filter,
            join_type,
            column_indices,
            join_metrics,
            context,
            accumulator,
            state: GraceJoinState::WaitPartitioning,
        }
    }

    /// Core state machine logic (poll implementation)
    fn poll_next_impl(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                GraceJoinState::WaitPartitioning => {
                    let shared = ready!(self.spill_fut.get_shared(cx))?;

                    let acc = Arc::clone(&self.accumulator);
                    let left = shared.left.clone();
                    let right = shared.right.clone();
                    // Use 0 partition as the main
                    let wait_all_fut = if shared.partition == 0 {
                        OnceFut::new(async move {
                            acc.report_partition(shared.partition, left, right).await;
                            let all = acc.wait_all().await;
                            Ok(all)
                        })
                    } else {
                        OnceFut::new(async move {
                            acc.report_partition(shared.partition, left, right).await;
                            acc.wait_ready().await;
                            Ok(vec![])
                        })
                    };
                    self.state = GraceJoinState::WaitAllPartitions {
                        wait_all_fut: Some(wait_all_fut),
                    };
                    continue;
                }
                GraceJoinState::WaitAllPartitions { wait_all_fut } => {
                    if let Some(fut) = wait_all_fut {
                        let all_arc = ready!(fut.get_shared(cx))?;
                        let mut all = (*all_arc).clone();
                        all.sort_by_key(|s| s.partition);

                        self.state = GraceJoinState::JoinPartition {
                            current: 0,
                            all_parts: Arc::from(all),
                            current_stream: None,
                            left_fut: None,
                            right_fut: None,
                        };
                        continue;
                    } else {
                        return Poll::Pending;
                    }
                }
                GraceJoinState::JoinPartition {
                    current,
                    all_parts,
                    current_stream,
                    left_fut,
                    right_fut,
                } => {
                    if *current >= all_parts.len() {
                        self.state = GraceJoinState::Done;
                        continue;
                    }

                    // If we don't have a stream yet, create one for the current partition pair
                    if current_stream.is_none() {
                        if left_fut.is_none() && right_fut.is_none() {
                            let spill_fut = &all_parts[*current];
                            *left_fut = Some(load_partition_async(
                                Arc::clone(&self.spill_left),
                                spill_fut.left.clone(),
                            ));
                            *right_fut = Some(load_partition_async(
                                Arc::clone(&self.spill_right),
                                spill_fut.right.clone(),
                            ));
                        }

                        let left_batches =
                            (*ready!(left_fut.as_mut().unwrap().get_shared(cx))?).clone();
                        let right_batches =
                            (*ready!(right_fut.as_mut().unwrap().get_shared(cx))?)
                                .clone();

                        let stream = build_in_memory_join_stream(
                            Arc::clone(&self.schema),
                            left_batches,
                            right_batches,
                            &self.on_left,
                            &self.on_right,
                            self.projection.clone(),
                            self.filter.clone(),
                            self.join_type,
                            &self.column_indices,
                            &self.join_metrics,
                            &self.context,
                        )?;

                        *current_stream = Some(stream);
                        *left_fut = None;
                        *right_fut = None;
                    }

                    // Drive current stream forward
                    if let Some(stream) = current_stream {
                        match ready!(stream.poll_next_unpin(cx)) {
                            Some(Ok(batch)) => return Poll::Ready(Some(Ok(batch))),
                            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                            None => {
                                *current += 1;
                                *current_stream = None;
                                continue;
                            }
                        }
                    }
                }
                GraceJoinState::Done => return Poll::Ready(None),
            }
        }
    }
}

fn load_partition_async(
    spill_manager: Arc<SpillManager>,
    partitions: Vec<PartitionIndex>,
) -> OnceFut<Vec<RecordBatch>> {
    OnceFut::new(async move {
        let mut all_batches = Vec::new();

        for p in partitions {
            for chunk in p.chunks {
                let mut reader = spill_manager.load_spilled_batch(&chunk)?;
                while let Some(batch_result) = reader.next().await {
                    let batch = batch_result?;
                    all_batches.push(batch);
                }
            }
        }
        Ok(all_batches)
    })
}

/// Build an in-memory HashJoinExec for one pair of spilled partitions
fn build_in_memory_join_stream(
    output_schema: SchemaRef,
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    on_left: &[PhysicalExprRef],
    on_right: &[PhysicalExprRef],
    projection: Option<Vec<usize>>,
    filter: Option<JoinFilter>,
    join_type: JoinType,
    _column_indices: &[ColumnIndex],
    _join_metrics: &BuildProbeJoinMetrics,
    context: &Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    if left_batches.is_empty() && right_batches.is_empty() {
        return EmptyExec::new(output_schema).execute(0, Arc::clone(context));
    }

    let left_schema = left_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));

    let right_schema = right_batches
        .first()
        .map(|b| b.schema())
        .unwrap_or_else(|| Arc::new(Schema::empty()));

    // Build memory execution nodes for each side
    let left_plan: Arc<dyn ExecutionPlan> =
        Arc::new(TestMemoryExec::try_new(&[left_batches], left_schema, None)?);
    let right_plan: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::try_new(
        &[right_batches],
        right_schema,
        None,
    )?);

    // Combine join expressions into pairs
    let on: Vec<(PhysicalExprRef, PhysicalExprRef)> = on_left
        .iter()
        .cloned()
        .zip(on_right.iter().cloned())
        .collect();

    // For one partition pair: always CollectLeft (build left, stream right)
    let join_exec = HashJoinExec::try_new(
        left_plan,
        right_plan,
        on,
        filter,
        &join_type,
        projection,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNothing,
    )?;

    // Each join executes locally with the same context
    join_exec.execute(0, Arc::clone(context))
}

impl Stream for GraceHashJoinStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

#[derive(Debug)]
pub struct GraceAccumulator {
    expected: usize,
    collected: Mutex<Vec<SpillFut>>,
    notify: tokio::sync::Notify,
}

impl GraceAccumulator {
    pub fn new(expected: usize) -> Arc<Self> {
        Arc::new(Self {
            expected,
            collected: Mutex::new(vec![]),
            notify: tokio::sync::Notify::new(),
        })
    }

    pub async fn report_partition(
        &self,
        part_id: usize,
        left_idx: Vec<PartitionIndex>,
        right_idx: Vec<PartitionIndex>,
    ) {
        let mut guard = self.collected.lock().await;
        if let Some(pos) = guard.iter().position(|s| s.partition == part_id) {
            guard[pos] = SpillFut::new(part_id, left_idx, right_idx);
        } else {
            guard.push(SpillFut::new(part_id, left_idx, right_idx));
        }

        if guard.len() == self.expected {
            self.notify.notify_waiters();
        }
    }

    pub async fn wait_all(&self) -> Vec<SpillFut> {
        loop {
            {
                let guard = self.collected.lock().await;
                if guard.len() == self.expected {
                    return guard.clone();
                }
            }
            self.notify.notified().await;
        }
    }
    pub async fn wait_ready(&self) {
        loop {
            {
                let guard = self.collected.lock().await;
                if guard.len() == self.expected {
                    return;
                }
            }
            self.notify.notified().await;
        }
    }
}
