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

//! Skeleton operator that will eventually range-partition its input on a
//! single order-key into N output partitions, with halo overlap for
//! bounded RANGE-frame window functions sitting above it.
//!
//! Today it is a pass-through *with a coordinator*: the first call to
//! `execute()` spawns a single task that
//!  1. opens `child.execute(k)` for every input partition `k`,
//!  2. drives each stream to its first batch (which is enough to make
//!     pipeline-breaking sort children populate their `SortExtremes`
//!     slot),
//!  3. reads `child.runtime_sort_extremes(k)` per input,
//!  4. lex-reduces the per-input results into a single global
//!     [`SortExtremes`] and logs it,
//!  5. hands each input partition's `(first_batch, remaining_stream)`
//!     pair off to the corresponding output partition through a
//!     [`oneshot`] channel.
//!
//! Output partition `i` returns a stream that awaits its handoff and then
//! emits the buffered first batch followed by the remainder. So the
//! coordinator demonstrates the K-way fan-in machinery that real range
//! routing will need, without yet performing any actual routing.

use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::LexOrdering;
use futures::StreamExt;
use log::info;
use tokio::sync::oneshot;

use crate::sorts::sort::lex_compare;
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, SortExtremes,
};

#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
    state: Arc<Mutex<State>>,
}

struct State {
    initialized: bool,
    /// One `oneshot::Receiver` per output partition, populated when the
    /// coordinator hands off this partition's data. `take()`n by the
    /// corresponding `execute(partition)` call.
    handoffs: Vec<Option<oneshot::Receiver<Result<PartitionData>>>>,
}

/// Per-partition payload the coordinator publishes to its output stream.
/// `first_batch` is the batch we had to pull from the input stream to
/// drive the sort's pipeline-break; `rest` is the still-unconsumed
/// remainder of that input stream.
struct PartitionData {
    first_batch: Option<RecordBatch>,
    rest: SendableRecordBatchStream,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("initialized", &self.initialized)
            .field("handoffs", &self.handoffs.len())
            .finish()
    }
}

impl RangeRepartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let n = input.output_partitioning().partition_count();
        let cache = Arc::clone(input.properties());
        Self {
            input,
            cache,
            state: Arc::new(Mutex::new(State {
                initialized: false,
                handoffs: (0..n).map(|_| None).collect(),
            })),
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for RangeRepartitionExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RangeRepartitionExec")
    }
}

impl ExecutionPlan for RangeRepartitionExec {
    fn name(&self) -> &'static str {
        "RangeRepartitionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut state = self.state.lock().map_err(|_| {
            internal_datafusion_err!("RangeRepartitionExec mutex poisoned")
        })?;
        if !state.initialized {
            state.initialized = true;
            let n = state.handoffs.len();
            let mut senders = Vec::with_capacity(n);
            for slot in state.handoffs.iter_mut() {
                let (tx, rx) = oneshot::channel();
                senders.push(tx);
                *slot = Some(rx);
            }
            let child = Arc::clone(&self.input);
            let ctx = Arc::clone(&context);
            tokio::spawn(coordinator(child, ctx, senders));
        }
        let rx = state
            .handoffs
            .get_mut(partition)
            .and_then(Option::take)
            .ok_or_else(|| {
                internal_datafusion_err!("partition {partition} already taken")
            })?;
        drop(state);

        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            partition_stream(schema, rx),
        )))
    }
}

/// Stream that awaits the coordinator's handoff for one output partition,
/// then yields the buffered first batch followed by the remaining input
/// stream. If the handoff sender is dropped (coordinator failed) it
/// surfaces an error.
fn partition_stream(
    schema: SchemaRef,
    rx: oneshot::Receiver<Result<PartitionData>>,
) -> impl futures::Stream<Item = Result<RecordBatch>> + Send {
    use futures::stream::{TryStreamExt, once};
    once(async move {
        let data = rx
            .await
            .map_err(|_| internal_datafusion_err!("coordinator dropped"))??;
        let head = futures::stream::iter(data.first_batch.into_iter().map(Ok));
        let merged: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, head.chain(data.rest)));
        Ok::<_, datafusion_common::DataFusionError>(merged)
    })
    .try_flatten()
}

/// Coordinator task: drive every input partition to first batch, gather
/// runtime extremes, log the lex-reduced global, then hand off per-input
/// payloads to their corresponding output partition.
async fn coordinator(
    child: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    mut senders: Vec<oneshot::Sender<Result<PartitionData>>>,
) {
    let n = senders.len();

    // Phase 1: open every input stream and pull the first batch from each.
    let mut firsts: Vec<(Option<RecordBatch>, SendableRecordBatchStream)> =
        Vec::with_capacity(n);
    for k in 0..n {
        let mut stream = match child.execute(k, Arc::clone(&ctx)) {
            Ok(s) => s,
            Err(e) => {
                let msg = format!("input {k} open failed: {e}");
                for tx in senders.drain(..) {
                    let _ = tx.send(Err(internal_datafusion_err!("{msg}")));
                }
                return;
            }
        };
        let first = match stream.next().await {
            Some(Ok(batch)) => Some(batch),
            Some(Err(e)) => {
                let msg = format!("first batch from input {k} failed: {e}");
                for tx in senders.drain(..) {
                    let _ = tx.send(Err(internal_datafusion_err!("{msg}")));
                }
                return;
            }
            None => None,
        };
        firsts.push((first, stream));
    }

    // Phase 2: collect per-input runtime extremes.
    let per_input: Vec<Option<SortExtremes>> = (0..n)
        .map(|k| child.runtime_sort_extremes(k).ok().flatten())
        .collect();

    // Phase 3: lex-reduce per-input → global, using the input's declared
    // output ordering so direction and null ordering are honored.
    let ordering: Option<LexOrdering> = child.output_ordering().cloned();
    let global = ordering
        .as_ref()
        .and_then(|o| reduce_global_extremes(&per_input, o));

    info!(
        "RangeRepartitionExec: coordinator gathered {} input partitions; \
         global extremes = {:?}",
        n, global
    );

    // Phase 4: hand off each input's payload to its corresponding output
    // partition. (Today: pass-through; future: per-output route streams.)
    for (sender, (first_batch, rest)) in senders.into_iter().zip(firsts.into_iter()) {
        let _ = sender.send(Ok(PartitionData { first_batch, rest }));
    }
}

/// Lex-reduce per-input partition extremes into one global [`SortExtremes`]
/// honoring `ordering`'s direction / nulls-first per key. Returns `None`
/// when no input partition produced extremes (e.g. all inputs were empty,
/// or no upstream supports the trait method).
fn reduce_global_extremes(
    per_input: &[Option<SortExtremes>],
    ordering: &LexOrdering,
) -> Option<SortExtremes> {
    let mut iter = per_input.iter().filter_map(Option::clone);
    let mut global = iter.next()?;
    for next in iter {
        if lex_compare(&next.min, &global.min, ordering).is_lt() {
            global.min = next.min;
        }
        if lex_compare(&next.max, &global.max, ordering).is_gt() {
            global.max = next.max;
        }
        global.row_count += next.row_count;
    }
    Some(global)
}
