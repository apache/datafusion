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

//! Execution plans for `WITH x AS MATERIALIZED (...)`.
//!
//! [`MaterializedCteExec`] owns the CTE body and the continuation (the rest of
//! the query). Every [`MaterializedCteScanExec`] in the continuation shares one
//! [`MaterializedCteBuffer`] with it. The body runs one time for each run of
//! the plan, and its output streams to every scan.
//!
//! # Streaming
//!
//! Each body partition has a fan-out: a log of batches, and one cursor for
//! each scan that reads the partition. Partition `p` of a scan with `n`
//! partitions reads the body partitions `p, p + n, ...` at the same time, and
//! interleaves their batches. The scan has no output ordering, so this does
//! not change its properties. The first scan partition that is executed in a
//! run executes every body partition. When a scan first polls a body
//! partition, a task starts for it. The task pulls the partition and appends
//! to its log. A scan reads the log at its cursor. The log drops the batches
//! that every scan has read.
//!
//! The task appends a batch only when the fastest scan that reads the log is
//! less than a few batches behind its end. When no scan reads the log yet, the
//! task reads ahead the same few batches. Thus when all scans read at the same
//! rate, only a few batches are in memory.
//!
//! The gate looks at the fastest scan, not the slowest, so it cannot deadlock.
//! For example, two scans are the build and the probe side of one hash join.
//! The join reads the build side to its end before it polls the probe side.
//! A scan waits only when it is at the end of the log. Then it is the fastest
//! scan, so the gate is open, and the task appends for it. The task waits
//! only when every scan that reads has batches to read, so each of these
//! scans can continue without the task. A scan that is slow, or that is not
//! polled, does not stop the task. Its batches stay in the log until it reads
//! them. A gate on the slowest scan, as in a bounded broadcast channel, would
//! deadlock here.
//!
//! A body can couple its partitions. For example, its outputs share one
//! input through a bounded channel for each output, so a full channel to
//! partition 1 stops partition 0. `RepartitionExec` does not do this, but the
//! operator contract allows it. A scan partition polls all of its body
//! partitions, so when it waits, it waits at the head of each of them, and
//! each task appends. Thus such a body completes when each of its partitions
//! is read by a scan partition that is polled. It can still wait forever if
//! a scan partition is never executed or never polled (for example after a
//! cancellation, or when a consumer polls one scan partition only after
//! another one ended) and no other scan reads its body partitions. It can
//! also wait forever when a scan partition computes the body again (see
//! below): that run executes only the body partitions that the scan
//! partition reads. The design that buffered the whole body did not have
//! this limit. When every reader of a body partition has finished, its
//! fan-out drops the body stream, also a stream that no reader started, so
//! that it does not stop the other partitions.
//!
//! The log is accounted in the memory pool. The logs of one CTE keep at most
//! half of a bounded pool in memory, so that the other operators keep memory.
//! Two CTEs can take the whole pool. When a log cannot grow, its in-memory
//! tail is written to a spill file, and the next batches go to the same file
//! until it is large, or until a scan needs to read it. The task writes
//! without the lock of the log, so scans can read meanwhile. A scan reads a
//! spill file one piece at a time, with a reservation for one decoded piece.
//! A batch larger than 2 MiB is written in pieces, so that this reservation is
//! small. A spill file is deleted when every scan has read it.
//!
//! A body is executed with its own [`TaskContext`], whose [`RunState`] keeps
//! the outputs of the run weakly. The body stream is owned by a task that is
//! in the outputs, so a strong reference would keep the run alive. Thus the
//! run is released when the caller drops its streams, also when a task waits
//! at its gate for a scan partition that is never executed, and also when a
//! body operator keeps its context in its stream.
//!
//! The output of a run is not kept in the plan. It is kept in the
//! [`RunState`] of the [`TaskContext`], so each run of a plan has its own
//! output, and two runs of one plan that overlap do not share it. All
//! `execute` calls of one run must get the same [`TaskContext`]. With a
//! different [`TaskContext`] for each call, each call computes the body again.
//!
//! The output of a run is released when no scan can read it again: when each
//! partition of each bound scan was executed in the run, and each of these
//! executions has finished or was dropped. See `RunOutput`. Then the tasks of
//! the body stop. Thus a scan below a `LIMIT` that is the only reader stops the
//! body. Until the release, the [`RunState`] holds the output. A scan
//! partition that is never executed, or that has not reached a body
//! partition, keeps the whole log of that body partition.
//!
//! A scan partition that is executed again in a run reads the log from the
//! start if no batch was dropped. Else it computes the body again, as a second
//! run of the plan with the same [`TaskContext`] does. An operator that
//! executes a scan more than one time in a run must hold the output. See
//! `hold_materialized_cte_outputs`. A hold keeps every batch.
//!
//! The plan nodes keep no state of a run, so the default
//! [`ExecutionPlan::reset_state`] is correct for them: it returns a new node
//! and the old node stays usable.
//!
//! `RecursiveQueryExec` executes its recursive term one time for each
//! iteration, with the same [`TaskContext`]. It gives each iteration new
//! outputs for the CTEs that the recursive term declares. See
//! `with_new_materialized_cte_outputs`. Its recursive term can also read a
//! CTE that is declared outside it, one time for each iteration. It holds the
//! output of such a CTE until its last iteration. See
//! `hold_materialized_cte_outputs`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_execution::{RunState, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::coop::cooperative;
use crate::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, SchedulingType,
};
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ReplaceChildrenOptions,
};
use fanout::{Fanout, FanoutMetrics, FanoutOptions, FanoutReader};

mod fanout;

/// A copy of `context` for a body that a fan-out executes. The scans in the
/// body keep the outputs of the run weakly. See [`OutputsRef`].
fn internal_context(context: &Arc<TaskContext>) -> Arc<TaskContext> {
    let run = context.run_state().get_or_init::<MaterializedCteOutputs>();
    let outputs = MaterializedCteOutputs {
        weak_parent: Some(Arc::downgrade(&run)),
        internal: true,
        ..Default::default()
    };
    // A new `RunState`, not a child of the run's state: a body stream can
    // keep this context, and a strong reference to the run would keep the
    // run alive. Only materialized CTEs keep state in a `RunState`.
    let run_state = RunState::new_child(Arc::new(RunState::new()), Arc::new(outputs));
    Arc::new(context.as_ref().clone().with_run_state(Arc::new(run_state)))
}

/// The output of one materialized CTE in one run: one [`Fanout`] for each
/// body partition.
///
/// Reader slot `s` of each fan-out is scan `s`. Partition `p` of a scan with
/// `n` partitions reads the body partitions `p, p + n, ...` one after the
/// other, so exactly one partition of each scan reads each body partition.
struct CteOutput {
    options: FanoutOptions,
    /// One fan-out for each body partition, or `None` for a body partition
    /// that this output does not compute.
    fanouts: Vec<Option<Arc<Fanout>>>,
}

impl CteOutput {
    /// A new output that executes the body `partitions`, or every body
    /// partition. It keeps no reference to `context`, because it is in the
    /// [`RunState`] of `context`.
    fn new(
        options: FanoutOptions,
        context: &Arc<TaskContext>,
        partitions: Option<&[usize]>,
    ) -> Self {
        let metrics = FanoutMetrics::new(&options.metrics);
        let count = options.body.output_partitioning().partition_count();
        let context = &internal_context(context);
        let fanouts = (0..count)
            .map(|partition| {
                partitions
                    .is_none_or(|p| p.contains(&partition))
                    .then(|| Fanout::new(&options, partition, context, metrics.clone()))
            })
            .collect();
        Self { options, fanouts }
    }

    /// A new output of the same body, for one reader of the body
    /// `partitions`. A scan partition that is executed again after the
    /// batches it needs were retired reads it.
    fn recompute(&self, context: &Arc<TaskContext>, partitions: &[usize]) -> Self {
        let options = FanoutOptions {
            name: self.options.name.clone(),
            body: Arc::clone(&self.options.body),
            scans: 1,
            metrics: self.options.metrics.clone(),
        };
        Self::new(options, context, Some(partitions))
    }

    /// Register a reader of body partition `partition` for scan `scan`.
    fn reader(&self, scan: usize, partition: usize) -> Option<FanoutReader> {
        self.fanouts.get(partition)?.as_ref()?.open(scan)
    }

    fn each(&self) -> impl Iterator<Item = &Arc<Fanout>> {
        self.fanouts.iter().flatten()
    }

    fn add_hold(&self) {
        self.each().for_each(|f| f.add_hold());
    }

    fn remove_hold(&self) {
        self.each().for_each(|f| f.remove_hold());
    }

    fn shut_down(&self) {
        self.each().for_each(|f| f.shut_down());
    }
}

/// The output of one materialized CTE in one run of a plan, and the count of
/// its readers.
///
/// The output is released when `not_executed` and `users` are both zero.
/// Then each partition of each bound scan was executed, and each of these
/// executions has finished or was dropped, so a plan in which each scan
/// partition is executed one time cannot read the output again.
///
/// The count is of the partitions that can still be executed, not of the
/// streams that are open. Thus the output is not released between two scans
/// that are executed one after the other. A partition that is never executed
/// (for example because the query was cancelled) keeps the output until the
/// run ends, which is the time of release without this count.
///
/// The count does not know that a partition is executed more than one time.
/// When a partition is executed after the release, a new [`RunOutput`] is
/// created and the body is computed again. The scan never reads an empty
/// output. An operator that executes a scan more than one time in a run must
/// hold the output. See [`hold_materialized_cte_outputs`].
struct RunOutput {
    /// Set by the first reader, without the lock of the outputs, because it
    /// executes the body, and the body can read another CTE of the run. A
    /// concurrent reader waits for it, so the body is executed one time.
    output: Arc<OnceLock<Arc<CteOutput>>>,
    /// For each bound scan, whether each of its partitions was executed.
    executed: Vec<Vec<bool>>,
    /// The number of partitions in `executed` that were not executed.
    not_executed: usize,
    /// The number of [`MaterializedCteOutputUse`]s: the scan streams that
    /// are open, and the holds.
    users: usize,
}

impl Drop for RunOutput {
    /// Stop the producers and release the log at once. A producer task holds
    /// its fan-out, so the fan-outs are not dropped with the output.
    fn drop(&mut self) {
        if let Some(output) = self.output.get() {
            output.shut_down();
        }
    }
}

impl RunOutput {
    fn new() -> Self {
        Self {
            output: Arc::default(),
            executed: vec![],
            not_executed: 0,
            users: 0,
        }
    }

    /// Add the scans in `scan_partitions` that are not yet counted.
    fn add_scans(&mut self, scan_partitions: &[usize]) {
        for &partitions in scan_partitions.iter().skip(self.executed.len()) {
            self.executed.push(vec![false; partitions]);
            self.not_executed += partitions;
        }
    }
}

/// The outputs of the materialized CTEs in one run of a plan, by CTE id.
///
/// The [`RunState`] of the [`TaskContext`] holds it. An output is removed
/// when its last reader has finished (see [`RunOutput`]), or else it lives
/// until the last [`TaskContext`] of the run is dropped.
#[derive(Default)]
struct MaterializedCteOutputs {
    /// The outputs of the enclosing execution. `None` for a whole run.
    parent: Option<Arc<MaterializedCteOutputs>>,
    /// The outputs of the run, for an `internal` context. Weak, so that a
    /// body stream that keeps the context does not keep the run.
    weak_parent: Option<Weak<MaterializedCteOutputs>>,
    /// The CTEs that have their own output here when `parent` is set. The
    /// output of every other CTE is the output in `parent`.
    renewed: HashSet<u64>,
    outputs: Mutex<HashMap<u64, RunOutput>>,
    /// The context of a CTE body that a fan-out executes. Its scans keep the
    /// outputs weakly. See [`MaterializedCteOutputUse`].
    internal: bool,
}

impl MaterializedCteOutputs {
    /// Register a reader of the output of the CTE `id`, and return the
    /// output. The output is created, empty, if there is none.
    ///
    /// `scan` is the index and the partition of the scan that is executed.
    /// `None` registers a hold: a reader that does not execute a scan.
    /// The output is not released before the returned
    /// [`MaterializedCteOutputUse`] is dropped.
    fn start_use(
        self: &Arc<Self>,
        id: u64,
        scan_partitions: &[usize],
        scan: Option<(usize, usize)>,
        make: &dyn Fn() -> CteOutput,
        internal: bool,
    ) -> Result<(Arc<CteOutput>, MaterializedCteOutputUse)> {
        let internal = internal || self.internal;
        if let Some(parent) = &self.weak_parent {
            let Some(parent) = parent.upgrade() else {
                return internal_err!("MaterializedCte: the run of the CTE {id} ended");
            };
            return parent.start_use(id, scan_partitions, scan, make, internal);
        }
        if let Some(parent) = &self.parent
            && !self.renewed.contains(&id)
        {
            return parent.start_use(id, scan_partitions, scan, make, internal);
        }
        let mut outputs = self.outputs.lock();
        let output = outputs.entry(id).or_insert_with(RunOutput::new);
        output.add_scans(scan_partitions);
        output.users += 1;
        if let Some((scan, partition)) = scan
            && let Some(executed) = output
                .executed
                .get_mut(scan)
                .and_then(|scan| scan.get_mut(partition))
            && !*executed
        {
            *executed = true;
            output.not_executed -= 1;
        }
        let cell = Arc::clone(&output.output);
        drop(outputs);
        // The use is counted, so the output is not released before
        // `in_use` is dropped. If `make` panics, the drop of `in_use` ends
        // the use.
        let mut in_use = MaterializedCteOutputUse {
            outputs: if internal {
                OutputsRef::Weak(Arc::downgrade(self))
            } else {
                OutputsRef::Strong(Arc::clone(self))
            },
            id,
            hold: None,
        };
        let output = Arc::clone(cell.get_or_init(|| Arc::new(make())));
        if scan.is_none() {
            output.add_hold();
            in_use.hold = Some(Arc::clone(&output));
        }
        Ok((output, in_use))
    }
}

/// The reference of a reader to the outputs of its run.
///
/// A scan stream that the caller of the plan polls keeps the outputs, and with
/// them the run, alive. A scan stream in a CTE body that a fan-out executes is
/// owned by the producer task of that fan-out. That task is in the outputs, so
/// a strong reference would keep the run alive after the caller dropped every
/// stream, and the task would wait at its gate forever. That scan keeps the
/// outputs weakly.
enum OutputsRef {
    Strong(Arc<MaterializedCteOutputs>),
    Weak(Weak<MaterializedCteOutputs>),
}

/// One reader of the output of a materialized CTE in a run: a scan stream
/// that is open, or a hold. To drop it ends the read, and releases the output
/// if no reader remains and none can start. See [`RunOutput`].
pub(crate) struct MaterializedCteOutputUse {
    outputs: OutputsRef,
    id: u64,
    /// The output, for a hold.
    hold: Option<Arc<CteOutput>>,
}

impl Drop for MaterializedCteOutputUse {
    fn drop(&mut self) {
        if let Some(output) = self.hold.take() {
            output.remove_hold();
        }
        let outputs = match &self.outputs {
            OutputsRef::Strong(outputs) => Arc::clone(outputs),
            OutputsRef::Weak(outputs) => match outputs.upgrade() {
                Some(outputs) => outputs,
                // The run ended, and its outputs were released.
                None => return,
            },
        };
        let released = {
            let mut outputs = outputs.outputs.lock();
            // An output is removed only when it has no users, so this is the
            // output that `start_use` returned.
            let Some(output) = outputs.get_mut(&self.id) else {
                return;
            };
            output.users -= 1;
            if output.users == 0 && output.not_executed == 0 {
                outputs.remove(&self.id)
            } else {
                None
            }
        };
        // Drop the output without the lock. If the body has not completed,
        // this drops the streams of the body, which can be the last users of
        // other outputs in the same map.
        drop(released);
    }
}

/// Return the ids of the materialized CTEs that `plan` declares.
pub(crate) fn materialized_cte_ids(plan: &Arc<dyn ExecutionPlan>) -> HashSet<u64> {
    let mut ids = HashSet::new();
    plan.apply(|node| {
        if let Some(cte) = node.downcast_ref::<MaterializedCteExec>() {
            ids.insert(cte.buffer.id);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the closure does not fail");
    ids
}

/// Return a copy of `context` in which the materialized CTEs `ids` have no
/// output, so that they are computed again. All other CTEs keep the output
/// that they have in `context`.
///
/// `RecursiveQueryExec` calls this for each iteration with the CTEs that its
/// recursive term declares. A CTE that is declared outside the recursive term
/// and read inside it is not in `ids`, so it is computed one time for the
/// whole query.
pub(crate) fn with_new_materialized_cte_outputs(
    context: &TaskContext,
    ids: &HashSet<u64>,
) -> TaskContext {
    let run_state = context.run_state();
    let outputs = MaterializedCteOutputs {
        parent: Some(run_state.get_or_init()),
        renewed: ids.clone(),
        ..Default::default()
    };
    let run_state = RunState::new_child(Arc::clone(run_state), Arc::new(outputs));
    context.clone().with_run_state(Arc::new(run_state))
}

/// Hold the outputs of the materialized CTEs that `plan` reads and that are
/// not in `declared`, until the returned values are dropped.
///
/// `RecursiveQueryExec` calls this with its recursive term, and the CTEs that
/// the recursive term declares. It executes the recursive term one time for
/// each iteration, so the scans of a CTE that is declared outside it are
/// executed more than one time in a run. Without the hold, the output would
/// be released after an iteration, and the next iteration would compute the
/// body again.
pub(crate) fn hold_materialized_cte_outputs(
    plan: &Arc<dyn ExecutionPlan>,
    declared: &HashSet<u64>,
    context: &TaskContext,
) -> Vec<MaterializedCteOutputUse> {
    let mut buffers = HashMap::new();
    plan.apply(|node| {
        if let Some(scan) = node.downcast_ref::<MaterializedCteScanExec>()
            && let Some(buffer) = &scan.buffer
            && !declared.contains(&buffer.id)
        {
            buffers.insert(buffer.id, Arc::clone(buffer));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("the closure does not fail");
    if buffers.is_empty() {
        return vec![];
    }
    let context = Arc::new(context.clone());
    buffers
        .values()
        .filter_map(|buffer| {
            // The only errors are a buffer without a body, which has no
            // output to hold, and a run that ended. Neither needs a hold.
            let (_, hold) = buffer.materialize(&context, None).ok()?;
            Some(hold)
        })
        .collect()
}

/// The plan-time state shared by one [`MaterializedCteExec`] and all its
/// [`MaterializedCteScanExec`]s. It holds no output: the output of a run is in
/// the [`RunState`] of the [`TaskContext`] of that run.
pub struct MaterializedCteBuffer {
    id: u64,
    name: String,
    /// The body to run. [`MaterializedCteExec`] updates it every time it is
    /// rebuilt (for example by a physical optimizer rule), so it is the body of
    /// the final plan. A scan can run the body before the owning
    /// `MaterializedCteExec` executes, for example from a scalar subquery.
    body: Mutex<Option<Arc<dyn ExecutionPlan>>>,
    /// The partition count of every scan bound to this buffer.
    scan_partitions: Mutex<Vec<usize>>,
    metrics: ExecutionPlanMetricsSet,
}

impl fmt::Debug for MaterializedCteBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaterializedCteBuffer")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl MaterializedCteBuffer {
    pub fn new(id: u64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            body: Mutex::new(None),
            scan_partitions: Mutex::new(vec![]),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// The id that binds scans to this buffer.
    pub fn id(&self) -> u64 {
        self.id
    }

    fn set_body(&self, body: Arc<dyn ExecutionPlan>) {
        *self.body.lock() = Some(body);
    }

    /// Register a reader of the output of this CTE in the run of `context`,
    /// and return the output. The output is created, and its body starts
    /// when the first reader polls.
    ///
    /// `scan` is the index and the partition of the scan that reads the
    /// output, or `None` for a hold. The caller must keep the returned
    /// [`MaterializedCteOutputUse`] until it has read the output. See
    /// [`MaterializedCteOutputs::start_use`].
    fn materialize(
        self: &Arc<Self>,
        context: &Arc<TaskContext>,
        scan: Option<(usize, usize)>,
    ) -> Result<(Arc<CteOutput>, MaterializedCteOutputUse)> {
        let Some(body) = self.body.lock().clone() else {
            return internal_err!("MaterializedCte {} has no body", self.name);
        };
        let scan_partitions = self.scan_partitions.lock().clone();
        let make = || {
            let options = FanoutOptions {
                name: self.name.clone(),
                body: Arc::clone(&body),
                scans: scan_partitions.len(),
                metrics: self.metrics.clone(),
            };
            CteOutput::new(options, context, None)
        };
        context
            .run_state()
            .get_or_init::<MaterializedCteOutputs>()
            .start_use(self.id, &scan_partitions, scan, &make, false)
    }
}

/// The read of one scan partition: the body partitions `partition,
/// partition + n, ...` at the same time, where `n` is the partition count of
/// the scan. The batches of the body partitions are interleaved.
///
/// The readers are registered when the scan partition is executed, so the
/// log keeps every batch for them. The scan returns `Pending` only after it
/// polled every reader, so when the scan waits, every reader is active and
/// waits at the head of its fan-out, and the gate of each fan-out is open. A body that couples its
/// partitions can make progress.
struct ScanReplay {
    readers: Readers,
    /// The output that this read computes again, if the shared output has
    /// retired batches that it needs. See [`Fanout::open`].
    private: Option<Arc<CteOutput>>,
}

impl ScanReplay {
    fn new(
        output: &CteOutput,
        context: &Arc<TaskContext>,
        scan: usize,
        partition: usize,
        n: usize,
    ) -> Result<Self> {
        let partitions: Vec<usize> = (partition..output.fanouts.len())
            .step_by(n.max(1))
            .collect();
        let shared: Option<Vec<FanoutReader>> =
            partitions.iter().map(|&p| output.reader(scan, p)).collect();
        if let Some(readers) = shared {
            return Ok(Self {
                readers: Readers::new(readers),
                private: None,
            });
        }
        let private = Arc::new(output.recompute(context, &partitions));
        let readers: Option<Vec<FanoutReader>> =
            partitions.iter().map(|&p| private.reader(0, p)).collect();
        let Some(readers) = readers else {
            return internal_err!("MaterializedCte: a new output has no reader");
        };
        Ok(Self {
            readers: Readers::new(readers),
            private: Some(private),
        })
    }
}

impl Stream for ScanReplay {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.readers.poll_next(cx)
    }
}

impl Drop for ScanReplay {
    fn drop(&mut self) {
        self.readers = Readers::new(vec![]);
        if let Some(private) = &self.private {
            private.shut_down();
        }
    }
}

/// The readers of one scan partition, polled in turn.
///
/// This interleaves the batches of the readers, as `select_all` does, but it
/// does not allocate for each batch. A scan partition usually has one
/// reader, and then it polls only that reader.
struct Readers {
    readers: Vec<FanoutReader>,
    /// The reader to poll first, so that each reader gets its turn.
    next: usize,
}

impl Readers {
    fn new(readers: Vec<FanoutReader>) -> Self {
        Self { readers, next: 0 }
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let mut i = 0;
        while i < self.readers.len() {
            let index = (self.next + i) % self.readers.len();
            match self.readers[index].poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    self.next = index + 1;
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(None) => {
                    // Drop the reader now, so that its fan-out can retire
                    // the batches. Poll the remaining readers.
                    self.readers.remove(index);
                    i = 0;
                    self.next = index;
                }
                Poll::Pending => i += 1,
            }
        }
        if self.readers.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

/// The stream of one scan partition. It is a reader of the output from the
/// call of `execute` until it ends, returns an error, or is dropped.
struct ScanStream {
    /// `None` after the end of the stream. Declared before `in_use`, so that
    /// it drops its reference to the output first.
    stream: Option<BoxStream<'static, Result<RecordBatch>>>,
    in_use: Option<MaterializedCteOutputUse>,
}

impl Stream for ScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };
        let item = futures::ready!(stream.poll_next_unpin(cx));
        if !matches!(item, Some(Ok(_))) {
            // The scan does not read the output again. Do not wait for the
            // consumer to drop the stream.
            self.stream = None;
            self.in_use = None;
        }
        Poll::Ready(item)
    }
}

/// Computes a CTE body once and runs the continuation, whose
/// [`MaterializedCteScanExec`]s read the buffered body output.
///
/// Children: `[body, continuation]`. The output is the output of the
/// continuation.
#[derive(Debug)]
pub struct MaterializedCteExec {
    body: Arc<dyn ExecutionPlan>,
    continuation: Arc<dyn ExecutionPlan>,
    buffer: Arc<MaterializedCteBuffer>,
    cache: Arc<PlanProperties>,
}

impl MaterializedCteExec {
    pub fn new(
        body: Arc<dyn ExecutionPlan>,
        continuation: Arc<dyn ExecutionPlan>,
        buffer: Arc<MaterializedCteBuffer>,
    ) -> Self {
        buffer.set_body(Arc::clone(&body));
        let cache = Arc::clone(continuation.properties());
        Self {
            body,
            continuation,
            buffer,
            cache,
        }
    }

    pub fn buffer(&self) -> &Arc<MaterializedCteBuffer> {
        &self.buffer
    }
}

impl DisplayAs for MaterializedCteExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteExec: name={}", self.buffer.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.buffer.name),
        }
    }
}

impl ExecutionPlan for MaterializedCteExec {
    fn name(&self) -> &'static str {
        "MaterializedCteExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.body, &self.continuation]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!("MaterializedCteExec takes 2 children");
        }
        let continuation = children.pop().unwrap();
        let body = children.pop().unwrap();
        Ok(Arc::new(Self::new(
            body,
            continuation,
            Arc::clone(&self.buffer),
        )))
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true, false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // The body runs lazily, when the first scan polls it.
        self.buffer.set_body(Arc::clone(&self.body));
        self.continuation.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.buffer.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::Skip, ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::clone(&input_stats[1]))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    /// The output is the output of the continuation, so parent filters go to
    /// the continuation only. The body is shared by every scan and must not
    /// be filtered for one consumer.
    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::new()
            .with_child(ChildFilterDescription::all_unsupported(&parent_filters))
            .with_child(ChildFilterDescription::from_child(
                &parent_filters,
                &self.continuation,
            )?))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // The body always reports `No`, so a filter is handled when the
        // continuation handles it.
        Ok(FilterPushdownPropagation::if_any(child_pushdown_result))
    }
}

/// Reads the output of a [`MaterializedCteExec`] body as it streams.
#[derive(Debug)]
pub struct MaterializedCteScanExec {
    id: u64,
    name: String,
    schema: SchemaRef,
    buffer: Option<Arc<MaterializedCteBuffer>>,
    /// The index of this scan in the scans that are bound to `buffer`.
    scan_index: usize,
    metrics: ExecutionPlanMetricsSet,
    cache: Arc<PlanProperties>,
}

impl MaterializedCteScanExec {
    /// Create a scan that is not yet bound to a buffer. See [`Self::bind`].
    pub fn new(
        id: u64,
        name: impl Into<String>,
        schema: SchemaRef,
        partitions: usize,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(partitions.max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative);
        Self {
            id,
            name: name.into(),
            schema,
            buffer: None,
            scan_index: 0,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        }
    }

    /// Bind this scan to the buffer of its [`MaterializedCteExec`].
    pub fn bind(&self, buffer: Arc<MaterializedCteBuffer>) -> Self {
        let scan_index = {
            let mut scan_partitions = buffer.scan_partitions.lock();
            scan_partitions.push(self.cache.partitioning.partition_count());
            scan_partitions.len() - 1
        };
        Self {
            id: self.id,
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            buffer: Some(buffer),
            scan_index,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::clone(&self.cache),
        }
    }

    /// The id of the [`MaterializedCteBuffer`] this scan reads.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Whether [`Self::bind`] has been called on this scan.
    pub fn is_bound(&self) -> bool {
        self.buffer.is_some()
    }
}

impl DisplayAs for MaterializedCteScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MaterializedCteScanExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => write!(f, "name={}", self.name),
        }
    }
}

impl ExecutionPlan for MaterializedCteScanExec {
    fn name(&self) -> &'static str {
        "MaterializedCteScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Some(buffer) = &self.buffer else {
            return internal_err!(
                "MaterializedCteScanExec {} is not bound to its CTE",
                self.name
            );
        };
        let (output, in_use) =
            buffer.materialize(&context, Some((self.scan_index, partition)))?;
        let output_partitions = self.cache.partitioning.partition_count();
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let stream = ScanReplay::new(
            &output,
            &context,
            self.scan_index,
            partition,
            output_partitions,
        )?
        .inspect_ok(move |batch| baseline.record_output(batch.num_rows()));
        let stream = ScanStream {
            stream: Some(stream.boxed()),
            in_use: Some(in_use),
        };
        // The replay of in-memory batches never returns `Pending`, so it must
        // consume the Tokio budget explicitly.
        Ok(Box::pin(cooperative(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    /// The statistics are unknown: a leaf cannot ask the body for its
    /// statistics through [`StatisticsArgs`].
    ///
    /// This is a known limitation, and it can make a plan worse than the
    /// plan of the inlined CTE. For example, `JoinSelection` does not know
    /// that a scan is small, and can put the other, larger input of a hash
    /// join on the build side.
    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::collect;
    use crate::empty::EmptyExec;
    use crate::execution_plan::reset_plan_states;
    use crate::filter_pushdown::PushedDown;
    use crate::test::TestMemoryExec;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::expressions::{col, lit};
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn parent_filters_go_to_the_continuation_only() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let body = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let continuation = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let exec = MaterializedCteExec::new(
            body,
            continuation,
            Arc::new(MaterializedCteBuffer::new(0, "c")),
        );
        let filter = Arc::new(datafusion_physical_expr::expressions::BinaryExpr::new(
            col("a", &schema)?,
            datafusion_expr::Operator::Eq,
            lit(1i32),
        )) as Arc<dyn PhysicalExpr>;

        let description = exec.gather_filters_for_pushdown(
            FilterPushdownPhase::Pre,
            vec![filter],
            &ConfigOptions::default(),
        )?;
        let parent_filters = description.parent_filters();
        assert_eq!(parent_filters.len(), 2);
        assert!(matches!(parent_filters[0][0].discriminant, PushedDown::No));
        assert!(matches!(parent_filters[1][0].discriminant, PushedDown::Yes));
        Ok(())
    }

    /// Counts the `execute` calls of `input`.
    #[derive(Debug)]
    struct CountingExec {
        input: Arc<dyn ExecutionPlan>,
        executions: Arc<AtomicUsize>,
        /// The count of batches that the streams yielded.
        batches: Arc<AtomicUsize>,
        /// The count of streams that were dropped.
        dropped: Arc<AtomicUsize>,
        /// A partition whose `execute` fails.
        fail_partition: Option<usize>,
        /// `execute` panics.
        panic: bool,
    }

    /// Counts its drop.
    struct DropCount(Arc<AtomicUsize>);

    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Wait until `count` is at least `n`, without a sleep.
    async fn wait_for(count: &AtomicUsize, n: usize) {
        let wait = async {
            while count.load(Ordering::Relaxed) < n {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(20), wait)
            .await
            .expect("the count must reach the value");
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "CountingExec")
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: children.swap_remove(0),
                executions: Arc::clone(&self.executions),
                batches: Arc::clone(&self.batches),
                dropped: Arc::clone(&self.dropped),
                fail_partition: self.fail_partition,
                panic: self.panic,
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.executions.fetch_add(1, Ordering::Relaxed);
            assert!(!self.panic, "execute panics");
            if self.fail_partition == Some(partition) {
                return internal_err!("execute of partition {partition} fails");
            }
            let batches = Arc::clone(&self.batches);
            let dropped = DropCount(Arc::clone(&self.dropped));
            let stream = self.input.execute(partition, context)?;
            let schema = stream.schema();
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream.inspect_ok(move |_| {
                    let _ = &dropped;
                    batches.fetch_add(1, Ordering::Relaxed);
                }),
            )))
        }
    }

    /// A plan `MaterializedCteExec(body, scan)` whose body has the rows
    /// `0..rows` in batches of one row, and the count of body executions.
    fn counting_plan(rows: i32) -> Result<(Arc<dyn ExecutionPlan>, Arc<AtomicUsize>)> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batches = (0..rows)
            .map(|i| {
                let values = Int32Array::from(vec![i]);
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let executions = Arc::new(AtomicUsize::new(0));
        let body = Arc::new(CountingExec {
            input: TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?,
            executions: Arc::clone(&executions),
            batches: Arc::default(),
            dropped: Arc::default(),
            fail_partition: None,
            panic: false,
        });
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", schema, 1);
        let scan = Arc::new(scan.bind(Arc::clone(&buffer)));
        let plan = Arc::new(MaterializedCteExec::new(body, scan, buffer));
        Ok((plan, executions))
    }

    /// The paths of the spill files of the output of the CTE 0 in the run of
    /// `context`.
    fn spill_paths(context: &TaskContext) -> Vec<std::path::PathBuf> {
        let outputs = context.run_state().get_or_init::<MaterializedCteOutputs>();
        let outputs = outputs.outputs.lock();
        outputs
            .get(&0)
            .map(|o| {
                o.output
                    .get()
                    .into_iter()
                    .flat_map(|o| o.each())
                    .flat_map(|f| f.spill_paths())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Hold the outputs of the CTE of `plan`, a plan from `counting_plan`.
    fn hold(
        plan: &Arc<dyn ExecutionPlan>,
        context: &TaskContext,
    ) -> Vec<MaterializedCteOutputUse> {
        let cte = plan.downcast_ref::<MaterializedCteExec>().unwrap();
        hold_materialized_cte_outputs(&cte.continuation, &HashSet::new(), context)
    }

    fn values(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                let array = b.column(0).as_any().downcast_ref::<Int32Array>();
                array.unwrap().values().to_vec()
            })
            .collect()
    }

    /// Two runs of one plan overlap. Each run has its own `TaskContext`, so
    /// each run computes the body and holds its own output.
    #[tokio::test]
    async fn overlapping_runs_do_not_share_the_output() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context_1 = Arc::new(TaskContext::default());
        let context_2 = Arc::new(TaskContext::default());
        // The first run executes the scan partition two times, so it holds
        // the output, as `RecursiveQueryExec` does.
        let hold_1 = hold(&plan, &context_1);

        let mut run_1 = plan.execute(0, Arc::clone(&context_1))?;
        let first = run_1.next().await.transpose()?.unwrap();
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert!(context_1.memory_pool().reserved() > 0);
        assert_eq!(context_2.memory_pool().reserved(), 0);

        // The second run starts and completes while the first run is open.
        let run_2 = collect(plan.execute(0, Arc::clone(&context_2))?).await?;
        assert_eq!(values(&run_2), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        // The end of the second run releases its output only.
        assert_eq!(context_2.memory_pool().reserved(), 0);
        assert!(context_1.memory_pool().reserved() > 0);

        // One more scan in the first run, while that run is open, replays
        // the output of that run.
        let again = collect(plan.execute(0, Arc::clone(&context_1))?).await?;
        assert_eq!(values(&again), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        assert!(context_1.memory_pool().reserved() > 0);

        let mut run_1_batches = vec![first];
        run_1_batches.extend(collect(run_1).await?);
        assert_eq!(values(&run_1_batches), vec![0, 1, 2]);
        drop(hold_1);
        assert_eq!(context_1.memory_pool().reserved(), 0);
        Ok(())
    }

    /// `reset_state` returns a new plan and does not change the old plan or
    /// the output of a run.
    #[tokio::test]
    async fn reset_state_leaves_the_old_plan_usable() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());
        // The run executes the scan partition more than one time.
        let _hold = hold(&plan, &context);

        let mut stream = plan.execute(0, Arc::clone(&context))?;
        let first = stream.next().await.transpose()?.unwrap();
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        let reset = reset_plan_states(Arc::clone(&plan))?;
        assert!(!Arc::ptr_eq(&plan, &reset));

        // A scan of the old plan that starts after the reset, in the run
        // that is open, replays the output of that run.
        let old = collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&old), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        // The new plan in the same run also replays that output.
        let new = collect(reset.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&new), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        let mut batches = vec![first];
        batches.extend(collect(stream).await?);
        assert_eq!(values(&batches), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        // The new plan in a new run computes the body again.
        let new = collect(reset.execute(0, Arc::new(TaskContext::default()))?).await?;
        assert_eq!(values(&new), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        Ok(())
    }

    /// A context from `with_new_materialized_cte_outputs` computes the given
    /// CTEs again and keeps the output of the other CTEs.
    #[tokio::test]
    async fn new_outputs_are_only_for_the_given_ctes() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());
        // The run executes the scan partition more than one time.
        let _hold = hold(&plan, &context);
        // A stream that is open keeps the output of its context.
        let _open = plan.execute(0, Arc::clone(&context))?;
        collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        // The CTE has the id 0, so it keeps its output.
        let other = with_new_materialized_cte_outputs(&context, &HashSet::from([1]));
        collect(plan.execute(0, Arc::new(other))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 1);

        let ids = materialized_cte_ids(&plan);
        assert_eq!(ids, HashSet::from([0]));
        let renewed = Arc::new(with_new_materialized_cte_outputs(&context, &ids));
        let _hold_renewed = hold(&plan, &renewed);
        let _open_renewed = plan.execute(0, Arc::clone(&renewed))?;
        collect(plan.execute(0, Arc::clone(&renewed))?).await?;
        collect(plan.execute(0, Arc::clone(&renewed))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 2);

        // A nested context reads the CTEs that it does not renew from the
        // enclosing context.
        let nested = with_new_materialized_cte_outputs(&renewed, &HashSet::new());
        collect(plan.execute(0, Arc::new(nested))?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 2);

        // The first context still has its output.
        collect(plan.execute(0, context)?).await?;
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        Ok(())
    }

    /// A plan with a body of two partitions (the rows `0..rows` in batches of
    /// 100 rows, one half in each partition), and two scans of two partitions
    /// each. Also returns the count of body executions.
    #[expect(clippy::type_complexity)]
    fn two_scans_plan(
        rows: i32,
    ) -> Result<(
        Arc<MaterializedCteExec>,
        [Arc<MaterializedCteScanExec>; 2],
        Arc<AtomicUsize>,
    )> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let partition = |range: std::ops::Range<i32>| {
            range
                .step_by(100)
                .map(|i| {
                    let values = Int32Array::from_iter_values(i..i + 100);
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        };
        let partitions = [partition(0..rows / 2)?, partition(rows / 2..rows)?];
        let executions = Arc::new(AtomicUsize::new(0));
        let body = Arc::new(CountingExec {
            input: TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None)?,
            executions: Arc::clone(&executions),
            batches: Arc::default(),
            dropped: Arc::default(),
            fail_partition: None,
            panic: false,
        });
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 2);
        let scans = [
            Arc::new(scan.bind(Arc::clone(&buffer))),
            Arc::new(scan.bind(Arc::clone(&buffer))),
        ];
        let exec = Arc::new(MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(schema)),
            buffer,
        ));
        Ok((exec, scans, executions))
    }

    /// The memory reservations and the spill files are released when the
    /// last partition of the last scan finishes. The plan, the context of the
    /// run and the finished streams are still alive.
    #[tokio::test]
    async fn output_is_released_when_the_last_scan_partition_finishes() -> Result<()> {
        let (exec, scans, executions) = two_scans_plan(10_000)?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(20_000, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let pool = Arc::clone(context.memory_pool());

        // Read the first three scan partitions one after the other. The
        // output is not released between them, because the other partitions
        // can still be executed.
        let mut paths = vec![];
        for (scan, partition) in [(0, 0), (1, 1), (0, 1)] {
            let mut stream = scans[scan].execute(partition, Arc::clone(&context))?;
            let mut batches = vec![];
            while let Some(batch) = stream.next().await.transpose()? {
                batches.push(batch);
            }
            let start = partition as i32 * 5000;
            assert_eq!(values(&batches), (start..start + 5000).collect::<Vec<_>>());
            // The stream has ended but is not dropped. The output is on disk.
            if paths.is_empty() {
                paths = spill_paths(&context);
                assert!(!paths.is_empty(), "the body did not spill");
            }
            assert!(paths.iter().all(|path| path.exists()));
        }

        let mut last = scans[1].execute(0, Arc::clone(&context))?;
        let mut batches = vec![];
        // The last reader releases each batch when it has read it.
        while let Some(batch) = last.next().await.transpose()? {
            batches.push(batch);
        }
        assert_eq!(values(&batches), (0..5000).collect::<Vec<_>>());
        // The stream has ended. The stream, the plan and the context of the
        // run are alive.
        assert_eq!(pool.reserved(), 0);
        assert!(!paths.iter().any(|path| path.exists()));
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        drop((last, exec, context));
        Ok(())
    }

    /// A scan stream that is dropped before it is polled, or before its end,
    /// does not keep the output.
    #[tokio::test]
    async fn output_is_released_when_scan_streams_are_dropped_early() -> Result<()> {
        let (_exec, scans, executions) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        // Not polled.
        let unpolled = scans[0].execute(0, Arc::clone(&context))?;
        // Dropped after one batch, as below a `LIMIT`.
        let mut limited = scans[0].execute(1, Arc::clone(&context))?;
        limited.next().await.transpose()?.unwrap();
        let mut partial = scans[1].execute(0, Arc::clone(&context))?;
        partial.next().await.transpose()?.unwrap();
        let full = collect(scans[1].execute(1, Arc::clone(&context))?).await?;
        assert_eq!(values(&full), (500..1000).collect::<Vec<_>>());

        // Each scan partition was executed, and three streams are open.
        assert!(pool.reserved() > 0);
        drop(unpolled);
        drop(limited);
        assert!(pool.reserved() > 0);
        drop(partial);
        assert_eq!(pool.reserved(), 0);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        Ok(())
    }

    /// When every scan stream is dropped before the body completes, the body
    /// is cancelled and its memory is released.
    #[tokio::test]
    async fn body_is_cancelled_when_all_scan_streams_are_dropped() -> Result<()> {
        let (_exec, scans, _) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let streams = [(0, 0), (0, 1), (1, 0), (1, 1)].map(|(scan, partition)| {
            scans[scan].execute(partition, Arc::clone(&context))
        });
        drop(streams);
        let outputs = context.run_state().get_or_init::<MaterializedCteOutputs>();
        assert!(outputs.outputs.lock().is_empty());
        assert_eq!(context.memory_pool().reserved(), 0);
        Ok(())
    }

    /// A scan partition that is never executed keeps the output until the
    /// context of the run is dropped. The other scans read correct rows.
    #[tokio::test]
    async fn partition_that_is_not_executed_keeps_the_output() -> Result<()> {
        let (_exec, scans, executions) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        for (scan, partition) in [(0, 0), (0, 1), (1, 0)] {
            let batches =
                collect(scans[scan].execute(partition, Arc::clone(&context))?).await?;
            let start = partition as i32 * 500;
            assert_eq!(values(&batches), (start..start + 500).collect::<Vec<_>>());
        }
        // Partition 1 of the second scan can still be executed.
        assert!(pool.reserved() > 0);
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        drop(context);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A scan partition that is executed after the release of the output
    /// computes the body again. It does not read an empty output.
    #[tokio::test]
    async fn scan_after_the_release_computes_the_body_again() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        let first = collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&first), vec![0, 1, 2]);
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert_eq!(pool.reserved(), 0);

        // The same scan partition, in the same run.
        let mut late = plan.execute(0, Arc::clone(&context))?;
        let mut batches = vec![late.next().await.transpose()?.unwrap()];
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        assert!(pool.reserved() > 0);
        batches.extend(collect(late).await?);
        assert_eq!(values(&batches), vec![0, 1, 2]);
        // The new output is released in the same way.
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A hold keeps the output for scans that are executed again, as the
    /// recursive term of a recursive query is.
    #[tokio::test]
    async fn hold_keeps_the_output_between_executions() -> Result<()> {
        let (plan, executions) = counting_plan(3)?;
        let cte = plan.downcast_ref::<MaterializedCteExec>().unwrap();
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());

        // The continuation reads the CTE and does not declare it.
        let holds =
            hold_materialized_cte_outputs(&cte.continuation, &HashSet::new(), &context);
        assert_eq!(holds.len(), 1);
        // The plan declares the CTE, so there is nothing to hold.
        let ids = materialized_cte_ids(&plan);
        assert!(hold_materialized_cte_outputs(&plan, &ids, &context).is_empty());

        for _ in 0..3 {
            let batches = collect(plan.execute(0, Arc::clone(&context))?).await?;
            assert_eq!(values(&batches), vec![0, 1, 2]);
            assert_eq!(executions.load(Ordering::Relaxed), 1);
            assert!(pool.reserved() > 0);
        }
        drop(holds);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A body with `partitions` partitions. Partition `p` has the batches of
    /// 100 rows that start at `p * 100, (p + partitions) * 100, ...`, for the
    /// rows `0..rows`. Also returns the counts of body executions and of body
    /// batches.
    fn streaming_body(
        rows: i32,
        partitions: usize,
    ) -> Result<(Arc<CountingExec>, Arc<AtomicUsize>, Arc<AtomicUsize>)> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let mut parts = vec![vec![]; partitions];
        for (i, start) in (0..rows).step_by(100).enumerate() {
            let values = Int32Array::from_iter_values(start..(start + 100).min(rows));
            parts[i % partitions].push(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(values)],
            )?);
        }
        let executions = Arc::new(AtomicUsize::new(0));
        let batches = Arc::new(AtomicUsize::new(0));
        let body = Arc::new(CountingExec {
            input: TestMemoryExec::try_new_exec(&parts, schema, None)?,
            executions: Arc::clone(&executions),
            batches: Arc::clone(&batches),
            dropped: Arc::default(),
            fail_partition: None,
            panic: false,
        });
        Ok((body, executions, batches))
    }

    /// `MaterializedCteExec(body, c a JOIN c b ON a.a = b.a)`, with `a` the
    /// build side. `build` wraps the build side scan. Each scan has
    /// `scan_partitions` partitions.
    fn self_join_plan(
        body: Arc<dyn ExecutionPlan>,
        scan_partitions: usize,
        build: impl Fn(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use crate::joins::{HashJoinExec, PartitionMode};
        use datafusion_common::{JoinType, NullEquality};
        use datafusion_physical_expr::expressions::Column;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", schema, scan_partitions);
        let left = build(Arc::new(scan.bind(Arc::clone(&buffer))))?;
        let right = Arc::new(scan.bind(Arc::clone(&buffer)));
        let on = vec![(
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let join = Arc::new(HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &JoinType::Inner,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?);
        Ok(Arc::new(MaterializedCteExec::new(body, join, buffer)))
    }

    /// Collect all partitions of `plan`, with a timeout that fails on a
    /// deadlock.
    async fn collect_all(
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<Vec<RecordBatch>> {
        let collected = crate::collect(plan, context);
        tokio::time::timeout(std::time::Duration::from_secs(20), collected)
            .await
            .expect("the plan must not deadlock")
    }

    fn sorted(batches: &[RecordBatch]) -> Vec<i32> {
        let mut values = values(batches);
        values.sort_unstable();
        values
    }

    /// The two references are the two sides of one hash join. The build side
    /// reads the whole body before the probe side is polled. The body runs
    /// one time, and the probe side reads the batches that wait for it.
    #[tokio::test]
    async fn both_sides_of_a_hash_join_complete() -> Result<()> {
        let (body, executions, _) = streaming_body(20_000, 1)?;
        let plan = self_join_plan(body, 1, Ok)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let batches = collect_all(plan, context).await?;
        assert_eq!(sorted(&batches), (0..20_000).collect::<Vec<_>>());
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// Each scan has one partition and the body has four. Each scan reads
    /// the four body partitions at the same time.
    #[tokio::test]
    async fn scan_with_fewer_partitions_than_the_body_on_both_join_sides() -> Result<()> {
        let (body, executions, _) = streaming_body(20_000, 4)?;
        let plan = self_join_plan(body, 1, Ok)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let batches = collect_all(plan, context).await?;
        assert_eq!(sorted(&batches), (0..20_000).collect::<Vec<_>>());
        assert_eq!(executions.load(Ordering::Relaxed), 4);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// The scans have two partitions and the body has three, so scan
    /// partition 0 reads the body partitions 0 and 2.
    #[tokio::test]
    async fn scan_partitions_read_every_other_body_partition() -> Result<()> {
        let (body, _, _) = streaming_body(3000, 3)?;
        let plan = self_join_plan(body, 2, |scan| {
            Ok(Arc::new(
                crate::coalesce_partitions::CoalescePartitionsExec::new(scan),
            ))
        })?;
        let batches = collect_all(plan, Arc::new(TaskContext::default())).await?;
        assert_eq!(sorted(&batches), (0..3000).collect::<Vec<_>>());
        Ok(())
    }

    /// The memory pool is too small for the batches that wait for the probe
    /// side, so they are spilled and read back. The build side keeps 10 rows,
    /// so the hash table is small. At the end, no memory and no spill file is
    /// left.
    #[tokio::test]
    async fn spill_with_both_join_sides() -> Result<()> {
        use crate::filter::FilterExec;
        use datafusion_expr::Operator;
        use datafusion_physical_expr::expressions::BinaryExpr;
        for partitions in [1, 3] {
            let (body, executions, _) = streaming_body(50_000, partitions)?;
            let plan = self_join_plan(body, 1, |scan| {
                let predicate = Arc::new(BinaryExpr::new(
                    col("a", &scan.schema())?,
                    Operator::Lt,
                    lit(10i32),
                ));
                Ok(Arc::new(FilterExec::try_new(predicate, scan)?))
            })?;
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_limit(100_000, 1.0)
                .build_arc()?;
            let context = Arc::new(TaskContext::default().with_runtime(runtime));
            let pool = Arc::clone(context.memory_pool());
            // The hold keeps the spill files after the scans end, so that the
            // test can see them.
            let hold = hold(&plan, &context);
            let batches = collect_all(Arc::clone(&plan), Arc::clone(&context)).await?;
            let paths = spill_paths(&context);
            assert!(!paths.is_empty(), "the body did not spill");
            assert!(paths.iter().all(|path| path.exists()));
            drop(hold);
            assert_eq!(sorted(&batches), (0..10).collect::<Vec<_>>());
            assert_eq!(executions.load(Ordering::Relaxed), partitions);
            let metrics = plan.metrics().unwrap();
            let spilled = metrics.spill_count().unwrap_or(0);
            assert!(spilled > 0, "the body did not spill");
            // The hash join keeps its hash table until the plan is dropped.
            drop(plan);
            assert_eq!(pool.reserved(), 0);
            assert!(!paths.iter().any(|path| path.exists()));
        }
        Ok(())
    }

    /// Two readers read in step. The log never holds more than a few
    /// batches.
    #[tokio::test]
    async fn readers_in_step_hold_a_few_batches() -> Result<()> {
        let (_exec, scans, _) = two_scans_plan(100_000)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let batch_size = {
            let schema = scans[0].schema();
            let values = Int32Array::from_iter_values(0..100);
            RecordBatch::try_new(schema, vec![Arc::new(values)])?.get_array_memory_size()
        };
        let mut max_reserved = 0;
        for partition in 0..2 {
            let mut a = scans[0].execute(partition, Arc::clone(&context))?;
            let mut b = scans[1].execute(partition, Arc::clone(&context))?;
            loop {
                let x = a.next().await.transpose()?;
                let y = b.next().await.transpose()?;
                max_reserved = max_reserved.max(pool.reserved());
                if x.is_none() && y.is_none() {
                    break;
                }
            }
        }
        // Each of the two body partitions has a window and the batch that is
        // read, and the other body partition reads ahead.
        assert!(
            max_reserved <= 4 * (fanout::WINDOW + 1) * batch_size,
            "reserved {max_reserved}"
        );
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A scan below a `LIMIT` reads one batch and is dropped. It is the only
    /// reader, so the body stops.
    #[tokio::test]
    async fn limit_stops_the_body() -> Result<()> {
        let (body, _, batches) = streaming_body(1_000_000, 1)?;
        let dropped = Arc::clone(&body.dropped);
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", schema, 1);
        let scan: Arc<dyn ExecutionPlan> = Arc::new(scan.bind(Arc::clone(&buffer)));
        let limit = Arc::new(crate::limit::GlobalLimitExec::new(scan, 0, Some(10)));
        let plan = Arc::new(MaterializedCteExec::new(body, limit, buffer));
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let rows = collect_all(plan, context).await?;
        assert_eq!(values(&rows), (0..10).collect::<Vec<_>>());
        // The body stream is dropped when its task stops.
        wait_for(&dropped, 1).await;
        let produced = batches.load(Ordering::Relaxed);
        assert!(produced <= fanout::WINDOW + 2, "produced {produced}");
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A scan of one partition below a `LIMIT` reads two body partitions at
    /// the same time. It reads one batch and is dropped. Each task appends
    /// only a few batches ahead of the scan, and stops.
    #[tokio::test]
    async fn limit_stops_every_body_partition_that_the_scan_reads() -> Result<()> {
        let (body, _, batches) = streaming_body(1_000_000, 2)?;
        let dropped = Arc::clone(&body.dropped);
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", schema, 1);
        let scan: Arc<dyn ExecutionPlan> = Arc::new(scan.bind(Arc::clone(&buffer)));
        let limit = Arc::new(crate::limit::GlobalLimitExec::new(scan, 0, Some(10)));
        let plan = Arc::new(MaterializedCteExec::new(body, limit, buffer));
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let rows = collect_all(plan, context).await?;
        assert_eq!(values(&rows), (0..10).collect::<Vec<_>>());
        wait_for(&dropped, 2).await;
        let produced = batches.load(Ordering::Relaxed);
        assert!(produced <= 2 * (fanout::WINDOW + 2), "produced {produced}");
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// One reader is dropped after one batch. The other reads every row, and
    /// the dropped reader does not hold batches for itself.
    #[tokio::test]
    async fn limit_on_one_reader_does_not_hold_the_log() -> Result<()> {
        let (_exec, scans, _) = two_scans_plan(100_000)?;
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        for partition in 0..2 {
            let mut limited = scans[0].execute(partition, Arc::clone(&context))?;
            limited.next().await.transpose()?.unwrap();
            drop(limited);
        }
        let mut max_reserved = 0;
        for partition in 0..2 {
            let mut stream = scans[1].execute(partition, Arc::clone(&context))?;
            let mut count = 0;
            while let Some(batch) = stream.next().await.transpose()? {
                count += batch.num_rows();
                max_reserved = max_reserved.max(pool.reserved());
            }
            assert_eq!(count, 50_000);
        }
        // 50 KB of a body of 400 KB.
        assert!(max_reserved < 50_000, "reserved {max_reserved}");
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// When every scan stream is dropped while the body runs, the body
    /// stops and the log is released.
    #[tokio::test]
    async fn dropped_scans_stop_the_body() -> Result<()> {
        let (body, _, batches) = streaming_body(1_000_000, 1)?;
        let dropped = Arc::clone(&body.dropped);
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let mut streams = scans
            .iter()
            .map(|scan| scan.execute(0, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        for stream in &mut streams {
            stream.next().await.transpose()?.unwrap();
        }
        drop(streams);
        assert_eq!(pool.reserved(), 0);
        wait_for(&dropped, 1).await;
        let produced = batches.load(Ordering::Relaxed);
        assert!(produced <= fanout::WINDOW + 2, "produced {produced}");
        Ok(())
    }

    /// An error of the body goes to every reader.
    #[tokio::test]
    async fn body_error_goes_to_every_reader() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let body = Arc::new(crate::test::exec::ErrorExec::new());
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", body.schema(), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let _exec = MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(schema)),
            Arc::clone(&buffer),
        );
        let context = Arc::new(TaskContext::default());
        for scan in &scans {
            let result = collect(scan.execute(0, Arc::clone(&context))?).await;
            assert!(result.is_err());
        }
        Ok(())
    }

    /// CTE `b` reads CTE `a`. One partition of a scan of `b` reads one batch
    /// and is dropped. The other partition is never executed, so the
    /// producer of `b` waits at its gate, and its body stream holds a scan
    /// of `a`. When the caller drops its streams and the context, the run is
    /// released.
    #[tokio::test]
    async fn chained_ctes_with_an_unexecuted_partition_release_the_run() -> Result<()> {
        let (a_body, _, _) = streaming_body(10_000, 1)?;
        let schema = a_body.schema();
        let a_buffer = Arc::new(MaterializedCteBuffer::new(0, "a"));
        let a_scan = MaterializedCteScanExec::new(0, "a", Arc::clone(&schema), 1);
        let b_body = Arc::new(a_scan.bind(Arc::clone(&a_buffer)));
        let b_buffer = Arc::new(MaterializedCteBuffer::new(1, "b"));
        let b_scan = MaterializedCteScanExec::new(1, "b", Arc::clone(&schema), 2);
        let b_scan = Arc::new(b_scan.bind(Arc::clone(&b_buffer)));
        let b_exec = Arc::new(MaterializedCteExec::new(
            b_body,
            Arc::clone(&b_scan) as Arc<dyn ExecutionPlan>,
            b_buffer,
        ));
        let _a_exec = MaterializedCteExec::new(a_body, b_exec, a_buffer);

        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let run_state = Arc::downgrade(context.run_state());
        let mut stream = b_scan.execute(0, Arc::clone(&context))?;
        stream.next().await.transpose()?.unwrap();
        drop(stream);
        drop(context);
        assert!(run_state.upgrade().is_none(), "the run is kept alive");
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// The `execute` of one body partition fails. The readers of that body
    /// partition get the error, and the readers of the other body partition
    /// read their rows.
    #[tokio::test]
    async fn execute_error_of_a_body_partition_goes_to_its_readers() -> Result<()> {
        let (body, _, _) = streaming_body(1000, 2)?;
        let body = Arc::new(CountingExec {
            input: Arc::clone(&body.input),
            executions: Arc::default(),
            batches: Arc::default(),
            dropped: Arc::default(),
            fail_partition: Some(1),
            panic: false,
        });
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 2);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let context = Arc::new(TaskContext::default());
        let read = |scan: usize, partition: usize| {
            let stream = scans[scan].execute(partition, Arc::clone(&context));
            async move {
                let collected = collect(stream?);
                tokio::time::timeout(std::time::Duration::from_secs(20), collected)
                    .await
                    .expect("a reader must not hang")
            }
        };
        assert!(read(1, 1).await.is_err());
        assert!(read(0, 1).await.is_err());
        for scan in 0..2 {
            assert_eq!(read(scan, 0).await?.len(), 5);
        }
        Ok(())
    }

    /// Two scan partitions start to read and are dropped before the
    /// producer appends a batch, so the producer stops. The same scan
    /// partition is executed again and computes the body again.
    #[tokio::test]
    async fn execution_after_a_stopped_producer_computes_the_body_again() -> Result<()> {
        let (_exec, scans, executions) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        let mut a = scans[0].execute(0, Arc::clone(&context))?;
        let mut b = scans[1].execute(0, Arc::clone(&context))?;
        assert!(futures::poll!(a.next()).is_pending());
        assert!(futures::poll!(b.next()).is_pending());
        drop((a, b));
        let again = scans[0].execute(0, Arc::clone(&context))?;
        let collected =
            tokio::time::timeout(std::time::Duration::from_secs(20), collect(again))
                .await
                .expect("the scan must not hang")?;
        assert_eq!(values(&collected), (0..500).collect::<Vec<_>>());
        // The new output executes only the body partition that it reads.
        assert_eq!(executions.load(Ordering::Relaxed), 3);
        Ok(())
    }

    /// A scan partition that is executed again after its batches were
    /// retired computes only the body partitions that it reads.
    #[tokio::test]
    async fn recompute_executes_only_the_partitions_it_reads() -> Result<()> {
        let (_exec, scans, executions) = two_scans_plan(1000)?;
        let context = Arc::new(TaskContext::default());
        for scan in &scans {
            collect(scan.execute(0, Arc::clone(&context))?).await?;
        }
        assert_eq!(executions.load(Ordering::Relaxed), 2);
        let again = collect(scans[0].execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&again), (0..500).collect::<Vec<_>>());
        assert_eq!(executions.load(Ordering::Relaxed), 3);
        Ok(())
    }

    /// The first reader reads part of the log. The second reader does not
    /// read, so the log spills, also the part that the first reader is in.
    /// Both readers read every row in order.
    #[tokio::test]
    async fn spill_keeps_the_order_for_a_reader_in_a_spilled_chunk() -> Result<()> {
        let (body, _, _) = streaming_body(20_000, 1)?;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(40_000, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut first = scans[0].execute(0, Arc::clone(&context))?;
        let second = scans[1].execute(0, Arc::clone(&context))?;
        let mut batches = vec![];
        for _ in 0..3 {
            batches.push(first.next().await.transpose()?.unwrap());
        }
        batches.extend(collect(first).await?);
        assert_eq!(values(&batches), (0..20_000).collect::<Vec<_>>());
        assert!(!spill_paths(&context).is_empty());
        let batches = collect(second).await?;
        assert_eq!(values(&batches), (0..20_000).collect::<Vec<_>>());
        Ok(())
    }

    /// Under constant memory pressure, the log writes many batches to one
    /// spill file, not one file for each batch.
    #[tokio::test]
    async fn spill_under_pressure_writes_few_files() -> Result<()> {
        let (body, _, _) = streaming_body(100_000, 1)?;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(20_000, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        // Another operator takes almost all of the pool, so no batch fits in
        // memory.
        let other = datafusion_execution::memory_pool::MemoryConsumer::new("other")
            .register(context.memory_pool());
        other.try_grow(19_000)?;
        // The second scan never reads, so the log keeps every batch.
        let _second = scans[1].execute(0, Arc::clone(&context))?;
        let batches = collect(scans[0].execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&batches), (0..100_000).collect::<Vec<_>>());
        let files = exec.metrics().unwrap().spill_count().unwrap_or(0);
        // 1000 batches.
        assert!(files > 0 && files <= 100, "{files} spill files");
        Ok(())
    }

    /// One scan partition is executed two times before the first stream is
    /// polled. Each stream reads every row.
    #[tokio::test]
    async fn two_executions_of_a_partition_before_a_poll_read_all_rows() -> Result<()> {
        let (plan, _) = counting_plan(3)?;
        let context = Arc::new(TaskContext::default());
        let first = plan.execute(0, Arc::clone(&context))?;
        let second = collect(plan.execute(0, Arc::clone(&context))?).await?;
        assert_eq!(values(&second), vec![0, 1, 2]);
        assert_eq!(values(&collect(first).await?), vec![0, 1, 2]);
        Ok(())
    }

    /// Two executions of one scan partition are polled in turn. Each stream
    /// reads every row.
    #[tokio::test]
    async fn two_executions_of_a_partition_polled_in_turn_read_all_rows() -> Result<()> {
        let (plan, _) = counting_plan(6)?;
        let context = Arc::new(TaskContext::default());
        let mut a = plan.execute(0, Arc::clone(&context))?;
        let mut b = plan.execute(0, Arc::clone(&context))?;
        let (mut from_a, mut from_b) = (vec![], vec![]);
        loop {
            let x = a.next().await.transpose()?;
            let y = b.next().await.transpose()?;
            if x.is_none() && y.is_none() {
                break;
            }
            from_a.extend(x);
            from_b.extend(y);
        }
        assert_eq!(values(&from_a), (0..6).collect::<Vec<_>>());
        assert_eq!(values(&from_b), (0..6).collect::<Vec<_>>());
        Ok(())
    }

    /// A spill write fails while a reader waits for an open spill file. The
    /// reader gets the error.
    #[tokio::test]
    async fn spill_write_error_goes_to_a_reader_of_an_open_file() -> Result<()> {
        let (body, _, _) = streaming_body(100_000, 1)?;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(20_000, 1.0)
            .with_max_temp_directory_size(30_000)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let other = datafusion_execution::memory_pool::MemoryConsumer::new("other")
            .register(context.memory_pool());
        other.try_grow(19_000)?;
        let _idle = scans[1].execute(0, Arc::clone(&context))?;
        let read = collect(scans[0].execute(0, Arc::clone(&context))?);
        let result = tokio::time::timeout(std::time::Duration::from_secs(20), read)
            .await
            .expect("the reader must not hang");
        let error = result.expect_err("the disk limit is exceeded").to_string();
        assert!(error.contains("limit"), "{error}");
        Ok(())
    }

    /// The body is a `RepartitionExec`, whose unpolled streams keep their
    /// context. Scan partition 1 is never executed, so the task of body
    /// partition 1 waits. When the caller drops its stream and the context,
    /// the run is released.
    #[tokio::test]
    async fn body_stream_that_keeps_its_context_does_not_keep_the_run() -> Result<()> {
        use crate::repartition::RepartitionExec;
        let (input, _, _) = streaming_body(10_000, 1)?;
        let body = Arc::new(RepartitionExec::try_new(
            input,
            Partitioning::RoundRobinBatch(2),
        )?);
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 2);
        let scan = Arc::new(scan.bind(Arc::clone(&buffer)));
        let plan = Arc::new(MaterializedCteExec::new(
            body,
            Arc::clone(&scan) as Arc<dyn ExecutionPlan>,
            buffer,
        ));
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let run_state = Arc::downgrade(context.run_state());
        let mut stream = plan.execute(0, Arc::clone(&context))?;
        stream.next().await.transpose()?.unwrap();
        drop(stream);
        drop(context);
        assert!(run_state.upgrade().is_none(), "the run is kept alive");
        // The channels of the `RepartitionExec` are in the plan.
        drop((plan, scan));
        let wait = async {
            while pool.reserved() > 0 {
                tokio::task::yield_now().await;
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(20), wait)
            .await
            .expect("the memory must be released");
        Ok(())
    }

    /// The `execute` of the body panics. The output of the run is released,
    /// and not kept by the count of its readers.
    #[test]
    fn panic_in_body_execute_does_not_keep_the_output() -> Result<()> {
        let (body, _, _) = streaming_body(100, 1)?;
        let body = Arc::new(CountingExec {
            input: Arc::clone(&body.input),
            executions: Arc::default(),
            batches: Arc::default(),
            dropped: Arc::default(),
            fail_partition: None,
            panic: true,
        });
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scan = scan.bind(Arc::clone(&buffer));
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let context = Arc::new(TaskContext::default());
        let executed = std::panic::catch_unwind(AssertUnwindSafe(|| {
            scan.execute(0, Arc::clone(&context))
        }));
        assert!(executed.is_err());
        let outputs = context.run_state().get_or_init::<MaterializedCteOutputs>();
        assert!(outputs.outputs.lock().is_empty());
        Ok(())
    }

    /// Two readers read strictly in turn, with a pool of 6 batches, so the
    /// cap of the log is 3 batches. No reader lags, so nothing spills.
    #[tokio::test]
    async fn readers_in_step_with_a_small_pool_do_not_spill() -> Result<()> {
        let (body, _, _) = streaming_body(10_000, 1)?;
        let batch_size = {
            let values = Int32Array::from_iter_values(0..100);
            RecordBatch::try_new(body.schema(), vec![Arc::new(values)])?
                .get_array_memory_size()
        };
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(6 * batch_size, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut a = scans[0].execute(0, Arc::clone(&context))?;
        let mut b = scans[1].execute(0, Arc::clone(&context))?;
        let (mut from_a, mut from_b) = (vec![], vec![]);
        loop {
            let x = a.next().await.transpose()?;
            let y = b.next().await.transpose()?;
            if x.is_none() && y.is_none() {
                break;
            }
            from_a.extend(x);
            from_b.extend(y);
        }
        assert_eq!(values(&from_a), (0..10_000).collect::<Vec<_>>());
        assert_eq!(values(&from_b), (0..10_000).collect::<Vec<_>>());
        let files = exec.metrics().unwrap().spill_count().unwrap_or(0);
        assert_eq!(files, 0, "{files} spill files");
        Ok(())
    }

    /// A second scan is bound after the output was created. The two scans
    /// read in step, and the log holds only a few batches.
    #[tokio::test]
    async fn scan_bound_after_the_output_was_created_does_not_hold_the_log() -> Result<()>
    {
        let (body, _, _) = streaming_body(10_000, 1)?;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let first = scan.bind(Arc::clone(&buffer));
        let _exec = MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Arc::clone(&buffer),
        );
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let mut a = first.execute(0, Arc::clone(&context))?;
        let second = scan.bind(Arc::clone(&buffer));
        let mut b = second.execute(0, Arc::clone(&context))?;
        let (mut rows, mut max_reserved) = (0, 0);
        loop {
            let x = a.next().await.transpose()?;
            let y = b.next().await.transpose()?;
            max_reserved = max_reserved.max(pool.reserved());
            if x.is_none() && y.is_none() {
                break;
            }
            rows += x.map_or(0, |b| b.num_rows()) + y.map_or(0, |b| b.num_rows());
        }
        assert_eq!(rows, 20_000);
        // 100 batches of about 500 bytes.
        assert!(max_reserved < 10_000, "reserved {max_reserved}");
        Ok(())
    }

    /// A body whose output partitions share one input. A task sends input
    /// batch `i` to output `targets[i]`, through a channel of capacity 1
    /// for each output. When the channel of one output is full, the task
    /// waits, so no other output gets a batch. A body that is only legal
    /// under the operator contract, as a custom operator can be.
    #[derive(Debug)]
    struct SharedInputExec {
        schema: SchemaRef,
        targets: Vec<usize>,
        cache: Arc<PlanProperties>,
        receivers: Mutex<Option<SharedInput>>,
    }

    /// The task that reads the shared input, and the receivers of the
    /// outputs that were not executed yet.
    struct SharedInput {
        _task: datafusion_common_runtime::SpawnedTask<()>,
        receivers: Vec<Option<tokio::sync::mpsc::Receiver<RecordBatch>>>,
    }

    impl fmt::Debug for SharedInput {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("SharedInput")
        }
    }

    impl SharedInputExec {
        /// Input batch `i` goes to output `i % partitions`.
        fn new(partitions: usize, batches: usize) -> Self {
            Self::with_targets(partitions, (0..batches).map(|i| i % partitions).collect())
        }

        fn with_targets(partitions: usize, targets: Vec<usize>) -> Self {
            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
            let cache = PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&schema)),
                Partitioning::UnknownPartitioning(partitions),
                EmissionType::Incremental,
                Boundedness::Bounded,
            );
            Self {
                schema,
                targets,
                cache: Arc::new(cache),
                receivers: Mutex::new(None),
            }
        }

        /// Start the task that reads the shared input, and return the
        /// receivers of the outputs.
        fn start(&self) -> SharedInput {
            let partitions = self.cache.output_partitioning().partition_count();
            let (senders, receivers): (Vec<_>, Vec<_>) = (0..partitions)
                .map(|_| tokio::sync::mpsc::channel(1))
                .unzip();
            let schema = Arc::clone(&self.schema);
            let targets = self.targets.clone();
            let task = datafusion_common_runtime::SpawnedTask::spawn(async move {
                for (i, target) in targets.into_iter().enumerate() {
                    let i = i as i32;
                    let values = Int32Array::from_iter_values(i * 100..(i + 1) * 100);
                    let batch =
                        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
                            .unwrap();
                    // An output that was dropped gets no more batches.
                    let _ = senders[target].send(batch).await;
                }
            });
            SharedInput {
                _task: task,
                receivers: receivers.into_iter().map(Some).collect(),
            }
        }
    }

    impl DisplayAs for SharedInputExec {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "SharedInputExec")
        }
    }

    impl ExecutionPlan for SharedInputExec {
        fn name(&self) -> &'static str {
            "SharedInputExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let mut receivers = self.receivers.lock();
            let input = receivers.get_or_insert_with(|| self.start());
            let Some(receiver) = input.receivers[partition].take() else {
                return internal_err!("partition {partition} was executed before");
            };
            let stream = futures::stream::unfold(receiver, |mut receiver| async move {
                let batch = receiver.recv().await?;
                Some((Ok(batch), receiver))
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            )))
        }
    }

    /// The two body partitions share one input, and a scan of one partition
    /// reads both. The input waits until body partition 1 takes its batch,
    /// so a scan that reads body partition 0 to its end first waits forever.
    /// The scan reads both at the same time, so this completes, also with a
    /// small memory pool.
    #[tokio::test]
    async fn body_partitions_that_share_an_input_do_not_deadlock() -> Result<()> {
        for memory_limit in [None, Some(20_000)] {
            let batches = 20 * (fanout::WINDOW + 1);
            let body = Arc::new(SharedInputExec::new(2, batches));
            let schema = body.schema();
            let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
            let scan = MaterializedCteScanExec::new(0, "c", schema, 1);
            let scan = Arc::new(scan.bind(Arc::clone(&buffer)));
            let plan = Arc::new(MaterializedCteExec::new(body, scan, buffer));
            let mut runtime = RuntimeEnvBuilder::new();
            if let Some(limit) = memory_limit {
                runtime = runtime.with_memory_limit(limit, 1.0);
            }
            let context =
                Arc::new(TaskContext::default().with_runtime(runtime.build_arc()?));
            let pool = Arc::clone(context.memory_pool());
            let rows = collect_all(Arc::clone(&plan) as _, context).await?;
            let expected: Vec<i32> = (0..batches as i32 * 100).collect();
            assert_eq!(sorted(&rows), expected);
            // The scan reads both body partitions at the same time, so they
            // stay in step and do not spill.
            let spilled = plan.metrics().unwrap().spill_count().unwrap_or(0);
            assert_eq!(spilled, 0);
            assert_eq!(pool.reserved(), 0);
        }
        Ok(())
    }

    /// Scan `x` has two partitions, and only partition 0 is executed, so no
    /// reader of `x` reads body partition 1. The shared input then waits for
    /// body partition 1. Scan `a` is executed later and reads body partition
    /// 1, so the body continues.
    #[tokio::test]
    async fn poll_of_a_late_scan_starts_the_producer() -> Result<()> {
        let batches = 20 * (fanout::WINDOW + 1);
        let body = Arc::new(SharedInputExec::new(2, batches));
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let x = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 2)
            .bind(Arc::clone(&buffer));
        let a = Arc::new(
            MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1)
                .bind(Arc::clone(&buffer)),
        );
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let context = Arc::new(TaskContext::default());
        let mut x0 = x.execute(0, Arc::clone(&context))?;
        x0.next().await.transpose()?.unwrap();
        // Let the producer of body partition 1 read ahead and wait.
        for _ in 0..1000 {
            tokio::task::yield_now().await;
        }
        let stream = a.execute(0, Arc::clone(&context))?;
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(10), collect(stream))
                .await
                .expect("the plan must not deadlock")?;
        assert_eq!(sorted(&read), (0..batches as i32 * 100).collect::<Vec<_>>());
        drop(x0);
        Ok(())
    }

    /// A reader reads to its end, and is not dropped yet. Under a hold, a new
    /// reader reuses its slot. The drop of the old reader must not end the
    /// new reader.
    #[tokio::test]
    async fn drop_of_a_finished_reader_does_not_end_the_reader_of_its_slot() -> Result<()>
    {
        let (plan, _) = counting_plan(3)?;
        let cte = plan.downcast_ref::<MaterializedCteExec>().unwrap();
        let context = Arc::new(TaskContext::default());
        let (output, _hold) = cte.buffer.materialize(&context, None)?;
        let _first = output.reader(0, 0).unwrap();
        let mut old = output.reader(0, 0).unwrap();
        let mut rows = 0;
        while let Some(batch) = old.next().await.transpose()? {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 3);
        let new = output.reader(0, 0).unwrap();
        drop(old);
        let read =
            collect(Box::pin(RecordBatchStreamAdapter::new(plan.schema(), new))).await?;
        assert_eq!(values(&read), vec![0, 1, 2]);
        Ok(())
    }

    /// One reader reads a body of 300 batches. The log retires each batch of
    /// the in-memory chunk when the reader has read it, not the whole chunk
    /// at its end.
    #[tokio::test]
    async fn one_reader_retires_each_batch_that_it_read() -> Result<()> {
        let (body, _, _) = streaming_body(30_000, 1)?;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1)
            .bind(Arc::clone(&buffer));
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let mut stream = scan.execute(0, Arc::clone(&context))?;
        let (mut rows, mut max_reserved) = (0, 0);
        while let Some(batch) = stream.next().await.transpose()? {
            rows += batch.num_rows();
            tokio::task::yield_now().await;
            max_reserved = max_reserved.max(pool.reserved());
        }
        assert_eq!(rows, 30_000);
        // 300 batches of about 500 bytes.
        assert!(max_reserved < 20_000, "reserved {max_reserved}");
        Ok(())
    }

    /// A hold keeps every batch. The drop of the hold retires the batches
    /// that the reader has read, before the reader reads again.
    #[tokio::test]
    async fn drop_of_a_hold_retires_the_batches_that_were_read() -> Result<()> {
        let (body, _, _) = streaming_body(20_000, 1)?;
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1)
            .bind(Arc::clone(&buffer));
        let _exec = MaterializedCteExec::new(
            body,
            Arc::new(EmptyExec::new(schema)),
            Arc::clone(&buffer),
        );
        let context = Arc::new(TaskContext::default());
        let pool = Arc::clone(context.memory_pool());
        let (_, hold) = buffer.materialize(&context, None)?;
        let mut stream = scan.execute(0, Arc::clone(&context))?;
        for _ in 0..100 {
            stream.next().await.transpose()?.unwrap();
        }
        let held = pool.reserved();
        drop(hold);
        let after = pool.reserved();
        assert!(after * 10 < held, "held {held}, after {after}");
        Ok(())
    }

    /// A batch of 4 MB is spilled. It is written in two pieces of about
    /// 2 MiB, and a reader reads the pieces back in order. The row count is
    /// odd, so the last piece is shorter.
    #[tokio::test]
    async fn large_batch_is_spilled_in_pieces_and_read_in_order() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = |range: std::ops::Range<i32>| {
            let values = Int32Array::from_iter_values(range);
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)])
        };
        let parts = vec![vec![
            batch(0..100)?,
            batch(100..1_000_101)?,
            batch(1_000_101..1_000_201)?,
        ]];
        let body = TestMemoryExec::try_new_exec(&parts, Arc::clone(&schema), None)?;
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1);
        let scans = [
            scan.bind(Arc::clone(&buffer)),
            scan.bind(Arc::clone(&buffer)),
        ];
        let exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(6_000_000, 1.0)
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let pool = Arc::clone(context.memory_pool());
        let b = scans[1].execute(0, Arc::clone(&context))?;
        let read = async {
            let a = collect(scans[0].execute(0, Arc::clone(&context))?).await?;
            Result::<_>::Ok((a, collect(b).await?))
        };
        let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(20), read)
            .await
            .expect("the read must not hang")?;
        let expected: Vec<i32> = (0..1_000_201).collect();
        assert_eq!(values(&a), expected);
        assert_eq!(values(&b), expected);
        // The reader of the spill file returns the pieces. The large batch
        // has 4_000_004 bytes of values, so it has two pieces.
        let rows: Vec<usize> = b.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(rows, vec![100, 500_001, 500_000, 100]);
        let spilled = exec.metrics().unwrap().spill_count().unwrap_or(0);
        assert!(spilled > 0, "the body did not spill");
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    /// A scan of one partition reads a body of two partitions to its end.
    /// The scan reads both at the same time, so neither body partition is
    /// kept whole: with a small pool nothing spills, and with an unbounded
    /// pool only a few batches are in memory.
    #[tokio::test]
    async fn scan_with_fewer_partitions_than_the_body_keeps_a_few_batches() -> Result<()>
    {
        for memory_limit in [Some(1_000_000), None] {
            let (body, _, _) = streaming_body(200_000, 2)?;
            let schema = body.schema();
            let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
            let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 1)
                .bind(Arc::clone(&buffer));
            let exec =
                MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
            let mut runtime = RuntimeEnvBuilder::new();
            if let Some(limit) = memory_limit {
                runtime = runtime.with_memory_limit(limit, 1.0);
            }
            let context =
                Arc::new(TaskContext::default().with_runtime(runtime.build_arc()?));
            let pool = Arc::clone(context.memory_pool());
            let mut stream = scan.execute(0, Arc::clone(&context))?;
            let (mut rows, mut max_reserved) = (0, 0);
            while let Some(batch) = stream.next().await.transpose()? {
                rows += batch.num_rows();
                max_reserved = max_reserved.max(pool.reserved());
            }
            assert_eq!(rows, 200_000);
            // 2000 batches of about 500 bytes, 1 MB in all.
            assert!(max_reserved < 20_000, "reserved {max_reserved}");
            let spilled = exec.metrics().unwrap().spill_count().unwrap_or(0);
            assert_eq!(spilled, 0);
        }
        Ok(())
    }

    /// The body stream panics after one batch. The panic is an error of the
    /// reader, not an early end of the output.
    #[tokio::test]
    async fn panic_of_the_body_stream_goes_to_the_reader() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let body = Arc::new(
            crate::test::exec::PanicExec::new(Arc::clone(&schema), 1)
                .with_partition_panic(0, 1),
        );
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", schema, 1);
        let scan = Arc::new(scan.bind(Arc::clone(&buffer)));
        let plan = Arc::new(MaterializedCteExec::new(body, scan, buffer));
        let error = collect_all(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("panicked"), "{error}");
        Ok(())
    }

    /// The body partitions share one input, and body partition 0 gets two
    /// batches. Scan partition 0 is executed and dropped before it polls.
    /// The fan-out must drop the body stream that it did not start, so the
    /// input continues, and body partition 1 ends.
    #[tokio::test]
    async fn drop_of_an_unpolled_scan_drops_its_body_stream() -> Result<()> {
        let body = Arc::new(SharedInputExec::with_targets(2, vec![0, 0]));
        let schema = body.schema();
        let buffer = Arc::new(MaterializedCteBuffer::new(0, "c"));
        let scan = MaterializedCteScanExec::new(0, "c", Arc::clone(&schema), 2)
            .bind(Arc::clone(&buffer));
        let _exec =
            MaterializedCteExec::new(body, Arc::new(EmptyExec::new(schema)), buffer);
        let context = Arc::new(TaskContext::default());
        drop(scan.execute(0, Arc::clone(&context))?);
        let stream = scan.execute(1, Arc::clone(&context))?;
        let read =
            tokio::time::timeout(std::time::Duration::from_secs(10), collect(stream))
                .await
                .expect("the plan must not deadlock")?;
        assert!(read.is_empty());
        Ok(())
    }
}
