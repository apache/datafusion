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

//! Test-only mocks for exercising `FileStream`'s morsel scheduler.
//!
//! These utilities let tests describe morsel-planning behavior directly,
//! without depending on a particular file format implementation. They are used
//! to verify:
//! - the sequence in which `FileStream` calls `morselize`, `plan`, and polls I/O
//! - the order in which morsels and child planners are consumed
//! - the eventual order of `RecordBatch` output produced by the scheduler

use crate::PartitionedFile;
use crate::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{Result, internal_err};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, Stream};
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Display, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

/// Identifier for a mock planner in test traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerId(pub usize);

/// Identifier for a mock morsel in test traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MorselId(pub usize);

/// Identifier for a mock I/O future in test traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoFutureId(pub usize);

/// Identifier for a produced batch in test traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchId(pub usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MorselEvent {
    /// `FileStream` asked the `Morselizer` to start work for a file.
    MorselizeFile { path: String },
    /// A root planner was created from a test specification.
    PlannerCreated { planner_id: PlannerId },
    /// `MorselPlanner::plan()` was invoked.
    PlannerPlanCalled { planner_id: PlannerId },
    /// A planner returned one or more child planners.
    PlannerProducedChild {
        planner_id: PlannerId,
        child_planner_id: PlannerId,
    },
    /// A planner returned an I/O future.
    IoFutureCreated {
        planner_id: PlannerId,
        io_future_id: IoFutureId,
    },
    /// `FileStream` polled a planner's outstanding I/O future.
    IoFuturePolled {
        planner_id: PlannerId,
        io_future_id: IoFutureId,
    },
    /// A planner's I/O future completed successfully.
    IoFutureResolved {
        planner_id: PlannerId,
        io_future_id: IoFutureId,
    },
    /// A planner produced a morsel that is ready for CPU work.
    MorselProduced {
        planner_id: PlannerId,
        morsel_id: MorselId,
    },
    /// `FileStream` began reading a morsel.
    MorselStreamStarted { morsel_id: MorselId },
    /// A morsel stream yielded one batch.
    MorselStreamBatchProduced {
        morsel_id: MorselId,
        batch_id: BatchId,
    },
    /// A morsel stream reached EOF.
    MorselStreamFinished { morsel_id: MorselId },
}

/// Observer of `MorselEvent`s emitted by the test harness.
#[derive(Debug, Default, Clone)]
pub struct MorselObserver {
    events: Arc<Mutex<Vec<MorselEvent>>>,
}

impl MorselObserver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Clear any previously buffered events
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// Push a new [`MorselEvent`]
    pub fn push(&self, event: MorselEvent) {
        self.events.lock().unwrap().push(event);
    }

    /// Return a copy of the current list of [`MorselEvents`]
    pub fn events(&self) -> Vec<MorselEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Format the recorded events as a stable, human-readable snapshot.
    ///
    /// We prefer snapshotting the event trace in tests rather than asserting it
    /// programmatically because `FileStream` scheduling behavior is easier to
    /// review as a full ordered trace. When the scheduler changes, updating the
    /// snapshot is typically simpler and more informative than rebuilding a
    /// hand-authored sequence of enum constructors.
    pub fn format_events(&self) -> String {
        self.events()
            .into_iter()
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Format only the high-level scheduler events.
    ///
    /// This is useful for tests where the exact `plan()` / `poll()` interleave
    /// is not the main point of the assertion. Summary snapshots are easier to
    /// review when validating broader behavior such as ordering or outstanding
    /// I/O limits.
    pub fn format_summary_events(&self) -> String {
        self.events()
            .into_iter()
            .filter(MorselEvent::is_summary_event)
            .map(|event| event.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl MorselEvent {
    /// Return true for the higher-level events that are most useful in compact
    /// scheduler snapshots.
    pub fn is_summary_event(&self) -> bool {
        matches!(
            self,
            MorselEvent::MorselizeFile { .. }
                | MorselEvent::PlannerCreated { .. }
                | MorselEvent::PlannerProducedChild { .. }
                | MorselEvent::IoFutureCreated { .. }
                | MorselEvent::IoFutureResolved { .. }
                | MorselEvent::MorselProduced { .. }
                | MorselEvent::MorselStreamBatchProduced { .. }
        )
    }
}

/// Test [`Morselizer`] that maps file paths to a fixed set of planner specs.
///
/// This lets tests describe file-level morselization behavior without any real
/// file metadata or object-store access.
#[derive(Debug, Clone)]
pub struct MockMorselizer {
    observer: MorselObserver,
    files: HashMap<String, MockPlanner>,
}

impl MockMorselizer {
    pub fn new() -> Self {
        Self {
            observer: MorselObserver::new(),
            files: HashMap::new(),
        }
    }

    /// Return a reference to the observer
    pub fn observer(&self) -> &MorselObserver {
        &self.observer
    }

    /// Add the description of how a file will be planned
    pub fn with_file(mut self, path: impl Into<String>, planner: MockPlanner) -> Self {
        self.files.insert(path.into(), planner);
        self
    }
}

impl Morselizer for MockMorselizer {
    fn morselize(&self, file: PartitionedFile) -> Result<Vec<Box<dyn MorselPlanner>>> {
        let path = file.object_meta.location.to_string();
        self.observer
            .push(MorselEvent::MorselizeFile { path: path.clone() });

        Ok(self
            .files
            .get(&path)
            .cloned()
            .into_iter()
            .map(|planner| {
                self.observer.push(MorselEvent::PlannerCreated {
                    planner_id: planner.planner_id,
                });
                Box::new(MockMorselPlanner::new(self.observer.clone(), planner))
                    as Box<dyn MorselPlanner>
            })
            .collect())
    }
}

/// Steps for a MockPlanner
///
/// Tests build these specs up front, and `MockMorselizer` turns them into real
/// planners when `FileStream` starts work on a file.
#[derive(Debug, Clone)]
pub struct MockPlanner {
    planner_id: PlannerId,
    steps: Vec<PlannerStep>,
}

impl MockPlanner {
    /// Create a fluent builder for a planner specification.
    pub fn builder() -> MockPlannerBuilder {
        MockPlannerBuilder::default()
    }
}

/// One scheduler-visible step in a mock planner's lifecycle.
///
/// A single step can produce morsels, child planners, and an I/O future, which
/// makes it possible to model the generic `MorselPlan` API closely in tests.
#[derive(Debug, Clone)]
pub enum PlannerStep {
    Plan {
        morsels: Vec<MockMorselSpec>,
        planners: Vec<MockPlanner>,
        /// Optional identifier for the I/O future returned by this step.
        ///
        /// Tests use this to assert the ordering of multiple outstanding I/O
        /// phases. It must be `Some` when `io_polls > 0`.
        io_future_id: Option<IoFutureId>,
        io_polls: usize,
    },
    None,
    Error {
        message: String,
    },
}

/// Builder for [`MockPlanner`].
///
/// This keeps `FileStream` scheduler tests readable as they grow to include
/// child planners and multiple I/O phases.
#[derive(Debug, Default)]
pub struct MockPlannerBuilder {
    planner_id: Option<PlannerId>,
    steps: Vec<PlannerStep>,
}

impl MockPlannerBuilder {
    pub fn with_id(mut self, planner_id: PlannerId) -> Self {
        self.planner_id = Some(planner_id);
        self
    }

    pub fn return_morsel(self, morsel_id: MorselId, batch_id: i32) -> Self {
        self.return_plan(
            ReturnPlanBuilder::new()
                .with_morsel(MockMorselSpec::single_batch(morsel_id, batch_id)),
        )
    }

    pub fn return_plan(mut self, plan: ReturnPlanBuilder) -> Self {
        self.steps.push(plan.build());
        self
    }

    pub fn return_none(mut self) -> Self {
        self.steps.push(PlannerStep::None);
        self
    }

    pub fn return_error(mut self, message: impl Into<String>) -> Self {
        self.steps.push(PlannerStep::Error {
            message: message.into(),
        });
        self
    }

    pub fn build(self) -> MockPlanner {
        MockPlanner {
            planner_id: self
                .planner_id
                .expect("MockPlannerBuilder requires planner_id"),
            steps: self.steps,
        }
    }
}

/// Builder for `PlannerStep::ReturnPlan`.
#[derive(Debug, Default)]
pub struct ReturnPlanBuilder {
    /// Morsels that should be returned immediately by this planner step.
    morsels: Vec<MockMorselSpec>,
    /// Child planners that should be returned immediately by this planner step.
    planners: Vec<MockPlanner>,
    /// Identifier for the mock I/O future returned by this step, if any.
    io_future_id: Option<IoFutureId>,
    /// Number of `Poll::Pending` results the mock I/O future should yield
    /// before resolving successfully.
    ///
    /// This is a deterministic test-only knob. It does not model elapsed time
    /// or bytes read; it only controls how many scheduler polls are required
    /// before the mock I/O future becomes ready.
    io_polls: usize,
}

impl ReturnPlanBuilder {
    /// Create an empty return-plan builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a single-batch morsel that should be returned immediately by this
    /// planner step.
    pub fn return_morsel(self, morsel_id: MorselId, batch_id: i32) -> Self {
        self.with_morsel(MockMorselSpec::single_batch(morsel_id, batch_id))
    }

    /// Add a morsel that should be returned immediately by this planner step.
    pub fn with_morsel(mut self, morsel: MockMorselSpec) -> Self {
        self.morsels.push(morsel);
        self
    }

    /// Add a child planner that should be returned immediately by this step.
    pub fn with_planner(mut self, planner: MockPlanner) -> Self {
        self.planners.push(planner);
        self
    }

    /// Return a mock I/O future from this step.
    ///
    /// `io_future_id` is recorded in the emitted `MorselEvent`s so tests can
    /// distinguish multiple I/O phases from the same planner.
    ///
    /// `io_polls` controls how many times that future returns `Poll::Pending`
    /// before it resolves with `Poll::Ready(Ok(()))`.
    ///
    /// For example, `with_io(id, 1)` means:
    /// - first poll: `Poll::Pending`
    /// - second poll: `Poll::Ready(Ok(()))`
    ///
    /// So the total number of polls observed in the trace is `io_polls + 1`.
    pub fn with_io(mut self, io_future_id: IoFutureId, io_polls: usize) -> Self {
        self.io_future_id = Some(io_future_id);
        self.io_polls = io_polls;
        self
    }

    /// Build the corresponding [`PlannerStep::Plan`]
    pub fn build(self) -> PlannerStep {
        PlannerStep::Plan {
            morsels: self.morsels,
            planners: self.planners,
            io_future_id: self.io_future_id,
            io_polls: self.io_polls,
        }
    }
}

/// Declarative description of a mock morsel and the batches it should yield.
///
/// Each batch id is turned into a one-row `RecordBatch`, which makes output
/// order easy to assert in `FileStream` tests.
#[derive(Debug, Clone)]
pub struct MockMorselSpec {
    morsel_id: MorselId,
    batch_ids: Vec<i32>,
}

impl MockMorselSpec {
    pub fn single_batch(morsel_id: MorselId, batch_id: i32) -> Self {
        Self {
            morsel_id,
            batch_ids: vec![batch_id],
        }
    }
}

/// Concrete `MorselPlanner` used by `FileStream` tests.
///
/// It consumes a queue of `PlannerStep`s so tests can deterministically control
/// when a planner emits morsels, yields child planners, blocks on I/O, or
/// finishes.
struct MockMorselPlanner {
    observer: MorselObserver,
    planner_id: PlannerId,
    steps: VecDeque<PlannerStep>,
}

impl MockMorselPlanner {
    fn new(observer: MorselObserver, spec: MockPlanner) -> Self {
        Self {
            observer,
            planner_id: spec.planner_id,
            steps: spec.steps.into(),
        }
    }
}

impl Debug for MockMorselPlanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockMorselPlanner")
            .field("planner_id", &self.planner_id)
            .finish()
    }
}

impl MorselPlanner for MockMorselPlanner {
    fn plan(&mut self) -> Result<Option<MorselPlan>> {
        self.observer.push(MorselEvent::PlannerPlanCalled {
            planner_id: self.planner_id,
        });

        let Some(step) = self.steps.pop_front() else {
            return Ok(None);
        };

        match step {
            PlannerStep::Plan {
                morsels,
                planners,
                io_future_id,
                io_polls,
            } => {
                let mut plan = MorselPlan::new();

                if !morsels.is_empty() {
                    for morsel in &morsels {
                        self.observer.push(MorselEvent::MorselProduced {
                            planner_id: self.planner_id,
                            morsel_id: morsel.morsel_id,
                        });
                    }
                    plan = plan.with_morsels(
                        morsels
                            .into_iter()
                            .map(|morsel| {
                                Box::new(MockMorsel::new(self.observer.clone(), morsel))
                                    as Box<dyn Morsel>
                            })
                            .collect(),
                    );
                }

                if !planners.is_empty() {
                    for planner in &planners {
                        self.observer.push(MorselEvent::PlannerProducedChild {
                            planner_id: self.planner_id,
                            child_planner_id: planner.planner_id,
                        });
                    }
                    plan = plan.with_planners(
                        planners
                            .into_iter()
                            .map(|planner| {
                                Box::new(MockMorselPlanner::new(
                                    self.observer.clone(),
                                    planner,
                                ))
                                    as Box<dyn MorselPlanner>
                            })
                            .collect(),
                    );
                }

                if io_polls > 0 {
                    let io_future_id = io_future_id.expect(
                        "PlannerStep::ReturnPlan with io_polls > 0 must specify io_future_id",
                    );
                    self.observer.push(MorselEvent::IoFutureCreated {
                        planner_id: self.planner_id,
                        io_future_id,
                    });
                    plan = plan.with_io_future(Box::pin(MockIoFuture::new(
                        self.observer.clone(),
                        self.planner_id,
                        io_future_id,
                        io_polls,
                    ))
                        as BoxFuture<'static, Result<()>>);
                }

                Ok(Some(plan))
            }
            PlannerStep::None => Ok(None),
            PlannerStep::Error { message } => internal_err!("{message}"),
        }
    }
}

/// Concrete `Morsel` used by the test harness.
///
/// It yields a deterministic sequence of one-row batches and records lifecycle
/// events so tests can correlate scheduler activity with produced output.
struct MockMorsel {
    observer: MorselObserver,
    morsel_id: MorselId,
    batch_ids: Vec<i32>,
}

impl MockMorsel {
    fn new(observer: MorselObserver, spec: MockMorselSpec) -> Self {
        Self {
            observer,
            morsel_id: spec.morsel_id,
            batch_ids: spec.batch_ids,
        }
    }
}

impl Debug for MockMorsel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockMorsel")
            .field("morsel_id", &self.morsel_id)
            .finish()
    }
}

impl Morsel for MockMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.observer.push(MorselEvent::MorselStreamStarted {
            morsel_id: self.morsel_id,
        });
        Box::pin(MockMorselStream::new(
            self.observer.clone(),
            self.morsel_id,
            self.batch_ids,
        ))
    }

    fn split(&mut self) -> Result<Vec<Box<dyn Morsel>>> {
        Ok(vec![])
    }
}

/// Stream returned by `MockMorsel::into_stream`.
///
/// This stream exists so tests can observe exactly when a morsel starts,
/// produces batches, and finishes.
struct MockMorselStream {
    observer: MorselObserver,
    morsel_id: MorselId,
    batches: VecDeque<i32>,
    finished: bool,
}

impl MockMorselStream {
    fn new(observer: MorselObserver, morsel_id: MorselId, batch_ids: Vec<i32>) -> Self {
        Self {
            observer,
            morsel_id,
            batches: batch_ids.into(),
            finished: false,
        }
    }
}

impl Stream for MockMorselStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(batch_id) = self.batches.pop_front() {
            self.observer.push(MorselEvent::MorselStreamBatchProduced {
                morsel_id: self.morsel_id,
                batch_id: BatchId(batch_id as usize),
            });
            return Poll::Ready(Some(Ok(single_value_batch(batch_id))));
        }

        if !self.finished {
            self.finished = true;
            self.observer.push(MorselEvent::MorselStreamFinished {
                morsel_id: self.morsel_id,
            });
        }

        Poll::Ready(None)
    }
}

/// Deterministic future used to simulate planner I/O in tests.
///
/// It resolves after a configured number of pending polls and self-wakes so
/// `FileStream` can make forward progress without timers or real async I/O.
struct MockIoFuture {
    observer: MorselObserver,
    planner_id: PlannerId,
    io_future_id: IoFutureId,
    pending_polls_remaining: usize,
}

impl MockIoFuture {
    fn new(
        observer: MorselObserver,
        planner_id: PlannerId,
        io_future_id: IoFutureId,
        pending_polls: usize,
    ) -> Self {
        Self {
            observer,
            planner_id,
            io_future_id,
            pending_polls_remaining: pending_polls,
        }
    }
}

impl Future for MockIoFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.observer.push(MorselEvent::IoFuturePolled {
            planner_id: self.planner_id,
            io_future_id: self.io_future_id,
        });
        if self.pending_polls_remaining > 0 {
            self.pending_polls_remaining -= 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        self.observer.push(MorselEvent::IoFutureResolved {
            planner_id: self.planner_id,
            io_future_id: self.io_future_id,
        });
        Poll::Ready(Ok(()))
    }
}

fn single_value_batch(value: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))]).unwrap()
}

impl Display for MorselEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MorselEvent::MorselizeFile { path } => write!(f, "morselize_file: {path}"),
            MorselEvent::PlannerCreated { planner_id } => {
                write!(f, "planner_created: {planner_id:?}")
            }
            MorselEvent::PlannerPlanCalled { planner_id } => {
                write!(f, "planner_called: {planner_id:?}")
            }
            MorselEvent::PlannerProducedChild {
                planner_id,
                child_planner_id,
            } => write!(
                f,
                "planner_produced_child: {planner_id:?} -> {child_planner_id:?}"
            ),
            MorselEvent::IoFutureCreated {
                planner_id,
                io_future_id,
            } => write!(f, "io_future_created: {planner_id:?}, {io_future_id:?}"),
            MorselEvent::IoFuturePolled {
                planner_id,
                io_future_id,
            } => write!(f, "io_future_polled: {planner_id:?}, {io_future_id:?}"),
            MorselEvent::IoFutureResolved {
                planner_id,
                io_future_id,
            } => write!(f, "io_future_resolved: {planner_id:?}, {io_future_id:?}"),
            MorselEvent::MorselProduced {
                planner_id,
                morsel_id,
            } => write!(f, "morsel_produced: {planner_id:?}, {morsel_id:?}"),
            MorselEvent::MorselStreamStarted { morsel_id } => {
                write!(f, "morsel_stream_started: {morsel_id:?}")
            }
            MorselEvent::MorselStreamBatchProduced {
                morsel_id,
                batch_id,
            } => {
                write!(
                    f,
                    "morsel_stream_batch_produced: {morsel_id:?}, {batch_id:?}"
                )
            }
            MorselEvent::MorselStreamFinished { morsel_id } => {
                write!(f, "morsel_stream_finished: {morsel_id:?}")
            }
        }
    }
}
