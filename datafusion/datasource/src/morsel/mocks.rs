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

//! Test-only mocks for exercising the morsel-driven `FileStream` scheduler.

use std::collections::{HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::PartitionedFile;
use crate::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{DataFusionError, Result, internal_datafusion_err};
use futures::stream::BoxStream;
use futures::{Future, FutureExt};

// Use thin wrappers around usize so the test setups are more explicit

/// Identifier for a mock morsel in scheduler snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct MorselId(pub usize);

/// Identifier for a produced batch in scheduler snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct BatchId(pub usize);

/// Identifier for a mock I/O future in scheduler snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IoFutureId(pub usize);

/// Number of pending polls before a mock I/O future resolves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PollsToResolve(pub usize);

/// Error message returned by a mock planner or I/O future.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MockError(pub String);

impl Display for MockError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for MockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Scheduler-visible event captured by the mock morsel test harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MorselEvent {
    MorselizeFile {
        path: String,
    },
    PlannerCreated {
        planner_name: String,
    },
    PlannerCalled {
        planner_name: String,
    },
    IoFutureCreated {
        planner_name: String,
        io_future_id: IoFutureId,
    },
    IoFuturePolled {
        planner_name: String,
        io_future_id: IoFutureId,
    },
    IoFutureResolved {
        planner_name: String,
        io_future_id: IoFutureId,
    },
    IoFutureErrored {
        planner_name: String,
        io_future_id: IoFutureId,
        message: String,
    },
    MorselProduced {
        planner_name: String,
        morsel_id: MorselId,
    },
    MorselStreamStarted {
        morsel_id: MorselId,
    },
    MorselStreamBatchProduced {
        morsel_id: MorselId,
        batch_id: BatchId,
    },
    MorselStreamFinished {
        morsel_id: MorselId,
    },
}

impl Display for MorselEvent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MorselEvent::MorselizeFile { path } => {
                write!(f, "morselize_file: {path}")
            }
            MorselEvent::PlannerCreated { planner_name } => {
                write!(f, "planner_created: {planner_name}")
            }
            MorselEvent::PlannerCalled { planner_name } => {
                write!(f, "planner_called: {planner_name}")
            }
            MorselEvent::IoFutureCreated {
                planner_name,
                io_future_id,
            } => write!(f, "io_future_created: {planner_name}, {io_future_id:?}"),
            MorselEvent::IoFuturePolled {
                planner_name,
                io_future_id,
            } => write!(f, "io_future_polled: {planner_name}, {io_future_id:?}"),
            MorselEvent::IoFutureResolved {
                planner_name,
                io_future_id,
            } => write!(f, "io_future_resolved: {planner_name}, {io_future_id:?}"),
            MorselEvent::IoFutureErrored {
                planner_name,
                io_future_id,
                message,
            } => write!(
                f,
                "io_future_errored: {planner_name}, {io_future_id:?}, {message}"
            ),
            MorselEvent::MorselProduced {
                planner_name,
                morsel_id,
            } => write!(f, "morsel_produced: {planner_name}, {morsel_id:?}"),
            MorselEvent::MorselStreamStarted { morsel_id } => {
                write!(f, "morsel_stream_started: {morsel_id:?}")
            }
            MorselEvent::MorselStreamBatchProduced {
                morsel_id,
                batch_id,
            } => write!(
                f,
                "morsel_stream_batch_produced: {morsel_id:?}, {batch_id:?}"
            ),
            MorselEvent::MorselStreamFinished { morsel_id } => {
                write!(f, "morsel_stream_finished: {morsel_id:?}")
            }
        }
    }
}

/// Shared observer that records scheduler events for snapshot tests.
#[derive(Debug, Default, Clone)]
pub(crate) struct MorselObserver {
    events: Arc<Mutex<Vec<MorselEvent>>>,
}

impl MorselObserver {
    /// Clears any previously recorded events.
    pub(crate) fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    /// Records one new scheduler event.
    pub(crate) fn push(&self, event: MorselEvent) {
        self.events.lock().unwrap().push(event);
    }

    /// Formats all recorded events into a stable, snapshot-friendly trace.
    pub(crate) fn format_events(&self) -> String {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// Declarative planner spec used by the mock morselizer.
#[derive(Debug, Clone)]
pub(crate) struct MockPlanner {
    file_path: String,
    steps: VecDeque<PlannerStep>,
}

impl MockPlanner {
    /// Creates a fluent builder for one mock planner.
    pub(crate) fn builder(file_path: impl Into<String>) -> MockPlannerBuilder {
        MockPlannerBuilder {
            file_path: file_path.into(),
            ..Default::default()
        }
    }

    /// Returns the file path associated with this planner.
    pub(crate) fn file_path(&self) -> &str {
        &self.file_path
    }
}

/// One scheduler-visible result from calling `MorselPlanner::plan`.
#[derive(Debug, Clone)]
enum PlannerStep {
    Plan {
        morsels: Vec<MockMorselSpec>,
        ready_planners: Vec<MockPlanner>,
        pending_planner: Option<MockPendingPlanner>,
    },
    Error {
        error: MockError,
    },
    None,
}

/// One mock morsel returned from a planning step.
#[derive(Debug, Clone)]
struct MockMorselSpec {
    morsel_id: MorselId,
    batch_ids: Vec<i32>,
}

/// One pending planner I/O future returned from a planning step.
#[derive(Debug, Clone)]
struct MockPendingPlanner {
    io_future_id: IoFutureId,
    polls_to_resolve: PollsToResolve,
    result: std::result::Result<(), MockError>,
}

/// Builder for one mock `PlannerStep::Plan`.
#[derive(Debug, Default)]
pub(crate) struct MockPlanBuilder {
    morsels: Vec<MockMorselSpec>,
    ready_planners: Vec<MockPlanner>,
    pending_planner: Option<MockPendingPlanner>,
}

impl MockPlanBuilder {
    /// Create an empty mock plan.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Add one ready morsel with a single batch.
    pub(crate) fn with_morsel(mut self, morsel_id: MorselId, batch_id: i32) -> Self {
        self.morsels.push(MockMorselSpec {
            morsel_id,
            batch_ids: vec![batch_id],
        });
        self
    }

    /// Add one ready morsel with multiple batches.
    pub(crate) fn with_morsel_batches(
        mut self,
        morsel_id: MorselId,
        batch_ids: Vec<i32>,
    ) -> Self {
        self.morsels.push(MockMorselSpec {
            morsel_id,
            batch_ids,
        });
        self
    }

    /// Add a pending planner I/O future produced by this planning step.
    pub(crate) fn with_pending_planner(
        mut self,
        io_future_id: IoFutureId,
        polls_to_resolve: PollsToResolve,
        result: std::result::Result<(), MockError>,
    ) -> Self {
        self.pending_planner = Some(MockPendingPlanner {
            io_future_id,
            polls_to_resolve,
            result,
        });
        self
    }

    /// Add a ready child planner
    pub(crate) fn with_ready_planner(
        self,
        ready_planner: impl Into<MockPlanner>,
    ) -> Self {
        self.with_ready_planners(vec![ready_planner.into()])
    }

    /// Add ready child planners produced by this planning step.
    pub(crate) fn with_ready_planners(
        mut self,
        ready_planners: Vec<MockPlanner>,
    ) -> Self {
        self.ready_planners.extend(ready_planners);
        self
    }

    /// Build the planner step.
    fn build(self) -> PlannerStep {
        PlannerStep::Plan {
            morsels: self.morsels,
            ready_planners: self.ready_planners,
            pending_planner: self.pending_planner,
        }
    }
}

/// Builder for a planning step that only returns a pending planner.
#[derive(Debug, Clone)]
pub(crate) struct PendingPlannerBuilder {
    io_future_id: IoFutureId,
    polls_to_resolve: PollsToResolve,
    result: std::result::Result<(), MockError>,
}

impl From<PendingPlannerBuilder> for MockPlanBuilder {
    fn from(builder: PendingPlannerBuilder) -> Self {
        builder.build()
    }
}

impl PendingPlannerBuilder {
    /// Create a pending-planner step with a successful I/O future.
    pub(crate) fn new(io_future_id: IoFutureId) -> Self {
        Self {
            io_future_id,
            polls_to_resolve: PollsToResolve(0),
            result: Ok(()),
        }
    }

    /// Configure how many pending polls occur before the I/O future resolves.
    pub(crate) fn with_polls_to_resolve(
        mut self,
        polls_to_resolve: PollsToResolve,
    ) -> Self {
        self.polls_to_resolve = polls_to_resolve;
        self
    }

    /// Configure a failing I/O future for this pending planner.
    pub(crate) fn with_error(mut self, message: impl Into<String>) -> Self {
        self.result = Err(MockError(message.into()));
        self
    }

    /// Build a `MockPlanBuilder` containing only this pending planner.
    pub(crate) fn build(self) -> MockPlanBuilder {
        MockPlanBuilder::new().with_pending_planner(
            self.io_future_id,
            self.polls_to_resolve,
            self.result,
        )
    }
}

/// Fluent builder for [`MockPlanner`] test specs.
#[derive(Debug, Default)]
pub(crate) struct MockPlannerBuilder {
    file_path: String,
    steps: Vec<PlannerStep>,
}

impl From<MockPlannerBuilder> for MockPlanner {
    fn from(value: MockPlannerBuilder) -> Self {
        value.build()
    }
}

impl MockPlannerBuilder {
    pub(crate) fn add_plan(mut self, builder: impl Into<MockPlanBuilder>) -> Self {
        let builder = builder.into();
        self.steps.push(builder.build());
        self
    }

    /// Adds one planning step that reports the planner is exhausted.
    pub(crate) fn return_none(mut self) -> Self {
        self.steps.push(PlannerStep::None);
        self
    }

    /// Adds one planning step that fails during CPU planning.
    pub(crate) fn return_error(mut self, message: impl Into<String>) -> Self {
        self.steps.push(PlannerStep::Error {
            error: MockError(message.into()),
        });
        self
    }

    /// Finalizes the configured mock planner.
    pub(crate) fn build(self) -> MockPlanner {
        let Self { file_path, steps } = self;

        MockPlanner {
            file_path,
            steps: VecDeque::from(steps),
        }
    }
}

/// Mock [`Morselizer`] that maps file paths to fixed planner specs.
#[derive(Debug, Clone, Default)]
pub(crate) struct MockMorselizer {
    observer: MorselObserver,
    files: HashMap<String, MockPlanner>,
}

impl MockMorselizer {
    /// Creates an empty mock morselizer.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns the shared event observer for this test harness.
    pub(crate) fn observer(&self) -> &MorselObserver {
        &self.observer
    }

    /// Specify the return planner for the specified file_path
    pub(crate) fn with_planner(mut self, planner: impl Into<MockPlanner>) -> Self {
        let planner = planner.into();
        self.files.insert(planner.file_path.clone(), planner);
        self
    }
}

impl Morselizer for MockMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        let path = file.object_meta.location.to_string();
        self.observer
            .push(MorselEvent::MorselizeFile { path: path.clone() });

        let planner = self.files.get(&path).cloned().ok_or_else(|| {
            internal_datafusion_err!("No mock planner configured for file: {path}")
        })?;

        self.observer.push(MorselEvent::PlannerCreated {
            planner_name: planner.file_path.clone(),
        });

        Ok(Box::new(MockMorselPlanner::new(
            self.observer.clone(),
            planner,
        )))
    }
}

/// Concrete mock planner that executes one predefined step per `plan()` call.
#[derive(Debug)]
struct MockMorselPlanner {
    observer: MorselObserver,
    planner_name: String,
    steps: VecDeque<PlannerStep>,
}

impl MockMorselPlanner {
    /// Creates a concrete planner from its declarative test spec.
    fn new(observer: MorselObserver, planner: MockPlanner) -> Self {
        Self {
            observer,
            planner_name: planner.file_path,
            steps: planner.steps,
        }
    }
}

/// Rebuilds the mock planner continuation after one step completes.
fn current_planner_continuation(
    observer: MorselObserver,
    planner_name: String,
    steps: VecDeque<PlannerStep>,
) -> Vec<Box<dyn MorselPlanner>> {
    let only_none_remaining =
        matches!(steps.front(), Some(PlannerStep::None)) && steps.len() == 1;

    if steps.is_empty() || only_none_remaining {
        Vec::new()
    } else {
        vec![Box::new(MockMorselPlanner {
            observer,
            planner_name,
            steps,
        }) as Box<dyn MorselPlanner>]
    }
}

/// Create any child planners produced by this planning step.
fn child_planners(
    observer: MorselObserver,
    ready_planners: Vec<MockPlanner>,
) -> Vec<Box<dyn MorselPlanner>> {
    ready_planners
        .into_iter()
        .map(|planner| {
            observer.push(MorselEvent::PlannerCreated {
                planner_name: planner.file_path.clone(),
            });
            Box::new(MockMorselPlanner::new(observer.clone(), planner))
                as Box<dyn MorselPlanner>
        })
        .collect()
}

impl MorselPlanner for MockMorselPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self {
            observer,
            planner_name,
            mut steps,
        } = *self;

        observer.push(MorselEvent::PlannerCalled {
            planner_name: planner_name.clone(),
        });

        let Some(step) = steps.pop_front() else {
            return Ok(None);
        };

        match step {
            PlannerStep::Plan {
                morsels,
                ready_planners,
                pending_planner,
            } => {
                let mut ready_morsels = Vec::new();
                for MockMorselSpec {
                    morsel_id,
                    batch_ids,
                } in morsels
                {
                    observer.push(MorselEvent::MorselProduced {
                        planner_name: planner_name.clone(),
                        morsel_id,
                    });
                    ready_morsels.push(Box::new(MockMorsel::new(
                        observer.clone(),
                        morsel_id,
                        batch_ids,
                    )) as Box<dyn Morsel>);
                }

                let mut planners = child_planners(observer.clone(), ready_planners);
                if pending_planner.is_none() {
                    planners.extend(current_planner_continuation(
                        observer.clone(),
                        planner_name.clone(),
                        steps.clone(),
                    ));
                }

                let mut plan = MorselPlan::new()
                    .with_morsels(ready_morsels)
                    .with_planners(planners);

                if let Some(MockPendingPlanner {
                    io_future_id,
                    polls_to_resolve,
                    result,
                }) = pending_planner
                {
                    observer.push(MorselEvent::IoFutureCreated {
                        planner_name: planner_name.clone(),
                        io_future_id,
                    });
                    let io_future = MockIoFuture::new(
                        observer.clone(),
                        planner_name.clone(),
                        io_future_id,
                        polls_to_resolve,
                        result,
                    )
                    .map(move |result| {
                        result?;
                        Ok(Box::new(MockMorselPlanner {
                            observer,
                            planner_name,
                            steps,
                        }) as Box<dyn MorselPlanner>)
                    })
                    .boxed();
                    plan = plan.with_pending_planner(io_future);
                }

                Ok(Some(plan))
            }
            PlannerStep::Error { error } => {
                Err(DataFusionError::External(Box::new(error)))
            }
            PlannerStep::None => Ok(None),
        }
    }
}

/// Concrete morsel used by the mock scheduler tests.
#[derive(Debug)]
pub(crate) struct MockMorsel {
    observer: MorselObserver,
    morsel_id: MorselId,
    batch_ids: Vec<i32>,
}

impl MockMorsel {
    /// Creates a mock morsel with a deterministic sequence of batches.
    fn new(observer: MorselObserver, morsel_id: MorselId, batch_ids: Vec<i32>) -> Self {
        Self {
            observer,
            morsel_id,
            batch_ids,
        }
    }
}

impl Morsel for MockMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.observer.push(MorselEvent::MorselStreamStarted {
            morsel_id: self.morsel_id,
        });
        Box::pin(MockMorselStream {
            observer: self.observer.clone(),
            morsel_id: self.morsel_id,
            batch_ids: self.batch_ids.into(),
            finished: false,
        })
    }
}

/// Stream returned by [`MockMorsel::into_stream`].
struct MockMorselStream {
    observer: MorselObserver,
    morsel_id: MorselId,
    batch_ids: VecDeque<i32>,
    finished: bool,
}

impl futures::Stream for MockMorselStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(batch_id) = self.batch_ids.pop_front() {
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
struct MockIoFuture {
    observer: MorselObserver,
    planner_name: String,
    io_future_id: IoFutureId,
    pending_polls_remaining: usize,
    result: std::result::Result<(), MockError>,
}

impl MockIoFuture {
    /// Creates a future that resolves after `io_polls` pending polls.
    fn new(
        observer: MorselObserver,
        planner_name: String,
        io_future_id: IoFutureId,
        polls_to_resolve: PollsToResolve,
        result: std::result::Result<(), MockError>,
    ) -> Self {
        Self {
            observer,
            planner_name,
            io_future_id,
            pending_polls_remaining: polls_to_resolve.0,
            result,
        }
    }
}

impl Future for MockIoFuture {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.observer.push(MorselEvent::IoFuturePolled {
            planner_name: self.planner_name.clone(),
            io_future_id: self.io_future_id,
        });

        if self.pending_polls_remaining > 0 {
            self.pending_polls_remaining -= 1;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        match &self.result {
            Ok(()) => {
                self.observer.push(MorselEvent::IoFutureResolved {
                    planner_name: self.planner_name.clone(),
                    io_future_id: self.io_future_id,
                });
                Poll::Ready(Ok(()))
            }
            Err(e) => {
                self.observer.push(MorselEvent::IoFutureErrored {
                    planner_name: self.planner_name.clone(),
                    io_future_id: self.io_future_id,
                    message: e.0.clone(),
                });
                Poll::Ready(Err(DataFusionError::External(Box::new(e.clone()))))
            }
        }
    }
}

/// Creates a one-row batch so snapshot output stays compact and readable.
fn single_value_batch(value: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![value]))]).unwrap()
}
