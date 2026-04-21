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

//! Utility for replaying a one-shot input `RecordBatchStream` through spill.
//!
//! See comments in [`ReplayableStreamSource`] for details.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use datafusion_execution::RecordBatchStream;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_execution::disk_manager::RefCountedTempFile;
use futures::Stream;
use parking_lot::Mutex;

use crate::EmptyRecordBatchStream;
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::spill::spill_manager::SpillManager;

/// Spill-backed replayable stream source.
///
/// [`ReplayableStreamSource`] is constructed from an input stream, usually produced
/// by executing an input `ExecutionPlan`.
///
/// - On the first pass, it evaluates the input stream, produces `RecordBatch`es,
///   caches those batches to a local spill file, and also forwards them to the
///   output.
/// - On subsequent passes, it reads directly from the spill file.
///
/// ```text
/// first pass:
///
/// RecordBatch stream
///     |
///     v
///   [batch] -> output
///     |
///     +----> spill file
///
///
/// later passes:
///
/// spill file
///     |
///     v
///   [batch] -> output
/// ```
///
/// This is useful when an input stream must be replayed and:
/// - Re-evaluation is expensive because the input stream may come from a long
///   and complex pipeline.
/// - The parent operator is under memory pressure and cannot cache the input in
///   memory for replay.
///
/// # Concurrency assumption
/// Passes must be opened and consumed sequentially.
/// Opening another pass before exhausting the current one returns an error.
pub(crate) struct ReplayableStreamSource {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    spill_manager: SpillManager,
    request_description: String,
    /// Inner state is owned by either the source or one active stream to ensure
    /// sequential access; see struct docs for the concurrency contract.
    ///
    /// Ownership model:
    /// - No active stream: source owns the state (`source.state = Some(state)`).
    /// - Active stream: the stream owns the state (`source.state = None`).
    state: Arc<Mutex<Option<StateInner>>>,
}

/// Inner state exclusively owned by either [`ReplayableStreamSource`] or one [`ReplayableSpillStream`]
enum StateInner {
    Unopened,
    Replayable(Option<RefCountedTempFile>),
    Poisoned,
}

impl ReplayableStreamSource {
    /// Creates a replayable stream producer over a one-shot input stream.
    ///
    /// It caches the input into a local spill file on the first pass, then
    /// reads directly from that spill file on subsequent passes.
    pub(crate) fn new(
        input: SendableRecordBatchStream,
        spill_manager: SpillManager,
        request_description: impl Into<String>,
    ) -> Self {
        let schema = input.schema();
        Self {
            schema,
            input: Some(input),
            spill_manager,
            request_description: request_description.into(),
            state: Arc::new(Mutex::new(Some(StateInner::Unopened))),
        }
    }

    fn set_state(&self, state: StateInner) {
        *self.state.lock() = Some(state);
    }

    /// Opens the next pass over this input.
    ///
    /// The first call returns a stream that forwards upstream batches while
    /// caching them to spill. Later calls return streams that read directly
    /// from the completed spill file.
    ///
    /// # Note
    /// Subsequent passes MUST be opened only after the previous pass is fully
    /// consumed; otherwise, an error is returned.
    pub(crate) fn open_pass(&mut self) -> Result<SendableRecordBatchStream> {
        let state = self.state.lock().take();
        let Some(state) = state else {
            return internal_err!("ReplayableStreamSource pass is still active");
        };

        match state {
            StateInner::Unopened => {
                let Some(input) = self.input.take() else {
                    self.set_state(StateInner::Poisoned);
                    return internal_err!(
                        "ReplayableStreamSource missing first-pass input"
                    );
                };
                let spill_file = match self
                    .spill_manager
                    .create_in_progress_file(&self.request_description)
                {
                    Ok(spill_file) => spill_file,
                    Err(e) => {
                        self.input = Some(input);
                        self.set_state(StateInner::Unopened);
                        return Err(e);
                    }
                };

                Ok(Box::pin(ReplayableSpillStream::new_first(
                    Arc::clone(&self.schema),
                    input,
                    Arc::clone(&self.state),
                    spill_file,
                )))
            }
            StateInner::Poisoned => {
                internal_err!(
                    "ReplayableStreamSource first pass did not complete successfully"
                )
            }
            StateInner::Replayable(spill_file) => {
                let replay_state = spill_file.clone();
                match ReplayableSpillStream::new_replay(
                    Arc::clone(&self.schema),
                    &self.spill_manager,
                    Arc::clone(&self.state),
                    spill_file,
                ) {
                    Ok(stream) => Ok(Box::pin(stream)),
                    Err(e) => {
                        self.set_state(StateInner::Replayable(replay_state));
                        Err(e)
                    }
                }
            }
        }
    }
}

/// Makes a one-shot stream replayable using spill caching, keeping replays fast
/// and memory efficient.
///
/// On the first pass, it evaluates and forwards output from `inner` while
/// caching it to a spill file for future replays.
///
/// On later passes, it replays directly from the cached spill file.
///
/// See also [`ReplayableStreamSource`] for details.
struct ReplayableSpillStream {
    schema: SchemaRef,
    shared_state: Arc<Mutex<Option<StateInner>>>,
    held_state: Option<StateInner>,
    spill_file: Option<InProgressSpillFile>,
    inner: SendableRecordBatchStream,
}

impl ReplayableSpillStream {
    fn new_first(
        schema: SchemaRef,
        inner: SendableRecordBatchStream,
        shared_state: Arc<Mutex<Option<StateInner>>>,
        spill_file: InProgressSpillFile,
    ) -> Self {
        Self {
            schema,
            shared_state,
            held_state: Some(StateInner::Unopened),
            spill_file: Some(spill_file),
            inner,
        }
    }

    fn new_replay(
        schema: SchemaRef,
        spill_manager: &SpillManager,
        shared_state: Arc<Mutex<Option<StateInner>>>,
        spill_file: Option<RefCountedTempFile>,
    ) -> Result<Self> {
        let inner = if let Some(file) = spill_file.as_ref() {
            spill_manager.read_spill_as_stream(file.clone(), None)?
        } else {
            Box::pin(EmptyRecordBatchStream::new(Arc::clone(&schema)))
        };

        Ok(Self {
            schema,
            shared_state,
            held_state: Some(StateInner::Replayable(spill_file)),
            spill_file: None,
            inner,
        })
    }

    fn restore_held_state(&mut self) {
        if let Some(state) = self.held_state.take() {
            *self.shared_state.lock() = Some(state);
        }
    }

    fn set_state(&mut self, state: StateInner) {
        if self.held_state.take().is_some() {
            *self.shared_state.lock() = Some(state);
        }
    }

    fn poison(&mut self) {
        self.set_state(StateInner::Poisoned);
    }
}

impl Stream for ReplayableSpillStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                if batch.num_rows() > 0
                    && let Some(spill_file) = this.spill_file.as_mut()
                    && let Err(e) = spill_file.append_batch(&batch)
                {
                    this.spill_file.take();
                    this.poison();
                    return Poll::Ready(Some(Err(e)));
                }

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.spill_file.take();
                this.poison();
                Poll::Ready(Some(Err(e)))
            }
            // The stream is exhausted, give the inner state ownership back to `ReplayableStreamSource`
            Poll::Ready(None) => {
                if let Some(spill_file) = this.spill_file.as_mut() {
                    match spill_file.finish() {
                        Ok(file) => {
                            this.spill_file.take();
                            this.set_state(StateInner::Replayable(file));
                            Poll::Ready(None)
                        }
                        Err(e) => {
                            this.spill_file.take();
                            this.poison();
                            Poll::Ready(Some(Err(e)))
                        }
                    }
                } else {
                    this.restore_held_state();
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ReplayableSpillStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for ReplayableSpillStream {
    /// If a stream is dropped before it finishes, poison the state so later
    /// replay attempts fail.
    ///
    /// A partial first pass leaves the spill file incomplete, so replaying it
    /// would be unsafe.
    fn drop(&mut self) {
        if self.held_state.is_some() {
            self.poison();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, SpillMetrics,
    };
    use futures::{StreamExt, TryStreamExt};

    use crate::stream::RecordBatchStreamAdapter;

    fn build_spill_manager(schema: SchemaRef) -> Result<SpillManager> {
        let runtime = Arc::new(RuntimeEnvBuilder::new().build()?);
        let metrics_set = ExecutionPlanMetricsSet::new();
        let spill_metrics = SpillMetrics::new(&metrics_set, 0);
        Ok(SpillManager::new(runtime, spill_metrics, schema))
    }

    fn build_batch(schema: SchemaRef, values: Vec<i64>) -> Result<RecordBatch> {
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .map_err(Into::into)
    }

    #[tokio::test]
    async fn test_replayable_spill_input_replays_completed_first_pass() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch1 = build_batch(Arc::clone(&schema), vec![1, 2])?;
        let batch2 = build_batch(Arc::clone(&schema), vec![3, 4])?;

        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch1.clone()), Ok(batch2.clone())]),
        ));
        let spill_manager = build_spill_manager(Arc::clone(&schema))?;
        let mut replayable =
            ReplayableStreamSource::new(input, spill_manager, "test replayable spill");

        let pass1 = replayable.open_pass()?;
        let pass1_batches = pass1.try_collect::<Vec<_>>().await?;
        assert_eq!(pass1_batches, vec![batch1.clone(), batch2.clone()]);

        let pass2 = replayable.open_pass()?;
        let pass2_batches = pass2.try_collect::<Vec<_>>().await?;
        assert_eq!(pass2_batches, vec![batch1, batch2]);

        Ok(())
    }

    // Try to open a new pass, when the first pass has not finished.
    // The spill file is only partially written, so an error will be returned.
    #[tokio::test]
    async fn test_replayable_spill_input_poisoned_when_first_pass_dropped() -> Result<()>
    {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch1 = build_batch(Arc::clone(&schema), vec![1, 2])?;
        let batch2 = build_batch(Arc::clone(&schema), vec![3, 4])?;

        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch1), Ok(batch2)]),
        ));
        let spill_manager = build_spill_manager(Arc::clone(&schema))?;
        let mut replayable =
            ReplayableStreamSource::new(input, spill_manager, "test replayable spill");

        let mut pass1 = replayable.open_pass()?;
        let first = pass1.next().await.transpose()?;
        assert!(first.is_some());
        drop(pass1);

        let err = match replayable.open_pass() {
            Ok(_) => panic!("expected first pass to poison replayable spill input"),
            Err(err) => err.strip_backtrace(),
        };
        assert!(
            err.to_string().contains(
                "ReplayableStreamSource first pass did not complete successfully"
            )
        );

        Ok(())
    }

    // Open a new pass, when the previous pass from spill is still in progress.
    // An error is expected, since it requires sequential access.
    #[tokio::test]
    async fn test_replayable_spill_input_errors_when_replay_pass_in_progress()
    -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch1 = build_batch(Arc::clone(&schema), vec![1, 2])?;
        let batch2 = build_batch(Arc::clone(&schema), vec![3, 4])?;

        let input = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::iter(vec![Ok(batch1.clone()), Ok(batch2.clone())]),
        ));
        let spill_manager = build_spill_manager(Arc::clone(&schema))?;
        let mut replayable =
            ReplayableStreamSource::new(input, spill_manager, "test replayable spill");

        let pass1 = replayable.open_pass()?;
        let _ = pass1.try_collect::<Vec<_>>().await?;

        let pass2 = replayable.open_pass()?;
        let err = match replayable.open_pass() {
            Ok(_) => panic!("expected open_pass to fail while replay pass is active"),
            Err(err) => err.strip_backtrace(),
        };
        assert!(
            err.to_string()
                .contains("ReplayableStreamSource pass is still active")
        );
        drop(pass2);

        Ok(())
    }
}
