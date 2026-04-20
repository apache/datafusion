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

//! Utility for replaying a one-shot input `RecourdBatchStream` through spill.
//!
//! See comments in [`ReplayableStreamSource`] for details.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
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

const FIRST_PASS_ACTIVE_EPOCH: u32 = 1;
const POISONED_EPOCH: u32 = u32::MAX;

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
pub(crate) struct ReplayableStreamSource {
    schema: SchemaRef,
    input: Option<SendableRecordBatchStream>,
    spill_manager: SpillManager,
    request_description: String,
    /// 0 = unopened, 1 = first pass active, 2 = replayable/empty, MAX = poisoned
    /// on execution errors.
    epoch: Arc<AtomicU32>,
    spill_file: Arc<Mutex<Option<RefCountedTempFile>>>,
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
            epoch: Arc::new(AtomicU32::new(0)),
            spill_file: Arc::new(Mutex::new(None)),
        }
    }

    /// Opens the next pass over this input.
    ///
    /// The first call returns a stream that forwards upstream batches while
    /// caching them to spill. Later calls return streams that read directly
    /// from the completed spill file.
    ///
    /// # Note
    /// Subsequent passes MUST be opened only after the initial pass is fully
    /// consumed; otherwise, an error is returned.
    pub(crate) fn open_pass(&mut self) -> Result<SendableRecordBatchStream> {
        match self.epoch.load(Ordering::Relaxed) {
            0 => {
                let Some(input) = self.input.take() else {
                    return internal_err!(
                        "ReplayableStreamSource missing first-pass input"
                    );
                };
                let spill_file = self
                    .spill_manager
                    .create_in_progress_file(&self.request_description)?;
                *self.spill_file.lock() = None;
                self.epoch.store(FIRST_PASS_ACTIVE_EPOCH, Ordering::Relaxed);

                Ok(Box::pin(SpillCachingStream::new(
                    Arc::clone(&self.schema),
                    input,
                    Arc::clone(&self.epoch),
                    Arc::clone(&self.spill_file),
                    FIRST_PASS_ACTIVE_EPOCH,
                    spill_file,
                )))
            }
            FIRST_PASS_ACTIVE_EPOCH => {
                internal_err!("ReplayableStreamSource first pass is still active")
            }
            POISONED_EPOCH => {
                internal_err!(
                    "ReplayableStreamSource first pass did not complete successfully"
                )
            }
            _ => {
                let spill_file = self.spill_file.lock();
                if let Some(file) = spill_file.as_ref() {
                    self.spill_manager.read_spill_as_stream(file.clone(), None)
                } else {
                    Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                        &self.schema,
                    ))))
                }
            }
        }
    }
}

/// Evaluates and forwards the `inner` stream output while caching it to a spill file
/// for future replays.
struct SpillCachingStream {
    schema: SchemaRef,
    epoch_state: Arc<AtomicU32>,
    spill_file_state: Arc<Mutex<Option<RefCountedTempFile>>>,
    epoch: u32,
    spill_file: Option<InProgressSpillFile>,
    inner: SendableRecordBatchStream,
}

impl SpillCachingStream {
    fn new(
        schema: SchemaRef,
        inner: SendableRecordBatchStream,
        epoch_state: Arc<AtomicU32>,
        spill_file_state: Arc<Mutex<Option<RefCountedTempFile>>>,
        epoch: u32,
        spill_file: InProgressSpillFile,
    ) -> Self {
        Self {
            schema,
            epoch_state,
            spill_file_state,
            epoch,
            spill_file: Some(spill_file),
            inner,
        }
    }

    fn publish_result(
        epoch_state: &Arc<AtomicU32>,
        spill_file_state: &Arc<Mutex<Option<RefCountedTempFile>>>,
        epoch: u32,
        spill_file: Option<RefCountedTempFile>,
    ) {
        if epoch_state.load(Ordering::Relaxed) == epoch {
            *spill_file_state.lock() = spill_file;
            epoch_state.store(epoch.saturating_add(1), Ordering::Relaxed);
        }
    }

    fn poison(
        epoch_state: &Arc<AtomicU32>,
        spill_file_state: &Arc<Mutex<Option<RefCountedTempFile>>>,
        epoch: u32,
    ) {
        if epoch_state.load(Ordering::Relaxed) == epoch {
            *spill_file_state.lock() = None;
            epoch_state.store(POISONED_EPOCH, Ordering::Relaxed);
        }
    }
}

impl Stream for SpillCachingStream {
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
                    Self::poison(&this.epoch_state, &this.spill_file_state, this.epoch);
                    return Poll::Ready(Some(Err(e)));
                }

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.spill_file.take();
                Self::poison(&this.epoch_state, &this.spill_file_state, this.epoch);
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                let result = match this.spill_file.as_mut() {
                    Some(spill_file) => spill_file.finish(),
                    None => Ok(None),
                };

                match result {
                    Ok(file) => {
                        this.spill_file.take();
                        Self::publish_result(
                            &this.epoch_state,
                            &this.spill_file_state,
                            this.epoch,
                            file,
                        );
                        Poll::Ready(None)
                    }
                    Err(e) => {
                        this.spill_file.take();
                        Self::poison(
                            &this.epoch_state,
                            &this.spill_file_state,
                            this.epoch,
                        );
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for SpillCachingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for SpillCachingStream {
    fn drop(&mut self) {
        if self.spill_file.is_some() {
            Self::poison(&self.epoch_state, &self.spill_file_state, self.epoch);
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
        // The first pass has not finished, so the spill file is only partially
        // written and cannot be used to open subsequent replay passes.

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
}
