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

//! State machine implementation for the regular hash join execution path. (Inner/
//! Left/Right/Full join types are regular join)
//!
//! HashJoinExec is getting incrementally migrated to separated streams, see issue
//! for detail:
//! - <https://github.com/apache/datafusion/issues/26361>
//!
//! # Architecture
//! Regular join has 3 major layers, from bottom to top:
//!
//! 1. JoinIndexMap: Maps a probe row to matching build-side row IDs (equi-join equality).
//!   - e.g. `key1 -> [row0], key2 -> [row1, row3, row4]`
//!   - Multiple matches are only handle, the upper layer is responsible for materializing
//!     it incrementally.
//!  2. BatchedJoinProcessor: Performs the pipeline `equi-join matches -> residual join filter -> output batch construction`, processing matches in chunks bounded by `batch_size`.
//!  3. Entry point: The stream state machine drives `BatchedJoinProcessor`.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::{TryEmitter, async_try_stream};
use futures::TryStreamExt;

use super::batched_join_processor::{
    BatchedJoinProcessor, BatchedJoinProcessorConfig, JoinStep,
};
use super::build::HashJoinBuild;
use crate::SendableRecordBatchStream;
use crate::joins::utils::BuildProbeJoinMetrics;
use crate::metrics::BaselineMetrics;
use crate::stream::{ObservedStream, RecordBatchStreamAdapter};

/// Implementation for regular (Inner/Left/Right/Full) hash join.
///
/// See [`Self::into_stream`] for execution stages and transitions.
pub(super) struct RegularHashJoinStream {
    schema: SchemaRef,
    metrics: BaselineMetrics,
    stage: ExecutionStage,
}

/// Execution stages described in [`RegularHashJoinStream::into_stream`].
enum ExecutionStage {
    Building(Building),
    Probing(Probing),
    Outputting(BatchedJoinProcessor),
}

struct Building {
    build: HashJoinBuild,
    probe: SendableRecordBatchStream,
    config: BatchedJoinProcessorConfig,
    metrics: BuildProbeJoinMetrics,
}

struct Probing {
    probe: SendableRecordBatchStream,
    batch_size: usize,
    metrics: BuildProbeJoinMetrics,
    processor: BatchedJoinProcessor,
}

impl RegularHashJoinStream {
    pub(super) fn new(
        build: HashJoinBuild,
        probe: SendableRecordBatchStream,
        config: BatchedJoinProcessorConfig,
        metrics: BuildProbeJoinMetrics,
    ) -> Self {
        Self {
            schema: Arc::clone(&config.schema),
            metrics: metrics.baseline.clone(),
            stage: ExecutionStage::Building(Building {
                build,
                probe,
                config,
                metrics,
            }),
        }
    }

    /// Entry point for the regular hash join execution stages.
    ///
    /// ```text
    /// Building --> Probing --> Outputting --> end
    /// ```
    ///
    /// # Stages
    ///
    /// - Building: Load the build-side data, and handle cross-partition synchronization,
    ///   see [`HashJoinBuild`] for different physical strategies
    /// - Probing: Joining probe batch with build data incrementally, until probe
    ///   side input is exhausted.
    /// - Outputting: Drain buffered output, then emit unmatched build rows for
    ///   left/full joins.
    ///
    /// # Transition edges
    ///
    /// - `Building -> Probing`: The build data and its index are ready.
    /// - `Probing -> Outputting`: Probe input reaches EOF. Release it and report
    ///   completion to the processor.
    /// - `Outputting -> end`: Buffered output and any final unmatched build rows
    ///   have been emitted.
    pub(super) fn into_stream(self) -> SendableRecordBatchStream {
        let Self {
            schema,
            metrics,
            stage,
        } = self;
        let stream = async_try_stream(|mut emitter| async move {
            let mut stage = Some(stage);
            while let Some(current_stage) = stage {
                stage = match current_stage {
                    ExecutionStage::Building(building) => {
                        Some(building.handle_stage().await?)
                    }
                    ExecutionStage::Probing(probing) => {
                        probing.handle_stage(&mut emitter).await?
                    }
                    ExecutionStage::Outputting(mut processor) => {
                        loop {
                            match processor.next_output_batch()? {
                                JoinStep::Output(batch) => emitter.emit(batch).await,
                                JoinStep::Finished => break,
                                JoinStep::NeedInput => {
                                    return internal_err!(
                                        "BatchedJoinProcessor requested probe input after probe EOF"
                                    );
                                }
                            }
                        }
                        None
                    }
                };
            }
            Ok(())
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        Box::pin(ObservedStream::new(stream, metrics, None))
    }
}

impl Building {
    /// See comments at [`RegularHashJoinStream::into_stream`] for details.
    async fn handle_stage(self) -> Result<ExecutionStage> {
        let build = self.build.prepare().await?;
        Ok(ExecutionStage::Probing(Probing {
            probe: self.probe,
            batch_size: self.config.batch_size,
            metrics: self.metrics.clone(),
            processor: BatchedJoinProcessor::new(build, self.config, self.metrics),
        }))
    }
}

impl Probing {
    /// See comments at [`RegularHashJoinStream::into_stream`] for details.
    async fn handle_stage(
        mut self,
        emitter: &mut TryEmitter<RecordBatch, DataFusionError>,
    ) -> Result<Option<ExecutionStage>> {
        // Upstream operators may emit more rows than the configured batch size.
        // Retain that batch and the next slice's offset while the processor
        // drains each bounded input. Slicing shares the original array buffers.
        let mut pending: Option<(RecordBatch, usize)> = None;
        loop {
            match self.processor.next_output_batch()? {
                JoinStep::Output(batch) => emitter.emit(batch).await,
                JoinStep::NeedInput => {
                    let (batch, offset) = if let Some(pending) = pending.take() {
                        pending
                    } else {
                        match self.probe.try_next().await? {
                            Some(batch) => {
                                // Count source input, independently of how many
                                // slices the processor needs to consume it.
                                self.metrics.input_batches.add(1);
                                self.metrics.input_rows.add(batch.num_rows());
                                self.metrics.probe_hit_rate.add_total(batch.num_rows());
                                (batch, 0)
                            }
                            None => {
                                drop(self.probe);
                                self.processor.finish_probe()?;
                                return Ok(Some(ExecutionStage::Outputting(
                                    self.processor,
                                )));
                            }
                        }
                    };
                    let remaining = batch.num_rows() - offset;
                    let input = if remaining > self.batch_size {
                        let input = batch.slice(offset, self.batch_size);
                        pending = Some((batch, offset + self.batch_size));
                        input
                    } else if offset == 0 {
                        batch
                    } else {
                        batch.slice(offset, remaining)
                    };
                    self.processor.push_batch(input)?;
                }
                JoinStep::Finished => {
                    return Ok(None);
                }
            }
        }
    }
}
