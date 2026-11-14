// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! State machine implementation for the regular hash join execution path. (Inner/
//! Left/Right/Full join types are regular join)
//!
//! HashJoinExec is getting incrementally migrated to separated streams, see issue
//! for detail:
//! - <https://github.com/apache/datafusion/issues/26361>

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
        loop {
            match self.processor.next_output_batch()? {
                JoinStep::Output(batch) => emitter.emit(batch).await,
                JoinStep::NeedInput => match self.probe.try_next().await? {
                    Some(batch) => {
                        self.metrics.input_batches.add(1);
                        self.metrics.input_rows.add(batch.num_rows());
                        self.metrics.probe_hit_rate.add_total(batch.num_rows());
                        self.processor.push_batch(batch)?;
                    }
                    None => {
                        drop(self.probe);
                        self.processor.finish_probe()?;
                        return Ok(Some(ExecutionStage::Outputting(self.processor)));
                    }
                },
                JoinStep::Finished => {
                    return Ok(None);
                }
            }
        }
    }
}
