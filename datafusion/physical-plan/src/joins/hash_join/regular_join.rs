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

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_execution::{TryEmitter, async_try_stream};
use futures::TryStreamExt;

use super::build::HashJoinBuild;
use super::hash_join_table::{HashJoinTable, HashJoinTableConfig, JoinStep};
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
    Outputting(HashJoinTable),
}

struct Building {
    build: HashJoinBuild,
    probe: SendableRecordBatchStream,
    config: HashJoinTableConfig,
    metrics: BuildProbeJoinMetrics,
}

struct Probing {
    probe: SendableRecordBatchStream,
    table: HashJoinTable,
}

impl RegularHashJoinStream {
    pub(super) fn new(
        build: HashJoinBuild,
        probe: SendableRecordBatchStream,
        config: HashJoinTableConfig,
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
    /// - Outputting: Actually joining the probe batch and buffered build side, and
    ///   produce final output.
    ///
    /// # Transition edges
    ///
    /// - `Building -> Probing`: The build side has all read, and hash table built.
    /// - `Probing -> Outputting`: Probe input reaches EOF. Release it and report
    ///   completion to the table.
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
                    ExecutionStage::Outputting(mut table) => {
                        loop {
                            match table.next_output_batch()? {
                                JoinStep::Output(batch) => emitter.emit(batch).await,
                                JoinStep::Finished => break,
                                JoinStep::NeedInput => {
                                    return internal_err!(
                                        "HashJoinTable requested probe input after probe EOF"
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
            table: HashJoinTable::new(build, self.config, self.metrics),
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
            match self.table.next_output_batch()? {
                JoinStep::Output(batch) => emitter.emit(batch).await,
                JoinStep::NeedInput => match self.probe.try_next().await? {
                    Some(batch) => self.table.push_batch(batch)?,
                    None => {
                        drop(self.probe);
                        self.table.finish_probe()?;
                        return Ok(Some(ExecutionStage::Outputting(self.table)));
                    }
                },
                JoinStep::Finished => {
                    return Ok(None);
                }
            }
        }
    }
}
