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

//! This file implements the [`RepartitionExec`]  operator, which maps N input
//! partitions to M output partitions based on a partitioning scheme, optionally
//! maintaining the order of the input rows in the output.

use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{any::Any, vec};

use super::common::SharedMemoryReservation;
use super::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use super::{
    DisplayAs, ExecutionPlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use crate::coalesce::LimitedBatchCoalescer;
use crate::execution_plan::{CardinalityEffect, EvaluationType, SchedulingType};
use crate::hash_utils::create_hashes;
use crate::metrics::{BaselineMetrics, SpillMetrics};
use crate::projection::{ProjectionExec, all_columns, make_with_child, update_expr};
use crate::sorts::streaming_merge::StreamingMergeBuilder;
use crate::spill::spill_manager::SpillManager;
use crate::spill::spill_pool::{self, SpillPoolWriter};
#[cfg(feature = "stateless_plan")]
use crate::state::{PlanState, PlanStateNode};
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, Statistics};

use arrow::array::{PrimitiveArray, RecordBatch, RecordBatchOptions};
use arrow::compute::take_arrays;
use arrow::datatypes::{SchemaRef, UInt32Type};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::utils::transpose;
use datafusion_common::{
    ColumnStatistics, DataFusionError, HashMap, assert_or_internal_err, internal_err,
};
use datafusion_common::{Result, not_impl_err};
use datafusion_common_runtime::SpawnedTask;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryConsumer;
#[cfg(not(feature = "stateless_plan"))]
use datafusion_execution::metrics::MetricsSet;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::joins::SeededRandomState;
use crate::sort_pushdown::SortOrderPushdownResult;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::stream::Stream;
use futures::{FutureExt, StreamExt, TryStreamExt, ready};
use log::trace;
use parking_lot::Mutex;

mod distributor_channels;
use distributor_channels::{
    DistributionReceiver, DistributionSender, channels, partition_aware_channels,
};

/// A batch in the repartition queue - either in memory or spilled to disk.
///
/// This enum represents the two states a batch can be in during repartitioning.
/// The decision to spill is made based on memory availability when sending a batch
/// to an output partition.
///
/// # Batch Flow with Spilling
///
/// ```text
/// Input Stream ──▶ Partition Logic ──▶ try_grow()
///                                            │
///                            ┌───────────────┴────────────────┐
///                            │                                │
///                            ▼                                ▼
///                   try_grow() succeeds            try_grow() fails
///                   (Memory Available)              (Memory Pressure)
///                            │                                │
///                            ▼                                ▼
///                  RepartitionBatch::Memory         spill_writer.push_batch()
///                  (batch held in memory)           (batch written to disk)
///                            │                                │
///                            │                                ▼
///                            │                      RepartitionBatch::Spilled
///                            │                      (marker - no batch data)
///                            │                                │
///                            └────────┬───────────────────────┘
///                                     │
///                                     ▼
///                              Send to channel
///                                     │
///                                     ▼
///                            Output Stream (poll)
///                                     │
///                      ┌──────────────┴─────────────┐
///                      │                            │
///                      ▼                            ▼
///         RepartitionBatch::Memory      RepartitionBatch::Spilled
///         Return batch immediately       Poll spill_stream (blocks)
///                      │                            │
///                      └────────┬───────────────────┘
///                               │
///                               ▼
///                          Return batch
///                    (FIFO order preserved)
/// ```
///
/// See [`RepartitionExec`] for overall architecture and [`StreamState`] for
/// the state machine that handles reading these batches.
#[derive(Debug)]
enum RepartitionBatch {
    /// Batch held in memory (counts against memory reservation)
    Memory(RecordBatch),
    /// Marker indicating a batch was spilled to the partition's SpillPool.
    /// The actual batch can be retrieved by reading from the SpillPoolStream.
    /// This variant contains no data itself - it's just a signal to the reader
    /// to fetch the next batch from the spill stream.
    Spilled,
}

type MaybeBatch = Option<Result<RepartitionBatch>>;
type InputPartitionsToCurrentPartitionSender = Vec<DistributionSender<MaybeBatch>>;
type InputPartitionsToCurrentPartitionReceiver = Vec<DistributionReceiver<MaybeBatch>>;

/// Output channel with its associated memory reservation and spill writer
struct OutputChannel {
    sender: DistributionSender<MaybeBatch>,
    reservation: SharedMemoryReservation,
    spill_writer: SpillPoolWriter,
}

/// Channels and resources for a single output partition.
///
/// Each output partition has channels to receive data from all input partitions.
/// To handle memory pressure, each (input, output) pair gets its own
/// [`SpillPool`](crate::spill::spill_pool) channel via [`spill_pool::channel`].
///
/// # Structure
///
/// For an output partition receiving from N input partitions:
/// - `tx`: N senders (one per input) for sending batches to this output
/// - `rx`: N receivers (one per input) for receiving batches at this output
/// - `spill_writers`: N spill writers (one per input) for writing spilled data
/// - `spill_readers`: N spill readers (one per input) for reading spilled data
///
/// This 1:1 mapping between input partitions and spill channels ensures that
/// batches from each input are processed in FIFO order, even when some batches
/// are spilled to disk and others remain in memory.
///
/// See [`RepartitionExec`] for the overall N×M architecture.
///
/// [`spill_pool::channel`]: crate::spill::spill_pool::channel
struct PartitionChannels {
    /// Senders for each input partition to send data to this output partition
    tx: InputPartitionsToCurrentPartitionSender,
    /// Receivers for each input partition sending data to this output partition
    rx: InputPartitionsToCurrentPartitionReceiver,
    /// Memory reservation for this output partition
    reservation: SharedMemoryReservation,
    /// Spill writers for writing spilled data.
    /// SpillPoolWriter is Clone, so multiple writers can share state in non-preserve-order mode.
    spill_writers: Vec<SpillPoolWriter>,
    /// Spill readers for reading spilled data - one per input partition (FIFO semantics).
    /// Each (input, output) pair gets its own reader to maintain proper ordering.
    spill_readers: Vec<SendableRecordBatchStream>,
}

struct ConsumingInputStreamsState {
    /// Channels for sending batches from input partitions to output partitions.
    /// Key is the partition number.
    channels: HashMap<usize, PartitionChannels>,

    /// Helper that ensures that background jobs are killed once they are no longer needed.
    abort_helper: Arc<Vec<SpawnedTask<()>>>,
}

impl Debug for ConsumingInputStreamsState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumingInputStreamsState")
            .field("num_channels", &self.channels.len())
            .field("abort_helper", &self.abort_helper)
            .finish()
    }
}

/// Inner state of [`RepartitionExec`].
#[derive(Default)]
enum RepartitionExecStateInner {
    /// Not initialized yet. This is the default state stored in the RepartitionExec node
    /// upon instantiation.
    #[default]
    NotInitialized,
    /// Input streams are initialized, but they are still not being consumed. The node
    /// transitions to this state when the arrow's RecordBatch stream is created in
    /// RepartitionExec::execute(), but before any message is polled.
    InputStreamsInitialized(Vec<(SendableRecordBatchStream, RepartitionMetrics)>),
    /// The input streams are being consumed. The node transitions to this state when
    /// the first message in the arrow's RecordBatch stream is consumed.
    ConsumingInputStreams(ConsumingInputStreamsState),
}

impl Debug for RepartitionExecStateInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotInitialized => write!(f, "NotInitialized"),
            Self::InputStreamsInitialized(v) => {
                write!(f, "InputStreamsInitialized({:?})", v.len())
            }
            Self::ConsumingInputStreams(v) => {
                write!(f, "ConsumingInputStreams({v:?})")
            }
        }
    }
}

impl RepartitionExecStateInner {
    fn ensure_input_streams_initialized(
        &mut self,
        input: &Arc<dyn ExecutionPlan>,
        metrics: &ExecutionPlanMetricsSet,
        output_partitions: usize,
        ctx: &Arc<TaskContext>,
        #[cfg(feature = "stateless_plan")] state: &Arc<PlanStateNode>,
    ) -> Result<()> {
        if !matches!(self, Self::NotInitialized) {
            return Ok(());
        }

        let num_input_partitions = input.output_partitioning().partition_count();
        let mut streams_and_metrics = Vec::with_capacity(num_input_partitions);

        #[cfg(feature = "stateless_plan")]
        let child_state = state.child_state(0);
        for i in 0..num_input_partitions {
            let metrics = RepartitionMetrics::new(i, output_partitions, metrics);

            let timer = metrics.fetch_time.timer();

            #[cfg(not(feature = "stateless_plan"))]
            let stream = input.execute(i, Arc::clone(ctx))?;

            #[cfg(feature = "stateless_plan")]
            let stream = input.execute(i, Arc::clone(ctx), &child_state)?;

            timer.done();

            streams_and_metrics.push((stream, metrics));
        }
        *self = Self::InputStreamsInitialized(streams_and_metrics);
        Ok(())
    }

    #[expect(clippy::too_many_arguments)]
    fn consume_input_streams(
        &mut self,
        input: &Arc<dyn ExecutionPlan>,
        metrics: &ExecutionPlanMetricsSet,
        partitioning: &Partitioning,
        preserve_order: bool,
        name: &str,
        context: &Arc<TaskContext>,
        spill_manager: SpillManager,
        #[cfg(feature = "stateless_plan")] state: &Arc<PlanStateNode>,
    ) -> Result<&mut ConsumingInputStreamsState> {
        let streams_and_metrics = match self {
            Self::NotInitialized => {
                self.ensure_input_streams_initialized(
                    input,
                    metrics,
                    partitioning.partition_count(),
                    context,
                    #[cfg(feature = "stateless_plan")]
                    state,
                )?;
                let Self::InputStreamsInitialized(value) = self else {
                    // This cannot happen, as ensure_input_streams_initialized() was just called,
                    // but the compiler does not know.
                    return internal_err!(
                        "Programming error: RepartitionExecState must be in the InputStreamsInitialized state after calling RepartitionExecState::ensure_input_streams_initialized"
                    );
                };
                value
            }
            Self::ConsumingInputStreams(value) => return Ok(value),
            Self::InputStreamsInitialized(value) => value,
        };

        let num_input_partitions = streams_and_metrics.len();
        let num_output_partitions = partitioning.partition_count();

        let spill_manager = Arc::new(spill_manager);

        let (txs, rxs) = if preserve_order {
            // Create partition-aware channels with one channel per (input, output) pair
            // This provides backpressure while maintaining proper ordering
            let (txs_all, rxs_all) =
                partition_aware_channels(num_input_partitions, num_output_partitions);
            // Take transpose of senders and receivers. `state.channels` keeps track of entries per output partition
            let txs = transpose(txs_all);
            let rxs = transpose(rxs_all);
            (txs, rxs)
        } else {
            // Create one channel per *output* partition with backpressure
            let (txs, rxs) = channels(num_output_partitions);
            // Clone sender for each input partitions
            let txs = txs
                .into_iter()
                .map(|item| vec![item; num_input_partitions])
                .collect::<Vec<_>>();
            let rxs = rxs.into_iter().map(|item| vec![item]).collect::<Vec<_>>();
            (txs, rxs)
        };

        let mut channels = HashMap::with_capacity(txs.len());
        for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
            let reservation = Arc::new(Mutex::new(
                MemoryConsumer::new(format!("{name}[{partition}]"))
                    .with_can_spill(true)
                    .register(context.memory_pool()),
            ));

            // Create spill channels based on mode:
            // - preserve_order: one spill channel per (input, output) pair for proper FIFO ordering
            // - non-preserve-order: one shared spill channel per output partition since all inputs
            //   share the same receiver
            let max_file_size = context
                .session_config()
                .options()
                .execution
                .max_spill_file_size_bytes;
            let num_spill_channels = if preserve_order {
                num_input_partitions
            } else {
                1
            };
            let (spill_writers, spill_readers): (Vec<_>, Vec<_>) = (0
                ..num_spill_channels)
                .map(|_| spill_pool::channel(max_file_size, Arc::clone(&spill_manager)))
                .unzip();

            channels.insert(
                partition,
                PartitionChannels {
                    tx,
                    rx,
                    reservation,
                    spill_readers,
                    spill_writers,
                },
            );
        }

        // launch one async task per *input* partition
        let mut spawned_tasks = Vec::with_capacity(num_input_partitions);
        for (i, (stream, metrics)) in
            std::mem::take(streams_and_metrics).into_iter().enumerate()
        {
            let txs: HashMap<_, _> = channels
                .iter()
                .map(|(partition, channels)| {
                    // In preserve_order mode: each input gets its own spill writer (index i)
                    // In non-preserve-order mode: all inputs share spill writer 0 via clone
                    let spill_writer_idx = if preserve_order { i } else { 0 };
                    (
                        *partition,
                        OutputChannel {
                            sender: channels.tx[i].clone(),
                            reservation: Arc::clone(&channels.reservation),
                            spill_writer: channels.spill_writers[spill_writer_idx]
                                .clone(),
                        },
                    )
                })
                .collect();

            // Extract senders for wait_for_task before moving txs
            let senders: HashMap<_, _> = txs
                .iter()
                .map(|(partition, channel)| (*partition, channel.sender.clone()))
                .collect();

            let input_task = SpawnedTask::spawn(RepartitionExec::pull_from_input(
                stream,
                txs,
                partitioning.clone(),
                metrics,
                // preserve_order depends on partition index to start from 0
                if preserve_order { 0 } else { i },
                num_input_partitions,
            ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            let wait_for_task =
                SpawnedTask::spawn(RepartitionExec::wait_for_task(input_task, senders));
            spawned_tasks.push(wait_for_task);
        }
        *self = Self::ConsumingInputStreams(ConsumingInputStreamsState {
            channels,
            abort_helper: Arc::new(spawned_tasks),
        });
        match self {
            Self::ConsumingInputStreams(value) => Ok(value),
            _ => unreachable!(),
        }
    }
}

type RepartitionExecState = Arc<Mutex<RepartitionExecStateInner>>;

#[cfg(feature = "stateless_plan")]
impl PlanState for RepartitionExecState {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A utility that can be used to partition batches based on [`Partitioning`]
pub struct BatchPartitioner {
    state: BatchPartitionerState,
    timer: metrics::Time,
}

enum BatchPartitionerState {
    Hash {
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        num_partitions: usize,
        hash_buffer: Vec<u64>,
    },
    RoundRobin {
        num_partitions: usize,
        next_idx: usize,
    },
}

/// Fixed RandomState used for hash repartitioning to ensure consistent behavior across
/// executions and runs.
pub const REPARTITION_RANDOM_STATE: SeededRandomState =
    SeededRandomState::with_seeds(0, 0, 0, 0);

impl BatchPartitioner {
    /// Create a new [`BatchPartitioner`] with the provided [`Partitioning`]
    ///
    /// The time spent repartitioning will be recorded to `timer`
    pub fn try_new(
        partitioning: Partitioning,
        timer: metrics::Time,
        input_partition: usize,
        num_input_partitions: usize,
    ) -> Result<Self> {
        let state = match partitioning {
            Partitioning::RoundRobinBatch(num_partitions) => {
                BatchPartitionerState::RoundRobin {
                    num_partitions,
                    // Distribute starting index evenly based on input partition, number of input partitions and number of partitions
                    // to avoid they all start at partition 0 and heavily skew on the lower partitions
                    next_idx: ((input_partition * num_partitions) / num_input_partitions),
                }
            }
            Partitioning::Hash(exprs, num_partitions) => BatchPartitionerState::Hash {
                exprs,
                num_partitions,
                hash_buffer: vec![],
            },
            other => return not_impl_err!("Unsupported repartitioning scheme {other:?}"),
        };

        Ok(Self { state, timer })
    }

    /// Partition the provided [`RecordBatch`] into one or more partitioned [`RecordBatch`]
    /// based on the [`Partitioning`] specified on construction
    ///
    /// `f` will be called for each partitioned [`RecordBatch`] with the corresponding
    /// partition index. Any error returned by `f` will be immediately returned by this
    /// function without attempting to publish further [`RecordBatch`]
    ///
    /// The time spent repartitioning, not including time spent in `f` will be recorded
    /// to the [`metrics::Time`] provided on construction
    pub fn partition<F>(&mut self, batch: RecordBatch, mut f: F) -> Result<()>
    where
        F: FnMut(usize, RecordBatch) -> Result<()>,
    {
        self.partition_iter(batch)?.try_for_each(|res| match res {
            Ok((partition, batch)) => f(partition, batch),
            Err(e) => Err(e),
        })
    }

    /// Actual implementation of [`partition`](Self::partition).
    ///
    /// The reason this was pulled out is that we need to have a variant of `partition` that works w/ sync functions,
    /// and one that works w/ async. Using an iterator as an intermediate representation was the best way to achieve
    /// this (so we don't need to clone the entire implementation).
    fn partition_iter(
        &mut self,
        batch: RecordBatch,
    ) -> Result<impl Iterator<Item = Result<(usize, RecordBatch)>> + Send + '_> {
        let it: Box<dyn Iterator<Item = Result<(usize, RecordBatch)>> + Send> =
            match &mut self.state {
                BatchPartitionerState::RoundRobin {
                    num_partitions,
                    next_idx,
                } => {
                    let idx = *next_idx;
                    *next_idx = (*next_idx + 1) % *num_partitions;
                    Box::new(std::iter::once(Ok((idx, batch))))
                }
                BatchPartitionerState::Hash {
                    exprs,
                    num_partitions: partitions,
                    hash_buffer,
                } => {
                    // Tracking time required for distributing indexes across output partitions
                    let timer = self.timer.timer();

                    let arrays =
                        evaluate_expressions_to_arrays(exprs.as_slice(), &batch)?;

                    hash_buffer.clear();
                    hash_buffer.resize(batch.num_rows(), 0);

                    create_hashes(
                        &arrays,
                        REPARTITION_RANDOM_STATE.random_state(),
                        hash_buffer,
                    )?;

                    let mut indices: Vec<_> = (0..*partitions)
                        .map(|_| Vec::with_capacity(batch.num_rows()))
                        .collect();

                    for (index, hash) in hash_buffer.iter().enumerate() {
                        indices[(*hash % *partitions as u64) as usize].push(index as u32);
                    }

                    // Finished building index-arrays for output partitions
                    timer.done();

                    // Borrowing partitioner timer to prevent moving `self` to closure
                    let partitioner_timer = &self.timer;
                    let it = indices
                        .into_iter()
                        .enumerate()
                        .filter_map(|(partition, indices)| {
                            let indices: PrimitiveArray<UInt32Type> = indices.into();
                            (!indices.is_empty()).then_some((partition, indices))
                        })
                        .map(move |(partition, indices)| {
                            // Tracking time required for repartitioned batches construction
                            let _timer = partitioner_timer.timer();

                            // Produce batches based on indices
                            let columns = take_arrays(batch.columns(), &indices, None)?;

                            let mut options = RecordBatchOptions::new();
                            options = options.with_row_count(Some(indices.len()));
                            let batch = RecordBatch::try_new_with_options(
                                batch.schema(),
                                columns,
                                &options,
                            )
                            .unwrap();

                            Ok((partition, batch))
                        });

                    Box::new(it)
                }
            };

        Ok(it)
    }

    // return the number of output partitions
    fn num_partitions(&self) -> usize {
        match self.state {
            BatchPartitionerState::RoundRobin { num_partitions, .. } => num_partitions,
            BatchPartitionerState::Hash { num_partitions, .. } => num_partitions,
        }
    }
}

/// Maps `N` input partitions to `M` output partitions based on a
/// [`Partitioning`] scheme.
///
/// # Background
///
/// DataFusion, like most other commercial systems, with the
/// notable exception of DuckDB, uses the "Exchange Operator" based
/// approach to parallelism which works well in practice given
/// sufficient care in implementation.
///
/// DataFusion's planner picks the target number of partitions and
/// then [`RepartitionExec`] redistributes [`RecordBatch`]es to that number
/// of output partitions.
///
/// For example, given `target_partitions=3` (trying to use 3 cores)
/// but scanning an input with 2 partitions, `RepartitionExec` can be
/// used to get 3 even streams of `RecordBatch`es
///
///
///```text
///        ▲                  ▲                  ▲
///        │                  │                  │
///        │                  │                  │
///        │                  │                  │
/// ┌───────────────┐  ┌───────────────┐  ┌───────────────┐
/// │    GroupBy    │  │    GroupBy    │  │    GroupBy    │
/// │   (Partial)   │  │   (Partial)   │  │   (Partial)   │
/// └───────────────┘  └───────────────┘  └───────────────┘
///        ▲                  ▲                  ▲
///        └──────────────────┼──────────────────┘
///                           │
///              ┌─────────────────────────┐
///              │     RepartitionExec     │
///              │   (hash/round robin)    │
///              └─────────────────────────┘
///                         ▲   ▲
///             ┌───────────┘   └───────────┐
///             │                           │
///             │                           │
///        .─────────.                 .─────────.
///     ,─'           '─.           ,─'           '─.
///    ;      Input      :         ;      Input      :
///    :   Partition 0   ;         :   Partition 1   ;
///     ╲               ╱           ╲               ╱
///      '─.         ,─'             '─.         ,─'
///         `───────'                   `───────'
/// ```
///
/// # Error Handling
///
/// If any of the input partitions return an error, the error is propagated to
/// all output partitions and inputs are not polled again.
///
/// # Output Ordering
///
/// If more than one stream is being repartitioned, the output will be some
/// arbitrary interleaving (and thus unordered) unless
/// [`Self::with_preserve_order`] specifies otherwise.
///
/// # Spilling Architecture
///
/// RepartitionExec uses [`SpillPool`](crate::spill::spill_pool) channels to handle
/// memory pressure during repartitioning. Each (input partition, output partition)
/// pair gets its own SpillPool channel for FIFO ordering.
///
/// ```text
/// Input Partitions (N)          Output Partitions (M)
/// ────────────────────          ─────────────────────
///
///    Input 0 ──┐                      ┌──▶ Output 0
///              │  ┌──────────────┐    │
///              ├─▶│ SpillPool    │────┤
///              │  │ [In0→Out0]   │    │
///    Input 1 ──┤  └──────────────┘    ├──▶ Output 1
///              │                       │
///              │  ┌──────────────┐    │
///              ├─▶│ SpillPool    │────┤
///              │  │ [In1→Out0]   │    │
///    Input 2 ──┤  └──────────────┘    ├──▶ Output 2
///              │                      │
///              │       ... (N×M SpillPools total)
///              │                      │
///              │  ┌──────────────┐    │
///              └─▶│ SpillPool    │────┘
///                 │ [InN→OutM]   │
///                 └──────────────┘
///
/// Each SpillPool maintains FIFO order for its (input, output) pair.
/// See `RepartitionBatch` for details on the memory/spill decision logic.
/// ```
///
/// # Footnote
///
/// The "Exchange Operator" was first described in the 1989 paper
/// [Encapsulation of parallelism in the Volcano query processing
/// system Paper](https://dl.acm.org/doi/pdf/10.1145/93605.98720)
/// which uses the term "Exchange" for the concept of repartitioning
/// data across threads.
#[derive(Debug, Clone)]
pub struct RepartitionExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Boolean flag to decide whether to preserve ordering. If true means
    /// `SortPreservingRepartitionExec`, false means `RepartitionExec`.
    preserve_order: bool,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
    #[cfg(not(feature = "stateless_plan"))]
    /// Inner state that is initialized when the parent calls .execute() on this node
    /// and consumed as soon as the parent starts consuming this node.
    state: RepartitionExecState,
    /// Execution metrics
    #[cfg(not(feature = "stateless_plan"))]
    metrics: ExecutionPlanMetricsSet,
}

#[derive(Debug, Clone)]
struct RepartitionMetrics {
    /// Time in nanos to execute child operator and fetch batches
    fetch_time: metrics::Time,
    /// Repartitioning elapsed time in nanos
    repartition_time: metrics::Time,
    /// Time in nanos for sending resulting batches to channels.
    ///
    /// One metric per output partition.
    send_time: Vec<metrics::Time>,
}

impl RepartitionMetrics {
    pub fn new(
        input_partition: usize,
        num_output_partitions: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        // Time in nanos to execute child operator and fetch batches
        let fetch_time =
            MetricBuilder::new(metrics).subset_time("fetch_time", input_partition);

        // Time in nanos to perform repartitioning
        let repartition_time =
            MetricBuilder::new(metrics).subset_time("repartition_time", input_partition);

        // Time in nanos for sending resulting batches to channels
        let send_time = (0..num_output_partitions)
            .map(|output_partition| {
                let label =
                    metrics::Label::new("outputPartition", output_partition.to_string());
                MetricBuilder::new(metrics)
                    .with_label(label)
                    .subset_time("send_time", input_partition)
            })
            .collect();

        Self {
            fetch_time,
            repartition_time,
            send_time,
        }
    }
}

impl RepartitionExec {
    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning {
        &self.cache.partitioning
    }

    /// Get preserve_order flag of the RepartitionExec
    /// `true` means `SortPreservingRepartitionExec`, `false` means `RepartitionExec`
    pub fn preserve_order(&self) -> bool {
        self.preserve_order
    }

    /// Get name used to display this Exec
    pub fn name(&self) -> &str {
        "RepartitionExec"
    }
}

impl DisplayAs for RepartitionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_partition_count = self.input.output_partitioning().partition_count();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "{}: partitioning={}, input_partitions={}",
                    self.name(),
                    self.partitioning(),
                    input_partition_count,
                )?;

                if self.preserve_order {
                    write!(f, ", preserve_order=true")?;
                } else if input_partition_count <= 1
                    && self.input.output_ordering().is_some()
                {
                    // Make it explicit that repartition maintains sortedness for a single input partition even
                    // when `preserve_sort order` is false
                    write!(f, ", maintains_sort_order=true")?;
                }

                if let Some(sort_exprs) = self.sort_exprs() {
                    write!(f, ", sort_exprs={}", sort_exprs.clone())?;
                }
                Ok(())
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "partitioning_scheme={}", self.partitioning(),)?;
                let output_partition_count = self.partitioning().partition_count();
                let input_to_output_partition_str =
                    format!("{input_partition_count} -> {output_partition_count}");
                writeln!(
                    f,
                    "partition_count(in->out)={input_to_output_partition_str}"
                )?;

                if self.preserve_order {
                    writeln!(f, "preserve_order={}", self.preserve_order)?;
                }
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for RepartitionExec {
    fn name(&self) -> &'static str {
        "RepartitionExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut repartition = RepartitionExec::try_new(
            children.swap_remove(0),
            self.partitioning().clone(),
        )?;
        if self.preserve_order {
            repartition = repartition.with_preserve_order();
        }
        Ok(Arc::new(repartition))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![matches!(self.partitioning(), Partitioning::Hash(_, _))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Self::maintains_input_order_helper(self.input(), self.preserve_order)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        #[cfg(feature = "stateless_plan")] state: &Arc<PlanStateNode>,
    ) -> Result<SendableRecordBatchStream> {
        #[cfg(not(feature = "stateless_plan"))]
        #[expect(unused)]
        let state = ();
        use crate::plan_metrics;

        trace!(
            "Start {}::execute for partition: {}",
            self.name(),
            partition
        );

        let spill_metrics = SpillMetrics::new(plan_metrics!(self, state), partition);

        let input = Arc::clone(&self.input);
        let partitioning = self.partitioning().clone();
        let metrics = plan_metrics!(self, state).clone();
        let preserve_order = self.sort_exprs().is_some();
        let name = self.name().to_owned();
        let schema = self.schema();
        let schema_captured = Arc::clone(&schema);

        let spill_manager = SpillManager::new(
            Arc::clone(&context.runtime_env()),
            spill_metrics,
            input.schema(),
        );

        // Get existing ordering to use for merging
        let sort_exprs = self.sort_exprs().cloned();

        #[cfg(feature = "stateless_plan")]
        let exec_state = Arc::clone(state.get_or_init_state(|| {
            Arc::new(Mutex::new(RepartitionExecStateInner::default()))
        }));

        #[cfg(not(feature = "stateless_plan"))]
        let exec_state = Arc::clone(&self.state);

        if let Some(mut exec_state) = exec_state.try_lock() {
            exec_state.ensure_input_streams_initialized(
                &input,
                &metrics,
                partitioning.partition_count(),
                &context,
                #[cfg(feature = "stateless_plan")]
                state,
            )?;
        }

        let num_input_partitions = input.output_partitioning().partition_count();

        #[cfg(feature = "stateless_plan")]
        let state = Arc::clone(state);
        let stream = futures::stream::once(async move {
            // lock scope
            let (rx, reservation, spill_readers, abort_helper) = {
                // lock mutexes
                let mut exec_state = exec_state.lock();
                let exec_state = exec_state.consume_input_streams(
                    &input,
                    &metrics,
                    &partitioning,
                    preserve_order,
                    &name,
                    &context,
                    spill_manager.clone(),
                    #[cfg(feature = "stateless_plan")]
                    &state,
                )?;

                // now return stream for the specified *output* partition which will
                // read from the channel
                let PartitionChannels {
                    rx,
                    reservation,
                    spill_readers,
                    ..
                } = exec_state
                    .channels
                    .remove(&partition)
                    .expect("partition not used yet");

                (
                    rx,
                    reservation,
                    spill_readers,
                    Arc::clone(&exec_state.abort_helper),
                )
            };

            trace!(
                "Before returning stream in {name}::execute for partition: {partition}"
            );

            if preserve_order {
                // Store streams from all the input partitions:
                // Each input partition gets its own spill reader to maintain proper FIFO ordering
                let input_streams = rx
                    .into_iter()
                    .zip(spill_readers)
                    .map(|(receiver, spill_stream)| {
                        // In preserve_order mode, each receiver corresponds to exactly one input partition
                        Box::pin(PerPartitionStream::new(
                            Arc::clone(&schema_captured),
                            receiver,
                            Arc::clone(&abort_helper),
                            Arc::clone(&reservation),
                            spill_stream,
                            1, // Each receiver handles one input partition
                            BaselineMetrics::new(&metrics, partition),
                            None, // subsequent merge sort already does batching https://github.com/apache/datafusion/blob/e4dcf0c85611ad0bd291f03a8e03fe56d773eb16/datafusion/physical-plan/src/sorts/merge.rs#L286
                        )) as SendableRecordBatchStream
                    })
                    .collect::<Vec<_>>();
                // Note that receiver size (`rx.len()`) and `num_input_partitions` are same.

                // Merge streams (while preserving ordering) coming from
                // input partitions to this partition:
                let fetch = None;
                let merge_reservation =
                    MemoryConsumer::new(format!("{name}[Merge {partition}]"))
                        .register(context.memory_pool());
                StreamingMergeBuilder::new()
                    .with_streams(input_streams)
                    .with_schema(schema_captured)
                    .with_expressions(&sort_exprs.unwrap())
                    .with_metrics(BaselineMetrics::new(&metrics, partition))
                    .with_batch_size(context.session_config().batch_size())
                    .with_fetch(fetch)
                    .with_reservation(merge_reservation)
                    .with_spill_manager(spill_manager)
                    .build()
            } else {
                // Non-preserve-order case: single input stream, so use the first spill reader
                let spill_stream = spill_readers
                    .into_iter()
                    .next()
                    .expect("at least one spill reader should exist");

                Ok(Box::pin(PerPartitionStream::new(
                    schema_captured,
                    rx.into_iter()
                        .next()
                        .expect("at least one receiver should exist"),
                    abort_helper,
                    reservation,
                    spill_stream,
                    num_input_partitions,
                    BaselineMetrics::new(&metrics, partition),
                    Some(context.session_config().batch_size()),
                )) as SendableRecordBatchStream)
            }
        })
        .try_flatten();
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    #[cfg(not(feature = "stateless_plan"))]
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(partition) = partition {
            let partition_count = self.partitioning().partition_count();
            if partition_count == 0 {
                return Ok(Statistics::new_unknown(&self.schema()));
            }

            assert_or_internal_err!(
                partition < partition_count,
                "RepartitionExec invalid partition {} (expected less than {})",
                partition,
                partition_count
            );

            let mut stats = self.input.partition_statistics(None)?;

            // Distribute statistics across partitions
            stats.num_rows = stats
                .num_rows
                .get_value()
                .map(|rows| Precision::Inexact(rows / partition_count))
                .unwrap_or(Precision::Absent);
            stats.total_byte_size = stats
                .total_byte_size
                .get_value()
                .map(|bytes| Precision::Inexact(bytes / partition_count))
                .unwrap_or(Precision::Absent);

            // Make all column stats unknown
            stats.column_statistics = stats
                .column_statistics
                .iter()
                .map(|_| ColumnStatistics::new_unknown())
                .collect();

            Ok(stats)
        } else {
            self.input.partition_statistics(None)
        }
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If the projection does not narrow the schema, we should not try to push it down.
        if projection.expr().len() >= projection.input().schema().fields().len() {
            return Ok(None);
        }

        // If pushdown is not beneficial or applicable, break it.
        if projection.benefits_from_input_partitioning()[0]
            || !all_columns(projection.expr())
        {
            return Ok(None);
        }

        let new_projection = make_with_child(projection, self.input())?;

        let new_partitioning = match self.partitioning() {
            Partitioning::Hash(partitions, size) => {
                let mut new_partitions = vec![];
                for partition in partitions {
                    let Some(new_partition) =
                        update_expr(partition, projection.expr(), false)?
                    else {
                        return Ok(None);
                    };
                    new_partitions.push(new_partition);
                }
                Partitioning::Hash(new_partitions, *size)
            }
            others => others.clone(),
        };

        Ok(Some(Arc::new(RepartitionExec::try_new(
            new_projection,
            new_partitioning,
        )?)))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // RepartitionExec only maintains input order if preserve_order is set
        // or if there's only one partition
        if !self.maintains_input_order()[0] {
            return Ok(SortOrderPushdownResult::Unsupported);
        }

        // Delegate to the child and wrap with a new RepartitionExec
        self.input.try_pushdown_sort(order)?.try_map(|new_input| {
            let mut new_repartition =
                RepartitionExec::try_new(new_input, self.partitioning().clone())?;
            if self.preserve_order {
                new_repartition = new_repartition.with_preserve_order();
            }
            Ok(Arc::new(new_repartition) as Arc<dyn ExecutionPlan>)
        })
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        use Partitioning::*;
        let mut new_properties = self.cache.clone();
        new_properties.partitioning = match new_properties.partitioning {
            RoundRobinBatch(_) => RoundRobinBatch(target_partitions),
            Hash(hash, _) => Hash(hash, target_partitions),
            UnknownPartitioning(_) => UnknownPartitioning(target_partitions),
        };
        Ok(Some(Arc::new(Self {
            input: Arc::clone(&self.input),
            preserve_order: self.preserve_order,
            cache: new_properties,
            #[cfg(not(feature = "stateless_plan"))]
            state: Arc::clone(&self.state),
            #[cfg(not(feature = "stateless_plan"))]
            metrics: self.metrics.clone(),
        })))
    }
}

impl RepartitionExec {
    /// Create a new RepartitionExec, that produces output `partitioning`, and
    /// does not preserve the order of the input (see [`Self::with_preserve_order`]
    /// for more details)
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
    ) -> Result<Self> {
        let preserve_order = false;
        let cache = Self::compute_properties(&input, partitioning, preserve_order);
        Ok(RepartitionExec {
            input,
            preserve_order,
            cache,
            #[cfg(not(feature = "stateless_plan"))]
            state: Default::default(),
            #[cfg(not(feature = "stateless_plan"))]
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn maintains_input_order_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_order: bool,
    ) -> Vec<bool> {
        // We preserve ordering when repartition is order preserving variant or input partitioning is 1
        vec![preserve_order || input.output_partitioning().partition_count() <= 1]
    }

    fn eq_properties_helper(
        input: &Arc<dyn ExecutionPlan>,
        preserve_order: bool,
    ) -> EquivalenceProperties {
        // Equivalence Properties
        let mut eq_properties = input.equivalence_properties().clone();
        // If the ordering is lost, reset the ordering equivalence class:
        if !Self::maintains_input_order_helper(input, preserve_order)[0] {
            eq_properties.clear_orderings();
        }
        // When there are more than one input partitions, they will be fused at the output.
        // Therefore, remove per partition constants.
        if input.output_partitioning().partition_count() > 1 {
            eq_properties.clear_per_partition_constants();
        }
        eq_properties
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        preserve_order: bool,
    ) -> PlanProperties {
        PlanProperties::new(
            Self::eq_properties_helper(input, preserve_order),
            partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        )
        .with_scheduling_type(SchedulingType::Cooperative)
        .with_evaluation_type(EvaluationType::Eager)
    }

    /// Specify if this repartitioning operation should preserve the order of
    /// rows from its input when producing output. Preserving order is more
    /// expensive at runtime, so should only be set if the output of this
    /// operator can take advantage of it.
    ///
    /// If the input is not ordered, or has only one partition, this is a no op,
    /// and the node remains a `RepartitionExec`.
    pub fn with_preserve_order(mut self) -> Self {
        self.preserve_order =
                // If the input isn't ordered, there is no ordering to preserve
                self.input.output_ordering().is_some() &&
                // if there is only one input partition, merging is not required
                // to maintain order
                self.input.output_partitioning().partition_count() > 1;
        let eq_properties = Self::eq_properties_helper(&self.input, self.preserve_order);
        self.cache = self.cache.with_eq_properties(eq_properties);
        self
    }

    /// Return the sort expressions that are used to merge
    fn sort_exprs(&self) -> Option<&LexOrdering> {
        if self.preserve_order {
            self.input.output_ordering()
        } else {
            None
        }
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning
    ///
    /// `output_channels` holds the output sending channels for each output partition
    async fn pull_from_input(
        mut stream: SendableRecordBatchStream,
        mut output_channels: HashMap<usize, OutputChannel>,
        partitioning: Partitioning,
        metrics: RepartitionMetrics,
        input_partition: usize,
        num_input_partitions: usize,
    ) -> Result<()> {
        let mut partitioner = BatchPartitioner::try_new(
            partitioning,
            metrics.repartition_time.clone(),
            input_partition,
            num_input_partitions,
        )?;

        // While there are still outputs to send to, keep pulling inputs
        let mut batches_until_yield = partitioner.num_partitions();
        while !output_channels.is_empty() {
            // fetch the next batch
            let timer = metrics.fetch_time.timer();
            let result = stream.next().await;
            timer.done();

            // Input is done
            let batch = match result {
                Some(result) => result?,
                None => break,
            };

            // Handle empty batch
            if batch.num_rows() == 0 {
                continue;
            }

            for res in partitioner.partition_iter(batch)? {
                let (partition, batch) = res?;
                let size = batch.get_array_memory_size();

                let timer = metrics.send_time[partition].timer();
                // if there is still a receiver, send to it
                if let Some(channel) = output_channels.get_mut(&partition) {
                    let (batch_to_send, is_memory_batch) =
                        match channel.reservation.lock().try_grow(size) {
                            Ok(_) => {
                                // Memory available - send in-memory batch
                                (RepartitionBatch::Memory(batch), true)
                            }
                            Err(_) => {
                                // We're memory limited - spill to SpillPool
                                // SpillPool handles file handle reuse and rotation
                                channel.spill_writer.push_batch(&batch)?;
                                // Send marker indicating batch was spilled
                                (RepartitionBatch::Spilled, false)
                            }
                        };

                    if channel.sender.send(Some(Ok(batch_to_send))).await.is_err() {
                        // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                        // Only shrink memory if it was a memory batch
                        if is_memory_batch {
                            channel.reservation.lock().shrink(size);
                        }
                        output_channels.remove(&partition);
                    }
                }
                timer.done();
            }

            // If the input stream is endless, we may spin forever and
            // never yield back to tokio.  See
            // https://github.com/apache/datafusion/issues/5278.
            //
            // However, yielding on every batch causes a bottleneck
            // when running with multiple cores. See
            // https://github.com/apache/datafusion/issues/6290
            //
            // Thus, heuristically yield after producing num_partition
            // batches
            //
            // In round robin this is ideal as each input will get a
            // new batch. In hash partitioning it may yield too often
            // on uneven distributions even if some partition can not
            // make progress, but parallelism is going to be limited
            // in that case anyways
            if batches_until_yield == 0 {
                tokio::task::yield_now().await;
                batches_until_yield = partitioner.num_partitions();
            } else {
                batches_until_yield -= 1;
            }
        }

        // Spill writers will auto-finalize when dropped
        // No need for explicit flush
        Ok(())
    }

    /// Waits for `input_task` which is consuming one of the inputs to
    /// complete. Upon each successful completion, sends a `None` to
    /// each of the output tx channels to signal one of the inputs is
    /// complete. Upon error, propagates the errors to all output tx
    /// channels.
    async fn wait_for_task(
        input_task: SpawnedTask<Result<()>>,
        txs: HashMap<usize, DistributionSender<MaybeBatch>>,
    ) {
        // wait for completion, and propagate error
        // note we ignore errors on send (.ok) as that means the receiver has already shutdown.

        match input_task.join().await {
            // Error in joining task
            Err(e) => {
                let e = Arc::new(e);

                for (_, tx) in txs {
                    let err = Err(DataFusionError::Context(
                        "Join Error".to_string(),
                        Box::new(DataFusionError::External(Box::new(Arc::clone(&e)))),
                    ));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Error from running input task
            Ok(Err(e)) => {
                // send the same Arc'd error to all output partitions
                let e = Arc::new(e);

                for (_, tx) in txs {
                    // wrap it because need to send error to all output partitions
                    let err = Err(DataFusionError::from(&e));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {
                // notify each output partition that this input partition has no more data
                for (_partition, tx) in txs {
                    tx.send(None).await.ok();
                }
            }
        }
    }
}

/// State for tracking whether we're reading from memory channel or spill stream.
///
/// This state machine ensures proper ordering when batches are mixed between memory
/// and spilled storage. When a [`RepartitionBatch::Spilled`] marker is received,
/// the stream must block on the spill stream until the corresponding batch arrives.
///
/// # State Machine
///
/// ```text
///                        ┌─────────────────┐
///                   ┌───▶│  ReadingMemory  │◀───┐
///                   │    └────────┬────────┘    │
///                   │             │             │
///                   │     Poll channel          │
///                   │             │             │
///                   │  ┌──────────┼─────────────┐
///                   │  │          │             │
///                   │  ▼          ▼             │
///                   │ Memory   Spilled          │
///       Got batch   │ batch    marker           │
///       from spill  │  │          │             │
///                   │  │          ▼             │
///                   │  │  ┌──────────────────┐  │
///                   │  │  │ ReadingSpilled   │  │
///                   │  │  └────────┬─────────┘  │
///                   │  │           │            │
///                   │  │   Poll spill_stream    │
///                   │  │           │            │
///                   │  │           ▼            │
///                   │  │      Get batch         │
///                   │  │           │            │
///                   └──┴───────────┴────────────┘
///                                  │
///                                  ▼
///                           Return batch
///                     (Order preserved within
///                      (input, output) pair)
/// ```
///
/// The transition to `ReadingSpilled` blocks further channel polling to maintain
/// FIFO ordering - we cannot read the next item from the channel until the spill
/// stream provides the current batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamState {
    /// Reading from the memory channel (normal operation)
    ReadingMemory,
    /// Waiting for a spilled batch from the spill stream.
    /// Must not poll channel until spilled batch is received to preserve ordering.
    ReadingSpilled,
}

/// This struct converts a receiver to a stream.
/// Receiver receives data on an SPSC channel.
struct PerPartitionStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,

    /// channel containing the repartitioned batches
    receiver: DistributionReceiver<MaybeBatch>,

    /// Handle to ensure background tasks are killed when no longer needed.
    _drop_helper: Arc<Vec<SpawnedTask<()>>>,

    /// Memory reservation.
    reservation: SharedMemoryReservation,

    /// Infinite stream for reading from the spill pool
    spill_stream: SendableRecordBatchStream,

    /// Internal state indicating if we are reading from memory or spill stream
    state: StreamState,

    /// Number of input partitions that have not yet finished.
    /// In non-preserve-order mode, multiple input partitions send to the same channel,
    /// each sending None when complete. We must wait for all of them.
    remaining_partitions: usize,

    /// Execution metrics
    baseline_metrics: BaselineMetrics,

    /// None for sort preserving variant (merge sort already does coalescing)
    batch_coalescer: Option<LimitedBatchCoalescer>,
}

impl PerPartitionStream {
    #[expect(clippy::too_many_arguments)]
    fn new(
        schema: SchemaRef,
        receiver: DistributionReceiver<MaybeBatch>,
        drop_helper: Arc<Vec<SpawnedTask<()>>>,
        reservation: SharedMemoryReservation,
        spill_stream: SendableRecordBatchStream,
        num_input_partitions: usize,
        baseline_metrics: BaselineMetrics,
        batch_size: Option<usize>,
    ) -> Self {
        let batch_coalescer =
            batch_size.map(|s| LimitedBatchCoalescer::new(Arc::clone(&schema), s, None));
        Self {
            schema,
            receiver,
            _drop_helper: drop_helper,
            reservation,
            spill_stream,
            state: StreamState::ReadingMemory,
            remaining_partitions: num_input_partitions,
            baseline_metrics,
            batch_coalescer,
        }
    }

    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        use futures::StreamExt;
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let _timer = cloned_time.timer();

        loop {
            match self.state {
                StreamState::ReadingMemory => {
                    // Poll the memory channel for next message
                    let value = match self.receiver.recv().poll_unpin(cx) {
                        Poll::Ready(v) => v,
                        Poll::Pending => {
                            // Nothing from channel, wait
                            return Poll::Pending;
                        }
                    };

                    match value {
                        Some(Some(v)) => match v {
                            Ok(RepartitionBatch::Memory(batch)) => {
                                // Release memory and return batch
                                self.reservation
                                    .lock()
                                    .shrink(batch.get_array_memory_size());
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            Ok(RepartitionBatch::Spilled) => {
                                // Batch was spilled, transition to reading from spill stream
                                // We must block on spill stream until we get the batch
                                // to preserve ordering
                                self.state = StreamState::ReadingSpilled;
                                continue;
                            }
                            Err(e) => {
                                return Poll::Ready(Some(Err(e)));
                            }
                        },
                        Some(None) => {
                            // One input partition finished
                            self.remaining_partitions -= 1;
                            if self.remaining_partitions == 0 {
                                // All input partitions finished
                                return Poll::Ready(None);
                            }
                            // Continue to poll for more data from other partitions
                            continue;
                        }
                        None => {
                            // Channel closed unexpectedly
                            return Poll::Ready(None);
                        }
                    }
                }
                StreamState::ReadingSpilled => {
                    // Poll spill stream for the spilled batch
                    match self.spill_stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            self.state = StreamState::ReadingMemory;
                            return Poll::Ready(Some(Ok(batch)));
                        }
                        Poll::Ready(Some(Err(e))) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None) => {
                            // Spill stream ended, keep draining the memory channel
                            self.state = StreamState::ReadingMemory;
                        }
                        Poll::Pending => {
                            // Spilled batch not ready yet, must wait
                            // This preserves ordering by blocking until spill data arrives
                            return Poll::Pending;
                        }
                    }
                }
            }
        }
    }

    fn poll_next_and_coalesce(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
        coalescer: &mut LimitedBatchCoalescer,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let cloned_time = self.baseline_metrics.elapsed_compute().clone();
        let mut completed = false;

        loop {
            if let Some(batch) = coalescer.next_completed_batch() {
                return Poll::Ready(Some(Ok(batch)));
            }
            if completed {
                return Poll::Ready(None);
            }

            match ready!(self.poll_next_inner(cx)) {
                Some(Ok(batch)) => {
                    let _timer = cloned_time.timer();
                    if let Err(err) = coalescer.push_batch(batch) {
                        return Poll::Ready(Some(Err(err)));
                    }
                }
                Some(err) => {
                    return Poll::Ready(Some(err));
                }
                None => {
                    completed = true;
                    let _timer = cloned_time.timer();
                    if let Err(err) = coalescer.finish() {
                        return Poll::Ready(Some(Err(err)));
                    }
                }
            }
        }
    }
}

impl Stream for PerPartitionStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll;
        if let Some(mut coalescer) = self.batch_coalescer.take() {
            poll = self.poll_next_and_coalesce(cx, &mut coalescer);
            self.batch_coalescer = Some(coalescer);
        } else {
            poll = self.poll_next_inner(cx);
        }
        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for PerPartitionStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::execution_plan::execute_plan;
    use crate::test::TestMemoryExec;
    use crate::test::{collect_counting_rows, collect_partitions, collect_with};
    use crate::{
        test::{
            assert_is_pending,
            exec::{
                BarrierExec, BlockingExec, ErrorExec, MockExec,
                assert_strong_count_converges_to_zero,
            },
        },
        {collect, expressions::col},
    };

    use arrow::array::{ArrayRef, StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::cast::as_string_array;
    use datafusion_common::exec_err;
    use datafusion_common::test_util::batches_to_sort_string;
    use datafusion_common_runtime::JoinSet;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use insta::assert_snapshot;

    #[tokio::test]
    async fn one_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let partitions = vec![partition];

        // repartition from 1 input to 4 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(4)).await?;

        assert_eq!(4, output_partitions.len());
        for partition in &output_partitions {
            assert_eq!(1, partition.len());
        }
        assert_eq!(13 * 8, output_partitions[0][0].num_rows());
        assert_eq!(13 * 8, output_partitions[1][0].num_rows());
        assert_eq!(12 * 8, output_partitions[2][0].num_rows());
        assert_eq!(12 * 8, output_partitions[3][0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_one_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 1 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(1)).await?;

        assert_eq!(1, output_partitions.len());
        assert_eq!(150 * 8, output_partitions[0][0].num_rows());

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_round_robin() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        // repartition from 3 input to 5 output
        let output_partitions =
            repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await?;

        let total_rows_per_partition = 8 * 50 * 3 / 5;
        assert_eq!(5, output_partitions.len());
        for partition in output_partitions {
            assert_eq!(1, partition.len());
            assert_eq!(total_rows_per_partition, partition[0].num_rows());
        }

        Ok(())
    }

    #[tokio::test]
    async fn many_to_many_hash_partition() -> Result<()> {
        // define input partitions
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let partitions = vec![partition.clone(), partition.clone(), partition.clone()];

        let output_partitions = repartition(
            &schema,
            partitions,
            Partitioning::Hash(vec![col("c0", &schema)?], 8),
        )
        .await?;

        let total_rows: usize = output_partitions
            .iter()
            .map(|x| x.iter().map(|x| x.num_rows()).sum::<usize>())
            .sum();

        assert_eq!(8, output_partitions.len());
        assert_eq!(total_rows, 8 * 50 * 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_repartition_with_coalescing() -> Result<()> {
        let schema = test_schema();
        // create 50 batches, each having 8 rows
        let partition = create_vec_batches(50);
        let partitions = vec![partition.clone(), partition.clone()];
        let partitioning = Partitioning::RoundRobinBatch(1);

        let session_config = SessionConfig::new().with_batch_size(200);
        let task_ctx = TaskContext::default().with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec = TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(exec, partitioning)?);

        collect_with(exec, task_ctx, |_, batch| {
            assert_eq!(200, batch?.num_rows());
            Ok(())
        })
        .await?;

        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    async fn repartition(
        schema: &SchemaRef,
        input_partitions: Vec<Vec<RecordBatch>>,
        partitioning: Partitioning,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let task_ctx = Arc::new(TaskContext::default());
        // create physical plan
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(schema), None)?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(exec, partitioning)?);

        // execute and collect results
        let (output_partitions, _) = collect_partitions(exec, task_ctx).await?;
        Ok(output_partitions)
    }

    #[tokio::test]
    async fn many_to_many_round_robin_within_tokio_task() -> Result<()> {
        let handle: SpawnedTask<Result<Vec<Vec<RecordBatch>>>> =
            SpawnedTask::spawn(async move {
                // define input partitions
                let schema = test_schema();
                let partition = create_vec_batches(50);
                let partitions =
                    vec![partition.clone(), partition.clone(), partition.clone()];

                // repartition from 3 input to 5 output
                repartition(&schema, partitions, Partitioning::RoundRobinBatch(5)).await
            });

        let output_partitions = handle.join().await.unwrap().unwrap();

        let total_rows_per_partition = 8 * 50 * 3 / 5;
        assert_eq!(5, output_partitions.len());
        for partition in output_partitions {
            assert_eq!(1, partition.len());
            assert_eq!(total_rows_per_partition, partition[0].num_rows());
        }

        Ok(())
    }

    #[tokio::test]
    async fn unsupported_partitioning() {
        let task_ctx = Arc::new(TaskContext::default());
        // have to send at least one batch through to provoke error
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch)], schema);
        // This generates an error (partitioning type not supported)
        // but only after the plan is executed. The error should be
        // returned and no results produced
        let partitioning = Partitioning::UnknownPartitioning(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();
        let output_stream = execute_plan(Arc::new(exec), 0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string
                .contains("Unsupported repartitioning scheme UnknownPartitioning(1)"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn error_for_input_exec() {
        // This generates an error on a call to execute. The error
        // should be returned and no results produced.

        let task_ctx = Arc::new(TaskContext::default());
        let input = ErrorExec::new();
        let partitioning = Partitioning::RoundRobinBatch(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Expect that an error is returned
        let result_string = execute_plan(Arc::new(exec), 0, task_ctx)
            .err()
            .unwrap()
            .to_string();

        assert!(
            result_string.contains("ErrorExec, unsurprisingly, errored in partition 0"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn repartition_with_error_in_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let batch = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        // input stream returns one good batch and then one error. The
        // error should be returned.
        let err = exec_err!("bad data error");

        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch), err], schema);
        let partitioning = Partitioning::RoundRobinBatch(1);
        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        // Note: this should pass (the stream can be created) but the
        // error when the input is executed should get passed back
        let output_stream = execute_plan(Arc::new(exec), 0, task_ctx).unwrap();

        // Expect that an error is returned
        let result_string = crate::common::collect(output_stream)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            result_string.contains("bad data error"),
            "actual: {result_string}"
        );
    }

    #[tokio::test]
    async fn repartition_with_delayed_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        // The mock exec doesn't return immediately (instead it
        // requires the input to wait at least once)
        let schema = batch1.schema();
        let expected_batches = vec![batch1.clone(), batch2.clone()];
        let input = MockExec::new(vec![Ok(batch1), Ok(batch2)], schema);
        let partitioning = Partitioning::RoundRobinBatch(1);

        let exec = RepartitionExec::try_new(Arc::new(input), partitioning).unwrap();

        assert_snapshot!(batches_to_sort_string(&expected_batches), @r"
        +------------------+
        | my_awesome_field |
        +------------------+
        | bar              |
        | baz              |
        | foo              |
        | frob             |
        +------------------+
        ");

        let output_stream = execute_plan(Arc::new(exec), 0, task_ctx).unwrap();
        let batches = crate::common::collect(output_stream).await.unwrap();

        assert_snapshot!(batches_to_sort_string(&batches), @r"
        +------------------+
        | my_awesome_field |
        +------------------+
        | bar              |
        | baz              |
        | foo              |
        | frob             |
        +------------------+
        ");
    }

    #[tokio::test]
    async fn robin_repartition_with_dropping_output_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let partitioning = Partitioning::RoundRobinBatch(2);
        // The barrier exec waits to be pinged
        // requires the input to wait at least once)
        let input = Arc::new(make_barrier_exec());

        // partition into two output streams
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(
                Arc::clone(&input) as Arc<dyn ExecutionPlan>,
                partitioning,
            )
            .unwrap(),
        );

        let output_stream0 =
            execute_plan(Arc::clone(&exec), 0, Arc::clone(&task_ctx)).unwrap();
        let output_stream1 =
            execute_plan(Arc::clone(&exec), 1, Arc::clone(&task_ctx)).unwrap();

        // now, purposely drop output stream 0
        // *before* any outputs are produced
        drop(output_stream0);

        // Now, start sending input
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });

        // output stream 1 should *not* error and have one of the input batches
        let batches = crate::common::collect(output_stream1).await.unwrap();

        assert_snapshot!(batches_to_sort_string(&batches), @r"
        +------------------+
        | my_awesome_field |
        +------------------+
        | baz              |
        | frob             |
        | gar              |
        | goo              |
        +------------------+
        ");
    }

    #[tokio::test]
    // As the hash results might be different on different platforms or
    // with different compilers, we will compare the same execution with
    // and without dropping the output stream.
    async fn hash_repartition_with_dropping_output_stream() {
        let task_ctx = Arc::new(TaskContext::default());
        let partitioning = Partitioning::Hash(
            vec![Arc::new(crate::expressions::Column::new(
                "my_awesome_field",
                0,
            ))],
            2,
        );

        // We first collect the results without dropping the output stream.
        let input = Arc::new(make_barrier_exec());
        let exec = RepartitionExec::try_new(
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            partitioning.clone(),
        )
        .unwrap();
        let output_stream1 =
            execute_plan(Arc::new(exec), 1, Arc::clone(&task_ctx)).unwrap();
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });
        let batches_without_drop = crate::common::collect(output_stream1).await.unwrap();

        // run some checks on the result
        let items_vec = str_batches_to_vec(&batches_without_drop);
        let items_set: HashSet<&str> = items_vec.iter().copied().collect();
        assert_eq!(items_vec.len(), items_set.len());
        let source_str_set: HashSet<&str> =
            ["foo", "bar", "frob", "baz", "goo", "gar", "grob", "gaz"]
                .iter()
                .copied()
                .collect();
        assert_eq!(items_set.difference(&source_str_set).count(), 0);

        // Now do the same but dropping the stream before waiting for the barrier
        let input = Arc::new(make_barrier_exec());
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(
                Arc::clone(&input) as Arc<dyn ExecutionPlan>,
                partitioning,
            )
            .unwrap(),
        );
        let output_stream0 =
            execute_plan(Arc::clone(&exec), 0, Arc::clone(&task_ctx)).unwrap();
        let output_stream1 =
            execute_plan(Arc::clone(&exec), 1, Arc::clone(&task_ctx)).unwrap();
        // now, purposely drop output stream 0
        // *before* any outputs are produced
        drop(output_stream0);
        let mut background_task = JoinSet::new();
        background_task.spawn(async move {
            input.wait().await;
        });
        let batches_with_drop = crate::common::collect(output_stream1).await.unwrap();

        let items_vec_with_drop = str_batches_to_vec(&batches_with_drop);
        let items_set_with_drop: HashSet<&str> =
            items_vec_with_drop.iter().copied().collect();
        assert_eq!(
            items_set_with_drop.symmetric_difference(&items_set).count(),
            0
        );
    }

    fn str_batches_to_vec(batches: &[RecordBatch]) -> Vec<&str> {
        batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.columns().len(), 1);
                let string_array = as_string_array(batch.column(0))
                    .expect("Unexpected type for repartitioned batch");

                string_array
                    .iter()
                    .map(|v| v.expect("Unexpected null"))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
    }

    /// Create a BarrierExec that returns two partitions of two batches each
    fn make_barrier_exec() -> BarrierExec {
        let batch1 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef,
        )])
        .unwrap();

        let batch2 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["frob", "baz"])) as ArrayRef,
        )])
        .unwrap();

        let batch3 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["goo", "gar"])) as ArrayRef,
        )])
        .unwrap();

        let batch4 = RecordBatch::try_from_iter(vec![(
            "my_awesome_field",
            Arc::new(StringArray::from(vec!["grob", "gaz"])) as ArrayRef,
        )])
        .unwrap();

        // The barrier exec waits to be pinged
        // requires the input to wait at least once)
        let schema = batch1.schema();
        BarrierExec::new(vec![vec![batch1, batch2], vec![batch3, batch4]], schema)
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let repartition_exec = Arc::new(RepartitionExec::try_new(
            blocking_exec,
            Partitioning::UnknownPartitioning(1),
        )?);

        let fut = collect(repartition_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn hash_repartition_avoid_empty_batch() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let batch = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(StringArray::from(vec!["foo"])) as ArrayRef,
        )])
        .unwrap();
        let partitioning = Partitioning::Hash(
            vec![Arc::new(crate::expressions::Column::new("a", 0))],
            2,
        );
        let schema = batch.schema();
        let input = MockExec::new(vec![Ok(batch)], schema);
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(Arc::new(input), partitioning).unwrap());
        let output_stream0 = execute_plan(Arc::clone(&exec), 0, Arc::clone(&task_ctx))?;
        let batch0 = crate::common::collect(output_stream0).await.unwrap();
        let output_stream1 = execute_plan(Arc::clone(&exec), 1, Arc::clone(&task_ctx))?;
        let batch1 = crate::common::collect(output_stream1).await.unwrap();
        assert!(batch0.is_empty() || batch1.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn repartition_with_spilling() -> Result<()> {
        // Test that repartition successfully spills to disk when memory is constrained
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let input_partitions = vec![partition];
        let partitioning = Partitioning::RoundRobinBatch(4);

        // Set up context with very tight memory limit to force spilling
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1, 1.0)
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(&schema), None)?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(exec, partitioning)?);

        // Collect all partitions - should succeed by spilling to disk
        let (total_rows, metrics) = collect_counting_rows(exec, task_ctx).await?;

        // Verify we got all the data (50 batches * 8 rows each)
        assert_eq!(total_rows, 50 * 8);

        // Verify spilling metrics to confirm spilling actually happened

        assert!(
            metrics.spill_count().unwrap() > 0,
            "Expected spill_count > 0, but got {:?}",
            metrics.spill_count()
        );
        println!("Spilled {} times", metrics.spill_count().unwrap());
        assert!(
            metrics.spilled_bytes().unwrap() > 0,
            "Expected spilled_bytes > 0, but got {:?}",
            metrics.spilled_bytes()
        );
        println!(
            "Spilled {} bytes in {} spills",
            metrics.spilled_bytes().unwrap(),
            metrics.spill_count().unwrap()
        );
        assert!(
            metrics.spilled_rows().unwrap() > 0,
            "Expected spilled_rows > 0, but got {:?}",
            metrics.spilled_rows()
        );
        println!("Spilled {} rows", metrics.spilled_rows().unwrap());

        Ok(())
    }

    #[tokio::test]
    async fn repartition_with_partial_spilling() -> Result<()> {
        // Test that repartition can handle partial spilling (some batches in memory, some spilled)
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let input_partitions = vec![partition];
        let partitioning = Partitioning::RoundRobinBatch(4);

        // Set up context with moderate memory limit to force partial spilling
        // 2KB should allow some batches in memory but force others to spill
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(2 * 1024, 1.0)
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(&schema), None)?;
        let exec = RepartitionExec::try_new(exec, partitioning)?;

        // Collect all partitions - should succeed with partial spilling
        let (total_rows, metrics) =
            collect_counting_rows(Arc::new(exec), task_ctx).await?;

        // Verify we got all the data (50 batches * 8 rows each)
        assert_eq!(total_rows, 50 * 8);

        // Verify partial spilling metrics
        // Verify spilling metrics to confirm spilling actually happened
        let spill_count = metrics.spill_count().unwrap();
        let spilled_rows = metrics.spilled_rows().unwrap();
        let spilled_bytes = metrics.spilled_bytes().unwrap();

        assert!(
            spill_count > 0,
            "Expected some spilling to occur, but got spill_count={spill_count}"
        );
        assert!(
            spilled_rows > 0 && spilled_rows < total_rows,
            "Expected partial spilling (0 < spilled_rows < {total_rows}), but got spilled_rows={spilled_rows}"
        );
        assert!(
            spilled_bytes > 0,
            "Expected some bytes to be spilled, but got spilled_bytes={spilled_bytes}"
        );

        println!(
            "Partial spilling: spilled {} out of {} rows ({:.1}%) in {} spills, {} bytes",
            spilled_rows,
            total_rows,
            (spilled_rows as f64 / total_rows as f64) * 100.0,
            spill_count,
            spilled_bytes
        );

        Ok(())
    }

    #[tokio::test]
    async fn repartition_without_spilling() -> Result<()> {
        // Test that repartition does not spill when there's ample memory
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let input_partitions = vec![partition];
        let partitioning = Partitioning::RoundRobinBatch(4);

        // Set up context with generous memory limit - no spilling should occur
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(10 * 1024 * 1024, 1.0) // 10MB
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(&schema), None)?;
        let exec = RepartitionExec::try_new(exec, partitioning)?;

        // Collect all partitions - should succeed without spilling
        let (total_rows, metrics) =
            collect_counting_rows(Arc::new(exec), task_ctx).await?;
        // Verify we got all the data (50 batches * 8 rows each)
        assert_eq!(total_rows, 50 * 8);

        // Verify no spilling occurred
        assert_eq!(
            metrics.spill_count(),
            Some(0),
            "Expected no spilling, but got spill_count={:?}",
            metrics.spill_count()
        );
        assert_eq!(
            metrics.spilled_bytes(),
            Some(0),
            "Expected no bytes spilled, but got spilled_bytes={:?}",
            metrics.spilled_bytes()
        );
        assert_eq!(
            metrics.spilled_rows(),
            Some(0),
            "Expected no rows spilled, but got spilled_rows={:?}",
            metrics.spilled_rows()
        );

        println!("No spilling occurred - all data processed in memory");

        Ok(())
    }

    #[tokio::test]
    async fn oom() -> Result<()> {
        use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};

        // Test that repartition fails with OOM when disk manager is disabled
        let schema = test_schema();
        let partition = create_vec_batches(50);
        let input_partitions = vec![partition];
        let partitioning = Partitioning::RoundRobinBatch(4);

        // Setup context with memory limit but NO disk manager (explicitly disabled)
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1, 1.0)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(&schema), None)?;
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(RepartitionExec::try_new(exec, partitioning)?);

        // Attempt to execute - should fail with ResourcesExhausted error
        collect_with(exec, task_ctx, |_, result| {
            let err = result.unwrap_err();
            let err = err.find_root();
            assert!(
                matches!(err, DataFusionError::ResourcesExhausted(_)),
                "Wrong error type: {err}",
            );
            Ok(())
        })
        .await?;
        Ok(())
    }

    /// Create vector batches
    fn create_vec_batches(n: usize) -> Vec<RecordBatch> {
        let batch = create_batch();
        (0..n).map(|_| batch.clone()).collect()
    }

    /// Create batch
    fn create_batch() -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap()
    }

    /// Create batches with sequential values for ordering tests
    fn create_ordered_batches(num_batches: usize) -> Vec<RecordBatch> {
        let schema = test_schema();
        (0..num_batches)
            .map(|i| {
                let start = (i * 8) as u32;
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(UInt32Array::from(
                        (start..start + 8).collect::<Vec<_>>(),
                    ))],
                )
                .unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_repartition_ordering_with_spilling() -> Result<()> {
        // Test that repartition preserves ordering when spilling occurs
        // This tests the state machine fix where we must block on spill_stream
        // when a Spilled marker is received, rather than continuing to poll the channel

        let schema = test_schema();
        // Create batches with sequential values: batch 0 has [0,1,2,3,4,5,6,7],
        // batch 1 has [8,9,10,11,12,13,14,15], etc.
        let partition = create_ordered_batches(20);
        let input_partitions = vec![partition];

        // Use RoundRobinBatch to ensure predictable ordering
        let partitioning = Partitioning::RoundRobinBatch(2);

        // Set up context with very tight memory limit to force spilling
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1, 1.0)
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // create physical plan
        let exec =
            TestMemoryExec::try_new_exec(&input_partitions, Arc::clone(&schema), None)?;
        let exec = RepartitionExec::try_new(exec, partitioning)?;

        // Collect all output partitions
        let (all_batches, metrics) = collect_partitions(Arc::new(exec), task_ctx).await?;

        // Verify spilling occurred
        assert!(
            metrics.spill_count().unwrap() > 0,
            "Expected spilling to occur, but spill_count = 0"
        );

        // Verify ordering is preserved within each partition
        // With RoundRobinBatch, even batches go to partition 0, odd batches to partition 1
        for (partition_idx, batches) in all_batches.iter().enumerate() {
            let mut last_value = None;
            for batch in batches {
                let array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();

                for i in 0..array.len() {
                    let value = array.value(i);
                    if let Some(last) = last_value {
                        assert!(
                            value > last,
                            "Ordering violated in partition {partition_idx}: {value} is not greater than {last}"
                        );
                    }
                    last_value = Some(value);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use arrow::array::record_batch;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::assert_batches_eq;

    use super::*;
    use crate::test::TestMemoryExec;
    use crate::test::collect_with;
    use crate::union::UnionExec;

    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

    /// Asserts that the plan is as expected
    ///
    /// `$EXPECTED_PLAN_LINES`: input plan
    /// `$PLAN`: the plan to optimized
    macro_rules! assert_plan {
        ($PLAN: expr,  @ $EXPECTED: expr) => {
            let formatted = crate::displayable($PLAN).indent(true).to_string();

            insta::assert_snapshot!(
                formatted,
                @$EXPECTED
            );
        };
    }

    #[tokio::test]
    async fn test_preserve_order() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source1 = sorted_memory_exec(&schema, sort_exprs.clone());
        let source2 = sorted_memory_exec(&schema, sort_exprs);
        // output has multiple partitions, and is sorted
        let union = UnionExec::try_new(vec![source1, source2])?;
        let exec = RepartitionExec::try_new(union, Partitioning::RoundRobinBatch(10))?
            .with_preserve_order();

        // Repartition should preserve order
        assert_plan!(&exec, @r"
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2, preserve_order=true, sort_exprs=c0@0 ASC
          UnionExec
            DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC
            DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_one_partition() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source = sorted_memory_exec(&schema, sort_exprs);
        // output is sorted, but has only a single partition, so no need to sort
        let exec = RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(10))?
            .with_preserve_order();

        // Repartition should not preserve order
        assert_plan!(&exec, @r"
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1, maintains_sort_order=true
          DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_input_not_sorted() -> Result<()> {
        let schema = test_schema();
        let source1 = memory_exec(&schema);
        let source2 = memory_exec(&schema);
        // output has multiple partitions, but is not sorted
        let union = UnionExec::try_new(vec![source1, source2])?;
        let exec = RepartitionExec::try_new(union, Partitioning::RoundRobinBatch(10))?
            .with_preserve_order();

        // Repartition should not preserve order, as there is no order to preserve
        assert_plan!(&exec, @r"
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=2
          UnionExec
            DataSourceExec: partitions=1, partition_sizes=[0]
            DataSourceExec: partitions=1, partition_sizes=[0]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_preserve_order_with_spilling() -> Result<()> {
        use datafusion_execution::TaskContext;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        // Create sorted input data across multiple partitions
        // Partition1: [1,3], [5,7], [9,11]
        // Partition2: [2,4], [6,8], [10,12]
        let batch1 = record_batch!(("c0", UInt32, [1, 3])).unwrap();
        let batch2 = record_batch!(("c0", UInt32, [2, 4])).unwrap();
        let batch3 = record_batch!(("c0", UInt32, [5, 7])).unwrap();
        let batch4 = record_batch!(("c0", UInt32, [6, 8])).unwrap();
        let batch5 = record_batch!(("c0", UInt32, [9, 11])).unwrap();
        let batch6 = record_batch!(("c0", UInt32, [10, 12])).unwrap();
        let schema = batch1.schema();
        let sort_exprs = LexOrdering::new([PhysicalSortExpr {
            expr: col("c0", &schema).unwrap(),
            options: SortOptions::default().asc(),
        }])
        .unwrap();
        let partition1 = vec![batch1.clone(), batch3.clone(), batch5.clone()];
        let partition2 = vec![batch2.clone(), batch4.clone(), batch6.clone()];
        let input_partitions = vec![partition1, partition2];

        // Set up context with tight memory limit to force spilling
        // Sorting needs some non-spillable memory, so 64 bytes should force spilling while still allowing the query to complete
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(64, 1.0)
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // Create physical plan with order preservation
        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?
            .try_with_sort_information(vec![sort_exprs.clone(), sort_exprs])?;
        let exec = Arc::new(exec);
        let exec = Arc::new(TestMemoryExec::update_cache(&exec));
        // Repartition into 3 partitions with order preservation
        // We expect 1 batch per output partition after repartitioning
        let exec = RepartitionExec::try_new(exec, Partitioning::RoundRobinBatch(3))?
            .with_preserve_order();

        let mut batches = vec![];
        let metrics = collect_with(Arc::new(exec), task_ctx, |_, batch| {
            batches.push(batch?);
            Ok(())
        })
        .await?;

        #[rustfmt::skip]
        let expected = [
            [
                "+----+",
                "| c0 |",
                "+----+",
                "| 1  |",
                "| 2  |",
                "| 3  |",
                "| 4  |",
                "+----+",
            ],
            [
                "+----+",
                "| c0 |",
                "+----+",
                "| 5  |",
                "| 6  |",
                "| 7  |",
                "| 8  |",
                "+----+",
            ],
            [
                "+----+",
                "| c0 |",
                "+----+",
                "| 9  |",
                "| 10 |",
                "| 11 |",
                "| 12 |",
                "+----+",
            ],
        ];

        for (batch, expected) in batches.iter().zip(expected.iter()) {
            assert_batches_eq!(expected, std::slice::from_ref(batch));
        }

        // We should have spilled ~ all of the data.
        // - We spill data during the repartitioning phase
        // - We may also spill during the final merge sort
        let all_batches = [batch1, batch2, batch3, batch4, batch5, batch6];
        assert!(
            metrics.spill_count().unwrap() > input_partitions.len(),
            "Expected spill_count > {} for order-preserving repartition, but got {:?}",
            input_partitions.len(),
            metrics.spill_count()
        );
        assert!(
            metrics.spilled_bytes().unwrap()
                > all_batches
                    .iter()
                    .map(|b| b.get_array_memory_size())
                    .sum::<usize>(),
            "Expected spilled_bytes > {} for order-preserving repartition, got {}",
            all_batches
                .iter()
                .map(|b| b.get_array_memory_size())
                .sum::<usize>(),
            metrics.spilled_bytes().unwrap()
        );
        assert!(
            metrics.spilled_rows().unwrap()
                >= all_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            "Expected spilled_rows > {} for order-preserving repartition, got {}",
            all_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            metrics.spilled_rows().unwrap()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_hash_partitioning_with_spilling() -> Result<()> {
        use datafusion_execution::TaskContext;
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        // Create input data similar to the round-robin test
        let batch1 = record_batch!(("c0", UInt32, [1, 3])).unwrap();
        let batch2 = record_batch!(("c0", UInt32, [2, 4])).unwrap();
        let batch3 = record_batch!(("c0", UInt32, [5, 7])).unwrap();
        let batch4 = record_batch!(("c0", UInt32, [6, 8])).unwrap();
        let schema = batch1.schema();

        let partition1 = vec![batch1.clone(), batch3.clone()];
        let partition2 = vec![batch2.clone(), batch4.clone()];
        let input_partitions = vec![partition1, partition2];

        // Set up context with memory limit to test hash partitioning with spilling infrastructure
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1, 1.0)
            .build_arc()?;

        let task_ctx = TaskContext::default().with_runtime(runtime);
        let task_ctx = Arc::new(task_ctx);

        // Create physical plan with hash partitioning
        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(exec);
        let exec = Arc::new(TestMemoryExec::update_cache(&exec));
        // Hash partition into 2 partitions by column c0
        let hash_expr = col("c0", &schema)?;
        let exec: Arc<dyn ExecutionPlan> = Arc::new(RepartitionExec::try_new(
            exec,
            Partitioning::Hash(vec![hash_expr], 2),
        )?);

        #[cfg(feature = "stateless_plan")]
        let state = PlanStateNode::new_root_arc(Arc::clone(&exec));

        // Collect all partitions concurrently using JoinSet - this prevents deadlock
        // where the distribution channel gate closes when all output channels are full
        let mut join_set = tokio::task::JoinSet::new();
        for i in 0..exec.properties().partitioning.partition_count() {
            #[cfg(feature = "stateless_plan")]
            let stream = exec.execute(i, Arc::clone(&task_ctx), &state)?;

            #[cfg(not(feature = "stateless_plan"))]
            let stream = exec.execute(i, Arc::clone(&task_ctx))?;
            join_set.spawn(async move {
                let mut count = 0;
                futures::pin_mut!(stream);
                while let Some(result) = stream.next().await {
                    let batch = result?;
                    count += batch.num_rows();
                }
                Ok::<usize, DataFusionError>(count)
            });
        }

        // Wait for all partitions and sum the rows
        let mut total_rows = 0;
        while let Some(result) = join_set.join_next().await {
            total_rows += result.unwrap()?;
        }

        // Verify we got all rows back
        let all_batches = [batch1, batch2, batch3, batch4];
        let expected_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, expected_rows);

        // Verify metrics are available
        #[cfg(feature = "stateless_plan")]
        let metrics = state.metrics.clone_inner();

        #[cfg(not(feature = "stateless_plan"))]
        let metrics = exec.metrics().unwrap();

        // Just verify the metrics can be retrieved (spilling may or may not occur)
        let spill_count = metrics.spill_count().unwrap_or(0);
        assert!(spill_count > 0);
        let spilled_bytes = metrics.spilled_bytes().unwrap_or(0);
        assert!(spilled_bytes > 0);
        let spilled_rows = metrics.spilled_rows().unwrap_or(0);
        assert!(spilled_rows > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_repartition() -> Result<()> {
        let schema = test_schema();
        let sort_exprs = sort_exprs(&schema);
        let source = sorted_memory_exec(&schema, sort_exprs);
        // output is sorted, but has only a single partition, so no need to sort
        let exec = RepartitionExec::try_new(source, Partitioning::RoundRobinBatch(10))?
            .repartitioned(20, &Default::default())?
            .unwrap();

        // Repartition should not preserve order
        assert_plan!(exec.as_ref(), @r"
        RepartitionExec: partitioning=RoundRobinBatch(20), input_partitions=1, maintains_sort_order=true
          DataSourceExec: partitions=1, partition_sizes=[0], output_ordering=c0@0 ASC
        ");
        Ok(())
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn sort_exprs(schema: &Schema) -> LexOrdering {
        [PhysicalSortExpr {
            expr: col("c0", schema).unwrap(),
            options: SortOptions::default(),
        }]
        .into()
    }

    fn memory_exec(schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
        TestMemoryExec::try_new_exec(&[vec![]], Arc::clone(schema), None).unwrap()
    }

    fn sorted_memory_exec(
        schema: &SchemaRef,
        sort_exprs: LexOrdering,
    ) -> Arc<dyn ExecutionPlan> {
        let exec = TestMemoryExec::try_new(&[vec![]], Arc::clone(schema), None)
            .unwrap()
            .try_with_sort_information(vec![sort_exprs])
            .unwrap();
        let exec = Arc::new(exec);
        Arc::new(TestMemoryExec::update_cache(&exec))
    }
}
