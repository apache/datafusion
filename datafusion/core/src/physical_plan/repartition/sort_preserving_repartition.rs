use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::Stream;
use futures::{FutureExt, StreamExt};
use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalSortExpr, PhysicalSortRequirement};
use crate::physical_plan::{DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream, repartition, SendableRecordBatchStream, Statistics};
use crate::physical_plan::common::{AbortOnDropMany, AbortOnDropSingle, SharedMemoryReservation, spawn_execution};
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics, MetricsSet};
use crate::physical_plan::repartition::{BatchPartitioner, RepartitionMetrics, RepartitionExecState, RepartitionStream };
use crate::physical_plan::sorts::streaming_merge;
use crate::physical_plan::stream::RecordBatchReceiverStream;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::repartition::distributor_channels::{DistributionReceiver, DistributionSender, channels};

type MaybeBatch = Option<Result<RecordBatch>>;

/// The repartition operator maps N input partitions to M output partitions based on a
/// partitioning scheme meanwhile preserving their order.
/// To achieve this, we exploit from SortPreservingMergeStream:
/// with this, we first merge multiple input partitions into one output stream preserving their order,
/// then give this output into RepartitionExec
/// since RepartitionExec preserve the order when the the number of input partitions is one, we reach our goal, hopefully :)
/// SortPreservingRepartitionExec mainly combines the functionality of SortPreservingMergeStream in the first order and as the next, RepartitionExec
#[derive(Debug)]
pub struct SortPreservingRepartitionExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    /// Inner state that is initialized when the first output stream is created.
    state: Arc<Mutex<RepartitionExecState>>,
}

impl SortPreservingRepartitionExec {
    /// Create a new sort execution plan
    pub fn new(expr: Vec<PhysicalSortExpr>,
               input: Arc<dyn ExecutionPlan>,
               partitioning: Partitioning,
    ) -> Result<Self> {
        Ok(SortPreservingRepartitionExec {
            input,
            expr,
            metrics: ExecutionPlanMetricsSet::new(),
            partitioning,
            state: Arc::new(Mutex::new(RepartitionExecState {
                channels: HashMap::new(),
                abort_helper: Arc::new(AbortOnDropMany::<()>(vec![])),
            })),
        })
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] { &self.expr }

    /// Partitioning scheme to use
    pub fn partitioning(&self) -> &Partitioning { &self.partitioning }

    /// merge all input partitions into one SendableRecordBatchStream, preserving their order
    fn merge_input_into_single(&self, partition: usize, context: Arc<TaskContext>) -> SendableRecordBatchStream {

        let tracking_metrics =
            MemTrackingMetrics::new(&self.metrics, context.memory_pool(), partition);

        let input_partitions = self.input.output_partitioning().partition_count();
        let schema = self.schema();

        // Use tokio only if running from a tokio context (#2201)
        let receivers = match tokio::runtime::Handle::try_current() {
            Ok(_) => (0..input_partitions)
                .map(|part_i| {
                    let (sender, receiver) = mpsc::channel(1);
                    let join_handle = spawn_execution(
                        self.input.clone(),
                        sender,
                        part_i,
                        context.clone(),
                    );

                    RecordBatchReceiverStream::create(
                        &schema,
                        receiver,
                        join_handle,
                    )
                })
                .collect(),
            Err(_) => (0..input_partitions)
                .map(|partition| self.input.execute(partition, context.clone()))
                .collect::<Result<_>>().unwrap(),
        };

        let result = streaming_merge(
            receivers,
            schema,
            &self.expr,
            tracking_metrics,
            context.session_config().batch_size(),
        );

        result.unwrap()
    }

    /// Pulls data from the specified input plan, feeding it to the
    /// output partitions based on the desired partitioning.
    /// txs hold the output sending channels for each output partition
    /// SendableRecordBatchStream, which is a merged stream preserving the order, from SortPreservingMergeStream is given
    async fn pull_from_input(
        mut stream: SendableRecordBatchStream,
        mut txs: HashMap<
            usize,
            (DistributionSender<MaybeBatch>, SharedMemoryReservation),
        >,
        partitioning: Partitioning,
        r_metrics: RepartitionMetrics,
        context: Arc<TaskContext>,
    ) -> Result<()> {
        let mut partitioner =
            BatchPartitioner::try_new(partitioning, r_metrics.repart_time.clone())?;

        // While there are still outputs to send to, keep
        // pulling inputs
        while !txs.is_empty() {
            // fetch the next batch
            let timer = r_metrics.fetch_time.timer();
            let result = stream.next().await;
            timer.done();

            // Input is done
            let batch = match result {
                Some(result) => result?,
                None => break,
            };

            for res in partitioner.partition_iter(batch)? {
                let (partition, batch) = res?;
                let size = batch.get_array_memory_size();

                let timer = r_metrics.send_time.timer();
                // if there is still a receiver, send to it
                if let Some((tx, reservation)) = txs.get_mut(&partition) {
                    reservation.lock().try_grow(size)?;

                    if tx.send(Some(Ok(batch))).await.is_err() {
                        // If the other end has hung up, it was an early shutdown (e.g. LIMIT)
                        reservation.lock().shrink(size);
                        txs.remove(&partition);
                    }
                }
                timer.done();
            }

            // If the input stream is endless, we may spin forever and never yield back to tokio. Hence let us yield.
            // See https://github.com/apache/arrow-datafusion/issues/5278.
            tokio::task::yield_now().await;
        }

        Ok(())
    }

    /// Waits for `input_task` which is consuming one of the inputs to
    /// complete. Upon each successful completion, sends a `None` to
    /// each of the output tx channels to signal one of the inputs is
    /// complete. Upon error, propagates the errors to all output tx
    /// channels.
    async fn wait_for_task(
        input_task: AbortOnDropSingle<Result<()>>,
        txs: HashMap<usize, DistributionSender<Option<Result<RecordBatch>>>>,
    ) {
        // wait for completion, and propagate error
        // note we ignore errors on send (.ok) as that means the receiver has already shutdown.
        match input_task.await {
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
                let e = Arc::new(e);

                for (_, tx) in txs {
                    // wrap it because need to send error to all output partitions
                    let err = Err(DataFusionError::External(Box::new(e.clone())));
                    tx.send(Some(err)).await.ok();
                }
            }
            // Input task completed successfully
            Ok(Ok(())) => {
                // notify each output partition that this input partition has no more data
                for (_, tx) in txs {
                    tx.send(None).await.ok();
                }
            }
        }
    }
}


impl ExecutionPlan for SortPreservingRepartitionExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any { self }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef { self.input.schema() }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(PhysicalSortRequirement::from_sort_exprs(&self.expr))]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but it its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SortPreservingRepartitionExec::new(
            self.expr.clone(),
            children[0].clone(),
            self.partitioning.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {

        // lock mutexes
        let mut state = self.state.lock();

        //since we are merging all the input partitions in one stream, num_input_partitions is 1
        let num_input_partitions = 1;
        let num_output_partitions = self.partitioning.partition_count();

        // merge input partitions in one stream while preservinf their order
        let merged_result = self.merge_input_into_single(partition, context.clone());

        // if this is the first partition to be invoked then we need to set up initial state
        if state.channels.is_empty() {
            // create one channel per *output* partition
            // note we use a custom channel that ensures there is always data for each receiver
            // but limits the amount of buffering if required.
            let (txs, rxs) = channels(num_output_partitions);
            for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
                let reservation = Arc::new(Mutex::new(
                    MemoryConsumer::new(format!("RepartitionExec[{partition}]"))
                        .register(context.clone().memory_pool()),
                ));
                state.channels.insert(partition, (tx, rx, reservation));
            }

            // launch one async task per *input* partition, in this case since we merged inputs as 1 partition
            let mut join_handles = Vec::with_capacity(num_input_partitions);

            let txs: HashMap<_, _> = state
                .channels
                .iter()
                .map(|(partition, (tx, _rx, reservation))| {
                    (*partition, (tx.clone(), Arc::clone(reservation)))
                })
                .collect();

            let r_metrics = repartition::RepartitionMetrics::new(0, partition, &self.metrics);

            let input_task: JoinHandle<Result<()>> =
                tokio::spawn(Self::pull_from_input(
                    merged_result,
                    txs.clone(),
                    self.partitioning.clone(),
                    r_metrics,
                    context.clone(),
                ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            let join_handle = tokio::spawn(Self::wait_for_task(
                AbortOnDropSingle::new(input_task),
                txs.into_iter()
                    .map(|(partition, (tx, _reservation))| (partition, tx))
                    .collect(),
            ));
            join_handles.push(join_handle);

            state.abort_helper = Arc::new(AbortOnDropMany(join_handles))
        }


        // now return stream for the specified *output* partition which will
        // read from the channel
        let (_tx, rx, reservation) = state
            .channels
            .remove(&partition)
            .expect("partition not used yet");
        Ok(Box::pin(RepartitionStream {
            num_input_partitions,
            num_input_partitions_processed: 0,
            schema: self.input.schema(),
            input: rx,
            drop_helper: Arc::clone(&state.abort_helper),
            reservation,
        }))


    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                write!(f, "SortPreservingRepartitionExec: [{}], partitioning={:?}, input_partitions={}",
                       expr.join(","),
                       self.partitioning,
                       self.input.output_partitioning().partition_count()
                )
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }

}
