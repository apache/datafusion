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

//! Stage boundaries for driver-controlled physical execution.
//!
//! A driver can use a boundary to pause and resume execution deterministically
//! in tests, measure the time required to complete a stage without downstream
//! work overlapping it, or apply coarse admission control by choosing when to
//! prime the next stage. The trait does not prescribe storage, memory limits,
//! spilling, or row-level backpressure; those remain implementation concerns.
//! For example, a driver can consult the [`MemoryPool`] in its [`TaskContext`]
//! before priming another memory-intensive stage. A materializing boundary
//! implementation must still account for its own buffered batches with
//! [`MemoryReservation`]s.
//!
//! [`MemoryPool`]: datafusion_execution::memory_pool::MemoryPool
//! [`MemoryReservation`]: datafusion_execution::memory_pool::MemoryReservation

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_execution::TaskContext;

use crate::ExecutionPlan;

/// An [`ExecutionPlan`] that can be driven to a stage boundary independently
/// of downstream demand.
///
/// A boundary materializes each input partition when [`Self::prime`] is
/// called, but its output streams remain pending until [`Self::release`] is
/// called. This lets an external driver observe that a stage has reached a
/// terminal state, assert or measure the completed stage, make a decision, and
/// then allow downstream execution to continue.
///
/// Implementations must preserve the input schema, partitioning, ordering,
/// batches, and errors. Their plan properties should still describe the
/// boundary's execution behavior, such as final emission and eager evaluation.
///
/// Callers retain boundary handles when constructing a plan. This trait does
/// not provide discovery through an erased `dyn ExecutionPlan` plan tree.
///
/// # Driver example
///
/// The driver primes all partitions in a stage, waits for the cross-partition
/// barrier, performs any decision making, and releases the stage:
///
/// ```no_run
/// use std::sync::Arc;
///
/// use datafusion_common::Result;
/// use datafusion_execution::TaskContext;
/// use datafusion_physical_plan::StageBoundary;
///
/// async fn drive_stage(
///     boundaries: &[Arc<dyn StageBoundary>],
///     stage: u32,
///     context: Arc<TaskContext>,
/// ) -> Result<()> {
///     let stage_boundaries = boundaries
///         .iter()
///         .filter(|boundary| boundary.stage() == stage)
///         .collect::<Vec<_>>();
///
///     for boundary in &stage_boundaries {
///         let partition_count = boundary
///             .properties()
///             .output_partitioning()
///             .partition_count();
///         for partition in 0..partition_count {
///             boundary.prime(partition, Arc::clone(&context))?;
///         }
///     }
///
///     while !stage_boundaries.iter().all(|boundary| {
///         let partition_count = boundary
///             .properties()
///             .output_partitioning()
///             .partition_count();
///         (0..partition_count).all(|partition| boundary.is_ready(partition))
///     }) {
///         tokio::task::yield_now().await;
///     }
///
///     // Inspect, measure, or assert on the completed stage. Delaying release
///     // here deterministically pauses downstream execution.
///
///     for boundary in stage_boundaries {
///         boundary.release();
///     }
///     Ok(())
/// }
/// ```
pub trait StageBoundary: ExecutionPlan {
    /// Starts draining one input partition in the background.
    ///
    /// Calling this method more than once for the same partition is
    /// idempotent. The returned error reports failures that occur while
    /// starting the drain, including a synchronous failure from
    /// [`ExecutionPlan::execute`]. Stream errors are buffered for downstream
    /// consumers and make the partition ready.
    ///
    /// Returns an error if `partition` is outside the boundary's output
    /// partition range.
    fn prime(&self, partition: usize, context: Arc<TaskContext>) -> Result<()>;

    /// Returns whether a partition's input reached EOF or a stream error.
    ///
    /// Readiness is monotonic: once this method returns `true` for a valid
    /// partition, it continues to do so. Returns `false` for an invalid
    /// partition.
    fn is_ready(&self, partition: usize) -> bool;

    /// Allows all materialized output partitions to flow downstream.
    ///
    /// Drivers call this only after every boundary partition in the stage is
    /// ready. Calling this method more than once is idempotent.
    fn release(&self);

    /// Returns this boundary's stage number.
    ///
    /// Drivers prime stage `K + 1` only after releasing stage `K`, allowing a
    /// decision at stage `K` to replace the next stage's input before its drain
    /// starts.
    fn stage(&self) -> u32;
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_common::{DataFusionError, Result, assert_eq_or_internal_err};
    use datafusion_common_runtime::SpawnedTask;
    use datafusion_execution::TaskContext;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool,
    };
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::PhysicalExpr;
    use futures::future::try_join_all;
    use futures::task::AtomicWaker;
    use futures::{Stream, StreamExt, TryStreamExt, pin_mut, poll};
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use super::StageBoundary;
    use crate::execution_plan::{EmissionType, EvaluationType};
    use crate::stream::RecordBatchStreamAdapter;
    use crate::test::TestMemoryExec;
    use crate::test::exec::{ErrorExec, MockExec};
    use crate::{
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
        SendableRecordBatchStream, collect_partitioned, execute_stream_partitioned,
    };

    #[derive(Debug)]
    struct PartitionState {
        sender: Mutex<Option<mpsc::UnboundedSender<Result<RecordBatch>>>>,
        receiver: Mutex<Option<mpsc::UnboundedReceiver<Result<RecordBatch>>>>,
        task: Mutex<Option<SpawnedTask<()>>>,
        ready: Arc<AtomicBool>,
        consumer_waker: Arc<AtomicWaker>,
    }

    #[derive(Debug)]
    struct TestStageBoundary {
        input: Arc<dyn ExecutionPlan>,
        properties: Arc<PlanProperties>,
        partitions: Vec<PartitionState>,
        released: Arc<AtomicBool>,
        stage: u32,
    }

    impl TestStageBoundary {
        fn new(input: Arc<dyn ExecutionPlan>, stage: u32) -> Self {
            let partition_count =
                input.properties().output_partitioning().partition_count();
            let properties = PlanProperties::clone(input.properties())
                .with_emission_type(EmissionType::Final)
                .with_evaluation_type(EvaluationType::Eager);
            let partitions = (0..partition_count)
                .map(|_| {
                    let (sender, receiver) = mpsc::unbounded_channel();
                    PartitionState {
                        sender: Mutex::new(Some(sender)),
                        receiver: Mutex::new(Some(receiver)),
                        task: Mutex::new(None),
                        ready: Arc::new(AtomicBool::new(false)),
                        consumer_waker: Arc::new(AtomicWaker::new()),
                    }
                })
                .collect();
            Self {
                input,
                properties: Arc::new(properties),
                partitions,
                released: Arc::new(AtomicBool::new(false)),
                stage,
            }
        }
    }

    impl StageBoundary for TestStageBoundary {
        fn prime(&self, partition: usize, context: Arc<TaskContext>) -> Result<()> {
            let Some(state) = self.partitions.get(partition) else {
                return Err(DataFusionError::Internal(format!(
                    "TestStageBoundary invalid partition {partition} (expected less than {})",
                    self.partitions.len()
                )));
            };

            let mut task = state.task.lock().unwrap();
            if task.is_some() {
                return Ok(());
            }

            let mut input = self.input.execute(partition, context)?;
            let sender = state
                .sender
                .lock()
                .unwrap()
                .take()
                .expect("sender exists until its partition is primed");
            let ready = Arc::clone(&state.ready);
            *task = Some(SpawnedTask::spawn(async move {
                while let Some(item) = input.next().await {
                    let terminal_error = item.is_err();
                    if sender.send(item).is_err() {
                        return;
                    }
                    if terminal_error {
                        break;
                    }
                }
                ready.store(true, Ordering::Release);
            }));
            Ok(())
        }

        fn is_ready(&self, partition: usize) -> bool {
            self.partitions
                .get(partition)
                .is_some_and(|state| state.ready.load(Ordering::Acquire))
        }

        fn release(&self) {
            if self.released.swap(true, Ordering::AcqRel) {
                return;
            }
            for state in &self.partitions {
                state.consumer_waker.wake();
            }
        }

        fn stage(&self) -> u32 {
            self.stage
        }
    }

    impl DisplayAs for TestStageBoundary {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TestStageBoundary: stage={}", self.stage)
        }
    }

    impl ExecutionPlan for TestStageBoundary {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn maintains_input_order(&self) -> Vec<bool> {
            vec![true]
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

        fn replace_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
            _options: ReplaceChildrenOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq_or_internal_err!(
                children.len(),
                1,
                "TestStageBoundary expected one child"
            );
            Ok(Arc::new(Self::new(children.swap_remove(0), self.stage)))
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
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let Some(state) = self.partitions.get(partition) else {
                return Err(DataFusionError::Internal(format!(
                    "TestStageBoundary invalid partition {partition} (expected less than {})",
                    self.partitions.len()
                )));
            };
            let Some(receiver) = state.receiver.lock().unwrap().take() else {
                return Err(DataFusionError::Internal(format!(
                    "TestStageBoundary partition {partition} executed more than once"
                )));
            };
            Ok(Box::pin(BoundaryStream {
                schema: self.schema(),
                receiver,
                released: Arc::clone(&self.released),
                consumer_waker: Arc::clone(&state.consumer_waker),
            }))
        }
    }

    struct BoundaryStream {
        schema: SchemaRef,
        receiver: mpsc::UnboundedReceiver<Result<RecordBatch>>,
        released: Arc<AtomicBool>,
        consumer_waker: Arc<AtomicWaker>,
    }

    impl Stream for BoundaryStream {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if !self.released.load(Ordering::Acquire) {
                self.consumer_waker.register(cx.waker());
                if !self.released.load(Ordering::Acquire) {
                    return Poll::Pending;
                }
            }
            self.receiver.poll_recv(cx)
        }
    }

    impl RecordBatchStream for BoundaryStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[derive(Debug, Default)]
    struct Observations {
        execute_calls: AtomicUsize,
        batches: AtomicUsize,
    }

    #[derive(Debug)]
    struct ObservedExec {
        input: Arc<dyn ExecutionPlan>,
        properties: Arc<PlanProperties>,
        observations: Arc<Observations>,
    }

    impl ObservedExec {
        fn new(input: Arc<dyn ExecutionPlan>, observations: Arc<Observations>) -> Self {
            let properties = Arc::clone(input.properties());
            Self {
                input,
                properties,
                observations,
            }
        }
    }

    impl DisplayAs for ObservedExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "ObservedExec")
        }
    }

    impl ExecutionPlan for ObservedExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }

        fn maintains_input_order(&self) -> Vec<bool> {
            vec![true]
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

        fn replace_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
            _options: ReplaceChildrenOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert_eq_or_internal_err!(
                children.len(),
                1,
                "ObservedExec expected one child"
            );
            Ok(Arc::new(Self::new(
                children.swap_remove(0),
                Arc::clone(&self.observations),
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

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.observations
                .execute_calls
                .fetch_add(1, Ordering::Relaxed);
            let input = self.input.execute(partition, context)?;
            let observations = Arc::clone(&self.observations);
            let stream = input.map(move |item| {
                if item.is_ok() {
                    observations.batches.fetch_add(1, Ordering::Relaxed);
                }
                item
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                stream,
            )))
        }
    }

    fn stage_is_ready(boundaries: &[Arc<dyn StageBoundary>], stage: u32) -> bool {
        let mut found = false;
        let ready = boundaries
            .iter()
            .filter(|boundary| boundary.stage() == stage)
            .all(|boundary| {
                found = true;
                let partition_count = boundary
                    .properties()
                    .output_partitioning()
                    .partition_count();
                (0..partition_count).all(|partition| boundary.is_ready(partition))
            });
        found && ready
    }

    fn has_memory_headroom(context: &TaskContext, required: usize) -> bool {
        let pool = context.memory_pool();
        match pool.memory_limit() {
            MemoryLimit::Infinite => true,
            MemoryLimit::Finite(limit) => {
                limit.saturating_sub(pool.reserved()) >= required
            }
            MemoryLimit::Unknown => false,
        }
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        timeout(Duration::from_secs(5), async {
            while !predicate() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stage boundary did not become ready");
    }

    fn make_batches() -> Result<(SchemaRef, Vec<Vec<RecordBatch>>)> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let make_batch = |values| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(values))],
            )
        };
        let partitions =
            vec![vec![make_batch(vec![1, 2])?], vec![make_batch(vec![3, 4])?]];
        Ok((schema, partitions))
    }

    #[tokio::test]
    async fn driver_can_pause_and_resume_multi_partition_boundary() -> Result<()> {
        let (schema, partitions) = make_batches()?;
        let expected_plan: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        let expected = collect_partitioned(
            Arc::clone(&expected_plan),
            Arc::new(TaskContext::default()),
        )
        .await?;

        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&partitions, Arc::clone(&schema), None)?;
        let input_observations = Arc::new(Observations::default());
        let observed_input: Arc<dyn ExecutionPlan> =
            Arc::new(ObservedExec::new(input, Arc::clone(&input_observations)));
        let boundary = Arc::new(TestStageBoundary::new(observed_input, 0));
        let boundary_plan: Arc<dyn ExecutionPlan> =
            Arc::<TestStageBoundary>::clone(&boundary);
        let boundary_handle: Arc<dyn StageBoundary> =
            Arc::<TestStageBoundary>::clone(&boundary);
        let boundaries = vec![boundary_handle];

        assert_eq!(boundary.schema(), expected_plan.schema());
        assert!(matches!(
            boundary.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(2)
        ));
        assert_eq!(boundary.properties().emission_type, EmissionType::Final);
        assert_eq!(boundary.properties().evaluation_type, EvaluationType::Eager);

        let downstream_observations = Arc::new(Observations::default());
        let downstream: Arc<dyn ExecutionPlan> = Arc::new(ObservedExec::new(
            boundary_plan,
            Arc::clone(&downstream_observations),
        ));
        let mut streams =
            execute_stream_partitioned(downstream, Arc::new(TaskContext::default()))?;
        assert_eq!(
            downstream_observations
                .execute_calls
                .load(Ordering::Relaxed),
            2
        );
        for stream in &mut streams {
            let next = stream.next();
            pin_mut!(next);
            assert!(poll!(next.as_mut()).is_pending());
        }
        assert_eq!(downstream_observations.batches.load(Ordering::Relaxed), 0);

        let context = Arc::new(TaskContext::default());
        boundaries[0].prime(0, Arc::clone(&context))?;
        boundaries[0].prime(0, Arc::clone(&context))?;
        wait_until(|| boundaries[0].is_ready(0)).await;
        assert!(!boundaries[0].is_ready(1));
        assert!(!stage_is_ready(&boundaries, 0));
        assert_eq!(input_observations.execute_calls.load(Ordering::Relaxed), 1);

        boundaries[0].prime(1, context)?;
        wait_until(|| stage_is_ready(&boundaries, 0)).await;
        assert_eq!(input_observations.execute_calls.load(Ordering::Relaxed), 2);
        assert_eq!(input_observations.batches.load(Ordering::Relaxed), 2);
        assert_eq!(downstream_observations.batches.load(Ordering::Relaxed), 0);
        for stream in &mut streams {
            let next = stream.next();
            pin_mut!(next);
            assert!(poll!(next.as_mut()).is_pending());
        }

        boundaries[0].release();
        boundaries[0].release();
        let actual =
            try_join_all(streams.into_iter().map(|stream| async move {
                stream.try_collect::<Vec<RecordBatch>>().await
            }))
            .await?;
        assert_eq!(actual, expected);
        assert_eq!(downstream_observations.batches.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn driver_can_defer_priming_until_memory_is_available() -> Result<()> {
        let (schema, mut partitions) = make_batches()?;
        partitions.truncate(1);
        let input: Arc<dyn ExecutionPlan> =
            TestMemoryExec::try_new_exec(&partitions, schema, None)?;
        let observations = Arc::new(Observations::default());
        let observed_input: Arc<dyn ExecutionPlan> =
            Arc::new(ObservedExec::new(input, Arc::clone(&observations)));
        let boundary = Arc::new(TestStageBoundary::new(observed_input, 1));

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(100));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&memory_pool))
                .build()?,
        );
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        let running_stage =
            MemoryConsumer::new("running stage").register(context.memory_pool());
        running_stage.try_grow(75)?;

        assert!(!has_memory_headroom(&context, 50));
        assert_eq!(observations.execute_calls.load(Ordering::Relaxed), 0);
        assert!(!boundary.is_ready(0));

        assert_eq!(running_stage.free(), 75);
        assert!(has_memory_headroom(&context, 50));
        boundary.prime(0, Arc::clone(&context))?;
        wait_until(|| boundary.is_ready(0)).await;
        assert_eq!(observations.execute_calls.load(Ordering::Relaxed), 1);

        boundary.release();
        let boundary_plan: Arc<dyn ExecutionPlan> =
            Arc::<TestStageBoundary>::clone(&boundary);
        let actual = collect_partitioned(boundary_plan, context).await?;
        assert_eq!(actual, partitions);
        Ok(())
    }

    #[tokio::test]
    async fn stream_error_is_ready_but_withheld_until_release() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            MockExec::new(
                vec![Err(DataFusionError::Execution("drain failed".to_string()))],
                schema,
            )
            .with_unknown_statistics()
            .with_use_task(false),
        );
        let boundary = Arc::new(TestStageBoundary::new(input, 0));
        let mut stream = boundary.execute(0, Arc::new(TaskContext::default()))?;

        {
            let next = stream.next();
            pin_mut!(next);
            assert!(poll!(next.as_mut()).is_pending());
        }

        boundary.prime(0, Arc::new(TaskContext::default()))?;
        wait_until(|| boundary.is_ready(0)).await;
        {
            let next = stream.next();
            pin_mut!(next);
            assert!(poll!(next.as_mut()).is_pending());
        }

        boundary.release();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("drain failed"));
        Ok(())
    }

    #[tokio::test]
    async fn prime_returns_synchronous_execute_error() {
        let input: Arc<dyn ExecutionPlan> = Arc::new(ErrorExec::new());
        let boundary = TestStageBoundary::new(input, 0);
        let error = boundary
            .prime(0, Arc::new(TaskContext::default()))
            .unwrap_err();
        assert!(error.to_string().contains("errored in partition 0"));
        assert!(!boundary.is_ready(0));
        assert!(!boundary.is_ready(1));
    }
}
