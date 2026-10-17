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

//! Pause/resume, stage timing, and memory admission with an in-memory boundary.

use std::any::Any;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    DataFusionError, Result, assert_eq_or_internal_err, internal_err,
};
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::coop::make_cooperative;
use datafusion::physical_plan::execution_plan::{
    EmissionType, EvaluationType, SchedulingType,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
    StageBoundary, collect_partitioned, execute_stream_partitioned,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::future::try_join_all;
use futures::task::AtomicWaker;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, pin_mut, poll};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::time::timeout;

async fn input_plan(
    partitions: &[Vec<RecordBatch>],
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Keep the source partitions stable across machines running these examples.
    let context =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    context.register_table(
        "input",
        Arc::new(MemTable::try_new(schema, partitions.to_vec())?),
    )?;
    context.table("input").await?.create_physical_plan().await
}

#[derive(Debug)]
struct BufferedPartition {
    batches: Vec<Result<RecordBatch>>,
    reservation: MemoryReservation,
}

#[derive(Debug)]
struct PartitionState {
    buffered: Arc<Mutex<Option<BufferedPartition>>>,
    task: Mutex<Option<SpawnedTask<()>>>,
    ready: Arc<AtomicBool>,
    consumer_waker: Arc<AtomicWaker>,
    executed: AtomicBool,
    cancellation: watch::Sender<bool>,
}

/// Example boundary that buffers each partition in memory until release.
/// A failed memory reservation ends the drain with a stream error.
#[derive(Debug)]
struct InMemoryStageBoundaryExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    partitions: Vec<PartitionState>,
    released: Arc<AtomicBool>,
}

impl InMemoryStageBoundaryExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let partition_count = input.properties().output_partitioning().partition_count();
        let properties = PlanProperties::clone(input.properties())
            .with_emission_type(EmissionType::Final)
            .with_evaluation_type(EvaluationType::Eager)
            .with_scheduling_type(SchedulingType::Cooperative);
        let partitions = (0..partition_count)
            .map(|_| PartitionState {
                buffered: Arc::new(Mutex::new(None)),
                task: Mutex::new(None),
                ready: Arc::new(AtomicBool::new(false)),
                consumer_waker: Arc::new(AtomicWaker::new()),
                executed: AtomicBool::new(false),
                cancellation: watch::channel(false).0,
            })
            .collect();
        Self {
            input,
            properties: Arc::new(properties),
            partitions,
            released: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl StageBoundary for InMemoryStageBoundaryExec {
    fn prime(&self, partition: usize, context: Arc<TaskContext>) -> Result<()> {
        let Some(state) = self.partitions.get(partition) else {
            return internal_err!(
                "InMemoryStageBoundaryExec invalid partition {partition}"
            );
        };
        let mut task = state.task.lock().unwrap();
        if task.is_some() {
            return Ok(());
        }
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let mut input = make_cooperative(input);
        let reservation = MemoryConsumer::new(format!("stage boundary[{partition}]"))
            .register(context.memory_pool());
        let buffered = Arc::clone(&state.buffered);
        let ready = Arc::clone(&state.ready);
        let mut cancellation = state.cancellation.subscribe();
        *task = Some(SpawnedTask::spawn(async move {
            if *cancellation.borrow() {
                return;
            }
            let mut batches = Vec::new();
            loop {
                let polled = tokio::select! {
                    biased;
                    _ = cancellation.changed() => return,
                    polled = AssertUnwindSafe(input.next()).catch_unwind() => polled,
                };
                let item = match polled {
                    Ok(Some(item)) => item,
                    Ok(None) => break,
                    Err(panic) => Err(DataFusionError::Execution(format!(
                        "InMemoryStageBoundaryExec input stream panicked: {}",
                        panic_message(panic.as_ref())
                    ))),
                };
                let item = item.and_then(|batch| {
                    reservation.try_grow(batch.get_array_memory_size())?;
                    Ok(batch)
                });
                let terminal_error = item.is_err();
                batches.push(item);
                if terminal_error {
                    break;
                }
            }
            let mut buffered = buffered.lock().unwrap();
            if *cancellation.borrow() {
                return;
            }
            *buffered = Some(BufferedPartition {
                batches,
                reservation,
            });
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
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

impl DisplayAs for InMemoryStageBoundaryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InMemoryStageBoundaryExec")
    }
}

impl ExecutionPlan for InMemoryStageBoundaryExec {
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
            "InMemoryStageBoundaryExec expected one child"
        );
        Ok(Arc::new(Self::new(children.swap_remove(0))))
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
            return internal_err!(
                "InMemoryStageBoundaryExec invalid partition {partition}"
            );
        };
        if state.executed.swap(true, Ordering::AcqRel) {
            return internal_err!(
                "InMemoryStageBoundaryExec partition {partition} executed twice"
            );
        }
        let stream: SendableRecordBatchStream = Box::pin(BoundaryStream {
            schema: self.schema(),
            buffered: Arc::clone(&state.buffered),
            batches: None,
            reservation: None,
            released: Arc::clone(&self.released),
            consumer_waker: Arc::clone(&state.consumer_waker),
            cancellation: state.cancellation.clone(),
        });
        Ok(make_cooperative(stream))
    }
}

struct BoundaryStream {
    schema: SchemaRef,
    buffered: Arc<Mutex<Option<BufferedPartition>>>,
    batches: Option<std::vec::IntoIter<Result<RecordBatch>>>,
    reservation: Option<MemoryReservation>,
    released: Arc<AtomicBool>,
    consumer_waker: Arc<AtomicWaker>,
    cancellation: watch::Sender<bool>,
}

impl Drop for BoundaryStream {
    fn drop(&mut self) {
        self.cancellation.send_replace(true);
        self.buffered.lock().unwrap().take();
    }
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
        if self.batches.is_none() {
            let buffered = self
                .buffered
                .lock()
                .unwrap()
                .take()
                .expect("driver releases only ready partitions");
            self.batches = Some(buffered.batches.into_iter());
            self.reservation = Some(buffered.reservation);
        }
        let item = self.batches.as_mut().unwrap().next();
        if item.is_none() {
            self.reservation = None;
        }
        Poll::Ready(item)
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
        assert_eq_or_internal_err!(children.len(), 1, "ObservedExec expected one child");
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

fn boundaries_are_ready(boundaries: &[Arc<dyn StageBoundary>]) -> bool {
    boundaries
        .iter()
        .all(|boundary| boundary_is_ready(boundary.as_ref()))
}

fn boundary_is_ready(boundary: &dyn StageBoundary) -> bool {
    let partition_count = boundary
        .properties()
        .output_partitioning()
        .partition_count();
    (0..partition_count).all(|partition| boundary.is_ready(partition))
}

async fn prime_with_budget(
    boundary: &dyn StageBoundary,
    context: Arc<TaskContext>,
    budget: Arc<Semaphore>,
    required_bytes: u32,
) -> Result<OwnedSemaphorePermit> {
    // The caller budgets concurrent stages; each boundary accounts for actual batches.
    let permit = budget
        .acquire_many_owned(required_bytes)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let partition_count = boundary
        .properties()
        .output_partitioning()
        .partition_count();
    for partition in 0..partition_count {
        boundary.prime(partition, Arc::clone(&context))?;
    }
    Ok(permit)
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
    let partitions = vec![vec![make_batch(vec![1, 2])?], vec![make_batch(vec![3, 4])?]];
    Ok((schema, partitions))
}

pub async fn pause_and_resume() -> Result<()> {
    let (schema, partitions) = make_batches()?;
    let expected_plan: Arc<dyn ExecutionPlan> =
        input_plan(&partitions, Arc::clone(&schema)).await?;
    let expected =
        collect_partitioned(Arc::clone(&expected_plan), Arc::new(TaskContext::default()))
            .await?;

    let input: Arc<dyn ExecutionPlan> =
        input_plan(&partitions, Arc::clone(&schema)).await?;
    let input_observations = Arc::new(Observations::default());
    let observed_input: Arc<dyn ExecutionPlan> =
        Arc::new(ObservedExec::new(input, Arc::clone(&input_observations)));
    let boundary = Arc::new(InMemoryStageBoundaryExec::new(observed_input));
    let boundary_plan: Arc<dyn ExecutionPlan> =
        Arc::<InMemoryStageBoundaryExec>::clone(&boundary);
    let boundary_handle: Arc<dyn StageBoundary> =
        Arc::<InMemoryStageBoundaryExec>::clone(&boundary);
    let boundaries = vec![boundary_handle];

    assert_eq!(boundary.schema(), expected_plan.schema());
    assert!(matches!(
        boundary.properties().output_partitioning(),
        Partitioning::UnknownPartitioning(2)
    ));
    assert_eq!(boundary.properties().emission_type, EmissionType::Final);
    assert_eq!(boundary.properties().evaluation_type, EvaluationType::Eager);
    assert_eq!(
        boundary.properties().scheduling_type,
        SchedulingType::Cooperative
    );

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
    let started = Instant::now();
    boundaries[0].prime(0, Arc::clone(&context))?;
    boundaries[0].prime(0, Arc::clone(&context))?;
    wait_until(|| boundaries[0].is_ready(0)).await;
    assert!(!boundaries[0].is_ready(1));
    assert!(!boundaries_are_ready(&boundaries));
    assert_eq!(input_observations.execute_calls.load(Ordering::Relaxed), 1);

    boundaries[0].prime(1, context)?;
    wait_until(|| boundaries_are_ready(&boundaries)).await;
    println!(
        "Input materialized in {:?}; downstream has emitted no batches",
        started.elapsed()
    );
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
    let actual = try_join_all(
        streams
            .into_iter()
            .map(|stream| async move { stream.try_collect::<Vec<RecordBatch>>().await }),
    )
    .await?;
    assert_eq!(actual, expected);
    println!(
        "Released boundary: {} partitions returned unchanged",
        actual.len()
    );
    assert_eq!(downstream_observations.batches.load(Ordering::Relaxed), 2);
    Ok(())
}

pub async fn memory_admission() -> Result<()> {
    let pool_size = 1_048_576;
    let budget = Arc::new(Semaphore::new(pool_size));
    let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(pool_size));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&memory_pool))
            .build()?,
    );
    let context = Arc::new(TaskContext::default().with_runtime(runtime));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let make_batch = |value| -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![value; 8_192]))],
        )?)
    };
    let first_batches = vec![(0..24).map(|_| make_batch(1)).collect::<Result<Vec<_>>>()?];
    let second_batches =
        vec![(0..24).map(|_| make_batch(2)).collect::<Result<Vec<_>>>()?];
    // Each buffer fits individually, but together they exceed the shared pool.
    let first_bytes: usize = first_batches[0]
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum();
    let second_bytes: usize = second_batches[0]
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum();
    assert!(first_bytes <= pool_size && second_bytes <= pool_size);
    assert!(first_bytes + second_bytes > pool_size);
    let first = Arc::new(InMemoryStageBoundaryExec::new(
        input_plan(&first_batches, Arc::clone(&schema)).await?,
    ));
    let second_observations = Arc::new(Observations::default());
    let second = Arc::new(InMemoryStageBoundaryExec::new(Arc::new(ObservedExec::new(
        input_plan(&second_batches, schema).await?,
        Arc::clone(&second_observations),
    ))));

    let first_permit = prime_with_budget(
        first.as_ref(),
        Arc::clone(&context),
        Arc::clone(&budget),
        u32::try_from(first_bytes).expect("example budget fits u32"),
    )
    .await?;
    wait_until(|| boundary_is_ready(first.as_ref())).await;
    assert_eq!(memory_pool.reserved(), first_bytes);

    let second_admission = prime_with_budget(
        second.as_ref(),
        Arc::clone(&context),
        Arc::clone(&budget),
        u32::try_from(second_bytes).expect("example budget fits u32"),
    );
    pin_mut!(second_admission);
    assert!(poll!(second_admission.as_mut()).is_pending());
    assert_eq!(second_observations.execute_calls.load(Ordering::Relaxed), 0);
    assert!(!second.is_ready(0));
    println!("Second boundary is awaiting budget held by the first buffer");

    first.release();
    assert_eq!(memory_pool.reserved(), first_bytes);
    assert!(poll!(second_admission.as_mut()).is_pending());
    let first_plan: Arc<dyn ExecutionPlan> = first;
    let first_output = collect_partitioned(first_plan, Arc::clone(&context)).await?;
    assert_eq!(first_output, first_batches);
    drop(first_output);
    assert_eq!(memory_pool.reserved(), 0);
    // Return the admission permit only after consuming the buffered output.
    assert!(poll!(second_admission.as_mut()).is_pending());
    drop(first_permit);

    let second_permit = timeout(Duration::from_secs(5), second_admission)
        .await
        .map_err(|error| DataFusionError::External(Box::new(error)))??;
    assert_eq!(
        second_observations.execute_calls.load(Ordering::Relaxed),
        second.properties().output_partitioning().partition_count()
    );
    wait_until(|| boundary_is_ready(second.as_ref())).await;
    assert_eq!(memory_pool.reserved(), second_bytes);
    println!("First output consumed: the waiting second boundary has now started");

    second.release();
    let second_plan: Arc<dyn ExecutionPlan> =
        Arc::<InMemoryStageBoundaryExec>::clone(&second);
    assert_eq!(
        collect_partitioned(second_plan, context).await?,
        second_batches
    );
    assert_eq!(memory_pool.reserved(), 0);
    drop(second_permit);
    assert_eq!(budget.available_permits(), pool_size);
    println!("Both boundaries completed within the shared memory budget");
    Ok(())
}

pub async fn dependent_boundaries() -> Result<()> {
    let (schema, partitions) = make_batches()?;
    let input = input_plan(&partitions, schema).await?;
    let upstream = Arc::new(InMemoryStageBoundaryExec::new(input));
    let upstream_plan: Arc<dyn ExecutionPlan> =
        Arc::<InMemoryStageBoundaryExec>::clone(&upstream);
    let downstream = Arc::new(InMemoryStageBoundaryExec::new(upstream_plan));
    let context = Arc::new(TaskContext::default());

    // The plan connects downstream to upstream; the caller owns execution order.
    for partition in 0..partitions.len() {
        upstream.prime(partition, Arc::clone(&context))?;
    }
    wait_until(|| (0..partitions.len()).all(|p| upstream.is_ready(p))).await;
    assert!((0..partitions.len()).all(|p| !downstream.is_ready(p)));
    println!("Upstream boundary ready; downstream has not started");
    upstream.release();

    for partition in 0..partitions.len() {
        downstream.prime(partition, Arc::clone(&context))?;
    }
    wait_until(|| (0..partitions.len()).all(|p| downstream.is_ready(p))).await;
    downstream.release();
    let plan: Arc<dyn ExecutionPlan> = downstream;
    assert_eq!(collect_partitioned(plan, context).await?, partitions);
    println!("Downstream boundary completed using the released upstream output");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::test::exec::{
        BlockingExec, ErrorExec, MockExec, PanicExec,
    };

    #[tokio::test]
    async fn pause_and_resume_preserves_partitioned_output() -> Result<()> {
        pause_and_resume().await
    }
    #[tokio::test]
    async fn admission_waits_until_consumption_releases_budget() -> Result<()> {
        memory_admission().await
    }

    #[tokio::test]
    async fn cancelled_admission_does_not_start_input() -> Result<()> {
        let (schema, partitions) = make_batches()?;
        let observations = Arc::new(Observations::default());
        let boundary = InMemoryStageBoundaryExec::new(Arc::new(ObservedExec::new(
            input_plan(&partitions[..1], schema).await?,
            Arc::clone(&observations),
        )));
        let budget = Arc::new(Semaphore::new(10));
        let running = Arc::clone(&budget).acquire_many_owned(8).await.unwrap();
        {
            let admission = prime_with_budget(
                &boundary,
                Arc::new(TaskContext::default()),
                Arc::clone(&budget),
                5,
            );
            pin_mut!(admission);
            assert!(poll!(admission.as_mut()).is_pending());
        }
        assert_eq!(observations.execute_calls.load(Ordering::Relaxed), 0);
        assert!(!boundary.is_ready(0));
        assert_eq!(budget.available_permits(), 2);
        drop(running);
        assert_eq!(budget.available_permits(), 10);
        Ok(())
    }

    #[tokio::test]
    async fn startup_error_returns_admission_permit() {
        let boundary = InMemoryStageBoundaryExec::new(Arc::new(ErrorExec::new()));
        let budget = Arc::new(Semaphore::new(128));
        assert!(
            prime_with_budget(
                &boundary,
                Arc::new(TaskContext::default()),
                Arc::clone(&budget),
                64,
            )
            .await
            .is_err()
        );
        assert_eq!(budget.available_permits(), 128);
        assert!(!boundary.is_ready(0));
    }
    #[tokio::test]
    async fn dependencies_determine_execution_order() -> Result<()> {
        dependent_boundaries().await
    }

    #[tokio::test]
    async fn buffers_multiple_batches_and_an_empty_partition() -> Result<()> {
        let (schema, mut partitions) = make_batches()?;
        let second = partitions.pop().unwrap();
        partitions[0].extend(second);
        partitions.push(vec![]);
        let input = input_plan(&partitions, schema).await?;
        let boundary = Arc::new(InMemoryStageBoundaryExec::new(input));
        let context = Arc::new(TaskContext::default());
        for partition in 0..partitions.len() {
            boundary.prime(partition, Arc::clone(&context))?;
        }
        wait_until(|| (0..partitions.len()).all(|p| boundary.is_ready(p))).await;
        assert_eq!(
            boundary.partitions[0]
                .buffered
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .batches
                .len(),
            2
        );
        boundary.release();
        let plan: Arc<dyn ExecutionPlan> = boundary;
        assert_eq!(collect_partitioned(plan, context).await?, partitions);
        Ok(())
    }

    #[tokio::test]
    async fn memory_limit_error_is_buffered_until_release() -> Result<()> {
        let (schema, partitions) = make_batches()?;
        let input = input_plan(&partitions[..1], schema).await?;
        let boundary = InMemoryStageBoundaryExec::new(input);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(0));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        boundary.prime(0, Arc::clone(&context))?;
        wait_until(|| boundary.is_ready(0)).await;
        let mut stream = boundary.execute(0, context)?;
        {
            let next = stream.next();
            pin_mut!(next);
            assert!(poll!(next.as_mut()).is_pending());
        }
        boundary.release();
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert!(stream.next().await.is_none());
        assert_eq!(pool.reserved(), 0);
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
        let boundary = Arc::new(InMemoryStageBoundaryExec::new(input));
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
    async fn dropping_output_cancels_drain_and_releases_buffered_memory() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let blocking = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking.refs();
        let input: Arc<dyn ExecutionPlan> = blocking;
        let boundary = InMemoryStageBoundaryExec::new(input);
        let context = Arc::new(TaskContext::default());
        boundary.prime(0, Arc::clone(&context))?;
        wait_until(|| refs.strong_count() > 1).await;
        let stream = boundary.execute(0, context)?;
        drop(stream);
        wait_until(|| refs.strong_count() == 1).await;
        assert!(!boundary.is_ready(0));

        let (_schema, partitions) = make_batches()?;
        let input = input_plan(&partitions[..1], schema).await?;
        let boundary = InMemoryStageBoundaryExec::new(input);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool))
            .build_arc()?;
        let context = Arc::new(TaskContext::default().with_runtime(runtime));
        boundary.prime(0, Arc::clone(&context))?;
        wait_until(|| boundary.is_ready(0)).await;
        assert!(pool.reserved() > 0);
        let stream = boundary.execute(0, context)?;
        drop(stream);
        assert_eq!(pool.reserved(), 0);
        assert!(boundary.partitions[0].buffered.lock().unwrap().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn input_stream_panic_becomes_a_buffered_error() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(PanicExec::new(schema, 1));
        let boundary = InMemoryStageBoundaryExec::new(input);
        boundary.prime(0, Arc::new(TaskContext::default()))?;
        wait_until(|| boundary.is_ready(0)).await;
        let mut stream = boundary.execute(0, Arc::new(TaskContext::default()))?;
        boundary.release();
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("input stream panicked"));
        assert!(error.to_string().contains("PanickingStream did panic"));
        assert!(stream.next().await.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn prime_returns_synchronous_execute_error() {
        let input: Arc<dyn ExecutionPlan> = Arc::new(ErrorExec::new());
        let boundary = InMemoryStageBoundaryExec::new(input);
        let error = boundary
            .prime(0, Arc::new(TaskContext::default()))
            .unwrap_err();
        assert!(error.to_string().contains("errored in partition 0"));
        assert!(!boundary.is_ready(0));
        assert!(!boundary.is_ready(1));
    }
}
