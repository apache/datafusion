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

use std::fmt;
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
use datafusion::common::{Result, assert_eq_or_internal_err, internal_err};
use datafusion::datasource::MemTable;
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{EmissionType, EvaluationType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties, RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
    StageBoundary, collect_partitioned, execute_stream_partitioned,
};
use datafusion::prelude::SessionContext;
use futures::future::try_join_all;
use futures::task::AtomicWaker;
use futures::{Stream, StreamExt, TryStreamExt, pin_mut, poll};
use tokio::time::timeout;

async fn input_plan(
    partitions: &[Vec<RecordBatch>],
    schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let context = SessionContext::new();
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
            .with_evaluation_type(EvaluationType::Eager);
        let partitions = (0..partition_count)
            .map(|_| PartitionState {
                buffered: Arc::new(Mutex::new(None)),
                task: Mutex::new(None),
                ready: Arc::new(AtomicBool::new(false)),
                consumer_waker: Arc::new(AtomicWaker::new()),
                executed: AtomicBool::new(false),
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
        let mut input = self.input.execute(partition, Arc::clone(&context))?;
        let reservation = MemoryConsumer::new(format!("stage boundary[{partition}]"))
            .register(context.memory_pool());
        let buffered = Arc::clone(&state.buffered);
        let ready = Arc::clone(&state.ready);
        *task = Some(SpawnedTask::spawn(async move {
            let mut batches = Vec::new();
            while let Some(item) = input.next().await {
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
            *buffered.lock().unwrap() = Some(BufferedPartition {
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
        Ok(Box::pin(BoundaryStream {
            schema: self.schema(),
            buffered: Arc::clone(&state.buffered),
            batches: None,
            reservation: None,
            released: Arc::clone(&self.released),
            consumer_waker: Arc::clone(&state.consumer_waker),
        }))
    }
}

struct BoundaryStream {
    schema: SchemaRef,
    buffered: Arc<Mutex<Option<BufferedPartition>>>,
    batches: Option<std::vec::IntoIter<Result<RecordBatch>>>,
    reservation: Option<MemoryReservation>,
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
    boundaries.iter().all(|boundary| {
        let partition_count = boundary
            .properties()
            .output_partitioning()
            .partition_count();
        (0..partition_count).all(|partition| boundary.is_ready(partition))
    })
}

fn has_memory_headroom(context: &TaskContext, required: usize) -> bool {
    // This is a snapshot; the boundary still reserves each buffered batch.
    let pool = context.memory_pool();
    match pool.memory_limit() {
        MemoryLimit::Infinite => true,
        MemoryLimit::Finite(limit) => limit.saturating_sub(pool.reserved()) >= required,
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
    let (schema, mut partitions) = make_batches()?;
    partitions.truncate(1);
    let input: Arc<dyn ExecutionPlan> = input_plan(&partitions, schema).await?;
    let observations = Arc::new(Observations::default());
    let observed_input: Arc<dyn ExecutionPlan> =
        Arc::new(ObservedExec::new(input, Arc::clone(&observations)));
    let boundary = Arc::new(InMemoryStageBoundaryExec::new(observed_input));

    let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1_048_576));
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&memory_pool))
            .build()?,
    );
    let context = Arc::new(TaskContext::default().with_runtime(runtime));
    let running_stage =
        MemoryConsumer::new("running stage").register(context.memory_pool());
    running_stage.try_grow(786_432)?;

    assert!(!has_memory_headroom(&context, 524_288));
    println!("Admission deferred: a running stage holds 768 KiB of the 1 MiB pool");
    assert_eq!(observations.execute_calls.load(Ordering::Relaxed), 0);
    assert!(!boundary.is_ready(0));

    assert_eq!(running_stage.free(), 786_432);
    assert!(has_memory_headroom(&context, 524_288));
    println!("Reservation freed: starting the next boundary");
    boundary.prime(0, Arc::clone(&context))?;
    wait_until(|| boundary.is_ready(0)).await;
    assert_eq!(observations.execute_calls.load(Ordering::Relaxed), 1);
    assert!(memory_pool.reserved() > 0);

    boundary.release();
    let boundary_plan: Arc<dyn ExecutionPlan> =
        Arc::<InMemoryStageBoundaryExec>::clone(&boundary);
    let actual = collect_partitioned(boundary_plan, context).await?;
    assert_eq!(actual, partitions);
    assert_eq!(memory_pool.reserved(), 0);
    println!("Boundary consumed: its batch reservations were released");
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
    use datafusion::common::DataFusionError;
    use datafusion::physical_plan::test::exec::{ErrorExec, MockExec};

    #[tokio::test]
    async fn pause_and_resume_preserves_partitioned_output() -> Result<()> {
        pause_and_resume().await
    }
    #[tokio::test]
    async fn admission_defers_input_execution() -> Result<()> {
        memory_admission().await
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
