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

//! A plan with knobs for breaking the `ExecutionPlan` contract, and helpers
//! shared by the tests.

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex};

use arrow::array::{RecordBatch, UInt32Array, new_null_array};
use arrow::compute::{concat_batches, take_record_batch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::MemoryConsumer;
use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion_physical_expr::{
    EquivalenceProperties, LexOrdering, OrderingRequirements, PhysicalExpr,
};
use datafusion_physical_plan::execution_plan::{
    Boundedness, CardinalityEffect, EmissionType, EvaluationType, InvariantLevel,
};
use datafusion_physical_plan::stream::{
    RecordBatchReceiverStream, RecordBatchStreamAdapter,
};
use datafusion_physical_plan::{
    ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    Partitioning, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
    StatisticsArgs, apply_expression_roots,
};
use datafusion_property_tests::physical_plan::fixtures::{
    SourceSpec, StatisticsPrecision,
};
use datafusion_property_tests::physical_plan::{CheckKind, PlanChecker, checks};
use datafusion_property_tests::{Report, Severity};
use futures::StreamExt;
use futures::stream::BoxStream;

#[derive(Debug, Clone, Copy)]
pub enum Effect {
    Unknown,
    Equal,
    LowerEqual,
    GreaterEqual,
}

/// How `ConfigurableExec` changes each batch it passes through
#[derive(Debug, Clone, Copy)]
pub enum Transform {
    /// Pass batches through unchanged
    None,
    /// Keep only the first half of the rows of each batch
    DropHalf,
    /// Repeat every batch twice
    Duplicate,
    /// Reverse the rows of each batch
    Reverse,
    /// Replace the first column with nulls, and mark it nullable in the batch
    NullFirstColumn,
    /// Rename the first column in the batch
    RenameFirstColumn,
    /// Panic
    Panic,
}

impl Transform {
    fn apply(self, batch: RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        Ok(match self {
            Transform::None => batch,
            Transform::Panic => panic!("ConfigurableExec panicked"),
            Transform::DropHalf => batch.slice(0, rows / 2),
            Transform::Duplicate => concat_batches(&batch.schema(), [&batch, &batch])?,
            Transform::Reverse => {
                let indices = UInt32Array::from_iter_values((0..rows as u32).rev());
                take_record_batch(&batch, &indices)?
            }
            Transform::NullFirstColumn | Transform::RenameFirstColumn => {
                let schema = batch.schema();
                let first = schema.field(0);
                let (field, column) = match self {
                    Transform::NullFirstColumn => (
                        first.clone().with_nullable(true),
                        new_null_array(first.data_type(), rows),
                    ),
                    _ => (
                        first.clone().with_name("renamed"),
                        Arc::clone(batch.column(0)),
                    ),
                };
                let mut fields: Vec<Field> =
                    schema.fields().iter().map(|f| f.as_ref().clone()).collect();
                fields[0] = field;
                let mut columns = batch.columns().to_vec();
                columns[0] = column;
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?
            }
        })
    }
}

/// What `ConfigurableExec` does when its input returns an error
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnInputError {
    /// Return the error
    Propagate,
    /// Drop the error and continue with the next item
    Swallow,
    /// Stay pending forever
    Hang,
    /// Panic
    Panic,
}

/// How `ConfigurableExec` applies its fetch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchMode {
    /// Report the fetch without applying it, and return `None` from
    /// `with_fetch`
    Ignore,
    /// Stop after `fetch` rows per partition, without polling the input again
    Enforce,
    /// Keep one row more than the fetch
    KeepOneMore,
    /// Keep half as many rows as the fetch
    KeepHalf,
    /// Skip the first row of each partition, then keep `fetch` rows
    SkipFirstRow,
    /// Repeat the first row of each partition `fetch` times
    RepeatFirstRow,
}

/// What `ConfigurableExec` does when a partition of the same instance is
/// executed again
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnRerun {
    /// Produce the same rows again
    Same,
    /// Produce no rows, as a node whose first execution consumed shared state
    Empty,
    /// Return an error from `execute`
    Error,
    /// Panic in `execute`
    Panic,
}

/// What `ConfigurableExec` does when asked to execute a partition at or past
/// its partition count
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidPartition {
    /// Execute that partition of the input, as for any other partition
    PassToInput,
    /// Panic in `execute`
    PanicInExecute,
    /// Return a stream that panics when polled
    PanicInStream,
    /// Return a stream whose first item is an error
    ErrorInStream,
    /// Return a stream that ends at once
    EmptyStream,
}

/// Where `ConfigurableExec` keeps its input streams when its output stream is
/// dropped
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldInput {
    /// Drop them with the output stream
    No,
    /// Keep them in the plan until the plan is dropped
    InPlan,
    /// Never drop them
    Forever,
    /// Drop them from a spawned task, some time after the output stream is
    /// dropped
    InTask,
}

/// A single-child plan that passes its input through, with knobs to make it
/// break individual parts of the `ExecutionPlan` contract.
#[derive(Debug, Clone)]
pub struct ConfigurableExec {
    pub input: Arc<dyn ExecutionPlan>,
    pub effect: Effect,
    /// Overrides `effect` while the node has no fetch
    pub effect_without_fetch: Option<Effect>,
    pub fetch: Option<usize>,
    /// Overrides the overall num_rows
    pub num_rows: Option<Precision<usize>>,
    /// Overrides the num_rows of each partition
    pub partition_num_rows: Option<Vec<Precision<usize>>>,
    /// Skip the child in `child_stats_requests`
    pub skip_child_stats: bool,
    /// Drop this many column statistics entries
    pub drop_column_stats: usize,
    /// Return an error from `statistics_from_inputs`
    pub stats_error: bool,
    /// Overrides the length of `maintains_input_order`
    pub maintains_input_order_len: Option<usize>,
    /// Overrides the values of `maintains_input_order`
    pub maintains_order: Option<bool>,
    /// Require the input to be sorted by this ordering
    pub required_ordering: Option<LexOrdering>,
    /// Return an error from `check_invariants`
    pub invariants_error: bool,
    /// Return an error from `check_invariants(Executable)`
    pub executable_invariants_error: bool,
    /// Report this output ordering instead of the input's orderings
    pub claimed_ordering: Option<LexOrdering>,
    /// Report these equivalence properties instead of the input's
    pub claimed_eq_properties: Option<EquivalenceProperties>,
    /// Report this output partitioning instead of the input's
    pub claimed_partitioning: Option<Partitioning>,
    /// Return this from `schema()` instead of the schema of the equivalence
    /// properties
    pub claimed_schema: Option<SchemaRef>,
    /// The expressions `apply_expressions` visits
    pub expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// The expressions `dynamic_expressions_produced` returns
    pub dynamic_expressions: Vec<Arc<dyn PhysicalExpr>>,
    /// Replace every dynamic filter in `expressions` and `dynamic_expressions`
    /// by a new one in `reset_state`
    pub reset_dynamic_expressions: bool,
    /// Change each output batch
    pub transform: Transform,
    /// Return an error from `execute`
    pub execute_error: bool,
    /// Return a stream that never produces a batch or ends
    pub hang: bool,
    /// Overrides the reported boundedness
    pub boundedness: Option<Boundedness>,
    /// Overrides the reported emission type
    pub emission_type: Option<EmissionType>,
    /// Overrides the reported evaluation type
    pub evaluation_type: Option<EvaluationType>,
    /// Buffer all input and emit it when the input ends
    pub buffer_all: bool,
    /// Poll the input from a spawned task
    pub eager: bool,
    /// How to apply the fetch. `with_fetch` returns `None` unless the fetch is
    /// applied in some way.
    pub fetch_mode: FetchMode,
    /// Keep the fetch when `with_fetch(None)` is called
    pub keep_fetch_on_with_fetch_none: bool,
    /// Make the plans returned by `with_fetch` report no ordering
    pub with_fetch_drops_ordering: bool,
    /// Report no ordering
    pub drop_ordering: bool,
    /// Return true from `supports_limit_pushdown`
    pub limit_pushdown: bool,
    /// Skip the first `skip` rows of each partition, as an `OFFSET` would
    pub skip: usize,
    /// Report a single output partition, and produce the rows of every input
    /// partition in it
    pub coalesce: bool,
    /// End each partition after as many rows as the session batch size, as a
    /// node that only emits its first output batch would
    pub stop_after_session_batch: bool,
    /// What to do when the input returns an error
    pub on_input_error: OnInputError,
    /// Where to keep input streams when the output stream is dropped
    pub hold_input: HoldInput,
    /// Reserve memory in every partition and never release it
    pub leak_memory: bool,
    /// What to do when asked for a partition at or past the partition count
    pub invalid_partition: InvalidPartition,
    /// What to do when a partition of the same instance is executed again
    pub on_rerun: OnRerun,
    /// Keep the record of executed partitions in `reset_state`
    pub keep_state_on_reset: bool,
}

impl ConfigurableExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            effect: Effect::Equal,
            effect_without_fetch: None,
            fetch: None,
            num_rows: None,
            partition_num_rows: None,
            skip_child_stats: false,
            drop_column_stats: 0,
            stats_error: false,
            maintains_input_order_len: None,
            maintains_order: None,
            required_ordering: None,
            invariants_error: false,
            executable_invariants_error: false,
            claimed_ordering: None,
            claimed_eq_properties: None,
            claimed_partitioning: None,
            claimed_schema: None,
            expressions: vec![],
            dynamic_expressions: vec![],
            reset_dynamic_expressions: false,
            transform: Transform::None,
            execute_error: false,
            hang: false,
            boundedness: None,
            emission_type: None,
            evaluation_type: None,
            buffer_all: false,
            eager: false,
            fetch_mode: FetchMode::Ignore,
            keep_fetch_on_with_fetch_none: false,
            with_fetch_drops_ordering: false,
            drop_ordering: false,
            limit_pushdown: false,
            skip: 0,
            coalesce: false,
            stop_after_session_batch: false,
            on_input_error: OnInputError::Propagate,
            hold_input: HoldInput::No,
            leak_memory: false,
            invalid_partition: InvalidPartition::PassToInput,
            on_rerun: OnRerun::Same,
            keep_state_on_reset: false,
        }
    }

    pub fn effect(mut self, effect: Effect) -> Self {
        self.effect = effect;
        self
    }

    pub fn fetch(mut self, fetch: usize) -> Self {
        self.fetch = Some(fetch);
        self
    }

    pub fn num_rows(mut self, num_rows: Precision<usize>) -> Self {
        self.num_rows = Some(num_rows);
        self
    }

    pub fn partition_num_rows(mut self, num_rows: Vec<Precision<usize>>) -> Self {
        self.partition_num_rows = Some(num_rows);
        self
    }

    pub fn transform(mut self, transform: Transform) -> Self {
        self.transform = transform;
        self
    }

    pub fn build(self) -> Arc<dyn ExecutionPlan> {
        let cache = self.compute_properties();
        Arc::new(Built {
            exec: self,
            cache,
            held_inputs: HeldInputs::default(),
            executed: Arc::default(),
        })
    }

    fn compute_properties(&self) -> Arc<PlanProperties> {
        let overridden = self.claimed_ordering.is_some()
            || self.claimed_eq_properties.is_some()
            || self.claimed_partitioning.is_some()
            || self.drop_ordering
            || self.coalesce
            || self.boundedness.is_some()
            || self.emission_type.is_some()
            || self.evaluation_type.is_some();
        if !overridden {
            return Arc::clone(self.input.properties());
        }
        let mut properties = self.input.properties().as_ref().clone();
        if let Some(ordering) = &self.claimed_ordering {
            properties =
                properties.with_eq_properties(EquivalenceProperties::new_with_orderings(
                    self.input.schema(),
                    [ordering.clone()],
                ));
        }
        if let Some(eq_properties) = &self.claimed_eq_properties {
            properties = properties.with_eq_properties(eq_properties.clone());
        }
        if self.drop_ordering || self.coalesce {
            properties = properties
                .with_eq_properties(EquivalenceProperties::new(self.input.schema()));
        }
        if self.coalesce {
            properties =
                properties.with_partitioning(Partitioning::UnknownPartitioning(1));
        }
        if let Some(partitioning) = &self.claimed_partitioning {
            properties = properties.with_partitioning(partitioning.clone());
        }
        if let Some(boundedness) = self.boundedness {
            properties = properties.with_boundedness(boundedness);
        }
        if let Some(emission_type) = self.emission_type {
            properties = properties.with_emission_type(emission_type);
        }
        if let Some(evaluation_type) = self.evaluation_type {
            properties = properties.with_evaluation_type(evaluation_type);
        }
        Arc::new(properties)
    }
}

/// A [`ConfigurableExec`] with its plan properties computed
#[derive(Debug, Clone)]
struct Built {
    exec: ConfigurableExec,
    cache: Arc<PlanProperties>,
    /// Input streams kept for [`HoldInput::InPlan`]
    held_inputs: HeldInputs,
    /// The partitions executed so far, for [`ConfigurableExec::on_rerun`]
    executed: Arc<Mutex<HashSet<usize>>>,
}

/// Input streams kept alive by the plan
#[derive(Clone, Default)]
struct HeldInputs(Arc<Mutex<Vec<SendableRecordBatchStream>>>);

impl fmt::Debug for HeldInputs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "HeldInputs")
    }
}

/// Owns an input stream and keeps it according to a [`HoldInput`] when
/// dropped
struct HeldInput {
    stream: Option<SendableRecordBatchStream>,
    hold: HoldInput,
    held_inputs: HeldInputs,
}

impl Drop for HeldInput {
    fn drop(&mut self) {
        let Some(stream) = self.stream.take() else {
            return;
        };
        match self.hold {
            HoldInput::No => {}
            HoldInput::InPlan => self.held_inputs.0.lock().unwrap().push(stream),
            HoldInput::Forever => {
                Box::leak(Box::new(stream));
            }
            HoldInput::InTask => {
                // A detached task, which must outlive the stream that spawns it
                #[expect(clippy::disallowed_methods)]
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    drop(stream);
                });
            }
        }
    }
}

impl DisplayAs for Built {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ConfigurableExec")
    }
}

impl ExecutionPlan for Built {
    fn name(&self) -> &'static str {
        "ConfigurableExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn schema(&self) -> SchemaRef {
        match &self.exec.claimed_schema {
            Some(schema) => Arc::clone(schema),
            None => Arc::clone(self.cache.equivalence_properties().schema()),
        }
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        if self.exec.invariants_error {
            return internal_err!("ConfigurableExec invariants are broken");
        }
        if self.exec.executable_invariants_error
            && matches!(check, InvariantLevel::Executable)
        {
            return internal_err!("ConfigurableExec cannot be executed");
        }
        datafusion_physical_plan::execution_plan::check_default_invariants(self, check)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        let maintains = self.exec.maintains_order.unwrap_or(!self.exec.coalesce);
        vec![maintains; self.exec.maintains_input_order_len.unwrap_or(1)]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            self.exec
                .required_ordering
                .clone()
                .map(OrderingRequirements::from),
        ]
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.exec.limit_pushdown
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.exec.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(&self.exec.expressions, f)
    }

    fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exec.dynamic_expressions.clone()
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut exec = self.exec.clone();
        if exec.reset_dynamic_expressions {
            // Each dynamic filter and the new filter that replaces it
            let reset: Vec<_> = exec
                .dynamic_expressions
                .iter()
                .map(|expr| (Arc::clone(expr), new_dynamic_filter(expr)))
                .collect();
            let replace = |expr: &Arc<dyn PhysicalExpr>| {
                reset
                    .iter()
                    .find(|(old, _)| Arc::ptr_eq(old, expr))
                    .map_or_else(|| Arc::clone(expr), |(_, new)| Arc::clone(new))
            };
            exec.expressions = exec.expressions.iter().map(replace).collect();
            exec.dynamic_expressions =
                exec.dynamic_expressions.iter().map(replace).collect();
        }
        if exec.keep_state_on_reset {
            return Ok(Arc::new(Built {
                cache: exec.compute_properties(),
                exec,
                held_inputs: self.held_inputs.clone(),
                executed: Arc::clone(&self.executed),
            }));
        }
        Ok(exec.build())
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut exec = self.exec.clone();
        exec.input = children.swap_remove(0);
        Ok(exec.build())
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
        if self.exec.execute_error {
            return internal_err!("ConfigurableExec cannot execute");
        }
        let schema = self.schema();
        let stream = |items: Vec<Result<RecordBatch>>| -> SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(items),
            ))
        };
        let first_execution = self.executed.lock().unwrap().insert(partition);
        if !first_execution {
            match self.exec.on_rerun {
                OnRerun::Same => {}
                OnRerun::Empty => return Ok(stream(vec![])),
                OnRerun::Error => {
                    return internal_err!(
                        "ConfigurableExec partition {partition} was already executed"
                    );
                }
                OnRerun::Panic => {
                    panic!("ConfigurableExec partition {partition} was already executed")
                }
            }
        }
        if partition >= self.properties().output_partitioning().partition_count() {
            match self.exec.invalid_partition {
                InvalidPartition::PassToInput => {}
                InvalidPartition::PanicInExecute => {
                    panic!("ConfigurableExec has no partition {partition}")
                }
                InvalidPartition::PanicInStream => {
                    return Ok(Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&schema),
                        futures::stream::poll_fn(
                            move |_| -> std::task::Poll<Option<Result<RecordBatch>>> {
                                panic!("ConfigurableExec has no partition {partition}")
                            },
                        ),
                    )));
                }
                InvalidPartition::ErrorInStream => {
                    return Ok(stream(vec![internal_err!(
                        "ConfigurableExec has no partition {partition}"
                    )]));
                }
                InvalidPartition::EmptyStream => return Ok(stream(vec![])),
            }
        }
        if self.exec.hang {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::pending(),
            )));
        }
        if self.exec.leak_memory {
            let reservation =
                MemoryConsumer::new(format!("ConfigurableExec[{partition}]"))
                    .register(context.memory_pool());
            reservation.grow(1024);
            Box::leak(Box::new(reservation));
        }
        let mut input = if self.exec.coalesce {
            let partitions = self
                .exec
                .input
                .properties()
                .output_partitioning()
                .partition_count();
            let streams = (0..partitions)
                .map(|p| self.exec.input.execute(p, Arc::clone(&context)))
                .collect::<Result<Vec<_>>>()?;
            Box::pin(RecordBatchStreamAdapter::new(
                self.exec.input.schema(),
                futures::stream::iter(streams).flatten(),
            ))
        } else {
            self.exec.input.execute(partition, Arc::clone(&context))?
        };
        if self.exec.eager {
            let mut builder = RecordBatchReceiverStream::builder(self.schema(), 2);
            let tx = builder.tx();
            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if tx.send(item).await.is_err() {
                        break;
                    }
                }
                Ok(())
            });
            input = builder.build();
        }
        let mut held = HeldInput {
            stream: Some(input),
            hold: self.exec.hold_input,
            held_inputs: self.held_inputs.clone(),
        };
        let on_input_error = self.exec.on_input_error;
        let mut hung = false;
        let items = futures::stream::poll_fn(move |cx| {
            let stream = held.stream.as_mut().expect("input is present until drop");
            loop {
                if hung {
                    return std::task::Poll::Pending;
                }
                return match stream.poll_next_unpin(cx) {
                    std::task::Poll::Ready(Some(Err(e))) => match on_input_error {
                        OnInputError::Propagate => std::task::Poll::Ready(Some(Err(e))),
                        OnInputError::Swallow => continue,
                        OnInputError::Hang => {
                            hung = true;
                            continue;
                        }
                        OnInputError::Panic => panic!("ConfigurableExec got an error"),
                    },
                    poll => poll,
                };
            }
        });
        let transform = self.exec.transform;
        let mut stream = items.map(move |batch| transform.apply(batch?)).boxed();

        let fetch = self
            .exec
            .fetch
            .filter(|_| self.exec.fetch_mode != FetchMode::Ignore);
        let skip = match (fetch, self.exec.fetch_mode) {
            (Some(_), FetchMode::SkipFirstRow) => self.exec.skip + 1,
            _ => self.exec.skip,
        };
        let fetch_limit = fetch.and_then(|fetch| match self.exec.fetch_mode {
            FetchMode::Ignore | FetchMode::RepeatFirstRow => None,
            FetchMode::Enforce | FetchMode::SkipFirstRow => Some(fetch),
            FetchMode::KeepOneMore => Some(fetch + 1),
            FetchMode::KeepHalf => Some(fetch / 2),
        });
        let batch_limit = self
            .exec
            .stop_after_session_batch
            .then(|| context.session_config().batch_size());
        let limit = match (fetch_limit, batch_limit) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if skip > 0 || limit.is_some() {
            stream = skip_and_limit(stream, skip, limit);
        }
        if let (Some(fetch), FetchMode::RepeatFirstRow) = (fetch, self.exec.fetch_mode) {
            let schema = self.schema();
            stream = futures::stream::once(async move {
                let mut stream = stream;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if batch.num_rows() > 0 {
                        let indices = UInt32Array::from(vec![0; fetch]);
                        return Ok(take_record_batch(&batch, &indices)?);
                    }
                }
                Ok(RecordBatch::new_empty(schema))
            })
            .boxed();
        }
        if self.exec.buffer_all {
            let schema = self.schema();
            stream = futures::stream::once(async move {
                let batches = stream.collect::<Vec<_>>().await;
                let batches = batches.into_iter().collect::<Result<Vec<_>>>()?;
                Ok(concat_batches(&schema, &batches)?)
            })
            .boxed();
        }
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        if self.exec.skip_child_stats {
            vec![ChildStats::Skip]
        } else if self.exec.coalesce {
            // The only output partition has all input rows
            vec![ChildStats::At(None)]
        } else {
            vec![ChildStats::At(partition)]
        }
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        if self.exec.stats_error {
            return internal_err!("ConfigurableExec statistics are broken");
        }
        let mut stats = input_stats[0].as_ref().clone();
        let partitions = self.properties().output_partitioning().partition_count();
        let num_rows = match (args.partition(), &self.exec.partition_num_rows) {
            (None, _) => self.exec.num_rows,
            (Some(p), Some(rows)) => Some(rows[p]),
            // With a single partition, the overall override also applies to
            // the partition, keeping the statistics consistent
            (Some(_), None) if partitions == 1 => self.exec.num_rows,
            (Some(_), None) => None,
        };
        if let Some(num_rows) = num_rows {
            stats.num_rows = num_rows;
        }
        let keep = stats
            .column_statistics
            .len()
            .saturating_sub(self.exec.drop_column_stats);
        stats.column_statistics.truncate(keep);
        Ok(Arc::new(stats))
    }

    fn fetch(&self) -> Option<usize> {
        self.exec.fetch
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if self.exec.fetch_mode == FetchMode::Ignore {
            return None;
        }
        let mut exec = self.exec.clone();
        if fetch.is_some() || !exec.keep_fetch_on_with_fetch_none {
            exec.fetch = fetch;
        }
        if exec.with_fetch_drops_ordering {
            exec.drop_ordering = true;
        }
        Some(exec.build())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        let effect = match (self.exec.fetch, self.exec.effect_without_fetch) {
            (None, Some(effect)) => effect,
            _ => self.exec.effect,
        };
        match effect {
            Effect::Unknown => CardinalityEffect::Unknown,
            Effect::Equal => CardinalityEffect::Equal,
            Effect::LowerEqual => CardinalityEffect::LowerEqual,
            Effect::GreaterEqual => CardinalityEffect::GreaterEqual,
        }
    }
}

/// A new dynamic filter on the same columns as `expr`, in its initial state,
/// if `expr` is a dynamic filter, and otherwise `expr` itself
fn new_dynamic_filter(expr: &Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
    match expr.downcast_ref::<DynamicFilterPhysicalExpr>() {
        Some(filter) => Arc::new(DynamicFilterPhysicalExpr::new(
            filter.children().into_iter().cloned().collect(),
            lit(true),
        )),
        None => Arc::clone(expr),
    }
}

/// Skip the first `skip` rows of `stream`, then end after `limit` rows, if
/// set, without polling the input again
fn skip_and_limit(
    mut stream: BoxStream<'static, Result<RecordBatch>>,
    skip: usize,
    limit: Option<usize>,
) -> BoxStream<'static, Result<RecordBatch>> {
    let mut to_skip = skip;
    let mut remaining = limit;
    futures::stream::poll_fn(move |cx| {
        if remaining == Some(0) {
            return std::task::Poll::Ready(None);
        }
        match stream.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                let skipped = to_skip.min(batch.num_rows());
                to_skip -= skipped;
                let mut batch = batch.slice(skipped, batch.num_rows() - skipped);
                if let Some(remaining) = remaining.as_mut() {
                    let rows = batch.num_rows().min(*remaining);
                    *remaining -= rows;
                    batch = batch.slice(0, rows);
                }
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            poll => poll,
        }
    })
    .boxed()
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
}

/// A source with one partition per entry of `rows`
pub fn source(rows: &[usize], precision: StatisticsPrecision) -> Arc<dyn ExecutionPlan> {
    SourceSpec::new(schema())
        .with_partition_rows(rows)
        .with_statistics_precision(precision)
        .build_arc()
        .unwrap()
}

/// A source with a single partition and an exact row count
pub fn exact_source(num_rows: usize) -> Arc<dyn ExecutionPlan> {
    source(&[num_rows], StatisticsPrecision::Exact)
}

/// A source with a single partition and an inexact row count
pub fn inexact_source(num_rows: usize) -> Arc<dyn ExecutionPlan> {
    source(&[num_rows], StatisticsPrecision::Inexact)
}

/// A checker with the built-in checks named `names`
pub fn checker(names: &[&str]) -> PlanChecker {
    let checks: Vec<_> = checks::all_checks()
        .into_iter()
        .filter(|check| names.contains(&check.name))
        .collect();
    assert_eq!(checks.len(), names.len(), "unknown check in {names:?}");
    PlanChecker::with_checks(checks)
}

/// A checker with the built-in checks of the given kinds
pub fn checker_of(kinds: &[CheckKind]) -> PlanChecker {
    PlanChecker::with_checks(
        checks::all_checks()
            .into_iter()
            .filter(|check| kinds.contains(&check.kind))
            .collect(),
    )
}

/// `(severity, check name)` of every violation in the report
pub fn summary(report: &Report) -> Vec<(Severity, &'static str)> {
    report
        .violations
        .iter()
        .map(|v| (v.severity, v.check))
        .collect()
}

/// Messages of every violation in the report
pub fn messages(report: &Report) -> Vec<&str> {
    report
        .violations
        .iter()
        .map(|v| v.message.as_str())
        .collect()
}
