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

use std::io::{BufWriter, Write};
use std::marker::PhantomData;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{fs::File, pin::Pin};

use crate::error::{BallistaError, Result};
use crate::execution_plans::{
    DistributedQueryExec, ShuffleWriterExec, UnresolvedShuffleExec,
};
use crate::serde::scheduler::PartitionStats;

use crate::config::BallistaConfig;
use crate::serde::{AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec};
use async_trait::async_trait;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::{
    datatypes::SchemaRef, ipc::writer::FileWriter, record_batch::RecordBatch,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::{
    QueryPlanner, SessionConfig, SessionContext, SessionState,
};
use datafusion::logical_plan::LogicalPlan;

use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::common::batch_byte_size;
use datafusion::physical_plan::empty::EmptyExec;

use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::file_format::{CsvExec, ParquetExec};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{metrics, ExecutionPlan, RecordBatchStream};
use futures::{Stream, StreamExt};

/// Stream data to disk in Arrow IPC format

pub async fn write_stream_to_disk(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
    path: &str,
    disk_write_metric: &metrics::Time,
) -> Result<PartitionStats> {
    let file = File::create(&path).map_err(|e| {
        BallistaError::General(format!(
            "Failed to create partition file at {}: {:?}",
            path, e
        ))
    })?;

    let mut num_rows = 0;
    let mut num_batches = 0;
    let mut num_bytes = 0;
    let mut writer = FileWriter::try_new(file, stream.schema().as_ref())?;

    while let Some(result) = stream.next().await {
        let batch = result?;

        let batch_size_bytes: usize = batch_byte_size(&batch);
        num_batches += 1;
        num_rows += batch.num_rows();
        num_bytes += batch_size_bytes;

        let timer = disk_write_metric.timer();
        writer.write(&batch)?;
        timer.done();
    }
    let timer = disk_write_metric.timer();
    writer.finish()?;
    timer.done();
    Ok(PartitionStats::new(
        Some(num_rows as u64),
        Some(num_batches),
        Some(num_bytes as u64),
    ))
}

pub async fn collect_stream(
    stream: &mut Pin<Box<dyn RecordBatchStream + Send>>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = vec![];
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

pub fn produce_diagram(filename: &str, stages: &[Arc<ShuffleWriterExec>]) -> Result<()> {
    let write_file = File::create(filename)?;
    let mut w = BufWriter::new(&write_file);
    writeln!(w, "digraph G {{")?;

    // draw stages and entities
    for stage in stages {
        writeln!(w, "\tsubgraph cluster{} {{", stage.stage_id())?;
        writeln!(w, "\t\tlabel = \"Stage {}\";", stage.stage_id())?;
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(
            &mut w,
            stage.children()[0].as_ref(),
            stage.stage_id(),
            &mut id,
            true,
        )?;
        writeln!(w, "\t}}")?;
    }

    // draw relationships
    for stage in stages {
        let mut id = AtomicUsize::new(0);
        build_exec_plan_diagram(
            &mut w,
            stage.children()[0].as_ref(),
            stage.stage_id(),
            &mut id,
            false,
        )?;
    }

    write!(w, "}}")?;
    Ok(())
}

fn build_exec_plan_diagram(
    w: &mut BufWriter<&File>,
    plan: &dyn ExecutionPlan,
    stage_id: usize,
    id: &mut AtomicUsize,
    draw_entity: bool,
) -> Result<usize> {
    let operator_str = if plan.as_any().downcast_ref::<AggregateExec>().is_some() {
        "AggregateExec"
    } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
        "SortExec"
    } else if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
        "ProjectionExec"
    } else if plan.as_any().downcast_ref::<HashJoinExec>().is_some() {
        "HashJoinExec"
    } else if plan.as_any().downcast_ref::<ParquetExec>().is_some() {
        "ParquetExec"
    } else if plan.as_any().downcast_ref::<CsvExec>().is_some() {
        "CsvExec"
    } else if plan.as_any().downcast_ref::<FilterExec>().is_some() {
        "FilterExec"
    } else if plan.as_any().downcast_ref::<ShuffleWriterExec>().is_some() {
        "ShuffleWriterExec"
    } else if plan
        .as_any()
        .downcast_ref::<UnresolvedShuffleExec>()
        .is_some()
    {
        "UnresolvedShuffleExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalesceBatchesExec>()
        .is_some()
    {
        "CoalesceBatchesExec"
    } else if plan
        .as_any()
        .downcast_ref::<CoalescePartitionsExec>()
        .is_some()
    {
        "CoalescePartitionsExec"
    } else {
        println!("Unknown: {:?}", plan);
        "Unknown"
    };

    let node_id = id.load(Ordering::SeqCst);
    id.store(node_id + 1, Ordering::SeqCst);

    if draw_entity {
        writeln!(
            w,
            "\t\tstage_{}_exec_{} [shape=box, label=\"{}\"];",
            stage_id, node_id, operator_str
        )?;
    }
    for child in plan.children() {
        if let Some(shuffle) = child.as_any().downcast_ref::<UnresolvedShuffleExec>() {
            if !draw_entity {
                writeln!(
                    w,
                    "\tstage_{}_exec_1 -> stage_{}_exec_{};",
                    shuffle.stage_id, stage_id, node_id
                )?;
            }
        } else {
            // relationships within same entity
            let child_id =
                build_exec_plan_diagram(w, child.as_ref(), stage_id, id, draw_entity)?;
            if draw_entity {
                writeln!(
                    w,
                    "\t\tstage_{}_exec_{} -> stage_{}_exec_{};",
                    stage_id, child_id, stage_id, node_id
                )?;
            }
        }
    }
    Ok(node_id)
}

/// Create a client DataFusion context that uses the BallistaQueryPlanner to send logical plans
/// to a Ballista scheduler
pub fn create_df_ctx_with_ballista_query_planner<T: 'static + AsLogicalPlan>(
    scheduler_url: String,
    session_id: String,
    config: &BallistaConfig,
) -> SessionContext {
    let planner: Arc<BallistaQueryPlanner<T>> =
        Arc::new(BallistaQueryPlanner::new(scheduler_url, config.clone()));

    let session_config = SessionConfig::new()
        .with_target_partitions(config.default_shuffle_partitions())
        .with_information_schema(true);
    let mut session_state = SessionState::with_config_rt(
        session_config,
        Arc::new(RuntimeEnv::new(RuntimeConfig::default()).unwrap()),
    )
    .with_query_planner(planner);
    session_state.session_id = session_id;
    // the SessionContext created here is the client side context, but the session_id is from server side.
    SessionContext::with_state(session_state)
}

pub struct BallistaQueryPlanner<T: AsLogicalPlan> {
    scheduler_url: String,
    config: BallistaConfig,
    extension_codec: Arc<dyn LogicalExtensionCodec>,
    plan_repr: PhantomData<T>,
}

impl<T: 'static + AsLogicalPlan> BallistaQueryPlanner<T> {
    pub fn new(scheduler_url: String, config: BallistaConfig) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec: Arc::new(DefaultLogicalExtensionCodec {}),
            plan_repr: PhantomData,
        }
    }

    pub fn with_extension(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            plan_repr: PhantomData,
        }
    }

    pub fn with_repr(
        scheduler_url: String,
        config: BallistaConfig,
        extension_codec: Arc<dyn LogicalExtensionCodec>,
        plan_repr: PhantomData<T>,
    ) -> Self {
        Self {
            scheduler_url,
            config,
            extension_codec,
            plan_repr,
        }
    }
}

#[async_trait]
impl<T: 'static + AsLogicalPlan> QueryPlanner for BallistaQueryPlanner<T> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        match logical_plan {
            LogicalPlan::CreateExternalTable(_) => {
                // table state is managed locally in the BallistaContext, not in the scheduler
                Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
            }
            _ => Ok(Arc::new(DistributedQueryExec::with_repr(
                self.scheduler_url.clone(),
                self.config.clone(),
                logical_plan.clone(),
                self.extension_codec.clone(),
                self.plan_repr,
                session_state.session_id.clone(),
            ))),
        }
    }
}

pub struct WrappedStream {
    stream: Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send>>,
    schema: SchemaRef,
}

impl WrappedStream {
    pub fn new(
        stream: Pin<Box<dyn Stream<Item = ArrowResult<RecordBatch>> + Send>>,
        schema: SchemaRef,
    ) -> Self {
        Self { stream, schema }
    }
}

impl RecordBatchStream for WrappedStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for WrappedStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
