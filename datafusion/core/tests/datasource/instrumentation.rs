// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Application instrumentation through the public data source and query APIs.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow::array::{Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, assert_batches_eq};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::instrumentation::{
    DataSourceInstrumentation, DataSourceInstrumentationExtension, DataSourceOpen,
};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_datasource_csv::source::CsvSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use datafusion_physical_plan::{ExecutionPlan, RecordBatchStream, collect};
use futures::{Stream, StreamExt};
use tempfile::Builder;

#[tokio::test]
async fn memory_query_observes_split_batches_and_retains_labeled_metrics() -> Result<()> {
    let hook = Arc::new(RecordingInstrumentation::default());
    let ctx = session(Arc::clone(&hook));
    let plan = ctx
        .read_batches(vec![batch()?])?
        .create_physical_plan()
        .await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_batches_eq!(
        [
            "+---+", "| a |", "+---+", "| 0 |", "| 1 |", "| 2 |", "| 3 |", "| 4 |",
            "+---+"
        ],
        &batches
    );
    assert_eq!(
        *hook.events.lock().unwrap(),
        ["open:memory", "batch:2", "batch:2", "batch:1", "finish"]
    );
    // MemorySourceConfig uses the default, nonpersistent DataSource metrics set.
    // Repeated metrics() calls must still expose the application's counters.
    for _ in 0..2 {
        assert_eq!(observed_rows(&plan)?, 5);
    }
    Ok(())
}

#[tokio::test]
async fn csv_query_preserves_source_metrics_and_output() -> Result<()> {
    let hook = Arc::new(RecordingInstrumentation::default());
    let ctx = session(Arc::clone(&hook));
    let file = Builder::new().suffix(".csv").tempfile()?;
    std::fs::write(file.path(), "a\n10\n20\n30\n")?;
    let schema = schema();
    let plan = ctx
        .read_csv(
            file.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?
        .create_physical_plan()
        .await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    assert_batches_eq!(
        [
            "+----+", "| a  |", "+----+", "| 10 |", "| 20 |", "| 30 |", "+----+"
        ],
        &batches
    );
    assert_eq!(observed_rows(&plan)?, 3);
    assert_eq!(hook.events.lock().unwrap()[0], "open:file");
    let mut source_rows = 0;
    plan.apply(|node| {
        if let Some(metrics) = node.metrics() {
            source_rows += metrics.output_rows().unwrap_or(0);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert!(source_rows >= 3);
    Ok(())
}

#[tokio::test]
async fn instrumentation_is_selected_at_execution_and_reset_with_the_plan() -> Result<()>
{
    let hook = Arc::new(RecordingInstrumentation::default());
    let ctx = session(hook);
    let batch = batch()?;
    let plan = MemorySourceConfig::try_new_exec(&[vec![batch]], schema(), None)?;
    collect(Arc::clone(&plan) as _, SessionContext::new().task_ctx()).await?;
    assert_eq!(observed_rows(&(Arc::clone(&plan) as _))?, 0);
    collect(Arc::clone(&plan) as _, ctx.task_ctx()).await?;
    assert_eq!(observed_rows(&(Arc::clone(&plan) as _))?, 5);

    let reset = Arc::clone(&plan).reset_state()?;
    assert_eq!(observed_rows(&reset)?, 0);
    collect(Arc::clone(&reset), ctx.task_ctx()).await?;
    assert_eq!(observed_rows(&reset)?, 5);
    assert_eq!(observed_rows(&(Arc::clone(&plan) as _))?, 5);

    let limited = plan.with_fetch(Some(3)).unwrap();
    let batches = collect(Arc::clone(&limited), ctx.task_ctx()).await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    assert_eq!(observed_rows(&limited)?, 3);
    assert_eq!(observed_rows(&(plan as _))?, 5);
    Ok(())
}

#[tokio::test]
async fn dropping_a_partially_consumed_stream_is_observable() -> Result<()> {
    let hook = Arc::new(RecordingInstrumentation::default());
    let ctx = session(Arc::clone(&hook));
    let plan = MemorySourceConfig::try_new_exec(&[vec![batch()?]], schema(), None)?;
    let mut stream = plan.execute(0, ctx.task_ctx())?;
    assert_eq!(stream.next().await.unwrap()?.num_rows(), 2);
    drop(stream);
    assert_eq!(
        *hook.events.lock().unwrap(),
        ["open:memory", "batch:2", "cancel"]
    );
    Ok(())
}

#[tokio::test]
async fn csv_read_errors_reach_the_observer_and_consumer() -> Result<()> {
    let hook = Arc::new(RecordingInstrumentation::default());
    let ctx = session(Arc::clone(&hook));
    let file = Builder::new().suffix(".csv").tempfile()?;
    std::fs::write(file.path(), "a\nnot-an-integer\n")?;
    let schema = schema();
    let plan = ctx
        .read_csv(
            file.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?
        .create_physical_plan()
        .await?;
    let error = collect(plan, ctx.task_ctx()).await.unwrap_err();
    assert!(error.to_string().contains("not-an-integer"));
    assert_eq!(*hook.events.lock().unwrap(), ["open:file", "error"]);
    Ok(())
}

#[tokio::test]
async fn opening_errors_are_observed_without_replacing_the_error() -> Result<()> {
    let hook = Arc::new(RecordingInstrumentation::default());
    let ctx = session(Arc::clone(&hook));
    let source = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("unregistered://bucket")?,
        Arc::new(CsvSource::new(schema())),
    )
    .with_file(PartitionedFile::new("test.csv", 10))
    .build();
    let plan = DataSourceExec::from_data_source(source);
    let error = plan
        .execute(0, ctx.task_ctx())
        .err()
        .expect("unregistered store");
    let without_hook = plan
        .execute(0, SessionContext::new().task_ctx())
        .err()
        .expect("unregistered store");
    assert_eq!(error.to_string(), without_hook.to_string());
    assert_eq!(*hook.events.lock().unwrap(), ["open:file", "open-error"]);
    Ok(())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
}

fn batch() -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int32Array::from_iter_values(0..5))],
    )?)
}

fn session(hook: Arc<RecordingInstrumentation>) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(2)
        .with_extension(Arc::new(DataSourceInstrumentationExtension(hook)));
    SessionContext::new_with_config(config)
}

fn observed_rows(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
    let mut rows = 0;
    plan.apply(|node| {
        if let Some(metrics) = node.metrics() {
            for metric in metrics
                .iter()
                .filter(|m| m.value().name() == "observed_rows")
            {
                assert!(
                    metric
                        .labels()
                        .iter()
                        .any(|l| l.name() == "application" && l.value() == "test")
                );
                rows += metric.value().as_usize();
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(rows)
}

#[derive(Debug, Default)]
struct RecordingInstrumentation {
    events: Arc<Mutex<Vec<String>>>,
}

impl DataSourceInstrumentation for RecordingInstrumentation {
    fn instrument(
        &self,
        source: &dyn DataSource,
        partition: usize,
        context: &TaskContext,
        metrics: &ExecutionPlanMetricsSet,
        open: DataSourceOpen<'_>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(context.session_config().batch_size(), 2);
        let kind = if source.is::<MemorySourceConfig>() {
            "memory"
        } else {
            assert!(source.is::<FileScanConfig>());
            "file"
        };
        self.events.lock().unwrap().push(format!("open:{kind}"));
        let input = open()
            .inspect_err(|_| self.events.lock().unwrap().push("open-error".into()))?;
        let rows = MetricBuilder::new(metrics)
            .with_new_label("application", "test")
            .counter("observed_rows", partition);
        Ok(Box::pin(ObservedStream {
            input,
            rows,
            events: Arc::clone(&self.events),
            terminal: false,
        }))
    }
}

struct ObservedStream {
    input: SendableRecordBatchStream,
    rows: Count,
    events: Arc<Mutex<Vec<String>>>,
    terminal: bool,
}

impl Stream for ObservedStream {
    type Item = Result<RecordBatch>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = self.input.as_mut().poll_next(cx);
        let event = match &result {
            Poll::Ready(Some(Ok(batch))) => {
                self.rows.add(batch.num_rows());
                Some(format!("batch:{}", batch.num_rows()))
            }
            Poll::Ready(Some(Err(_))) => {
                self.terminal = true;
                Some("error".into())
            }
            Poll::Ready(None) if !self.terminal => {
                self.terminal = true;
                Some("finish".into())
            }
            Poll::Ready(None) | Poll::Pending => None,
        };
        if let Some(event) = event {
            self.events.lock().unwrap().push(event);
        }
        result
    }
}
impl RecordBatchStream for ObservedStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
impl Drop for ObservedStream {
    fn drop(&mut self) {
        if !self.terminal {
            self.events.lock().unwrap().push("cancel".into());
        }
    }
}
