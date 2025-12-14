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

//! Defines the ANALYZE operator

use std::any::Any;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::Arc;

use super::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use super::{
    DisplayAs, Distribution, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use crate::display::DisplayableExecutionPlan;
use crate::metrics::MetricType;
use crate::{DisplayFormatType, ExecutionPlan, Partitioning};

use arrow::compute::concat_batches;
use arrow::util::pretty::pretty_format_batches;
use arrow::{array::StringBuilder, datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion_common::instant::Instant;
use datafusion_common::{DataFusionError, Result, assert_eq_or_internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::EquivalenceProperties;

use futures::StreamExt;

/// `EXPLAIN ANALYZE` execution plan operator. This operator runs its input,
/// discards the results, and then prints out an annotated plan with metrics
#[derive(Debug, Clone)]
pub struct AnalyzeExec {
    /// Control how much extra to print
    verbose: bool,
    /// If statistics should be displayed
    show_statistics: bool,
    /// Which metric categories should be displayed
    metric_types: Vec<MetricType>,
    /// The input plan (the plan being analyzed)
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// The output schema for RecordBatches of this exec node
    schema: SchemaRef,
    cache: PlanProperties,
    /// Whether the operator is executing in the `auto_explain` mode.
    /// In this mode, the underlying records are returned as they would without the `AnalyzeExec`,
    /// while the "explain analyze" output is sent to `auto_explain_output`. (default=false)
    auto_explain: bool,
    /// Where to store the output of `auto_explain`, if enabled.
    /// Possible values:
    ///   - stdout (default)
    ///   - stderr
    ///   - *path to file* (creates if it does not exist; appends to it if it does)
    auto_explain_output: String,
    /// In the `auto_explain` mode, only output if the execution is greater or equal to this value,
    /// in milliseconds. (default=0)
    auto_explain_min_duration: usize,
}

impl AnalyzeExec {
    /// Create a new AnalyzeExec
    pub fn new(
        verbose: bool,
        show_statistics: bool,
        metric_types: Vec<MetricType>,
        input: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Self {
        let cache = Self::compute_properties(&input, Arc::clone(&schema));
        AnalyzeExec {
            verbose,
            show_statistics,
            metric_types,
            input,
            schema,
            cache,
            auto_explain: false,
            auto_explain_output: "stdout".to_owned(),
            auto_explain_min_duration: 0,
        }
    }

    /// Access to verbose
    pub fn verbose(&self) -> bool {
        self.verbose
    }

    /// Access to show_statistics
    pub fn show_statistics(&self) -> bool {
        self.show_statistics
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }

    pub fn enable_auto_explain(&mut self) {
        self.auto_explain = true;
        self.cache =
            Self::compute_properties(&self.input, Arc::clone(&self.input.schema()));
    }

    pub fn set_auto_explain_output(&mut self, value: String) {
        self.auto_explain_output = value
    }

    pub fn set_auto_explain_min_duration(&mut self, value: usize) {
        self.auto_explain_min_duration = value
    }
}

impl DisplayAs for AnalyzeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AnalyzeExec verbose={}", self.verbose)
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for AnalyzeExec {
    fn name(&self) -> &'static str {
        "AnalyzeExec"
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

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.verbose,
            self.show_statistics,
            self.metric_types.clone(),
            children.pop().unwrap(),
            Arc::clone(&self.schema),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq_or_internal_err!(
            partition,
            0,
            "AnalyzeExec invalid partition. Expected 0, got {partition}"
        );

        // Gather futures that will run each input partition in
        // parallel (on a separate tokio task) using a JoinSet to
        // cancel outstanding futures on drop
        let num_input_partitions = self.input.output_partitioning().partition_count();
        let mut builder =
            RecordBatchReceiverStream::builder(self.schema(), num_input_partitions);

        for input_partition in 0..num_input_partitions {
            builder.run_input(
                Arc::clone(&self.input),
                input_partition,
                Arc::clone(&context),
            );
        }

        // Create future that computes the final output
        let start = Instant::now();
        let captured_input = Arc::clone(&self.input);
        let captured_schema = Arc::clone(&self.schema);
        let verbose = self.verbose;
        let show_statistics = self.show_statistics;
        let metric_types = self.metric_types.clone();

        // future that gathers the results from all the tasks in the
        // JoinSet that computes the overall row count and final
        // record batch
        let mut input_stream = builder.build();

        let inner_schema = Arc::clone(&self.input.schema());
        let auto_explain = self.auto_explain;
        let auto_explain_output = self.auto_explain_output.clone();
        let auto_explain_min_duration = self.auto_explain_min_duration;

        let output = async move {
            let mut batches = vec![];
            let mut total_rows = 0;
            while let Some(batch) = input_stream.next().await.transpose()? {
                total_rows += batch.num_rows();
                batches.push(batch);
            }

            let duration = Instant::now() - start;
            let out = create_output_batch(
                verbose,
                show_statistics,
                total_rows,
                duration,
                &captured_input,
                &captured_schema,
                &metric_types,
            )?;

            if auto_explain {
                if duration.as_millis() >= auto_explain_min_duration as u128 {
                    export_auto_explain(out, &auto_explain_output)?;
                }
                concat_batches(&inner_schema, &batches).map_err(DataFusionError::from)
            } else {
                Ok(out)
            }
        };

        let output_schema = if self.auto_explain {
            &self.input.schema()
        } else {
            &self.schema
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(output_schema),
            futures::stream::once(output),
        )))
    }
}

/// Creates the output of AnalyzeExec as a RecordBatch
fn create_output_batch(
    verbose: bool,
    show_statistics: bool,
    total_rows: usize,
    duration: std::time::Duration,
    input: &Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    metric_types: &[MetricType],
) -> Result<RecordBatch> {
    let mut type_builder = StringBuilder::with_capacity(1, 1024);
    let mut plan_builder = StringBuilder::with_capacity(1, 1024);

    // TODO use some sort of enum rather than strings?
    type_builder.append_value("Plan with Metrics");

    let annotated_plan = DisplayableExecutionPlan::with_metrics(input.as_ref())
        .set_metric_types(metric_types.to_vec())
        .set_show_statistics(show_statistics)
        .indent(verbose)
        .to_string();
    plan_builder.append_value(annotated_plan);

    // Verbose output
    // TODO make this more sophisticated
    if verbose {
        type_builder.append_value("Plan with Full Metrics");

        let annotated_plan = DisplayableExecutionPlan::with_full_metrics(input.as_ref())
            .set_metric_types(metric_types.to_vec())
            .set_show_statistics(show_statistics)
            .indent(verbose)
            .to_string();
        plan_builder.append_value(annotated_plan);

        type_builder.append_value("Output Rows");
        plan_builder.append_value(total_rows.to_string());

        type_builder.append_value("Duration");
        plan_builder.append_value(format!("{duration:?}"));
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(type_builder.finish()),
            Arc::new(plan_builder.finish()),
        ],
    )
    .map_err(DataFusionError::from)
}

fn export_auto_explain(batch: RecordBatch, output: &str) -> Result<()> {
    let fd: &mut dyn Write = match output {
        "stdout" => &mut io::stdout(),
        "stderr" => &mut io::stderr(),
        _ => &mut OpenOptions::new().create(true).append(true).open(output)?,
    };

    let formatted = pretty_format_batches(&[batch])?;
    writeln!(fd, "{formatted}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{
        collect,
        test::{
            TestMemoryExec, assert_is_pending,
            exec::{BlockingExec, assert_strong_count_converges_to_zero},
        },
    };

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion_common::test_util::batches_to_string;
    use datafusion_expr::LogicalPlan;
    use futures::FutureExt;
    use insta::assert_snapshot;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let analyze_exec = Arc::new(AnalyzeExec::new(
            true,
            false,
            vec![MetricType::SUMMARY, MetricType::DEV],
            blocking_exec,
            schema,
        ));

        let fut = collect(analyze_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_auto_explain() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec![
                Some("k1"),
                Some("k2"),
                Some("k3"),
            ]))],
        )?;
        let exec_plan: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::try_new(
            &[vec![batch]],
            Arc::clone(&schema),
            None,
        )?);

        let mut analyze_exec = AnalyzeExec::new(
            false,
            true,
            vec![MetricType::SUMMARY, MetricType::DEV],
            exec_plan,
            LogicalPlan::explain_schema(),
        );
        let tmp_dir = TempDir::new()?;
        let output_path = tmp_dir.path().join("auto_explain_output.txt");
        analyze_exec.enable_auto_explain();
        analyze_exec.set_auto_explain_output(
            output_path.as_os_str().to_str().unwrap().to_owned(),
        );

        // check that the original output remains the same
        let result =
            collect(Arc::new(analyze_exec.clone()), Arc::clone(&task_ctx)).await?;
        assert_snapshot!(
            batches_to_string(&result),
            @r"
        +----+
        | k  |
        +----+
        | k1 |
        | k2 |
        | k3 |
        +----+
        "
        );
        assert_snapshot!(fs::read_to_string(&output_path)?,
        @r"
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | plan_type         | plan                                                                                                                                    |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(3), Bytes=Exact(1160), [(Col[0]: Null=Exact(0))]] |
        |                   |                                                                                                                                         |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        ");

        // check that with a longer runtime a new plan is not printed
        analyze_exec.set_auto_explain_min_duration(1000000);
        collect(Arc::new(analyze_exec.clone()), Arc::clone(&task_ctx)).await?;
        assert_snapshot!(fs::read_to_string(&output_path)?,
        @r"
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | plan_type         | plan                                                                                                                                    |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(3), Bytes=Exact(1160), [(Col[0]: Null=Exact(0))]] |
        |                   |                                                                                                                                         |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        ");

        // test again with the default min duration
        analyze_exec.set_auto_explain_min_duration(0);
        collect(Arc::new(analyze_exec.clone()), Arc::clone(&task_ctx)).await?;
        assert_snapshot!(fs::read_to_string(&output_path)?,
        @r"
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | plan_type         | plan                                                                                                                                    |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(3), Bytes=Exact(1160), [(Col[0]: Null=Exact(0))]] |
        |                   |                                                                                                                                         |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | plan_type         | plan                                                                                                                                    |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(3), Bytes=Exact(1160), [(Col[0]: Null=Exact(0))]] |
        |                   |                                                                                                                                         |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        ");

        // check that auto_explain uses the pre-defined analyze parameters
        analyze_exec.show_statistics = false;
        collect(Arc::new(analyze_exec.clone()), Arc::clone(&task_ctx)).await?;
        analyze_exec.verbose = true;
        collect(Arc::new(analyze_exec.clone()), Arc::clone(&task_ctx)).await?;
        let complete_output = fs::read_to_string(&output_path)?;
        // remove the duration since it's not deterministic
        let output_without_duration =
            complete_output.rsplitn(4, '\n').nth(3).unwrap().to_string();
        assert_snapshot!(output_without_duration,
        @r"
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | plan_type         | plan                                                                                                                                    |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(3), Bytes=Exact(1160), [(Col[0]: Null=Exact(0))]] |
        |                   |                                                                                                                                         |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | plan_type         | plan                                                                                                                                    |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[], statistics=[Rows=Exact(3), Bytes=Exact(1160), [(Col[0]: Null=Exact(0))]] |
        |                   |                                                                                                                                         |
        +-------------------+-----------------------------------------------------------------------------------------------------------------------------------------+
        +-------------------+---------------------------------------------------------------+
        | plan_type         | plan                                                          |
        +-------------------+---------------------------------------------------------------+
        | Plan with Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[] |
        |                   |                                                               |
        +-------------------+---------------------------------------------------------------+
        +------------------------+---------------------------------------------------------------+
        | plan_type              | plan                                                          |
        +------------------------+---------------------------------------------------------------+
        | Plan with Metrics      | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[] |
        |                        |                                                               |
        | Plan with Full Metrics | DataSourceExec: partitions=1, partition_sizes=[1], metrics=[] |
        |                        |                                                               |
        | Output Rows            | 3                                                             |
        ");

        Ok(())
    }
}
